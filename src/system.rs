use crate::actors::*;
use crate::message::*;
use ccl::dashmap::DashMap;
use log::{error, warn};
use once_cell::sync::OnceCell;
use secc::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, WaitTimeoutResult};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use uuid::Uuid;

// This holds the actor system in a threadlocal so that the user can obtain a clone of it
// if needed at any time.
std::thread_local! {
    static ACTOR_SYSTEM: OnceCell<ActorSystem> = OnceCell::new();
}

/// An enum containing messages that are sent to actors by the actor system itself and are
/// universal to all actors.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SystemMsg {
    /// A message that is sent by the system and guaranteed to be the first message that the
    /// actor receives in its lifetime.
    Start,

    /// A message that instructs an actor to shut down. The actor receiving this message should
    /// shut down all open file handles and any other resources and return a [`Status::Stop`]
    /// from the call to the message processor. Regardless of the return from the actor's
    /// message processor the actor will be shut down by the actor system.
    Stop,

    /// A message sent to an actor when a monitored actor is stopped and thus not able to
    /// process additional messages. The value is the `aid` of the actor that stopped.
    Stopped(ActorId),
}

/// A type used for sending messages to other actor systems over TCP.
/// FIXME this shouldn't need to be cloneable
#[derive(Clone, Serialize, Deserialize)]
pub enum WireMessage {
    /// A message sent as a response to an actor system connecting to this actor system. The
    /// message contains the UUID of the actor system sending the message.
    Hello { system_actor_aid: ActorId },
    /// A message from one actor to another.
    ActorMsg {
        actor_uuid: Uuid,
        system_uuid: Uuid,
        message: Message,
    },
}

/// Configuration structure for the Axiom actor system. Note that this configuration implements
/// serde serialize and deserialize to allow users to read the config from any serde supported
/// means.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActorSystemConfig {
    /// The number of slots to allocate for the work channel. This is the channel that the worker
    /// threads use to schedule work on actors. The more traffic the actor system takes and the
    /// longer the messages take to process, the bigger this should be. The default value is 100.
    pub work_channel_size: u16,
    /// The size of the thread pool which governs how many worker threads there are in the system.
    /// The number of threads should be carefully considered to have sufficient concurrency but
    /// not overschedule the CPU on the target hardware. The default value is 4.
    pub thread_pool_size: u16,
    /// Amount of time to wait in milliseconds between polling an empty work channel. The higher
    /// this value is the longer threads will wait for polling and the kinder it will be to the
    /// CPU. However, larger values will impact performance and may lead to some threads never
    /// getting enough work to justify their existence. The default value is 10.
    pub thread_wait_time: u16,
}

impl Default for ActorSystemConfig {
    /// Create the config with the default values.
    fn default() -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: 100,
            thread_pool_size: 4,
            thread_wait_time: 10,
        }
    }
}

/// Contains the inner data used by the actor system.
struct ActorSystemData {
    /// Unique version 4 UUID for this actor system.
    uuid: Uuid,
    /// The config for the actor system which was passed to it when created.
    config: ActorSystemConfig,
    /// Sender side of the work channel. When an actor gets a message and its pending count
    /// goes from 0 to 1 it will put itself in the work channel via the sender. The actor will be
    /// resent to the channel by a thread after handling a message if it has more messages
    /// to process.
    sender: SeccSender<Arc<Actor>>,
    /// Receiver side of the work channel. All threads in the pool will be grabbing actors
    /// from this receiver to process messages.
    receiver: SeccReceiver<Arc<Actor>>,
    /// Holds handles to the pool of threads processing the work channel.
    thread_pool: Mutex<Vec<JoinHandle<()>>>,
    /// A flag holding whether or not the system is currently shutting down.
    shutdown_triggered: AtomicBool,
    // Stores the number of running threads with a Condvar that will be used to notify anyone
    // waiting on the condvar that all threads have exited.
    running_thread_count: Arc<(Mutex<u16>, Condvar)>,
    /// Holds the [`Actor`] objects keyed by the [`ActorId`]. The [`std::sync::RwLock`] will be
    /// locked for write only when a new actor is spawned but otherwise will be locked for read
    /// by the [`ActorId`] instances when they attempt to send an actor to the work channel for
    /// processing.
    actors_by_aid: Arc<DashMap<ActorId, Arc<Actor>>>,
    /// Holds a map of the actor ids by the UUID in the actor id. UUIDs of actor ids are assigned
    /// when an actor is spawned using version 4 UUIDs.
    aids_by_uuid: Arc<DashMap<Uuid, ActorId>>,
    /// Holds a map of user assigned names to actor ids set when the actors were spawned.
    aids_by_name: Arc<DashMap<String, ActorId>>,
    /// Holds a map of monitors where the key is the `aid` of the actor being monitored and
    /// the value is a vector of `aid`s that are monitoring the actor.
    monitoring_by_monitored: Arc<DashMap<ActorId, Vec<ActorId>>>,
    /// Holds a map of information objects about links to remote actor systems. The values in
    /// this map hold the remote info combined with the join handle of the thread that is reading
    /// from the receiver side of the channel.
    remotes: Arc<DashMap<Uuid, RemoteInfo>>,
}

/// Information for communicating with a remote actor system.
pub struct RemoteInfo {
    /// The UUID of the remote system.
    pub system_uuid: Uuid,
    /// The channel to use to send messages to the remote system.
    pub sender: SeccSender<WireMessage>,
    /// The channel to use to receive messages from the remote system.
    pub receiver: SeccReceiver<WireMessage>,
    /// The AID to the system actor for the remote system.
    pub system_actor_aid: ActorId,
    /// The handle returned by the thread processing remote messages.
    /// FIXME Shut this down in shutdown but dont expose it.
    _handle: JoinHandle<()>,
}

/// An actor system that contains and manages the actors spawned inside it.
#[derive(Clone)]
pub struct ActorSystem {
    /// This field means the user doesnt have to worry about declaring `Arc<ActorSystem>` all
    /// over the place but can just use `ActorSystem` instead. Wrapping the data also allows
    /// `&self` semantics on the methods which feels more natural.
    data: Arc<ActorSystemData>,
}

impl ActorSystem {
    /// Creates an actor system with the given config. The user should benchmark how
    /// many slots in the work channel, the number of threads they need and so on in order
    /// to satisfy the requirements of the software they are creating. Note that this will
    /// be a standalone actor system unless [`start_tcp_listener`] or [`connect`] is called.
    pub fn create(config: ActorSystemConfig) -> ActorSystem {
        let (sender, receiver) =
            secc::create::<Arc<Actor>>(config.work_channel_size, config.thread_wait_time);

        let thread_pool = Mutex::new(Vec::with_capacity(config.thread_pool_size as usize));
        let running_thread_count = Arc::new((Mutex::new(config.thread_pool_size), Condvar::new()));

        // Creates the actor system with the thread pools and actor map initialized.
        let system = ActorSystem {
            data: Arc::new(ActorSystemData {
                uuid: Uuid::new_v4(),
                config: config,
                sender,
                receiver,
                thread_pool,
                shutdown_triggered: AtomicBool::new(false),
                running_thread_count,
                actors_by_aid: Arc::new(DashMap::default()),
                aids_by_uuid: Arc::new(DashMap::default()),
                aids_by_name: Arc::new(DashMap::default()),
                monitoring_by_monitored: Arc::new(DashMap::default()),
                remotes: Arc::new(DashMap::default()),
            }),
        };

        // We have the thread pool in a mutex to avoid a chicken & egg situation with the actor
        // system not being created yet but needed by the thread. We put this code in a block to
        // get around rust borrow constraints without unnecessarily copying things.
        {
            let mut guard = system.data.thread_pool.lock().unwrap();
            for _ in 0..system.data.config.thread_pool_size {
                let thread = system.start_dispatcher_thread();
                guard.push(thread);
            }
        }

        // The system actor is a unique actor on the system registered with the name "System".
        system
            .spawn_named(&"System", false, system_actor_processor)
            .unwrap();

        system
    }

    /// Starts a thread for the dispatcher that will process actor messages. The dispatcher
    /// threads constantly grab at the work channel trying to get the next actor off the channel.
    /// When they get an actor they will process the message using the actor and then check to see
    /// if the actor has more receivable messages. If it does then the actor will be re-sent to
    /// the work channel to process the next message. This process allows thousands of actors to
    /// run and not take up resources if they have no messages to process but also prevents one
    /// super busy actor from starving out other actors that get messages only occasionally.
    fn start_dispatcher_thread(&self) -> JoinHandle<()> {
        // FIXME Issue #32: Add metrics to this to log a warning if messages or actors are spending too
        // long in the channel.
        let system = self.clone();
        let receiver = self.data.receiver.clone();
        let thread_timeout = self.data.config.thread_wait_time;

        thread::spawn(move || {
            system.init_current();
            while !system.data.shutdown_triggered.load(Ordering::Relaxed) {
                match receiver.receive_await_timeout(thread_timeout) {
                    Err(_) => (), // not an error, just loop and try again.
                    Ok(actor) => Actor::receive(actor),
                }
            }
            let (mutex, condvar) = &*system.data.running_thread_count;
            let mut count = mutex.lock().unwrap();
            *count = *count - 1;
            // If this is the last thread exiting we will notify any waiters.
            if *count == 0 {
                condvar.notify_all();
            }
        })
    }

    /// Locates the sender for the remote actor system with the given Uuid.
    pub(crate) fn remote_sender(&self, system_uuid: &Uuid) -> Option<SeccSender<WireMessage>> {
        self.data
            .remotes
            .get(system_uuid)
            .map(|info| info.sender.clone())
    }

    /// Adds a connection to a remote actor system. The remote info contains the information
    /// about the remote and when the connection is established the actor system will announce
    /// itself to the the remote system with a [`WireMessage::Hello`].
    pub fn connect(
        &self,
        sender: SeccSender<WireMessage>,
        receiver: SeccReceiver<WireMessage>,
    ) -> Uuid {
        // Announce ourselves to the other system and get their info.
        let hello = WireMessage::Hello {
            system_actor_aid: self.system_actor_aid(),
        };
        sender.send(hello).unwrap();
        println!("Sending hello from {}", self.data.uuid);

        // FIXME Add error handling and make timeout configurable.
        let system_actor_aid = match receiver.receive_await_timeout(100) {
            Ok(message) => match message {
                WireMessage::Hello { system_actor_aid } => system_actor_aid,
                _ => panic!("Expected first message to be a Hello"),
            },
            Err(e) => panic!("Expected to read a Hello message {:?}", e),
        };

        // Starts a thread to read incomming wire messages and process them.
        let system = self.clone();
        let receiver_clone = receiver.clone();
        let thread_timeout = self.data.config.thread_wait_time;
        let sys_uuid = system_actor_aid.system_uuid().clone();
        let handle = thread::spawn(move || {
            system.init_current();
            // FIXME Add soft-close mechanism.
            loop {
                match receiver_clone.receive_await_timeout(thread_timeout) {
                    Err(_) => (), // not an error, just loop and try again.
                    Ok(wire_msg) => system.process_wire_message(&sys_uuid, &wire_msg),
                }
            }
        });

        // Save the info and thread to the remotes map.
        let info = RemoteInfo {
            system_uuid: system_actor_aid.system_uuid().clone(),
            sender,
            receiver: receiver,
            _handle: handle,
            system_actor_aid,
        };

        let uuid = info.system_uuid.clone();
        self.data.remotes.insert(uuid.clone(), info);
        uuid
    }

    /// Connects two actor systems using two channels directly. This can be used as a utility
    /// in testing or to link two actor systems directly within the same process.
    pub fn connect_with_channels(system1: ActorSystem, system2: ActorSystem) {
        let (tx1, rx1) = secc::create::<WireMessage>(32, 10);
        let (tx2, rx2) = secc::create::<WireMessage>(32, 10);

        // We will do this in 2 threads because one connect would block waiting on a message
        // from another and deadlock.
        let h1 = thread::spawn(move || system1.connect(tx1, rx2));
        let h2 = thread::spawn(move || system2.connect(tx2, rx1));

        // Wait for the completion of the connection.
        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// A helper function to process a wire message from another actor system. The passed uuid
    /// is the uuid of the remote that sent the message and the sender is the sender to that
    /// remote to facilitate replying to the remote.
    fn process_wire_message(&self, _uuid: &Uuid, wire_message: &WireMessage) {
        match wire_message {
            WireMessage::ActorMsg {
                actor_uuid,
                system_uuid,
                message,
            } => {
                if ActorSystem::current().uuid() == *system_uuid {
                    // FIXME errors not handled
                    println!("Got actor message {:?}", actor_uuid);
                    let aid = ActorId::find_by_uuid(&actor_uuid).unwrap();
                    aid.send(message.clone());
                } else {
                    panic!("Error not handled yet");
                }
            }
            WireMessage::Hello { system_actor_aid } => {
                println!("{:?} Got Hello from {}", self.data.uuid, system_actor_aid);
            }
        }
    }

    /// Initialises this actor system to use for the current thread which is necessary if the
    /// user wishes to serialize and deserialize [`ActorId`]s. Note that this can be called only
    /// once per thread; on the second call it will panic.
    pub fn init_current(&self) {
        ACTOR_SYSTEM.with(|actor_system| {
            actor_system
                .set(self.clone())
                .expect("Unable to set ACTOR_SYSTEM.");
        });
    }

    /// Fetches a clone of a reference of the actor system for the current thread.
    #[inline]
    pub fn current() -> ActorSystem {
        ACTOR_SYSTEM.with(|actor_system| {
            actor_system
                .get()
                .expect("Thread local ACTOR_SYSTEM not set! See `ActorSystem::init_current()`")
                .clone()
        })
    }

    /// Returns the unique UUID for this actor system.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.data.uuid
    }

    /// Triggers a shutdown but doesn't wait for threads to stop.
    pub fn trigger_shutdown(&self) {
        self.data.shutdown_triggered.store(true, Ordering::Relaxed);
    }

    /// Awaits for the actor system to be shutdown using a relatively CPU minimal condvar as
    /// a signalling mechanism. This function will block until all actor system threads have
    /// stopped.
    pub fn await_shutdown(&self) {
        let &(ref mutex, ref condvar) = &*self.data.running_thread_count;
        let guard = mutex.lock().unwrap();
        let _condvar_guard = condvar.wait(guard).unwrap();
    }

    /// Awaits for the actor system to be shutdown using a relatively CPU minimal condvar as
    /// a signalling mechanism. This function will block until all actor system threads have
    /// stopped or the timeout has expired. This function will return true if the system
    /// shutdown without the timeout expiring and false if the system failed to shutdown before
    /// the timeout expired or an error occurred.
    pub fn await_shutdown_with_timeout(&self, timeout: Duration) -> bool {
        let &(ref mutex, ref condvar) = &*self.data.running_thread_count;
        let guard = mutex.lock().unwrap();
        match condvar.wait_timeout(guard, timeout) {
            Ok((_, timeout)) => timeout.timed_out(),
            Err(err) => {
                error!("Error while waiting for system to shitdown: {}", err);
                false
            }
        }
    }

    /// Triggers a shutdown of the system and returns only when all threads have joined.
    pub fn trigger_and_await_shutdown(&self) {
        self.trigger_shutdown();
        self.await_shutdown();
    }

    /// Returns the total number of times actors have been sent to the work channel.
    #[inline]
    pub fn sent(&self) -> usize {
        self.data.receiver.sent()
    }

    /// Returns the total number of times actors have been processed from the work channel.
    #[inline]
    pub fn received(&self) -> usize {
        self.data.receiver.received()
    }

    /// Returns the total number of actors that are currently pending in the work channel.
    #[inline]
    pub fn pending(&self) -> usize {
        self.data.receiver.pending()
    }

    // A internal helper to register an actor in the actor system.
    fn register_actor(&self, actor: Arc<Actor>) -> Result<ActorId, ActorError> {
        let actors_by_aid = &self.data.actors_by_aid;
        let aids_by_uuid = &self.data.aids_by_uuid;
        let aids_by_name = &self.data.aids_by_name;
        let aid = actor.context.aid.clone();
        if let Some(name_string) = &aid.name() {
            if aids_by_name.contains_key(name_string) {
                return Err(ActorError::NameAlreadyUsed(name_string.clone()));
            } else {
                aids_by_name.insert(name_string.clone(), aid.clone());
            }
        }
        aids_by_uuid.insert(aid.uuid(), aid.clone());
        actors_by_aid.insert(aid.clone(), actor);
        Ok(aid)
    }

    /// Spawns a new unnamed actor on the `system` using the given starting `state` for the actor
    /// and the given `processor` function that will be used to process actor messages. The
    /// spawned actor will use default values for the actor's config.
    ///
    /// # Examples
    /// ```
    /// use axiom::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default());
    ///
    /// let aid = system.spawn(
    ///     0 as usize,
    ///     |_state: &mut usize, _context: &Context, _message: &Message| Status::Processed,
    /// );
    /// aid.send(Message::new(11));
    /// ```
    pub fn spawn<F, State>(&self, state: State, processor: F) -> ActorId
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        let actor = Actor::new(self.clone(), None, state, processor);
        let result = self.register_actor(actor).unwrap();
        result.send(Message::new(SystemMsg::Start));
        result
    }

    /// Spawns a new named actor on the `system` using the given starting `state` for the actor
    /// and the given `processor` function that will be used to process actor messages.
    /// If the `name` is already registered then this function will return an [`std::Result::Err`]
    /// with the value [`ActorError::NameAlreadyUsed`] containing the name attempted to be
    /// registered.
    ///
    /// # Examples
    /// ```
    /// use axiom::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default());
    /// system.init_current();
    ///
    /// let aid = system.spawn_named(
    ///     "alpha",
    ///     0 as usize,
    ///     |_state: &mut usize, _context: &Context, message: &Message| Status::Processed,
    /// );
    /// ```
    pub fn spawn_named<F, State>(
        &self,
        name: &str,
        state: State,
        processor: F,
    ) -> Result<ActorId, ActorError>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        let actor = Actor::new(self.clone(), Some(name.to_string()), state, processor);
        let result = self.register_actor(actor)?;
        result.send(Message::new(SystemMsg::Start));
        Ok(result)
    }

    /// Reschedules an actor on the actor system.
    pub(crate) fn reschedule(&self, actor: Arc<Actor>) {
        self.data
            .sender
            .send(actor)
            .expect("Unable to Schedule actor: ")
    }

    /// Schedules the `aid` for work. Note that this is the only time that we have to use the
    /// lookup table. This function gets called when an actor goes from 0 receivable messages to
    /// 1 receivable message. If the actor has more receivable messages then this will not be
    /// needed to be called because the dispatcher threads will handle the process of resending
    /// the actor to the work channel.
    ///
    /// TODO Put tests verifying the resend on multiple messages.
    pub(crate) fn schedule(&self, aid: ActorId) {
        let actors_by_aid = &self.data.actors_by_aid;
        match actors_by_aid.get(&aid) {
            Some(actor) => self
                .data
                .sender
                .send(actor.clone())
                .expect("Unable to Schedule actor: "),
            None => {
                // The actor was removed from the map so ignore the problem and just log
                // a warning.
                warn!(
                    "Attempted to schedule actor with aid {:?} on system with node_id {:?} but 
                    the actor does not exist.",
                    aid.clone(),
                    self.data.uuid.to_string(),
                );
                ()
            }
        }
    }

    /// Stops an actor by shutting down its channels and removing it from the actors list and
    /// telling the actor id to not allow messages to be sent to the actor since the receiving
    /// side of the actor is gone.
    ///
    /// This is something that should rarely be called from the outside as it is much better to
    /// send the actor a [`SystemMsg::Stop`] message and allow it to stop gracefully.
    ///
    /// FIXME Add ability for an actor to report to monitors why it stopped.
    pub fn stop_actor(&self, aid: &ActorId) {
        {
            let actors_by_aid = &self.data.actors_by_aid;
            let aids_by_uuid = &self.data.aids_by_uuid;
            let aids_by_name = &self.data.aids_by_name;
            actors_by_aid.remove(aid);
            aids_by_uuid.remove(&aid.uuid());
            if let Some(name_string) = aid.name() {
                aids_by_name.remove(&name_string);
            }
            aid.stop().unwrap();
        }

        // Notify all of the actors monitoring the actor that is stopped and remove the
        // actor from the map of monitors.
        if let Some((_, monitoring)) = self.data.monitoring_by_monitored.remove(&aid) {
            for m_aid in monitoring {
                ActorId::send(&m_aid, Message::new(SystemMsg::Stopped(aid.clone())));
            }
        }
    }

    /// Checks to see if the actor with the given [`ActorId`] is alive within this actor system.
    pub fn is_actor_alive(&self, aid: &ActorId) -> bool {
        let actors_by_aid = &self.data.actors_by_aid;
        actors_by_aid.contains_key(aid)
    }

    /// Look up an [`ActorId`] by the unique UUID of the actor and either returns the located
    /// `aid` in a [`Option::Some`] or [`Option::None`] if not found.
    pub fn find_aid_by_uuid(&self, uuid: &Uuid) -> Option<ActorId> {
        let aids_by_uuid = &self.data.aids_by_uuid;
        aids_by_uuid.get(uuid).map(|aid| aid.clone())
    }

    /// Look up an [`ActorId`] by the user assigned name of the actor and either returns the
    /// located `aid` in a [`Option::Some`] or [`Option::None`] if not found.
    pub fn find_aid_by_name(&self, name: &str) -> Option<ActorId> {
        let aids_by_name = &self.data.aids_by_name;
        aids_by_name.get(&name.to_string()).map(|aid| aid.clone())
    }

    /// Returns the aid to the "System" actor for this actor system. The actor for the returned
    /// aid is started when the actor system is started and provides additional actor system
    /// functionality.
    pub fn system_actor_aid(&self) -> ActorId {
        self.find_aid_by_name(&"System").unwrap()
    }

    /// Adds a monitor so that `monitoring` will be informed if `monitored` stops.
    pub fn monitor(&self, monitoring: &ActorId, monitored: &ActorId) {
        let mut monitoring_by_monitored = self
            .data
            .monitoring_by_monitored
            .get_raw_mut_from_key(&monitored);
        let monitoring_vec = monitoring_by_monitored
            .entry(monitored.clone())
            .or_insert(Vec::new());
        monitoring_vec.push(monitoring.clone());
    }

    /// Asynchronously send a message to the system actors on all connected actor systems.
    /// FIXME Add try_send ability and make actor and secc error types extend std::Error.
    pub fn send_to_system_actors(&self, message: Message) {
        let remotes = &*self.data.remotes;
        for iter_ref in remotes.iter() {
            iter_ref.value().system_actor_aid.send(message.clone());
        }
    }
}

impl fmt::Debug for ActorSystem {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ActorSystem{{uuid: {}, config: {:?}}}",
            self.data.uuid.to_string(),
            self.data.config,
        )
    }
}

/// Messages that are sent to and received from the System Actor.
#[derive(Serialize, Deserialize, Debug)]
enum SystemActorMsg {
    /// Finds an actor by name.
    FindByName { reply_to: ActorId, name: String },

    /// A message sent as a result of calling [`ActorSystem::cluster_find_by_name()`].
    FindByNameResult {
        /// The UUID of the system that is responding.
        system_uuid: Uuid,
        /// The name that was searched for.
        name: String,
        /// The ActorId in a [`Some`] if found or [`None`] if not.
        aid: Option<ActorId>,
    },
}

/// A processor for the system actor.
fn system_actor_processor(_: &mut bool, context: &Context, message: &Message) -> Status {
    if let Some(msg) = message.content_as::<SystemActorMsg>() {
        match &*msg {
            SystemActorMsg::FindByName { reply_to, name } => {
                println!(
                    "{} @ {} => Processing: {:?}",
                    context.aid,
                    context.system.uuid(),
                    msg
                );
                let found = context.system.find_aid_by_name(&name);
                println!("{} => Found: {:?}", context.aid, found);

                reply_to.send(Message::new(SystemActorMsg::FindByNameResult {
                    system_uuid: context.system.uuid(),
                    name: name.clone(),
                    aid: found,
                }));
                Status::Processed
            }
            _ => Status::Processed,
        }
    } else if let Some(msg) = message.content_as::<SystemMsg>() {
        match &*msg {
            SystemMsg::Start => Status::Processed,
            _ => Status::Processed,
        }
    } else {
        error!("Unhandled message received.");
        Status::Processed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use std::thread;

    // A helper to start two actor systems and connect them.
    fn start_and_connect_two_systems() -> (ActorSystem, ActorSystem) {
        let system1 = ActorSystem::create(ActorSystemConfig::default());
        println!("System 1 ======> {}", system1.uuid());
        let system2 = ActorSystem::create(ActorSystemConfig::default());
        println!("System 2 ======> {}", system2.uuid());
        ActorSystem::connect_with_channels(system1.clone(), system2.clone());
        (system1, system2)
    }

    /// Helper to wait for 2 actor systems to shutdown or panic if they dont do so within
    /// 2000 milliseconds.
    fn await_two_system_shutdown(system1: ActorSystem, system2: ActorSystem) {
        let timeout = Duration::from_millis(2000);

        let h1 = thread::spawn(move || {
            assert_eq!(true, system1.await_shutdown_with_timeout(timeout));
        });

        let h2 = thread::spawn(move || {
            assert_eq!(true, system2.await_shutdown_with_timeout(timeout));
        });

        // Wait for the handles to be done.
        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// This test verifies that an actor can be found by its uuid.
    #[test]
    fn test_find_by_uuid() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn(0, simple_handler);
        aid.send_new(11);
        await_received(&aid, 2, 1000).unwrap();
        let found = system.find_aid_by_uuid(&aid.uuid()).unwrap();
        assert!(ActorId::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid_by_uuid(&Uuid::new_v4()));

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that an actor can be found by its name if it has one.
    #[test]
    fn test_find_by_name() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn_named("A", 0, simple_handler).unwrap();
        aid.send_new(11);
        await_received(&aid, 2, 1000).unwrap();
        let found = system.find_aid_by_name(&aid.name().unwrap()).unwrap();
        assert!(ActorId::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid_by_name("B"));

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that the system does not panic if we schedule to an actor
    /// that does not exist in the map. This can happen if the actor is stopped before
    /// the system notifies the actor id that it is dead.
    #[test]
    fn test_stop_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn_named("A", 0, simple_handler).unwrap();
        aid.send_new(11);
        await_received(&aid, 2, 1000).unwrap();

        // Now we stop the actor.
        system.stop_actor(&aid);
        assert_eq!(false, system.is_actor_alive(&aid));

        // Verify the actor is NOT in the maps.
        let sys_clone = system.clone();
        let actors_by_aid = &sys_clone.data.actors_by_aid;
        assert_eq!(false, actors_by_aid.contains_key(&aid));
        let aids_by_uuid = &sys_clone.data.aids_by_uuid;
        assert_eq!(false, aids_by_uuid.contains_key(&aid.uuid()));
        assert_eq!(None, system.find_aid_by_name("A"));
        assert_eq!(None, system.find_aid_by_uuid(&aid.uuid()));

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that the system does not panic if we schedule to an actor
    /// that does not exist in the map. This can happen if the actor is stopped before
    /// the system notifies the actor id that it is dead.
    #[test]
    fn test_actor_not_in_map() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn(0, simple_handler);
        await_received(&aid, 1, 1000).unwrap(); // Now it is started for sure.

        // We force remove the actor from the system without calling stop so now it cannot
        // be scheduled.
        let sys_clone = system.clone();
        let actors_by_aid = &sys_clone.data.actors_by_aid;
        actors_by_aid.remove(&aid);

        // Send a message to the actor which should not schedule it but write out a warning.
        aid.send_new(11);

        system.trigger_and_await_shutdown();
    }

    /// Tests connection between two different actor systems using channels.
    #[test]
    fn test_connect_with_channels() {
        let system1 = ActorSystem::create(ActorSystemConfig::default());
        let system2 = ActorSystem::create(ActorSystemConfig::default());
        ActorSystem::connect_with_channels(system1.clone(), system2.clone());
        {
            system1
                .data
                .remotes
                .get(&system2.data.uuid)
                .expect("Unable to find connection with system 2 in system 1");
        }
        {
            system2
                .data
                .remotes
                .get(&system1.data.uuid)
                .expect("Unable to find connection with system 1 in system 2");
        }
    }

    // Tests that monitors work in the actor system and send a message to monitorign actors
    // when monitored actors stop.
    #[test]
    fn test_monitors() {
        init_test_log();

        fn monitor_handler(state: &mut ActorId, _: &Context, message: &Message) -> Status {
            if let Some(msg) = message.content_as::<SystemMsg>() {
                match &*msg {
                    SystemMsg::Stopped(aid) => {
                        assert!(ActorId::ptr_eq(state, aid));
                        Status::Processed
                    }
                    SystemMsg::Start => Status::Processed,
                    _ => panic!("Received some other message!"),
                }
            } else {
                panic!("Received some other message!");
            }
        }

        let system = ActorSystem::create(ActorSystemConfig::default());
        let monitored = system.spawn(0 as usize, simple_handler);
        let not_monitoring = system.spawn(0 as usize, simple_handler);
        let monitoring1 = system.spawn(monitored.clone(), monitor_handler);
        let monitoring2 = system.spawn(monitored.clone(), monitor_handler);
        system.monitor(&monitoring1, &monitored);
        system.monitor(&monitoring2, &monitored);

        {
            // Validate the monitors are there in a block to release mutex afterwards.
            let monitoring_by_monitored = &system.data.monitoring_by_monitored;
            let m_vec = monitoring_by_monitored.get(&monitored).unwrap();
            assert!(m_vec.contains(&monitoring1));
            assert!(m_vec.contains(&monitoring2));
        }

        // Stop the actor and it should be out of the map.
        system.stop_actor(&monitored);
        await_received(&monitoring1, 2, 1000).unwrap();
        await_received(&monitoring2, 2, 1000).unwrap();
        await_received(&not_monitoring, 1, 1000).unwrap();

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that the concept of named actors works properly. When a user wants
    /// to declare a named actor they cannot register the same name twice and when the actor
    /// stops the name should be removed from the registered names and be available again.
    #[test]
    fn test_named_actor_restrictions() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default());
        let state = 0 as usize;

        let aid1 = system.spawn_named("A", state, simple_handler).unwrap();
        await_received(&aid1, 1, 1000).unwrap();

        let aid2 = system.spawn_named("B", state, simple_handler).unwrap();
        await_received(&aid2, 1, 1000).unwrap();

        // Spawn an actor that attempts to overwrite "A" in the names and make sure the
        // attempt returns an error to be handled.
        let result = system.spawn_named("A", state, simple_handler);
        assert_eq!(Err(ActorError::NameAlreadyUsed("A".to_string())), result);

        // Verify that the same actor has "A" name and is still up.
        let found1 = system.find_aid_by_name("A").unwrap();
        assert_eq!(true, system.is_actor_alive(&aid1));
        assert!(ActorId::ptr_eq(&aid1, &found1));

        // Stop "B" and verify that the actor system's maps are cleaned up.
        system.stop_actor(&aid2);
        assert_eq!(None, system.find_aid_by_name("B"));
        assert_eq!(None, system.find_aid_by_uuid(&aid2.uuid()));

        // Now we should be able to crate a new actor with the name bravo.
        let aid3 = system.spawn_named("B", state, simple_handler).unwrap();
        await_received(&aid3, 1, 1000).unwrap();
        let found2 = system.find_aid_by_name("B").unwrap();
        assert!(ActorId::ptr_eq(&aid3, &found2));

        system.trigger_and_await_shutdown();
    }

    /// Tests that remote actors can send and recieve messages from each other.
    #[test]
    fn test_remote_actors() {
        // In this test our messages are just structs.
        #[derive(Serialize, Deserialize, Debug)]
        struct Request {
            reply_to: ActorId,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct Reply {}

        init_test_log();
        let (system1, system2) = start_and_connect_two_systems();

        system1.init_current();
        let aid = system1.spawn((), |_: &mut (), context: &Context, message: &Message| {
            if let Some(msg) = message.content_as::<Request>() {
                msg.reply_to.send_new(Reply {});
                context.system.trigger_shutdown();
                Status::Stop
            } else if let Some(_) = message.content_as::<SystemMsg>() {
                Status::Processed
            } else {
                panic!("Unexpected message received!");
            }
        });

        let serialized = bincode::serialize(&aid).unwrap();
        system2.spawn(
            (),
            move |_: &mut (), context: &Context, message: &Message| {
                if let Some(_) = message.content_as::<Reply>() {
                    context.system.trigger_shutdown();
                    Status::Stop
                } else if let Some(msg) = message.content_as::<SystemMsg>() {
                    match &*msg {
                        SystemMsg::Start => {
                            let target_aid: ActorId = bincode::deserialize(&serialized).unwrap();
                            target_aid.send_new(Request {
                                reply_to: context.aid.clone(),
                            });
                            Status::Processed
                        }
                        _ => Status::Processed,
                    }
                } else {
                    panic!("Unexpected message received!");
                }
            },
        );

        await_two_system_shutdown(system1, system2);
    }

    /// Tests the ability to find an aid on a remote system by name using a SystemActor. This
    /// also
    #[test]
    fn test_system_actor_find_by_name() {
        init_test_log();
        let (system1, system2) = start_and_connect_two_systems();

        let aid1 = system1
            .spawn_named("A", (), |_: &mut (), context: &Context, _: &Message| {
                println!(
                    "system1 ==> {} @ {}",
                    context.aid.uuid(),
                    context.system.uuid()
                );
                context.system.trigger_shutdown();
                Status::Processed
            })
            .unwrap();

        system2.spawn(
            (),
            move |_: &mut (), context: &Context, message: &Message| {
                println!(
                    "system2 ==> {} @ {}",
                    context.aid.uuid(),
                    context.system.uuid()
                );
                if let Some(msg) = message.content_as::<SystemActorMsg>() {
                    match &*msg {
                        SystemActorMsg::FindByNameResult { aid: found, .. } => {
                            if let Some(target) = found {
                                println!("Processing FindByNameResult: {:?}", target);
                                assert_eq!(target.uuid(), aid1.uuid());
                                assert_eq!(false, target.is_local());
                                target.send_new(true);
                                context.system.trigger_shutdown();
                                Status::Processed
                            } else {
                                panic!("Didn't find AID.");
                            }
                        }
                        _ => panic!("Unexpected message received!"),
                    }
                } else if let Some(msg) = message.content_as::<SystemMsg>() {
                    if let SystemMsg::Start = &*msg {
                        context.system.send_to_system_actors(Message::new(
                            SystemActorMsg::FindByName {
                                reply_to: context.aid.clone(),
                                name: "A".to_string(),
                            },
                        ));
                        Status::Processed
                    } else {
                        panic!("Unexpected message received!");
                    }
                } else {
                    panic!("Unexpected message received!");
                }
            },
        );

        await_two_system_shutdown(system1, system2);
    }
}
