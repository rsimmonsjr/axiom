//! Implements the [`ActorSystem`] and related types of Axiom.
//!
//! When the actor system starts up, a number of dispatcher threads will be spawned that will
//! constantly try to pull work from the work channel and process messages with the actor. When
//! the time slice expires The actor will then be re-sent to the work channel if there are more
//! messages for that actor to process. This continues constantly until the actor system is
//! shutdown and all actors are stopped.
//!
//! The user should refer to test cases and examples as "how-to" guides for using Axiom.

use crate::actors::*;
use crate::message::*;
use crate::*;
use dashmap::DashMap;
use log::{debug, error, warn};
use once_cell::sync::OnceCell;
use secc::*;
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use uuid::Uuid;

// Holds an [`ActorSystem`] in a [`std::thread_local`] so that the [`Aid`] deserializer and
// other types can obtain a clone if needed at any time. This will be automatically set for all
// dispatcher threads that are processing messages with the actors.
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
    Stopped(Aid),
}

/// A type used for sending messages to other actor systems.
#[derive(Clone, Serialize, Deserialize)]
pub enum WireMessage {
    /// A message sent as a response to another actor system connecting to this actor system.
    Hello {
        /// The `aid` for the system actor on the actor system sending the message.
        system_actor_aid: Aid,
    },
    /// A container for a message from one actor on one system to an actor on another system.
    ActorMessage {
        /// The UUID of the [`Aid`] that the message is being sent to.
        actor_uuid: Uuid,
        /// The UUID of the system that the destination [`Aid`] is local to.
        system_uuid: Uuid,
        /// The message to be sent.
        message: Message,
    },
    /// A container for sending a message with a specified duration delay.
    DelayedActorMessage {
        /// The duration to use to delay the message.
        duration: Duration,
        /// The UUID of the [`Aid`] that the message is being sent to.
        actor_uuid: Uuid,
        /// The UUID of the system that the destination [`Aid`] is local to.
        system_uuid: Uuid,
        /// The message to be sent.
        message: Message,
    },
}

/// Configuration structure for the Axiom actor system. Note that this configuration implements
/// serde serialize and deserialize to allow users to read the config from any serde supported
/// means.
#[derive(Debug, Serialize, Deserialize)]
pub struct ActorSystemConfig {
    /// The default size for the channel that is created for each actor. This can be overridden on
    /// a per-actor basis during spawning as well. Making the default channel size bigger allows
    /// for more bandwidth in sending messages to actors but also takes more memory. Also the
    /// user should consider if their actor needs a large channel then it might need to be
    /// refactored or the threads size should be increased because messages arent being processed
    /// fast enough. The default value for this is 32.
    pub message_channel_size: u16,
    /// Max duration to wait between attempts to send to an actor's message channel. This is used
    /// to poll a busy channel that is at its capacity limit. The larger this value is, the longer
    /// `send` will wait for capacity in the channel but the user should be aware that if the
    /// system is often waiting on capacity that channel may be too small or the actor may need to
    /// be refactored to process messages faster.  The default value is 1 millisecond.
    pub send_timeout: Duration,
    /// The size of the thread pool which governs how many worker threads there are in the system.
    /// The number of threads should be carefully considered to have sufficient concurrency but
    /// not over-schedule the CPU on the target hardware. The default value is 4 * the number of
    /// logical CPUs.
    pub threads_size: u16,
    /// The threshold at which the dispatcher thread will warn the user that the message took too
    /// long to process. If this warning is being logged then the user probably should reconsider
    /// how their message processing works and refactor big tasks into a number of smaller tasks.
    /// The default value is 1 millisecond.
    pub warn_threshold: Duration,
    /// This controls how long a processor will spend working on messages for an actor before
    /// yielding to work on other actors in the system. The dispatcher will continue to pluck
    /// messages off the actor's channel and process them until this time slice is exceeded. Note
    /// that actors themselves can exceed this in processing a single message and if so, only one
    /// message will be processed before yielding. The default value is 1 millisecond.
    pub time_slice: Duration,
    /// The number of slots to allocate for the work channel. This is the channel that the worker
    /// threads use to schedule work on actors. The more traffic the actor system takes and the
    /// longer the messages take to process, the bigger this should be. Ideally the actor will be
    /// in the work channel a maximum of 1 time no matter how many messages it has pending.
    /// However, this can vary depending upon processing dynamics that might rarely have an actor
    /// in the channel twice. The default value is 100.
    pub work_channel_size: u16,
    /// The maximum amount of time to wait to be able to schedule an actor for work before
    /// reporting an error to the user. If this is timing out then the user should increase
    /// the work channel size to accommodate the flow. The default is 1 millisecond.
    pub work_channel_timeout: Duration,
    /// Amount of time to wait in milliseconds between polling an empty work channel. The higher
    /// this value is the longer threads will wait for polling and the kinder it will be to the
    /// CPU. However, larger values will impact performance and may lead to some threads never
    /// getting enough work to justify their existence. Note that the actual system uses `Condvar`
    /// mechanics of the `secc` crate so usually polling is not a big concern. However there are
    /// circumstances where `Condvar` notifications can be missed so the polling is in place to
    /// compensate for this. See `secc::SeccReceiver::receive_await_timeout` for more information.
    /// The default value is 10 milliseconds.
    pub thread_wait_time: Duration,
}

impl Default for ActorSystemConfig {
    /// Create the config with the default values.
    fn default() -> ActorSystemConfig {
        ActorSystemConfig {
            threads_size: (num_cpus::get() * 4) as u16,
            warn_threshold: Duration::from_millis(1),
            time_slice: Duration::from_millis(1),
            work_channel_size: 100,
            work_channel_timeout: Duration::from_millis(1),
            thread_wait_time: Duration::from_millis(100),
            message_channel_size: 32,
            send_timeout: Duration::from_millis(1),
        }
    }
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
    pub system_actor_aid: Aid,
    /// The handle returned by the thread processing remote messages.
    /// FIXME (Issue #76) Add graceful shutdown for threads handling remotes.
    _handle: JoinHandle<()>,
}

/// Stores a message that will be sent to an actor with a delay.
struct DelayedMessage {
    /// A unique identifier for a message.
    uuid: Uuid,
    /// The Aid that the message will be sent to.
    destination: Aid,
    /// The minimum instant that the message should be sent.
    instant: Instant,
    /// The message to sent.
    message: Message,
}

impl std::cmp::PartialEq for DelayedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}

impl std::cmp::Eq for DelayedMessage {}

impl std::cmp::PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &DelayedMessage) -> Option<std::cmp::Ordering> {
        Some(other.instant.cmp(&self.instant)) // Uses an inverted sort.
    }
}

impl std::cmp::Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("DelayedMessage::partial_cmp() returned None; can't happen")
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
    /// resent to the channel by a thread after handling a time slice of messages if it has more
    /// messages to process.
    sender: SeccSender<Arc<Actor>>,
    /// Receiver side of the work channel. All threads in the pool will be grabbing actors
    /// from this receiver to process messages.
    receiver: SeccReceiver<Arc<Actor>>,
    /// Holds handles to the pool of threads processing the work channel.
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// A flag holding whether or not the system is currently shutting down.
    shutdown_triggered: AtomicBool,
    // Stores the number of running threads with a Condvar that will be used to notify anyone
    // waiting on the condvar that all threads have exited.
    running_thread_count: Arc<(Mutex<u16>, Condvar)>,
    /// Holds the [`Actor`] objects keyed by the [`Aid`].
    actors_by_aid: Arc<DashMap<Aid, Arc<Actor>>>,
    /// Holds a map of the actor ids by the UUID in the actor id. UUIDs of actor ids are assigned
    /// when an actor is spawned using version 4 UUIDs.
    aids_by_uuid: Arc<DashMap<Uuid, Aid>>,
    /// Holds a map of user assigned names to actor ids set when the actors were spawned. Note
    /// that only actors with an assigned name will be in this map.
    aids_by_name: Arc<DashMap<String, Aid>>,
    /// Holds a map of monitors where the key is the `aid` of the actor being monitored and
    /// the value is a vector of `aid`s that are monitoring the actor.
    monitoring_by_monitored: Arc<DashMap<Aid, HashSet<Aid>>>,
    /// Holds a map of information objects about links to remote actor systems.
    remotes: Arc<DashMap<Uuid, RemoteInfo>>,
    /// Holds the messages that have been enqueued for delayed send.
    delayed_messages: Arc<(Mutex<BinaryHeap<DelayedMessage>>, Condvar)>,
}

/// An actor system that contains and manages the actors spawned inside it.
#[derive(Clone)]
pub struct ActorSystem {
    /// This field means the user doesnt have to worry about declaring `Arc<ActorSystem>` all
    /// over the place but can just use `ActorSystem` instead. Wrapping the data also allows
    /// `&self` semantics on the methods which feels more ergonomic.
    data: Arc<ActorSystemData>,
}

impl ActorSystem {
    /// Creates an actor system with the given config. The user should benchmark how many slots
    /// are needed in the work channel, the number of threads they need in the system and and so
    /// on in order to satisfy the requirements of the software they are creating.
    pub fn create(config: ActorSystemConfig) -> ActorSystem {
        let (sender, receiver) =
            secc::create::<Arc<Actor>>(config.work_channel_size, config.thread_wait_time);

        let threads = Mutex::new(Vec::with_capacity(config.threads_size as usize));
        let running_thread_count = Arc::new((Mutex::new(config.threads_size), Condvar::new()));

        // Creates the actor system with the thread pools and actor map initialized.
        let system = ActorSystem {
            data: Arc::new(ActorSystemData {
                uuid: Uuid::new_v4(),
                config: config,
                sender,
                receiver,
                threads,
                shutdown_triggered: AtomicBool::new(false),
                running_thread_count,
                actors_by_aid: Arc::new(DashMap::default()),
                aids_by_uuid: Arc::new(DashMap::default()),
                aids_by_name: Arc::new(DashMap::default()),
                monitoring_by_monitored: Arc::new(DashMap::default()),
                remotes: Arc::new(DashMap::default()),
                delayed_messages: Arc::new((Mutex::new(BinaryHeap::new()), Condvar::new())),
            }),
        };

        // We have the thread pool in a mutex to avoid a chicken & egg situation with the actor
        // system not being created yet but needed by the thread. We put this code in a block to
        // get around rust borrow constraints without unnecessarily copying things.
        {
            let mut guard = system.data.threads.lock().unwrap();

            // Start the dispatcher threads.
            for number in 0..system.data.config.threads_size {
                let thread = system.start_dispatcher_thread(number);
                guard.push(thread);
            }

            // Start the thread that reads from the `delayed_messages` queue.
            // FIXME Put in ability to confirm how many of these to start.
            for _ in 0..1 {
                let thread = system.start_send_after_thread();
                guard.push(thread);
            }
        }

        // The system actor is a unique actor on the system registered with the name "System".
        // This actor provides core functionality that other actors will utilize.
        system
            .spawn()
            .name("System")
            .with(false, system_actor_processor)
            .unwrap();

        system
    }

    /// Starts a thread for the dispatcher that will process actor messages. The dispatcher
    /// threads constantly grab at the work channel trying to get the next [`Aid`] off the
    /// channel. When they get an [`Aid`] they will process messages using the processor
    /// registered for that [`Aid`] for one time slice. After the time slice, the dispatcher
    /// thread will then check to see if the actor has more receivable messages. If the actor has
    /// more messages then the [`Aid`] for the actor will be re-sent to the work channel to
    /// process the next message. This allows thousands of actors to run and not take up resources
    /// if they have no messages to process but also prevents one super busy actor from starving
    /// out other actors that get messages only occasionally.
    fn start_dispatcher_thread(&self, number: u16) -> JoinHandle<()> {
        // FIXME (Issue #32) Add metrics to log a warning if messages or actors are spending too
        // long in the channel.
        let system = self.clone();
        let receiver = self.data.receiver.clone();
        let thread_timeout = self.data.config.thread_wait_time;
        let name = format!("DispatchThread{}", number);

        thread::Builder::new()
            .name(name)
            .spawn(move || {
                system.init_current();
                while !system.data.shutdown_triggered.load(Ordering::Relaxed) {
                    if let Ok(actor) = receiver.receive_await_timeout(thread_timeout) {
                        Actor::receive(actor);
                    }
                }

                // FIXME Refactor this into a function so all threads can utilize it.
                let (mutex, condvar) = &*system.data.running_thread_count;
                let mut count = mutex.lock().unwrap();
                *count = *count - 1;
                // If this is the last thread exiting we will notify any waiters.
                if *count == 0 {
                    condvar.notify_all();
                }
            })
            .unwrap()
    }

    /// Starts a thread that monitors the delayed_messages and sends the messages when their
    /// delays have elapsed.
    /// FIXME Add a graceful shutdown to this thread and notifications.
    fn start_send_after_thread(&self) -> JoinHandle<()> {
        let system = self.clone();
        let delayed_messages = self.data.delayed_messages.clone();
        thread::spawn(move || {
            while !system.data.shutdown_triggered.load(Ordering::Relaxed) {
                let (ref mutex, ref condvar) = &*delayed_messages;
                let mut data = mutex.lock().unwrap();
                match data.peek() {
                    None => {
                        // wait to be notified something is added.
                        let _result = condvar.wait(data).unwrap();
                    }
                    Some(msg) => {
                        let now = Instant::now();
                        if now >= msg.instant {
                            msg.destination
                                .send(msg.message.clone())
                                .unwrap_or_else(|error| {
                                    warn!(
                                        "Cannot send scheduled message to {}: Error {:?}",
                                        msg.destination, error
                                    );
                                });
                            data.pop();
                        } else {
                            let duration = msg.instant.duration_since(now);
                            let _result = condvar.wait_timeout(data, duration).unwrap();
                        }
                    }
                }
            }
        })
    }

    /// Returns a reference to the config for this actor system.
    pub fn config(&self) -> &ActorSystemConfig {
        &self.data.config
    }

    /// Locates the sender for the remote actor system with the given Uuid.
    pub(crate) fn remote_sender(&self, system_uuid: &Uuid) -> Option<SeccSender<WireMessage>> {
        self.data
            .remotes
            .get(system_uuid)
            .map(|info| info.sender.clone())
    }

    /// Adds a connection to a remote actor system. When the connection is established the
    /// actor system will announce itself to the remote system with a [`WireMessage::Hello`].
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
        debug!("Sending hello from {}", self.data.uuid);

        // FIXME (Issue #75) Make error handling in ActorSystem::connect more robust.
        let system_actor_aid =
            match receiver.receive_await_timeout(self.data.config.thread_wait_time) {
                Ok(message) => match message {
                    WireMessage::Hello { system_actor_aid } => system_actor_aid,
                    _ => panic!("Expected first message to be a Hello"),
                },
                Err(e) => panic!("Expected to read a Hello message {:?}", e),
            };

        // Starts a thread to read incoming wire messages and process them.
        let system = self.clone();
        let receiver_clone = receiver.clone();
        let thread_timeout = self.data.config.thread_wait_time;
        let sys_uuid = system_actor_aid.system_uuid().clone();
        let handle = thread::spawn(move || {
            system.init_current();
            // FIXME (Issue #76) Add graceful shutdown for threads handling remotes including
            // informing remote that the system is exiting.
            while !system.data.shutdown_triggered.load(Ordering::Relaxed) {
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
        let (tx1, rx1) = secc::create::<WireMessage>(32, system1.data.config.thread_wait_time);
        let (tx2, rx2) = secc::create::<WireMessage>(32, system2.data.config.thread_wait_time);

        // We will do this in 2 threads because one connect would block waiting on a message
        // from the other actor system, causing a deadlock.
        let h1 = thread::spawn(move || system1.connect(tx1, rx2));
        let h2 = thread::spawn(move || system2.connect(tx2, rx1));

        // Wait for the completion of the connection.
        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// A helper function to process a wire message from another actor system. The passed uuid
    /// is the uuid of the remote that sent the message.
    ///
    /// FIXME (Issue #74) Make error handling in ActorSystem::process_wire_message more robust.
    fn process_wire_message(&self, _uuid: &Uuid, wire_message: &WireMessage) {
        match wire_message {
            WireMessage::ActorMessage {
                actor_uuid,
                system_uuid,
                message,
            } => {
                self.find_aid(&system_uuid, &actor_uuid).map(|aid| {
                    aid.send(message.clone()).unwrap_or_else(|error| {
                        warn!("Could not send wire message to {}. Error: {}", aid, error);
                    })
                });
            }
            WireMessage::DelayedActorMessage {
                duration,
                actor_uuid,
                system_uuid,
                message,
            } => {
                self.find_aid(&system_uuid, &actor_uuid)
                    .map(|aid| self.send_after(message.clone(), aid.clone(), *duration))
                    .expect("Error not handled yet");
            }
            WireMessage::Hello { system_actor_aid } => {
                debug!("{:?} Got Hello from {}", self.data.uuid, system_actor_aid);
            }
        }
    }

    /// Initializes this actor system to use for the current thread which is necessary if the
    /// user wishes to serialize and deserialize [`Aid`]s. Note that this can be called only
    /// once per thread; on the second call it will panic. This is automatically called for all
    /// dispatcher threads that process actor messages.
    pub fn init_current(&self) {
        ACTOR_SYSTEM.with(|actor_system| {
            actor_system
                .set(self.clone())
                .expect("Unable to set ACTOR_SYSTEM.");
        });
    }

    /// Fetches a clone of a reference to the actor system for the current thread.
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

    /// Awaits for the actor system to be shutdown using a relatively CPU minimal `Condvar` as
    /// a signaling mechanism. This function will block until all actor system threads have
    /// stopped.
    pub fn await_shutdown(&self) {
        let &(ref mutex, ref condvar) = &*self.data.running_thread_count;
        let guard = mutex.lock().unwrap();
        let _condvar_guard = condvar.wait(guard).unwrap();
    }

    /// Awaits for the actor system to be shutdown using a relatively CPU minimal Condvar as
    /// a signaling mechanism. This function will block until all actor system threads have
    /// stopped or the timeout has expired. The function returns an `Ok` with the Duration
    /// that it waited or an `Err` if the system didn't shut down in time or something else went
    /// wrong.
    pub fn await_shutdown_with_timeout(&self, timeout: Duration) -> Result<Duration, AxiomError> {
        let start = Instant::now();
        let &(ref mutex, ref condvar) = &*self.data.running_thread_count;
        let guard = mutex.lock().unwrap();
        match condvar.wait_timeout(guard, timeout) {
            Ok((_, result)) => {
                if result.timed_out() {
                    Err(AxiomError::ShutdownError(format!(
                        "Timed Out after: {:?}",
                        timeout
                    )))
                } else {
                    Ok(Instant::elapsed(&start))
                }
            }
            Err(e) => Err(AxiomError::ShutdownError(format!(
                "Error occurred: {:?}",
                e
            ))),
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
    pub(crate) fn register_actor(&self, actor: Arc<Actor>) -> Result<Aid, AxiomError> {
        let actors_by_aid = &self.data.actors_by_aid;
        let aids_by_uuid = &self.data.aids_by_uuid;
        let aids_by_name = &self.data.aids_by_name;
        let aid = actor.context.aid.clone();
        if let Some(name_string) = &aid.name() {
            if aids_by_name.contains_key(name_string) {
                return Err(AxiomError::NameAlreadyUsed(name_string.clone()));
            } else {
                aids_by_name.insert(name_string.clone(), aid.clone());
            }
        }
        aids_by_uuid.insert(aid.uuid(), aid.clone());
        actors_by_aid.insert(aid.clone(), actor);
        aid.send(Message::new(SystemMsg::Start))?;
        Ok(aid)
    }

    /// Creates a single use builder for this actor system that allows a user to build actors
    /// using a chained syntax while optionally providing configuration parameters if desired.
    ///
    /// # Examples
    /// ```
    /// use axiom::*;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default());
    ///
    /// fn handler(count: &mut usize, _: &Context, _: &Message) -> AxiomResult {
    ///     *count += 1;
    ///     Ok(Status::Done)    
    /// }
    ///
    /// let state = 0 as usize;
    ///
    /// let aid1 = system.spawn().with(state, handler).unwrap();
    /// let aid2 = system.spawn().name("Foo").with(state, handler).unwrap();
    /// let aid3 = system.spawn().channel_size(10).with(state, handler).unwrap();
    /// ```
    pub fn spawn(&self) -> ActorBuilder {
        ActorBuilder {
            system: self.clone(),
            name: None,
            channel_size: None,
        }
    }

    /// Reschedules an actor on the actor system. This is called by a dispatcher thread after
    /// a time slice if the actor has more messages that are receivable.
    pub(crate) fn reschedule(&self, actor: Arc<Actor>) {
        self.data
            .sender
            .send_await_timeout(actor, self.data.config.work_channel_timeout)
            .expect("Unable to Schedule actor: ")
    }

    /// Schedules the `aid` for work. Note that this is the only time that we have to use the
    /// lookup table. This function gets called when an actor goes from 0 receivable messages to
    /// 1 receivable message. If the actor has more receivable messages then this will not be
    /// needed to be called because the dispatcher threads will handle the process of resending
    /// the actor to the work channel.
    ///
    /// TODO Put tests verifying the resend on multiple messages.
    pub(crate) fn schedule(&self, aid: Aid) {
        let actors_by_aid = &self.data.actors_by_aid;
        match actors_by_aid.get(&aid) {
            Some(actor) => self
                .data
                .sender
                .send_await_timeout(actor.clone(), self.data.config.work_channel_timeout)
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
    /// telling the [`Aid`] to not allow messages to be sent to the actor since the receiving
    /// side of the actor is gone.
    ///
    /// This is something that should rarely be called from the outside as it is much better to
    /// send the actor a [`SystemMsg::Stop`] message and allow it to stop gracefully.
    pub fn stop_actor(&self, aid: &Aid) {
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
                let value = SystemMsg::Stopped(aid.clone());
                m_aid.send(Message::new(value)).unwrap_or_else(|error| {
                    error!(
                        "Could not send 'Stopped' to monitoring actor {}: Error: {:?}",
                        m_aid, error
                    );
                });
            }
        }
    }

    /// Checks to see if the actor with the given [`Aid`] is alive within this actor system.
    pub fn is_actor_alive(&self, aid: &Aid) -> bool {
        let actors_by_aid = &self.data.actors_by_aid;
        actors_by_aid.contains_key(aid)
    }

    /// Look up an [`Aid`] by the unique UUID of the actor and either returns the located
    /// `aid` in a [`Option::Some`] or [`Option::None`] if not found.
    pub fn find_aid_by_uuid(&self, uuid: &Uuid) -> Option<Aid> {
        let aids_by_uuid = &self.data.aids_by_uuid;
        aids_by_uuid.get(uuid).map(|aid| aid.clone())
    }

    /// Look up an [`Aid`] by the user assigned name of the actor and either returns the
    /// located `aid` in a [`Option::Some`] or [`Option::None`] if not found.
    pub fn find_aid_by_name(&self, name: &str) -> Option<Aid> {
        let aids_by_name = &self.data.aids_by_name;
        aids_by_name.get(&name.to_string()).map(|aid| aid.clone())
    }

    /// A helper that finds an [`Aid`] on this system from the `system_uuid` and `actor_uuid`
    /// passed to the function. If the `system_uuid` doesn't match this system then a `None` will
    /// be returned. Also if the `system_uuid` matches but the actor is not found a `None` will
    /// be returned.
    fn find_aid(&self, system_uuid: &Uuid, actor_uuid: &Uuid) -> Option<Aid> {
        if self.uuid() == *system_uuid {
            self.find_aid_by_uuid(&actor_uuid)
        } else {
            None
        }
    }

    /// Returns the [`Aid`] to the "System" actor for this actor system.
    pub fn system_actor_aid(&self) -> Aid {
        self.find_aid_by_name(&"System").unwrap()
    }

    /// Adds a monitor so that `monitoring` will be informed if `monitored` stops.
    pub fn monitor(&self, monitoring: &Aid, monitored: &Aid) {
        let mut monitoring_by_monitored = self
            .data
            .monitoring_by_monitored
            .get_raw_mut_from_key(&monitored);
        let monitoring_vec = monitoring_by_monitored
            .entry(monitored.clone())
            .or_insert(HashSet::new());
        monitoring_vec.insert(monitoring.clone());
    }

    /// Asynchronously send a message to the system actors on all connected actor systems.
    /// FIXME (Issue #72) Add try_send ability.
    pub fn send_to_system_actors(&self, message: Message) {
        let remotes = &*self.data.remotes;
        for remote in remotes.iter() {
            let aid = &remote.value().system_actor_aid;
            aid.send(message.clone()).unwrap_or_else(|error| {
                error!("Could not send to system actor {}. Error: {}", aid, error)
            });
        }
    }

    /// Schedules a `message` to be sent to the `destination` [`Aid`] after a `delay`. Note
    /// That this method makes a best attempt at sending the message on time but the message may
    /// not be sent on exactly the delay passed. However, the message will never be sent before
    /// the given delay.
    pub(crate) fn send_after(&self, message: Message, destination: Aid, delay: Duration) {
        let instant = Instant::now().checked_add(delay).unwrap();
        let entry = DelayedMessage {
            uuid: Uuid::new_v4(),
            destination,
            instant,
            message,
        };
        let (ref mutex, ref condvar) = &*self.data.delayed_messages;
        let mut data = mutex.lock().unwrap();
        data.push(entry);
        condvar.notify_all();
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

/// Messages that are sent to and received from the System Actor.Aid
#[derive(Serialize, Deserialize, Debug)]
enum SystemActorMessage {
    /// Finds an actor by name.
    FindByName { reply_to: Aid, name: String },

    /// A message sent as a reply to a [`SystemActorMessage::FindByName`] request.
    FindByNameResult {
        /// The UUID of the system that is responding.
        system_uuid: Uuid,
        /// The name that was searched for.
        name: String,
        /// The Aid in a [`Some`] if found or [`None`] if not.
        aid: Option<Aid>,
    },
}

/// A processor for the system actor.
/// FIXME Issue #89: Refactor into a full struct based actor in another file.
fn system_actor_processor(_: &mut bool, context: &Context, message: &Message) -> AxiomResult {
    if let Some(msg) = message.content_as::<SystemActorMessage>() {
        match &*msg {
            // Someone requested that this system actor find an actor by name.
            SystemActorMessage::FindByName { reply_to, name } => {
                let found = context.system.find_aid_by_name(&name);
                let reply = Message::new(SystemActorMessage::FindByNameResult {
                    system_uuid: context.system.uuid(),
                    name: name.clone(),
                    aid: found,
                });
                // Note that you can't just unwrap or you could panic the dispatcher thread if
                // there is a problem sending the reply. In this case, the error is logged but the
                // actor moves on.
                reply_to.send(reply).unwrap_or_else(|error| {
                    error!(
                        "Could not send reply to FindByName to actor {}. Error: {:?}",
                        reply_to, error
                    )
                });
                Ok(Status::Done)
            }
            _ => Ok(Status::Done),
        }
    } else if let Some(msg) = message.content_as::<SystemMsg>() {
        match &*msg {
            SystemMsg::Start => Ok(Status::Done),
            _ => Ok(Status::Done),
        }
    } else {
        error!("Unhandled message received.");
        Ok(Status::Done)
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
        let system2 = ActorSystem::create(ActorSystemConfig::default());
        ActorSystem::connect_with_channels(system1.clone(), system2.clone());
        (system1, system2)
    }

    /// Helper to wait for 2 actor systems to shutdown or panic if they don't do so within
    /// 2000 milliseconds.
    fn await_two_system_shutdown(system1: ActorSystem, system2: ActorSystem) {
        let timeout = Duration::from_millis(2000);

        let h1 = thread::spawn(move || {
            system1.await_shutdown_with_timeout(timeout).unwrap();
        });

        let h2 = thread::spawn(move || {
            system2.await_shutdown_with_timeout(timeout).unwrap();
        });

        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// Test that verifies that the actor system shutdown mechanisms that wait for a specific
    /// timeout work properly.
    #[test]
    fn test_shutdown_await_timeout() {
        use std::time::Duration;

        let system = ActorSystem::create(ActorSystemConfig::default());
        system
            .spawn()
            .with((), |_state: &mut (), context: &Context, _: &Message| {
                thread::sleep(Duration::from_millis(5));
                context.system.trigger_shutdown();
                Ok(Status::Done)
            })
            .unwrap();
        system
            .await_shutdown_with_timeout(Duration::from_millis(1000))
            .unwrap();
    }

    /// This test verifies that an actor can be found by its uuid.
    #[test]
    fn test_find_by_uuid() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().with(0, simple_handler).unwrap();
        aid.send_new(11).unwrap();
        await_received(&aid, 2, 1000).unwrap();
        let found = system.find_aid_by_uuid(&aid.uuid()).unwrap();
        assert!(Aid::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid_by_uuid(&Uuid::new_v4()));

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that an actor can be found by its name if it has one.
    #[test]
    fn test_find_by_name() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().name("A").with(0, simple_handler).unwrap();
        aid.send_new(11).unwrap();
        await_received(&aid, 2, 1000).unwrap();
        let found = system.find_aid_by_name(&aid.name().unwrap()).unwrap();
        assert!(Aid::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid_by_name("B"));

        system.trigger_and_await_shutdown();
    }

    /// This tests the find_aid function that takes a system uuid and an actor uuid.
    #[test]
    fn test_find_aid() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().name("A").with(0, simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        let found = system.find_aid(&aid.system_uuid(), &aid.uuid()).unwrap();
        assert!(Aid::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid(&aid.system_uuid(), &Uuid::new_v4()));
        assert_eq!(None, system.find_aid(&Uuid::new_v4(), &aid.uuid()));

        system.trigger_and_await_shutdown();
    }

    /// Tests that actors that are stopped are removed from all relevant lookup maps and
    /// are reported as not being alive.
    #[test]
    fn test_stop_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().name("A").with(0, simple_handler).unwrap();
        aid.send_new(11).unwrap();
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

    /// This test verifies that the system can send a message after a particular delay.
    /// FIXME need separate test for remotes.
    #[test]
    fn test_send_after() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().name("A").with(0, simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();

        system.send_after(Message::new(11), aid.clone(), Duration::from_millis(10));
        thread::sleep(Duration::from_millis(5));
        assert_eq!(1, aid.received().unwrap());
        thread::sleep(Duration::from_millis(10));
        assert_eq!(2, aid.received().unwrap());

        system.trigger_and_await_shutdown();
    }

    /// Tests that if we execute two send_after calls, one for a longer duration than the
    /// second, that the message will be sent for the second one before the first one enqueued
    /// and that the second one will still arrive properly.
    #[test]
    fn test_send_after_before_current() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        thread::sleep(Duration::from_millis(10));

        let aid1 = system.spawn().name("A").with(0, simple_handler).unwrap();
        await_received(&aid1, 1, 1000).unwrap();
        let aid2 = system.spawn().name("B").with(0, simple_handler).unwrap();
        await_received(&aid2, 1, 1000).unwrap();

        aid1.send_after(Message::new(11), Duration::from_millis(200))
            .unwrap();
        thread::sleep(Duration::from_millis(5));

        aid2.send_after(Message::new(11), Duration::from_millis(50))
            .unwrap();
        thread::sleep(Duration::from_millis(5));

        assert_eq!(1, aid1.received().unwrap());
        assert_eq!(1, aid2.received().unwrap());

        // We overshoot the timing on the asserts because when the tests are run the CPU is
        // busy and the timing can be difficult. This happens sometimes running tests.
        thread::sleep(Duration::from_millis(55));
        assert_eq!(1, aid1.received().unwrap());
        assert_eq!(2, aid2.received().unwrap());

        thread::sleep(Duration::from_millis(160));
        assert_eq!(2, aid1.received().unwrap());
        assert_eq!(2, aid2.received().unwrap());

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that the system does not panic if we schedule to an actor that does
    /// not exist in the lookup map. This can happen if a message is sent to an actor after the
    /// actor is stopped but before the system notifies the [`Aid`] that the actor has been
    /// stopped.
    #[test]
    fn test_actor_not_in_map() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().with(0, simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap(); // Now it is started for sure.

        // We force remove the actor from the system without calling stop so now it cannot
        // be scheduled.
        let sys_clone = system.clone();
        let actors_by_aid = &sys_clone.data.actors_by_aid;
        actors_by_aid.remove(&aid);

        // Send a message to the actor which should not schedule it but write out a warning.
        aid.send_new(11).unwrap();

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

    // Tests that monitors work in the actor system and send a message to monitoring actors
    // when monitored actors stop.
    #[test]
    fn test_monitors() {
        init_test_log();

        fn monitor_handler(state: &mut Aid, _: &Context, message: &Message) -> AxiomResult {
            if let Some(msg) = message.content_as::<SystemMsg>() {
                match &*msg {
                    SystemMsg::Stopped(aid) => {
                        assert!(Aid::ptr_eq(state, aid));
                        Ok(Status::Done)
                    }
                    SystemMsg::Start => Ok(Status::Done),
                    _ => panic!("Received some other message!"),
                }
            } else {
                panic!("Received some other message!");
            }
        }

        let system = ActorSystem::create(ActorSystemConfig::default());
        let monitored = system.spawn().with(0 as usize, simple_handler).unwrap();
        let not_monitoring = system.spawn().with(0 as usize, simple_handler).unwrap();
        let monitoring1 = system
            .spawn()
            .with(monitored.clone(), monitor_handler)
            .unwrap();
        let monitoring2 = system
            .spawn()
            .with(monitored.clone(), monitor_handler)
            .unwrap();
        system.monitor(&monitoring1, &monitored);
        system.monitor(&monitoring2, &monitored);

        {
            // Validate the monitors are there in a block to release mutex afterwards.
            let monitoring_by_monitored = &system.data.monitoring_by_monitored;
            let m_set = monitoring_by_monitored.get(&monitored).unwrap();
            assert!(m_set.contains(&monitoring1));
            assert!(m_set.contains(&monitoring2));
        }

        // Stop the actor and it should be out of the monitors map.
        system.stop_actor(&monitored);
        await_received(&monitoring1, 2, 1000).unwrap();
        await_received(&monitoring2, 2, 1000).unwrap();
        await_received(&not_monitoring, 1, 1000).unwrap();

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that the concept of named actors works properly. When a user wants
    /// to declare a named actor they cannot register the same name twice. When the actor using
    /// the name currently stops the name should be removed from the registered names and be
    /// available again.
    #[test]
    fn test_named_actor_restrictions() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default());
        let state = 0 as usize;

        let aid1 = system
            .spawn()
            .name("A")
            .with(state, simple_handler)
            .unwrap();
        await_received(&aid1, 1, 1000).unwrap();

        let aid2 = system
            .spawn()
            .name("B")
            .with(state, simple_handler)
            .unwrap();
        await_received(&aid2, 1, 1000).unwrap();

        // Spawn an actor that attempts to overwrite "A" in the names and make sure the
        // attempt returns an error to be handled.
        let result = system.spawn().name("A").with(state, simple_handler);
        assert_eq!(Err(AxiomError::NameAlreadyUsed("A".to_string())), result);

        // Verify that the same actor has "A" name and is still up.
        let found1 = system.find_aid_by_name("A").unwrap();
        assert_eq!(true, system.is_actor_alive(&aid1));
        assert!(Aid::ptr_eq(&aid1, &found1));

        // Stop "B" and verify that the ActorSystem's maps are cleaned up.
        system.stop_actor(&aid2);
        assert_eq!(None, system.find_aid_by_name("B"));
        assert_eq!(None, system.find_aid_by_uuid(&aid2.uuid()));

        // Now we should be able to crate a new actor with the name "B".
        let aid3 = system
            .spawn()
            .name("B")
            .with(state, simple_handler)
            .unwrap();
        await_received(&aid3, 1, 1000).unwrap();
        let found2 = system.find_aid_by_name("B").unwrap();
        assert!(Aid::ptr_eq(&aid3, &found2));

        system.trigger_and_await_shutdown();
    }

    /// Tests that remote actors can send and receive messages between each other.
    #[test]
    fn test_remote_actors() {
        // In this test our messages are just structs.
        #[derive(Serialize, Deserialize, Debug)]
        struct Request {
            reply_to: Aid,
        }

        #[derive(Serialize, Deserialize, Debug)]
        struct Reply {}

        init_test_log();
        let (system1, system2) = start_and_connect_two_systems();

        system1.init_current();
        let aid = system1
            .spawn()
            .with((), |_: &mut (), context: &Context, message: &Message| {
                if let Some(msg) = message.content_as::<Request>() {
                    msg.reply_to.send_new(Reply {}).unwrap();
                    context.system.trigger_shutdown();
                    Ok(Status::Stop)
                } else if let Some(_) = message.content_as::<SystemMsg>() {
                    Ok(Status::Done)
                } else {
                    panic!("Unexpected message received!");
                }
            })
            .unwrap();
        await_received(&aid, 1, 1000).unwrap();

        let serialized = bincode::serialize(&aid).unwrap();
        system2
            .spawn()
            .with(
                (),
                move |_: &mut (), context: &Context, message: &Message| {
                    if let Some(_) = message.content_as::<Reply>() {
                        context.system.trigger_shutdown();
                        Ok(Status::Stop)
                    } else if let Some(msg) = message.content_as::<SystemMsg>() {
                        match &*msg {
                            SystemMsg::Start => {
                                let target_aid: Aid = bincode::deserialize(&serialized).unwrap();
                                target_aid
                                    .send_new(Request {
                                        reply_to: context.aid.clone(),
                                    })
                                    .unwrap();
                                Ok(Status::Done)
                            }
                            _ => Ok(Status::Done),
                        }
                    } else {
                        panic!("Unexpected message received!");
                    }
                },
            )
            .unwrap();

        await_two_system_shutdown(system1, system2);
    }

    /// Tests the ability to find an aid on a remote system by name using a `SystemActor`. This
    /// also serves as a test for cross system actor communication as well as testing broadcast
    /// to multiple system actors in the cluster.
    #[test]
    fn test_system_actor_find_by_name() {
        init_test_log();
        let (system1, system2) = start_and_connect_two_systems();

        let aid1 = system1
            .spawn()
            .name("A")
            .with((), |_: &mut (), context: &Context, _: &Message| {
                context.system.trigger_shutdown();
                Ok(Status::Done)
            })
            .unwrap();
        await_received(&aid1, 1, 1000).unwrap();

        system2
            .spawn()
            .with(
                (),
                move |_: &mut (), context: &Context, message: &Message| {
                    if let Some(msg) = message.content_as::<SystemActorMessage>() {
                        match &*msg {
                            SystemActorMessage::FindByNameResult { aid: found, .. } => {
                                if let Some(target) = found {
                                    assert_eq!(target.uuid(), aid1.uuid());
                                    target.send_new(true).unwrap();
                                    context.system.trigger_shutdown();
                                    Ok(Status::Done)
                                } else {
                                    panic!("Didn't find AID.");
                                }
                            }
                            _ => panic!("Unexpected message received!"),
                        }
                    } else if let Some(msg) = message.content_as::<SystemMsg>() {
                        if let SystemMsg::Start = &*msg {
                            context.system.send_to_system_actors(Message::new(
                                SystemActorMessage::FindByName {
                                    reply_to: context.aid.clone(),
                                    name: "A".to_string(),
                                },
                            ));
                            Ok(Status::Done)
                        } else {
                            panic!("Unexpected message received!");
                        }
                    } else {
                        panic!("Unexpected message received!");
                    }
                },
            )
            .unwrap();

        await_two_system_shutdown(system1, system2);
    }
}
