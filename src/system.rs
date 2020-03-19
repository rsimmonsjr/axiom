//! Implements the [`ActorSystem`] and related types of Axiom.
//!
//! When the [`ActorSystem`] starts up, a number of Reactors will be spawned that will iterate over
//! Actor's inbound messages, processing them asynchronously. Actors will be ran as many times as
//! they can over a given time slice until they are pending or have no more messages. If the Actor
//! is Pending, it will be re-queued when the pending future wakes it. If the Actor has no more
//! messages, it will be returned to the Executor until it has messages again. This process cycles
//! until the [`ActorSystem`] is shutdown.
//!
//! The user should refer to test cases and examples as "how-to" guides for using Axiom.

use crate::actors::{Actor, ActorBuilder, ActorStream};
use crate::executor::AxiomExecutor;
use crate::prelude::*;
use crate::system::system_actor::SystemActor;
use dashmap::DashMap;
use log::{debug, error, info, trace, warn};
use once_cell::sync::OnceCell;
use secc::{SeccReceiver, SeccSender};
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use uuid::Uuid;

mod system_actor;

// Holds an ActorSystem in a std::thread_local so that the Aid deserializer and other types can
// obtain a clone if needed at any time. This needs to be set by each Reactor that is processing
// messages with the actors.
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
    Stopped { aid: Aid, error: Option<String> },
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActorSystemConfig {
    /// The default size for the channel that is created for each actor. This can be overridden on
    /// a per-actor basis during spawning as well. Making the default channel size bigger allows
    /// for more bandwidth in sending messages to actors but also takes more memory. Also the
    /// user should consider if their actor needs a large channel then it might need to be
    /// refactored or the threads size should be increased because messages aren't being processed
    /// fast enough. The default value for this is 32.
    pub message_channel_size: u16,
    /// Max duration to wait between attempts to send to an actor's message channel. This is used
    /// to poll a busy channel that is at its capacity limit. The larger this value is, the longer
    /// `send` will wait for capacity in the channel but the user should be aware that if the
    /// system is often waiting on capacity that channel may be too small or the actor may need to
    /// be refactored to process messages faster. The default value is 1 millisecond.
    pub send_timeout: Duration,
    /// The size of the thread pool which governs how many worker threads there are in the system.
    /// The number of threads should be carefully considered to have sufficient parallelism but not
    /// over-schedule the CPU on the target hardware. The default value is 4 * the number of logical
    /// CPUs.
    pub thread_pool_size: u16,
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
    /// While Reactors will constantly attempt to get more work, they may run out. At that point,
    /// they will idle for this duration, or until they get a wakeup notification. Said
    /// notifications can be missed, so it's best to not set this too high. The default value is 10
    /// milliseconds. This implementation is backed by a [`Condvar`].
    pub thread_wait_time: Duration,
    /// Determines whether the actor system will immediately start when it is created. The default
    /// value is true.
    pub start_on_launch: bool,
}

impl ActorSystemConfig {
    /// Return a new config with the changed `message_channel_size`.
    pub fn message_channel_size(mut self, value: u16) -> Self {
        self.message_channel_size = value;
        self
    }

    /// Return a new config with the changed `send_timeout`.
    pub fn send_timeout(mut self, value: Duration) -> Self {
        self.send_timeout = value;
        self
    }

    /// Return a new config with the changed `thread_pool_size`.
    pub fn thread_pool_size(mut self, value: u16) -> Self {
        self.thread_pool_size = value;
        self
    }

    /// Return a new config with the changed `warn_threshold`.
    pub fn warn_threshold(mut self, value: Duration) -> Self {
        self.warn_threshold = value;
        self
    }

    /// Return a new config with the changed `time_slice`.
    pub fn time_slice(mut self, value: Duration) -> Self {
        self.time_slice = value;
        self
    }

    /// Return a new config with the changed `thread_wait_time`.
    pub fn thread_wait_time(mut self, value: Duration) -> Self {
        self.thread_wait_time = value;
        self
    }
}

impl Default for ActorSystemConfig {
    /// Create the config with the default values.
    fn default() -> ActorSystemConfig {
        ActorSystemConfig {
            thread_pool_size: (num_cpus::get() * 4) as u16,
            warn_threshold: Duration::from_millis(1),
            time_slice: Duration::from_millis(1),
            thread_wait_time: Duration::from_millis(100),
            message_channel_size: 32,
            send_timeout: Duration::from_millis(1),
            start_on_launch: true,
        }
    }
}

/// Errors produced by the ActorSystem
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SystemError {
    /// An error returned when an actor is already using a local name at the time the user tries
    /// to register that name for a new actor. The error contains the name that was attempted
    /// to be registered.
    NameAlreadyUsed(String),
}

impl std::fmt::Display for SystemError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for SystemError {}

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
    // FIXME (Issue #76) Add graceful shutdown for threads handling remotes.
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
pub(crate) struct ActorSystemData {
    /// Unique version 4 UUID for this actor system.
    pub(crate) uuid: Uuid,
    /// The config for the actor system which was passed to it when created.
    pub(crate) config: ActorSystemConfig,
    /// Holds handles to the pool of threads processing the work channel.
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// The Executor responsible for managing the runtime of the Actors
    executor: AxiomExecutor,
    /// Whether the ActorSystem has started or not.
    started: AtomicBool,
    /// A flag and condvar that can be used to send a signal when the system begins to shutdown.
    shutdown_triggered: Arc<(Mutex<bool>, Condvar)>,
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
    pub(crate) data: Arc<ActorSystemData>,
}

impl ActorSystem {
    /// Creates an actor system with the given config. The user should benchmark how many slots
    /// are needed in the work channel, the number of threads they need in the system and and so
    /// on in order to satisfy the requirements of the software they are creating.
    pub fn create(config: ActorSystemConfig) -> ActorSystem {
        let uuid = Uuid::new_v4();
        let threads = Mutex::new(Vec::with_capacity(config.thread_pool_size as usize));
        let shutdown_triggered = Arc::new((Mutex::new(false), Condvar::new()));

        let executor = AxiomExecutor::new(shutdown_triggered.clone());

        let start_on_launch = config.start_on_launch;

        // Creates the actor system with the thread pools and actor map initialized.
        let system = ActorSystem {
            data: Arc::new(ActorSystemData {
                uuid,
                config,
                threads,
                executor,
                started: AtomicBool::new(false),
                shutdown_triggered,
                actors_by_aid: Arc::new(DashMap::default()),
                aids_by_uuid: Arc::new(DashMap::default()),
                aids_by_name: Arc::new(DashMap::default()),
                monitoring_by_monitored: Arc::new(DashMap::default()),
                remotes: Arc::new(DashMap::default()),
                delayed_messages: Arc::new((Mutex::new(BinaryHeap::new()), Condvar::new())),
            }),
        };

        // Starts the actor system if configured to do so
        if start_on_launch {
            system.start();
        }

        system
    }

    /// Starts an unstarted ActorSystem. The function will do nothing if the ActorSystem has already been started.
    pub fn start(&self) {
        if !self
            .data
            .started
            .compare_and_swap(false, true, Ordering::Relaxed)
        {
            info!("ActorSystem {} has spawned", self.data.uuid);
            self.data.executor.init(self);

            // We have the thread pool in a mutex to avoid a chicken & egg situation with the actor
            // system not being created yet but needed by the thread. We put this code in a block to
            // get around rust borrow constraints without unnecessarily copying things.
            {
                let mut guard = self.data.threads.lock().unwrap();

                // Start the thread that reads from the `delayed_messages` queue.
                // FIXME Put in ability to confirm how many of these to start.
                for _ in 0..1 {
                    let thread = self.start_send_after_thread();
                    guard.push(thread);
                }
            }

            // Launch the SystemActor and give it the name "System"
            self.spawn()
                .name("System")
                .with(SystemActor, SystemActor::processor)
                .unwrap();
        }
    }

    /// Starts a thread that monitors the delayed_messages and sends the messages when their
    /// delays have elapsed.
    // FIXME Add a graceful shutdown to this thread and notifications.
    fn start_send_after_thread(&self) -> JoinHandle<()> {
        let system = self.clone();
        let delayed_messages = self.data.delayed_messages.clone();
        thread::spawn(move || {
            while !*system.data.shutdown_triggered.0.lock().unwrap() {
                let (ref mutex, ref condvar) = &*delayed_messages;
                let mut data = mutex.lock().unwrap();
                match data.peek() {
                    None => {
                        // wait to be notified something is added.
                        let _ = condvar.wait(data).unwrap();
                    }
                    Some(msg) => {
                        let now = Instant::now();
                        if now >= msg.instant {
                            trace!("Sending delayed message");
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
        sender: &SeccSender<WireMessage>,
        receiver: &SeccReceiver<WireMessage>,
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
        let sys_uuid = system_actor_aid.system_uuid();
        let handle = thread::spawn(move || {
            system.init_current();
            // FIXME (Issue #76) Add graceful shutdown for threads handling remotes including
            // informing remote that the system is exiting.
            while !*system.data.shutdown_triggered.0.lock().unwrap() {
                match receiver_clone.receive_await_timeout(thread_timeout) {
                    Err(_) => (), // not an error, just loop and try again.
                    Ok(wire_msg) => system.process_wire_message(&sys_uuid, &wire_msg),
                }
            }
        });

        // Save the info and thread to the remotes map.
        let info = RemoteInfo {
            system_uuid: system_actor_aid.system_uuid(),
            sender: sender.clone(),
            receiver: receiver.clone(),
            _handle: handle,
            system_actor_aid,
        };

        let uuid = info.system_uuid;
        self.data.remotes.insert(uuid.clone(), info);
        uuid
    }

    /// Disconnects this actor system from the remote actor system with the given UUID.
    // FIXME Connectivity management needs a lot of work and testing.
    pub fn disconnect(&self, system_uuid: Uuid) -> Result<(), AidError> {
        self.data.remotes.remove(&system_uuid);
        Ok(())
    }

    /// Connects two actor systems using two channels directly. This can be used as a utility
    /// in testing or to link two actor systems directly within the same process.
    pub fn connect_with_channels(system1: &ActorSystem, system2: &ActorSystem) {
        let (tx1, rx1) = secc::create::<WireMessage>(32, system1.data.config.thread_wait_time);
        let (tx2, rx2) = secc::create::<WireMessage>(32, system2.data.config.thread_wait_time);

        // We will do this in 2 threads because one connect would block waiting on a message
        // from the other actor system, causing a deadlock.
        let system1_clone = system1.clone();
        let system2_clone = system2.clone();
        let h1 = thread::spawn(move || system1_clone.connect(&tx1, &rx2));
        let h2 = thread::spawn(move || system2_clone.connect(&tx2, &rx1));

        // Wait for the completion of the connection.
        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// A helper function to process a wire message from another actor system. The passed uuid
    /// is the uuid of the remote that sent the message.
    // FIXME (Issue #74) Make error handling in ActorSystem::process_wire_message more robust.
    fn process_wire_message(&self, _uuid: &Uuid, wire_message: &WireMessage) {
        match wire_message {
            WireMessage::ActorMessage {
                actor_uuid,
                system_uuid,
                message,
            } => {
                if let Some(aid) = self.find_aid(&system_uuid, &actor_uuid) {
                    aid.send(message.clone()).unwrap_or_else(|error| {
                        warn!("Could not send wire message to {}. Error: {}", aid, error);
                    })
                }
            }
            WireMessage::DelayedActorMessage {
                duration,
                actor_uuid,
                system_uuid,
                message,
            } => {
                self.find_aid(&system_uuid, &actor_uuid)
                    .map(|aid| self.send_after(message.clone(), aid, *duration))
                    .expect("Error not handled yet");
            }
            WireMessage::Hello { system_actor_aid } => {
                debug!("{:?} Got Hello from {}", self.data.uuid, system_actor_aid);
            }
        }
    }

    /// Initializes this actor system to use for the current thread which is necessary if the
    /// user wishes to serialize and deserialize [`Aid`]s.
    ///
    /// ## Contract
    /// You must run this exactly once per thread where needed.
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

    /// Triggers a shutdown but doesn't wait for the Reactors to stop.
    pub fn trigger_shutdown(&self) {
        let (ref mutex, ref condvar) = &*self.data.shutdown_triggered;
        *mutex.lock().unwrap() = true;
        condvar.notify_all();
    }

    /// Awaits the Executor shutting down all Reactors. This is backed by a barrier that Reactors
    /// will wait on after [`ActorSystem::trigger_shutdown`] is called, blocking until all Reactors
    /// have stopped.
    pub fn await_shutdown(&self, timeout: impl Into<Option<Duration>>) -> ShutdownResult {
        info!("System awaiting shutdown");

        let start = Instant::now();
        let timeout = timeout.into();

        let result = match timeout {
            Some(dur) => self.await_shutdown_trigger_with_timeout(dur),
            None => self.await_shutdown_trigger_without_timeout(),
        };

        if let Some(r) = result {
            return r;
        }

        let timeout = {
            match timeout {
                Some(timeout) => {
                    let elapsed = Instant::now().duration_since(start);
                    if let Some(t) = timeout.checked_sub(elapsed) {
                        Some(t)
                    } else {
                        return ShutdownResult::TimedOut;
                    }
                }
                None => None,
            }
        };

        // Wait for the executor to finish shutting down
        self.data.executor.await_shutdown(timeout)
    }

    fn await_shutdown_trigger_with_timeout(&self, mut dur: Duration) -> Option<ShutdownResult> {
        let (ref mutex, ref condvar) = &*self.data.shutdown_triggered;
        let mut guard = mutex.lock().unwrap();
        while !*guard {
            let started = Instant::now();
            let (new_guard, timeout) = match condvar.wait_timeout(guard, dur) {
                Ok(ret) => ret,
                Err(_) => return Some(ShutdownResult::Panicked),
            };

            if timeout.timed_out() {
                return Some(ShutdownResult::TimedOut);
            }

            guard = new_guard;
            dur -= started.elapsed();
        }
        None
    }

    fn await_shutdown_trigger_without_timeout(&self) -> Option<ShutdownResult> {
        let (ref mutex, ref condvar) = &*self.data.shutdown_triggered;
        let mut guard = mutex.lock().unwrap();
        while !*guard {
            guard = match condvar.wait(guard) {
                Ok(ret) => ret,
                Err(_) => return Some(ShutdownResult::Panicked),
            };
        }
        None
    }

    /// Triggers a shutdown of the system and returns only when all Reactors have shutdown.
    pub fn trigger_and_await_shutdown(
        &self,
        timeout: impl Into<Option<Duration>>,
    ) -> ShutdownResult {
        self.trigger_shutdown();
        self.await_shutdown(timeout)
    }

    // An internal helper to register an actor in the actor system.
    pub(crate) fn register_actor(
        &self,
        actor: Arc<Actor>,
        stream: ActorStream,
    ) -> Result<Aid, SystemError> {
        let aids_by_name = &self.data.aids_by_name;
        let actors_by_aid = &self.data.actors_by_aid;
        let aids_by_uuid = &self.data.aids_by_uuid;
        let aid = actor.context.aid.clone();
        if let Some(name_string) = &aid.name() {
            if aids_by_name.contains_key(name_string) {
                return Err(SystemError::NameAlreadyUsed(name_string.clone()));
            } else {
                aids_by_name.insert(name_string.clone(), aid.clone());
            }
        }
        actors_by_aid.insert(aid.clone(), actor);
        aids_by_uuid.insert(aid.uuid(), aid.clone());
        self.data.executor.register_actor(stream);
        aid.send(Message::new(SystemMsg::Start)).unwrap(); // Actor was just made
        Ok(aid)
    }

    /// Creates a single use builder for this actor system that allows a user to build actors
    /// using a chained syntax while optionally providing configuration parameters if desired.
    ///
    /// # Examples
    /// ```
    /// use axiom::prelude::*;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// async fn handler(mut count: usize, _: Context, _: Message) -> ActorResult<usize> {
    ///     count += 1;
    ///     Ok(Status::done(count))
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

    /// Schedules the `aid` for work. Note that this is the only time that we have to use the
    /// lookup table. This function gets called when an actor goes from 0 receivable messages to
    /// 1 receivable message. If the actor has more receivable messages then this will not be
    /// needed to be called because the dispatcher threads will handle the process of resending
    /// the actor to the work channel.
    // TODO Put tests verifying the resend on multiple messages.
    pub(crate) fn schedule(&self, aid: Aid) {
        let actors_by_aid = &self.data.actors_by_aid;
        if actors_by_aid.contains_key(&aid) {
            self.data.executor.wake(aid);
        } else {
            // The actor was removed from the map so ignore the problem and just log
            // a warning.
            warn!(
                "Attempted to schedule actor with aid {:?} on system with node_id {:?} but
                the actor does not exist.",
                aid,
                self.data.uuid.to_string(),
            );
        }
    }

    /// Stops an actor by shutting down its channels and removing it from the actors list and
    /// telling the [`Aid`] to not allow messages to be sent to the actor since the receiving
    /// side of the actor is gone.
    ///
    /// This is something that should rarely be called from the outside as it is much better to
    /// send the actor a [`SystemMsg::Stop`] message and allow it to stop gracefully.
    pub fn stop_actor(&self, aid: &Aid) {
        self.internal_stop_actor(aid, None);
    }

    /// Internal implementation of stop_actor, so we have the ability to send an error along with
    /// the notification of stop.
    pub(crate) fn internal_stop_actor(&self, aid: &Aid, error: impl Into<Option<StdError>>) {
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
            let error = error.into().map(|e| format!("{}", e));
            for m_aid in monitoring {
                let value = SystemMsg::Stopped {
                    aid: aid.clone(),
                    error: error.clone(),
                };
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
    /// [`Aid`] in a [`Option::Some`] or [`Option::None`] if not found.
    pub fn find_aid_by_uuid(&self, uuid: &Uuid) -> Option<Aid> {
        let aids_by_uuid = &self.data.aids_by_uuid;
        aids_by_uuid.get(uuid).map(|aid| aid.clone())
    }

    /// Look up an [`Aid`] by the user assigned name of the actor and either returns the
    /// located [`Aid`] in a [`Option::Some`] or [`Option::None`] if not found.
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
    // FIXME (Issue #72) Add try_send ability.
    pub fn send_to_system_actors(&self, message: Message) {
        let remotes = &*self.data.remotes;
        trace!("Sending message to Remote System Actors");
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

    #[cfg(test)]
    pub(crate) fn executor(&self) -> &AxiomExecutor {
        &self.data.executor
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::system_actor::SystemActorMessage;
    use crate::tests::*;
    use futures::future;
    use std::thread;

    // A helper to start two actor systems and connect them.
    fn start_and_connect_two_systems() -> (ActorSystem, ActorSystem) {
        let system1 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let system2 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        ActorSystem::connect_with_channels(&system1, &system2);
        (system1, system2)
    }

    /// Helper to wait for 2 actor systems to shutdown or panic if they don't do so within
    /// 2000 milliseconds.
    fn await_two_system_shutdown(system1: ActorSystem, system2: ActorSystem) {
        let h1 = thread::spawn(move || {
            system1.await_shutdown(None);
        });

        let h2 = thread::spawn(move || {
            system2.await_shutdown(None);
        });

        h1.join().unwrap();
        h2.join().unwrap();
    }

    /// Test that verifies that the actor system shutdown mechanisms that wait for a specific
    /// timeout work properly.
    #[test]
    fn test_shutdown_await_timeout() {
        use std::time::Duration;

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        system
            .spawn()
            .with((), |_state: (), context: Context, _: Message| {
                async move {
                    // Block for enough time so we can test timeout twice
                    sleep(100);
                    context.system.trigger_shutdown();
                    Ok(Status::done(()))
                }
            })
            .unwrap();

        // Expecting to timeout
        assert_eq!(
            system.await_shutdown(Duration::from_millis(10)),
            ShutdownResult::TimedOut
        );

        // Expecting to NOT timeout
        assert_eq!(
            system.await_shutdown(Duration::from_millis(200)),
            ShutdownResult::Ok
        );

        // Validate that if the system is already shutdown the method doesn't hang.
        // FIXME Design a means that this cannot ever hang the test.
        system.await_shutdown(None);
    }

    /// This test verifies that an actor can be found by its uuid.
    #[test]
    fn test_find_by_uuid() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().with((), simple_handler).unwrap();
        aid.send_new(11).unwrap();
        await_received(&aid, 2, 1000).unwrap();
        let found = system.find_aid_by_uuid(&aid.uuid()).unwrap();
        assert!(Aid::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid_by_uuid(&Uuid::new_v4()));

        system.trigger_and_await_shutdown(None);
    }

    /// This test verifies that an actor can be found by its name if it has one.
    #[test]
    fn test_find_by_name() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().name("A").with((), simple_handler).unwrap();
        aid.send_new(11).unwrap();
        await_received(&aid, 2, 1000).unwrap();
        let found = system.find_aid_by_name(&aid.name().unwrap()).unwrap();
        assert!(Aid::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid_by_name("B"));

        system.trigger_and_await_shutdown(None);
    }

    /// This tests the find_aid function that takes a system uuid and an actor uuid.
    #[test]
    fn test_find_aid() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().name("A").with((), simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        let found = system.find_aid(&aid.system_uuid(), &aid.uuid()).unwrap();
        assert!(Aid::ptr_eq(&aid, &found));

        assert_eq!(None, system.find_aid(&aid.system_uuid(), &Uuid::new_v4()));
        assert_eq!(None, system.find_aid(&Uuid::new_v4(), &aid.uuid()));

        system.trigger_and_await_shutdown(None);
    }

    /// Tests that actors that are stopped are removed from all relevant lookup maps and
    /// are reported as not being alive.
    #[test]
    fn test_stop_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().name("A").with((), simple_handler).unwrap();
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

        system.trigger_and_await_shutdown(None);
    }

    /// This test verifies that the system can send a message after a particular delay.
    // FIXME need separate test for remotes.
    #[test]
    fn test_send_after() {
        init_test_log();

        info!("Preparing test");
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().name("A").with((), simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        info!("Test prepared, sending delayed message");

        system.send_after(Message::new(11), aid.clone(), Duration::from_millis(10));
        info!("Sleeping for initial check");
        sleep(5);
        assert_eq!(1, aid.received().unwrap());
        info!("Sleeping till we're 100% sure we should have the message");
        sleep(10);
        assert_eq!(2, aid.received().unwrap());

        system.trigger_and_await_shutdown(None);
    }

    /// Tests that if we execute two send_after calls, one for a longer duration than the
    /// second, that the message will be sent for the second one before the first one enqueued
    /// and that the second one will still arrive properly.
    #[test]
    fn test_send_after_before_current() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        let aid1 = system.spawn().name("A").with((), simple_handler).unwrap();
        await_received(&aid1, 1, 1000).unwrap();
        let aid2 = system.spawn().name("B").with((), simple_handler).unwrap();
        await_received(&aid2, 1, 1000).unwrap();

        aid1.send_after(Message::new(11), Duration::from_millis(50))
            .unwrap();

        aid2.send_after(Message::new(11), Duration::from_millis(10))
            .unwrap();

        assert_eq!(1, aid1.received().unwrap());
        assert_eq!(1, aid2.received().unwrap());

        // We overshoot the timing on the asserts because when the tests are run the CPU is
        // busy and the timing can be tricky.
        sleep(15);
        assert_eq!(1, aid1.received().unwrap());
        assert_eq!(2, aid2.received().unwrap());

        sleep(50);
        assert_eq!(2, aid1.received().unwrap());
        assert_eq!(2, aid2.received().unwrap());

        system.trigger_and_await_shutdown(None);
    }

    /// This test verifies that the system does not panic if we schedule to an actor that does
    /// not exist in the lookup map. This can happen if a message is sent to an actor after the
    /// actor is stopped but before the system notifies the [`Aid`] that the actor has been
    /// stopped.
    #[test]
    fn test_actor_not_in_map() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().with((), simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap(); // Now it is started for sure.

        // We force remove the actor from the system without calling stop so now it cannot
        // be scheduled.
        let sys_clone = system.clone();
        let actors_by_aid = &sys_clone.data.actors_by_aid;
        actors_by_aid.remove(&aid);

        // Send a message to the actor which should not schedule it but write out a warning.
        aid.send_new(11).unwrap();

        system.trigger_and_await_shutdown(None);
    }

    /// Tests connection between two different actor systems using channels.
    #[test]
    fn test_connect_with_channels() {
        let system1 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let system2 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        ActorSystem::connect_with_channels(&system1, &system2);
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

        let tracker = AssertCollect::new();
        async fn monitor_handler(
            state: (Aid, AssertCollect),
            _: Context,
            message: Message,
        ) -> ActorResult<(Aid, AssertCollect)> {
            if let Some(msg) = message.content_as::<SystemMsg>() {
                match &*msg {
                    SystemMsg::Stopped { aid, error } => {
                        state
                            .1
                            .assert(Aid::ptr_eq(&state.0, aid), "Pointers are not equal!");
                        state.1.assert(error.is_none(), "Actor was errored!");
                        Ok(Status::done(state))
                    }
                    SystemMsg::Start => Ok(Status::done(state)),
                    _ => state.1.panic("Received some other message!"),
                }
            } else {
                state.1.panic("Received some other message!")
            }
        }

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let monitored = system.spawn().with((), simple_handler).unwrap();
        let not_monitoring = system.spawn().with((), simple_handler).unwrap();
        let monitoring1 = system
            .spawn()
            .with((monitored.clone(), tracker.clone()), monitor_handler)
            .unwrap();
        let monitoring2 = system
            .spawn()
            .with((monitored.clone(), tracker.clone()), monitor_handler)
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

        system.trigger_and_await_shutdown(None);
        tracker.collect();
    }

    #[test]
    fn test_monitor_gets_panics_errors() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let tracker = AssertCollect::new();
        let t = tracker.clone();
        let aid = system
            .spawn()
            .with((), |_: (), _: Context, msg: Message| {
                if let Some(_) = msg.content_as::<SystemMsg>() {
                    debug!("Not panicking this time");
                    return future::ok(Status::done(()));
                }

                debug!("About to panic");
                panic!("I panicked")
            })
            .unwrap();
        let monitor = system
            .spawn()
            .with(aid.clone(), move |state: Aid, _: Context, msg: Message| {
                if let Some(msg) = msg.content_as::<SystemMsg>() {
                    match &*msg {
                        SystemMsg::Stopped { aid, error } => {
                            t.assert(*aid == state, "Aid is not expected Aid");
                            t.assert(error.is_some(), "Expected error");
                            t.assert(
                                error.as_ref().unwrap() == "I panicked",
                                "Error message does not match",
                            );
                            future::ok(Status::stop(state))
                        }
                        SystemMsg::Start => future::ok(Status::done(state)),
                        _ => t.panic("Unexpected message received!"),
                    }
                } else {
                    t.panic("Unexpected message received!")
                }
            })
            .unwrap();
        system.monitor(&monitor, &aid);
        aid.send_new(()).unwrap();
        await_received(&monitor, 2, 1000).unwrap();
        system.trigger_and_await_shutdown(Duration::from_millis(1000));
        tracker.collect();
    }

    /// This test verifies that the concept of named actors works properly. When a user wants
    /// to declare a named actor they cannot register the same name twice. When the actor using
    /// the name currently stops the name should be removed from the registered names and be
    /// available again.
    #[test]
    fn test_named_actor_restrictions() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        let aid1 = system.spawn().name("A").with((), simple_handler).unwrap();
        await_received(&aid1, 1, 1000).unwrap();

        let aid2 = system.spawn().name("B").with((), simple_handler).unwrap();
        await_received(&aid2, 1, 1000).unwrap();

        // Spawn an actor that attempts to overwrite "A" in the names and make sure the
        // attempt returns an error to be handled.
        let result = system.spawn().name("A").with((), simple_handler);
        assert_eq!(Err(SystemError::NameAlreadyUsed("A".to_string())), result);

        // Verify that the same actor has "A" name and is still up.
        let found1 = system.find_aid_by_name("A").unwrap();
        assert_eq!(true, system.is_actor_alive(&aid1));
        assert!(Aid::ptr_eq(&aid1, &found1));

        // Stop "B" and verify that the ActorSystem's maps are cleaned up.
        system.stop_actor(&aid2);
        assert_eq!(None, system.find_aid_by_name("B"));
        assert_eq!(None, system.find_aid_by_uuid(&aid2.uuid()));

        // Now we should be able to crate a new actor with the name "B".
        let aid3 = system.spawn().name("B").with((), simple_handler).unwrap();
        await_received(&aid3, 1, 1000).unwrap();
        let found2 = system.find_aid_by_name("B").unwrap();
        assert!(Aid::ptr_eq(&aid3, &found2));

        system.trigger_and_await_shutdown(None);
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
        let tracker = AssertCollect::new();
        let t = tracker.clone();
        let (system1, system2) = start_and_connect_two_systems();

        system1.init_current();
        let aid = system1
            .spawn()
            .with((), move |_: (), context: Context, message: Message| {
                let t = t.clone();
                async move {
                    if let Some(msg) = message.content_as::<Request>() {
                        msg.reply_to.send_new(Reply {}).unwrap();
                        context.system.trigger_shutdown();
                        Ok(Status::stop(()))
                    } else if let Some(_) = message.content_as::<SystemMsg>() {
                        Ok(Status::done(()))
                    } else {
                        t.panic("Unexpected message received!")
                    }
                }
            })
            .unwrap();
        await_received(&aid, 1, 1000).unwrap();

        let t = tracker.clone();
        let serialized = bincode::serialize(&aid).unwrap();
        system2
            .spawn()
            .with((), move |_: (), context: Context, message: Message| {
                if let Some(_) = message.content_as::<Reply>() {
                    debug!("Received reply, shutting down");
                    context.system.trigger_shutdown();
                    future::ok(Status::stop(()))
                } else if let Some(msg) = message.content_as::<SystemMsg>() {
                    match &*msg {
                        SystemMsg::Start => {
                            debug!("Starting request actor");
                            let target_aid: Aid = bincode::deserialize(&serialized).unwrap();
                            target_aid
                                .send_new(Request {
                                    reply_to: context.aid.clone(),
                                })
                                .unwrap();
                            future::ok(Status::done(()))
                        }
                        _ => future::ok(Status::done(())),
                    }
                } else {
                    t.panic("Unexpected message received!")
                }
            })
            .unwrap();

        await_two_system_shutdown(system1, system2);
        tracker.collect();
    }

    /// Tests the ability to find an aid on a remote system by name using a `SystemActor`. This
    /// also serves as a test for cross system actor communication as well as testing broadcast
    /// to multiple system actors in the cluster.
    #[test]
    fn test_system_actor_find_by_name() {
        init_test_log();
        let tracker = AssertCollect::new();
        let t = tracker.clone();
        let (system1, system2) = start_and_connect_two_systems();

        let aid1 = system1
            .spawn()
            .name("A")
            .with((), |_: (), context: Context, message: Message| async move {
                if let Some(_) = message.content_as::<bool>() {
                    context.system.trigger_shutdown();
                    Ok(Status::stop(()))
                } else {
                    Ok(Status::done(()))
                }
            })
            .unwrap();
        await_received(&aid1, 1, 1000).unwrap();

        system2
            .spawn()
            .with((), move |_: (), context: Context, message: Message| {
                // We have to do this so each async block future gets its own copy.
                let aid1 = aid1.clone();
                let t = t.clone();
                async move {
                    if let Some(msg) = message.content_as::<SystemActorMessage>() {
                        match &*msg {
                            SystemActorMessage::FindByNameResult { aid: found, .. } => {
                                debug!("FindByNameResult received");
                                if let Some(target) = found {
                                    t.assert(
                                        target.uuid() == aid1.uuid(),
                                        "Target is not expected Actor",
                                    );
                                    target.send_new(true).unwrap();
                                    context.system.trigger_shutdown();
                                    Ok(Status::done(()))
                                } else {
                                    t.panic("Didn't find AID.")
                                }
                            }
                            _ => t.panic("Unexpected message received!"),
                        }
                    } else if let Some(msg) = message.content_as::<SystemMsg>() {
                        debug!("Actor started, attempting to send FindByName request");
                        if let SystemMsg::Start = &*msg {
                            context.system.send_to_system_actors(Message::new(
                                SystemActorMessage::FindByName {
                                    reply_to: context.aid.clone(),
                                    name: "A".to_string(),
                                },
                            ));
                            Ok(Status::done(()))
                        } else {
                            t.panic("Unexpected message received!")
                        }
                    } else {
                        t.panic("Unexpected message received!")
                    }
                }
            })
            .unwrap();

        await_two_system_shutdown(system1, system2);
        tracker.collect();
    }
}
