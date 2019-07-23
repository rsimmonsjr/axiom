//! Implements actors and the actor system which are the core of Axiom.
//!
//! These are the core components that make up the features of Axiom. The actor model is designed
//! to allow the user maximum flexibility. It makes use of [`axiom::secc`] as a channel for
//! messages to the actor as well as for the work queue for the worker threads. The actors
//! can skip messages if they choose, enabling them to work as a *finite state machine* without
//! having to move messages around as would be necessary with Akka's `stash` implementation.
//! When the actor system starts up, a number of worker threads will be spawned that will
//! constantly try to pull work from the work channel and process messages with the actor. The
//! actor will then be re-sent to the work channel if there are more messages for that actor to
//! process. This continues constantly until the actor system is shutdown and all actors are
//! stopped.
//!
//! The user should refer to test cases for examples and "how-to" guides for using the
//! actor system.

use crate::secc;
use crate::secc::*;
use log::{error, warn};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use uuid::Uuid;

/// A type used by Axiom actors for sending a message through a channel to an actor.
///
/// All messages are sent as this type and it is up to the message handler to cast the message
/// properly and deal with it. The message is required to be both [`std::marker::Sync`] and
/// [`std::marker::Send`] because there is no way to predict what worker thread will be
/// processing the message.
pub type Message = dyn Any + Sync + Send;

/// Status of the message and potentially the actor as a resulting from processing a message
/// with the actor.
#[derive(Debug, Eq, PartialEq)]
pub enum Status {
    /// The message was processed and can be removed from the channel. Note that this doesn't
    /// necessarily mean that anything was done with the message, just that it can be removed.  
    /// It is up to the actor to decide what, if anything, to do with the message.
    Processed,

    /// The message was skipped and should remain in the queue. Once a message is skipped then
    /// a skip cursor will be created in the actor's message channel that will act as the actual
    /// head of the channel until an [`axiom::actors::Status::ResetSkip`] is returned from an
    /// actor's processor. This enables an actor to skip messages while working on a process and
    /// then clear the skip buffer and resume normal processing. This functionality is critical
    /// for actors that act as a finite state machine and thus might temporarily change the
    /// implementation of the processor and then switch back to a state where the previously
    /// sent messages are delivered.
    Skipped,

    /// Clears the skip tail on the channel. A skip cursor is present when a message has been
    /// skipped by an actor returning [`axiom::actors::Status::Skipped`] from a processor call.
    /// If no skip cursor is set than this status is semantically the same as
    /// [`axiom::actors::Status::Processed`].
    ResetSkip,

    /// Returned from an actor when the actor wants the system to stop the actor. When this
    /// status is returned the actor's [`axiom:actors::ActorId`] will no longer send any
    /// messages and the actor instance itself will be removed from the actors table in the
    /// [`axiom::actors::ActorSystem`]. The user is advised to do any cleanup needed before
    /// returning [`axiom::actors::Status::Stop`].
    Stop,
}

/// An enum containing messages that are sent to actors by the actor system itself and are
/// universal to all actors.
pub enum SystemMsg {
    /// A message that instructs an actor to shut down. The actor receiving this message
    /// should shut down all open file handles and any other resources and return a
    /// [`axiom::actors::Status::Stop`] from the process call. This is an attempt for the
    /// caller to shut down the actor nicely rather than just calling
    /// [`axiom::actors::ActorSystem::stop`] which would force a brutal stop.
    Stop,
}

/// Errors returned from actors and other parts of the actor system.
#[derive(Debug, Eq, PartialEq)]
pub enum ActorError {
    /// Error sent when attempting to send to an actor that has already been stopped. A stopped
    /// actor cannot accept any more messages and is shut down. The holder of an
    /// [`axiom::actors::ActorId`] to a stopped actor should throw away the
    /// [`axiom::actors::ActorId`] as the actor can never be started again.
    ActorStopped,

    /// An error returned when an actor is already using a local name but the user tries to
    /// register that name for a new actor. The error contains the name that was attempted
    /// to be registered.
    NameAlreadyUsed(String),

    /// Error Used for when an attempt is made to send a message to a remote actor. **This
    /// error will be removed when remote actors are implemented.**
    RemoteNotImplemented,
}

/// An enum that holds a sender for an actor.
///
/// An [`axiom::actors::ActorId`] uses the sender to send messages to the destination actor.
/// Messages that are sent locally  are sent by reference, sharing the memory. Those that are
/// sent to another node are sent via serializing the message to the remote system which will
/// then use an internal local sender to relay the message to the other node. Note that for
/// the purposes of Axiom, any actor system in another process is considered remote even if they
/// share the same hardware or even operating system. For example, if you start two instances of
/// an Axiom system on the same machine, the two instances are considered remote.
///
/// FIXME Made public to shut up warnings, make private when remote actors are implemented.
pub enum ActorSender {
    /// A sender used for sending messages to local actors.
    Local(SeccSender<Arc<Message>>),
    // FIXME (Issue #9) Implement remote actors.
    Remote,
}

impl fmt::Debug for ActorSender {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "{}",
            match *self {
                ActorSender::Local(_) => "ActorSender::Local",
                ActorSender::Remote => "ActorSender::Remote",
            }
        )
    }
}
/// Encapsulates an ID to an actor and is often referred to as an `aid`.
///
/// This is a unique reference to the actor within the entire cluster and can be used to send
/// messages to the actor regardless of location. The [`axiom::actors::ActorId`] does the heavy
/// lifting of deciding where the actor is and sending the message. However it is important that
/// the user at least has some notion of where the actor is for developing an efficient actor
/// architecture. This id can also be serialized to a remote system transparently, enabling one
/// actor to send an id to another actor easily.
pub struct ActorId {
    /// The unique id for this actor within the entire cluster.
    pub uuid: Uuid,
    /// The unique uuid for the node that this actor is on locally. This is where the actor
    /// actually lives but the user shouldn't need to worry about this under most circumstances
    /// because messages can be sent transparently across remote boundaries.
    pub node_uuid: Uuid,
    /// The name of the actor as assigned by the user at spawn time if any. Note that this name
    /// is local only, no guarantees are made that the name will be unique within the cluster.
    pub name: Option<String>,
    /// The handle to the sender side for the actor's message channel.
    sender: ActorSender,
    /// Holds a reference to the local actor system that the actor id lives at.
    system: Arc<ActorSystem>,
    /// Holds a boolean to indicate if the actor is stopped. A stopped actor will no longer
    /// accept further messages to be sent.
    is_stopped: AtomicBool,
}

impl ActorId {
    /// A helper to invoke [`axiom::actors::ActorId::try_send`] and simply panic if an error
    /// occurs.
    ///
    /// This should be used where the user doesn't expect an error to happen.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    ///
    /// let aid = ActorSystem::spawn(&system,
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, _message: &Arc<Message>| Status::Processed,
    ///  );
    ///
    /// ActorId::send(&aid, Arc::new(11));
    /// ```
    pub fn send(aid: &Arc<ActorId>, message: Arc<Message>) {
        match ActorId::try_send(aid, message) {
            Ok(_) => (),
            Err(e) => panic!("Error occurred sending to aid: {:?}", e),
        }
    }

    /// Attempts to send a message to the actor with the given [`axiom::actors::Arc<ActorId>`]
    /// and returns [`std::Result::Ok`] when the send was successful or
    /// [`std::Result::Err<ActorError>`] error if something went wrong with the send.
    ///
    /// This is useful when the user wants to send and feels that there is a possibility of an
    /// error and that possibility has to be handled gracefully.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    ///
    /// let aid = ActorSystem::spawn(&system,
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| Status::Processed,
    ///  );
    ///
    /// match ActorId::try_send(&aid, Arc::new(11)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    /// ```
    pub fn try_send(aid: &Arc<ActorId>, message: Arc<Message>) -> Result<(), ActorError> {
        if aid.is_stopped.load(Ordering::Relaxed) {
            Err(ActorError::ActorStopped)
        } else {
            match &aid.sender {
                ActorSender::Local(sender) => {
                    let receivable = sender.receivable();
                    sender.send_await(message).unwrap();
                    // The worst that happens here is the actor gets sent to the work channel
                    // more than once if several callers record receivable as 0 and then
                    // send the actor id. This is preferable to missing a message.
                    if receivable == 0 {
                        ActorSystem::schedule(aid.clone())
                    };

                    Ok(())
                }
                _ => Err(ActorError::RemoteNotImplemented),
            }
        }
    }

    /// Returns the total number of messages that have been sent to the actor regardless of
    /// whether or not they have been received or processed by the actor.
    pub fn sent(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.sent(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that have been received by the actor. Note that
    /// this doesn't mean that the actor did anything with the message, just that it was
    /// received and handled.
    pub fn received(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.received(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the number of messages that are receivable by the actor. This will not include
    /// any messages that have been skipped until the skip is reset.
    pub fn receivable(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.receivable(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that are pending to be received by the actor. This
    /// should include messages that have been skipped by the actor as well as those that
    /// are receivable.
    pub fn pending(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.pending(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Checks to see if the actor referenced by this [`axiom::actors::ActorId`] is actually
    /// stopped already.
    pub fn is_stopped(&self) -> bool {
        self.is_stopped.load(Ordering::AcqRel)
    }

    /// Marks the actor referenced by the [`axiom::actors::ActorId`] as stopped and puts
    /// mechanisms in place to cause no more messages to be sent to the actor. Note that once
    /// stopped, an actor id can never be started again.
    fn stop(&self) {
        self.is_stopped.fetch_or(true, Ordering::AcqRel);
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ActorId{{id: {}, node_id: {}, is_local: {}}}",
            self.uuid.to_string(),
            self.node_uuid.to_string(),
            // FIXME Fix when remote actors are introduced.
            if let ActorSender::Local(_) = self.sender {
                true
            } else {
                false
            }
        )
    }
}

impl PartialEq for ActorId {
    fn eq(&self, other: &ActorId) -> bool {
        self.uuid == other.uuid && self.node_uuid == other.node_uuid
    }
}

impl Eq for ActorId {}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &'_ mut H) {
        self.uuid.hash(state);
        self.node_uuid.hash(state);
    }
}

/// A type for a user function that processes messages for an actor.
///
/// This will be passed to a spawn function to specify the handler used for managing the state of
/// the actor based on the messages passed to the actor. The processor takes three arguments:
/// * `state`   - A mutable reference to the current state of the actor.
/// * `aid`     - The [`axiom::actors::ActorId`] for this actor enclosed in an [`std::sync::Arc`]
///               to allow access to the actor system for spawning, sending to self and so on.  
/// * `message` - The current message to process in a reference to an [`std::sync::Arc`]. Note
///               that messages are often shared amongst actors (sent to several actors at once)
///               but their contents must be immutable to comply with the rules of an actor system.
pub trait Processor<State: Send + Sync>:
    (FnMut(&mut State, Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync
{
}

// Allows any function, static or closure, to be used as a processor.
impl<F, State> Processor<State> for F
where
    State: Send + Sync + 'static,
    F: (FnMut(&mut State, Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static,
{
}

/// This is the internal type for the handler that will manage the state for the actor using the
/// user-provided message processor.
trait Handler: (FnMut(Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static {}

impl<F> Handler for F where F: (FnMut(Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static
{}

/// An actual actor in the system. Please see overview and library documentation for more detail.
struct Actor {
    /// Id of the associated actor.
    aid: Arc<ActorId>,
    /// Receiver for the actor channel.
    receiver: SeccReceiver<Arc<Message>>,
    /// The function that processes messages that are sent to the actor wrapped in a closure to
    /// erase the state type that the actor is managing. Note that this is in a mutex because
    /// the handler itself is `FnMut` and we also don't want there to be any possibility of two
    /// threads calling the handler concurrently as that would break the actor model rules.
    handler: Mutex<Box<dyn Handler>>,
}

impl Actor {
    /// Creates a new actor on the given actor system with the given processor function. The
    /// user will pass the initial state of the actor as well as the processor that will be
    /// used to process messages sent to the actor. The system and node id are passed separately
    /// because of restrictions on mutex guards not being re-entrant in Rust.
    pub fn new<F, State>(
        system: Arc<ActorSystem>,
        node_id: Uuid,
        name: Option<String>,
        mut state: State,
        mut processor: F,
    ) -> Arc<Actor>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        // TODO Let the user pass the size of the channel queue when creating the actor.
        // Create the channel for the actor.
        let (sender, receiver) = secc::create::<Arc<Message>>(32, 10);

        // The sender will be put inside the actor id.
        let aid = ActorId {
            uuid: Uuid::new_v4(),
            node_uuid: node_id,
            name,
            sender: ActorSender::Local(sender),
            system: system.clone(),
            is_stopped: AtomicBool::new(false),
        };

        // This handler will manage the state for the actor.
        let handler = Box::new({
            move |aid: Arc<ActorId>, message: &Arc<Message>| processor(&mut state, aid, message)
        });

        // This is the receiving side of the actor which holds the processor wrapped in the
        // handler type.
        let actor = Actor {
            aid: Arc::new(aid),
            receiver,
            handler: Mutex::new(handler),
        };

        Arc::new(actor)
    }

    /// Receive a message from the channel and processes it with the actor.  This function is
    /// the core of the processing pipeline and what the thread pool will be calling to handle
    /// each message.
    fn receive(actor: Arc<Actor>) {
        match actor.receiver.peek() {
            Result::Err(err) => {
                // This happening should be very rare but it would mean that the thread pool
                // tried to process a message for an actor and was beaten to it by another
                // thread. In this case we will just ignore the error and write out a debug
                // message for purposes of later optimization.
                warn!("Error Occurred {:?}", err);
                ()
            }
            Result::Ok(message) => {
                // In this case there is a message in the channel that we have to process through
                // the actor.
                let mut guard = actor.handler.lock().unwrap();
                let result = (&mut *guard)(actor.aid.clone(), message);
                match result {
                    Status::Processed => {
                        match actor.receiver.pop() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel pop(): {:?}.", e);
                                actor.aid.system.stop(actor.aid.clone())
                            }
                        }
                        if actor.receiver.receivable() > 0 {
                            actor.aid.system.sender.send_await(actor.clone()).unwrap();
                        }
                    }
                    Status::Skipped => {
                        match actor.receiver.skip() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel skip(): {:?}.", e);
                                actor.aid.system.stop(actor.aid.clone())
                            }
                        }

                        if actor.receiver.receivable() > 0 {
                            actor.aid.system.sender.send_await(actor.clone()).unwrap();
                        }
                    }
                    Status::ResetSkip => {
                        match actor.receiver.reset_skip() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel reset_skip(): {:?}.", e);
                                actor.aid.system.stop(actor.aid.clone())
                            }
                        }
                        if actor.receiver.receivable() > 0 {
                            actor.aid.system.sender.send_await(actor.clone()).unwrap();
                        }
                    }
                    Status::Stop => {
                        actor.aid.system.stop(actor.aid.clone());
                        // Even though the actor is stopping we want to pop the message to make
                        // sure that the metrics on the actor's channel are correct. Then we will
                        // stop the actor in the actor system.
                        match actor.receiver.pop() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel pop(): {:?}.", e);
                                actor.aid.system.stop(actor.aid.clone())
                            }
                        }
                    }
                };
            }
        }
    }
}

/// Configuration structure for the Axiom actor system which is built using the builder
/// pattern that allows a user to override default values.
pub struct ActorSystemConfig {
    /// The number of slots to allocate for the work channel. This is the channel that the
    /// worker threads use to schedule work on actors.
    pub work_channel_size: u16,
    /// The size of the thread pool which governs how many worker threads there are in
    /// the system.
    pub thread_pool_size: u16,
    /// Amount of time to wait in milliseconds for between polling an empty work channel.
    pub thread_wait_time: u16,
}

impl ActorSystemConfig {
    /// Create the config with the default values.
    pub fn create() -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: 100,
            thread_pool_size: 4,
            thread_wait_time: 10,
        }
    }

    /// Create a new config object based on the one passed with the new value
    /// for `work_channel_size`.
    pub fn work_channel_size(&self, value: u16) -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: value,
            thread_pool_size: self.thread_pool_size,
            thread_wait_time: self.thread_wait_time,
        }
    }

    /// Create a new config object based on the one passed with the new value
    /// for `thread_pool_size`.
    pub fn thread_pool_size(&self, value: u16) -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: self.work_channel_size,
            thread_pool_size: value,
            thread_wait_time: self.thread_wait_time,
        }
    }

    /// Create a new config object based on the one passed with the new value
    /// for `thread_wait_time`.
    pub fn thread_wait_time(&self, value: u16) -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: self.work_channel_size,
            thread_pool_size: self.thread_pool_size,
            thread_wait_time: value,
        }
    }
}

/// An actor system that contains and manages the actors spawned inside it.
///
/// The actors live inside the actor system and are managed by the actor system throughout
/// their lives. Although it would be very unusual to have two actor systems on the same hardware
/// and OS process, it would be possible. Nevertheless, each actor system has a unique node id
/// that can be used by other actor systems to connect and send messages.
pub struct ActorSystem {
    /// Id for this actor system and node.
    pub node_id: Uuid,
    /// Sender side of the work channel. When an actor gets a message and its pending count
    /// goes from 0 to 1 it will put itself in the channel via the sender. The actor will be
    /// resent to the channel by a thread if it has more messages to process after handling a
    /// message.
    sender: Arc<SeccSender<Arc<Actor>>>,
    /// Receiver side of the work channel. All threads in the pool will be grabbing actors
    /// from this receiver to process messages.
    receiver: Arc<SeccReceiver<Arc<Actor>>>,
    /// Holds the [`axiom::actors::Actor`] objects keyed by the [`axiom::actors::ActorId`]. The
    /// [`std::sync::RwLock`] will be locked for write only when a new actor is spawned but
    /// otherwise will be locked for read by the [`axiom::actors::ActorId`] instances when they
    /// attempt to send an actor to the work channel for processing.
    actors_by_aid: Arc<RwLock<HashMap<Arc<ActorId>, Arc<Actor>>>>,
    /// Holds handles to the pool of threads processing the work channel.
    thread_pool: Mutex<Vec<JoinHandle<()>>>,
    /// A flag holding whether or not the system is currently shutting down.
    shutdown_triggered: AtomicBool,
    /// Duration for threads to wait for new messages before cycling and checking again. Defaults
    /// to 10 milliseconds.
    /// TODO Allow user to pass this as configuration.
    thread_wait_time: u16,
    /// Holds a map of the actor ids by the UUID in the actor id. UUIDs of actor ids are assigned
    /// when an actor is spawned and are cluster unique.
    aids_by_uuid: Arc<RwLock<HashMap<Uuid, Arc<ActorId>>>>,
    /// Holds a map of user assigned names to actor ids. The name is set when the actor is
    /// spawned and is not guaranteed to be unique in the cluster.
    aids_by_name: Arc<RwLock<HashMap<String, Arc<ActorId>>>>,
}

impl ActorSystem {
    /// Creates an actor system with the given size for the dispatcher channel and thread pool.  
    /// The user should benchmark how many slots in the work channel and the number of threads
    /// they need in order to satisfy the requirements of the system they are creating.
    pub fn create(config: ActorSystemConfig) -> Arc<ActorSystem> {
        // Amount of time to wait in ms for between polling an empty work channel.
        let (sender, receiver) =
            secc::create_with_arcs::<Arc<Actor>>(config.work_channel_size, config.thread_wait_time);

        // Creates the actor system with the thread pools and actor map initialized.
        let system = Arc::new(ActorSystem {
            node_id: Uuid::new_v4(),
            sender,
            receiver,
            actors_by_aid: Arc::new(RwLock::new(HashMap::new())),
            aids_by_uuid: Arc::new(RwLock::new(HashMap::new())),
            aids_by_name: Arc::new(RwLock::new(HashMap::new())),
            thread_pool: Mutex::new(Vec::with_capacity(config.thread_pool_size as usize)),
            shutdown_triggered: AtomicBool::new(false),
            thread_wait_time: config.thread_wait_time,
        });

        // We have the thread pool in a mutex to avoid a chicken & egg situation with the actor
        // system not being created but needed by the thread. We put this in a block to get
        // around rust borrow constraints without unnecessarily copying things.
        {
            let mut guard = system.thread_pool.lock().unwrap();
            for _ in 0..config.thread_pool_size {
                let thread = ActorSystem::start_dispatcher_thread(system.clone());
                guard.push(thread);
            }
        }

        system
    }

    /// Starts a thread for the dispatcher that will process actor messages. The dispatcher
    /// threads constantly grab at the work channel trying to get the next actor off the channel.
    /// When they get an actor they will process the message using the actor and then check to
    /// see if the actor has more receivable messages. If it does then the actor will be re-sent
    /// to the work channel to process the next message. This process allows thousands of actors
    /// to run and not take up resources if they have no messages to process but also prevents
    /// one super busy actor from starving out actors of that get messages only occasionally.
    fn start_dispatcher_thread(system: Arc<ActorSystem>) -> JoinHandle<()> {
        // FIXME Add metrics to this to log warnings if the messages take to long to process.
        // FIXME Add metrics to this to log a warning if messages or actors are spending too
        // long in the channel.
        let receiver = system.receiver.clone();

        thread::spawn(move || {
            while !system.shutdown_triggered.load(Ordering::Relaxed) {
                match receiver.receive_await_timeout(system.thread_wait_time) {
                    Err(_) => (), // not an error, just loop and try again.
                    Ok(actor) => Actor::receive(actor),
                }
            }
        })
    }

    /// Triggers a shutdown of the system and returns only when all threads have joined. This
    /// allows a graceful shutdown of the actor system but any messages pending in actor channels
    /// will get discarded.
    pub fn shutdown(system: Arc<ActorSystem>) {
        system.shutdown_triggered.store(true, Ordering::Relaxed);
        let mut guard = system.thread_pool.lock().unwrap();
        let vec = std::mem::replace(&mut *guard, vec![]);
        for handle in vec {
            handle.join().unwrap();
        }
    }

    /// Returns the total number of times actors have been sent to the work channel.
    pub fn sent(&self) -> usize {
        self.receiver.sent()
    }

    /// Returns the total number of times actors have been processed from the work channel.
    pub fn received(&self) -> usize {
        self.receiver.received()
    }

    /// Returns the total number of actors that are currently pending in the work channel.
    pub fn pending(&self) -> usize {
        self.receiver.pending()
    }

    // A internal helper to register an actor in the actor system.
    fn register_actor(&self, actor: Arc<Actor>) -> Result<Arc<ActorId>, ActorError> {
        let mut actors_by_aid = self.actors_by_aid.write().unwrap();
        let mut aids_by_uuid = self.aids_by_uuid.write().unwrap();
        let mut aids_by_name = self.aids_by_name.write().unwrap();
        let aid = actor.aid.clone();
        if let Some(name_string) = &aid.name {
            if aids_by_name.contains_key(name_string) {
                return Err(ActorError::NameAlreadyUsed(name_string.clone()));
            } else {
                aids_by_name.insert(name_string.clone(), aid.clone());
            }
        }
        aids_by_uuid.insert(aid.uuid, aid.clone());
        actors_by_aid.insert(aid.clone(), actor);
        Ok(aid)
    }

    /// Spawns a new unnamed actor on the `system` using the given `starting_state` for the actor
    /// and the given `processor` function that will be used to process actor messages.
    ///
    /// The returned [`ActorId`] can be used to send messages to the actor.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    ///
    /// let aid = ActorSystem::spawn(&system,
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| Status::Processed,
    /// );
    /// ```
    pub fn spawn<F, State>(system: &Arc<Self>, state: State, processor: F) -> Arc<ActorId>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        let actor = Actor::new(system.clone(), system.node_id, None, state, processor);
        system.register_actor(actor).unwrap()
    }

    /// Spawns a new named actor on the `system` using the given `starting_state` for the
    /// actor and the given `processor` function that will be used to process actor messages.
    /// If the `name` is already registered then this function will return an [`std::Result::Err`]
    /// with the value [`axiom::actors::ActorError::NameAlreadyUsed`] containing the name
    /// attempted to be registered.
    ///
    /// The returned [`ActorId`] can be used to send messages to the actor.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    ///
    /// let aid = ActorSystem::spawn_named(
    ///     &system,
    ///     "alpha",
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| Status::Processed,
    /// );
    /// ```
    pub fn spawn_named<F, State>(
        system: &Arc<Self>,
        name: &str,
        state: State,
        processor: F,
    ) -> Result<Arc<ActorId>, ActorError>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        let actor = Actor::new(
            system.clone(),
            system.node_id,
            Some(name.to_string()),
            state,
            processor,
        );
        system.register_actor(actor)
    }

    /// Schedules the `aid` for work on the given actor system. Note that this is the only time
    /// that we have to use the lookup table. This function gets called when an actor goes from
    /// 0 receivable messages to 1 receivable message. If the actor has more receivable messages
    /// then this will not be needed to be called because the dispatcher threads will handle
    /// the process of resending the actor to the work channel.
    fn schedule(aid: Arc<ActorId>) {
        // Note that this is implemented here rather than calling the sender directly
        // from the send in order to allow internal optimization of the actor system.
        let actors_by_aid = aid.system.actors_by_aid.read().unwrap();
        match actors_by_aid.get(&aid) {
            Some(actor) => aid
                .system
                .sender
                .send(actor.clone())
                .expect("Unable to Schedule actor: "),
            None => {
                // The actor was removed from the map so ignore the problem and just log
                // a warning.
                warn!(
                    "Attempted to schedule actor with aid {:?} but the actor does not exist.",
                    aid.clone()
                );
                ()
            }
        }
    }

    /// Stops an actor by shutting down its channels and removing it from the actors list and
    /// telling the actor id to not allow send to the actor since the receiving side of the
    /// actor is gone.
    ///
    /// This is something that should rarely be called from the outside as it is much better to
    /// send the actor a [`axiom::actors::SystemMsg::Shutdown`] message and allow it to shutdown
    /// gracefully.
    pub fn stop(&self, aid: Arc<ActorId>) {
        let mut actors_by_aid = self.actors_by_aid.write().unwrap();
        let mut aids_by_uuid = self.aids_by_uuid.write().unwrap();
        let mut aids_by_name = self.aids_by_name.write().unwrap();
        actors_by_aid.remove(&aid);
        aids_by_uuid.remove(&aid.uuid);
        if let Some(name_string) = &aid.name {
            aids_by_name.remove(name_string);
        }
        aid.stop();
    }

    /// Checks to see if the actor with the given [`axiom::actors::ActorId`] is alive within
    /// this actor system.
    pub fn is_alive(&self, aid: &Arc<ActorId>) -> bool {
        let actors_by_aid = self.actors_by_aid.write().unwrap();
        actors_by_aid.contains_key(aid)
    }

    /// Look up an [`axiom::actors::ActorId`] by the unique UUID of the actor and either returns
    /// the located `aid` in a [`std::Option::Some`] or [`std::Option::None`] if not found.
    pub fn find_aid_by_uuid(&self, uuid: &Uuid) -> Option<Arc<ActorId>> {
        let aids_by_uuid = self.aids_by_uuid.read().unwrap();
        aids_by_uuid.get(uuid).map(|aid| aid.clone())
    }

    /// Look up an [`axiom::actors::ActorId`] by the user assigned name of the actor and either
    /// returns the located `aid` in a [`std::Option::Some`] or [`std::Option::None`] if not
    /// found.
    pub fn find_aid_by_name(&self, name: &str) -> Option<Arc<ActorId>> {
        let aids_by_name = self.aids_by_name.read().unwrap();
        aids_by_name.get(&name.to_string()).map(|aid| aid.clone())
    }
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use std::time::Duration;

    /// A test helper to assert that a certain number of messages arrived in a certain time.
    fn assert_await_received(aid: &Arc<ActorId>, count: u8, timeout_ms: u64) {
        use std::time::Instant;
        let start = Instant::now();
        let duration = Duration::from_millis(timeout_ms);
        while aid.received() < count as usize {
            if Instant::elapsed(&start) > duration {
                assert!(
                    false,
                    "Timed out! count: {} timeout_ms: {}",
                    count, timeout_ms
                )
            }
        }
    }

    /// A function that just returns [`axiom::actors::Status::Processed`] which can be
    /// used as a handler for a simple actor.
    fn simple_handler(_state: &mut usize, _aid: Arc<ActorId>, _message: &Arc<Message>) -> Status {
        Status::Processed
    }

    #[test]
    fn test_simplest_actor() {
        init_test_log();

        // This test shows how the simplest actor can be built and used. This actor uses a closure
        // that simply returns that the message is processed.
        let system = ActorSystem::create(ActorSystemConfig::create());

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let aid = ActorSystem::spawn(
            &system,
            0 as usize,
            |_state: &mut usize, _aid: Arc<ActorId>, _message: &Arc<Message>| Status::Processed,
        );

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11));

        // Wait for the message to get there because test is asynchronous.
        assert_await_received(&aid, 1, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_simplest_struct_actor() {
        init_test_log();

        // This test shows how the simplest struct-based actor can be built and used. This actor
        // merely returns that the message was processed.
        let system = ActorSystem::create(ActorSystemConfig::create());

        // We declare a basic struct that has a handle method that does basically nothing.
        // Subsequently we will create that struct when we spawn the actor and then send the
        // actor a message.
        struct Data {}

        impl Data {
            fn handle(&mut self, _aid: Arc<ActorId>, _message: &Arc<Message>) -> Status {
                Status::Processed
            }
        }

        let aid = ActorSystem::spawn(&system, Data {}, Data::handle);

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11));

        // Wait for the message to get there because test is asynchronous.
        assert_await_received(&aid, 1, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_dispatching_with_closure() {
        init_test_log();

        // This test shows how a closure-based actor can be used and process different kinds of
        // messages and mutate its state based upon the messages passed. Note that the state of
        // the actor is not available outside the actor itself. There is no way to get access to
        // the state without going through the actor.
        let system = ActorSystem::create(ActorSystemConfig::create());

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let starting_state: usize = 0 as usize;
        let closure = |state: &mut usize, aid: Arc<ActorId>, message: &Arc<Message>| {
            // Expected messages in the expected order.
            let expected: Vec<i32> = vec![11, 13, 17];
            // Attempt to downcast to expected message.
            if let Some(msg) = message.downcast_ref::<i32>() {
                assert_eq!(expected[*state], *msg);
                assert_eq!(*state, aid.received());
                *state += 1;
                assert_eq!(aid.pending(), aid.sent() - aid.received());
                Status::Processed
            } else {
                assert!(false, "Failed to dispatch properly");
                Status::Processed // This assertion will fail but we still have to return.
            }
        };

        let aid = ActorSystem::spawn(&system, starting_state, closure);

        // Send some messages to the actor in the order required in the test. In a real actor
        // its unlikely any order restriction would be needed. However this test makes sure that
        // the messages are processed correctly.
        ActorId::send(&aid, Arc::new(11 as i32));
        assert_eq!(1, aid.sent());
        ActorId::send(&aid, Arc::new(13 as i32));
        assert_eq!(2, aid.sent());
        ActorId::send(&aid, Arc::new(17 as i32));
        assert_eq!(3, aid.sent());

        // Wait for all of the messages to get there because test is asynchronous.
        assert_await_received(&aid, 3, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_dispatching_with_struct() {
        init_test_log();

        // This test shows how a struct-based actor can be used and process different kinds of
        // messages and mutate its state based upon the messages passed. Note that the state of
        // the actor is not available outside the actor itself.
        let system = ActorSystem::create(ActorSystemConfig::create());

        // We create a basic struct with a handler and use that handler to dispatch to other
        // inherent methods in the struct. Note that we don't have to implement any traits here
        // and there is nothing forcing the handler to be an inherent method.
        struct Data {
            value: i32,
        }

        impl Data {
            fn handle_bool(&mut self, _aid: Arc<ActorId>, message: &bool) -> Status {
                if *message {
                    self.value += 1;
                } else {
                    self.value -= 1;
                }
                Status::Processed // This assertion will fail but we still have to return.
            }

            fn handle_i32(&mut self, _aid: Arc<ActorId>, message: &i32) -> Status {
                self.value += *message;
                Status::Processed // This assertion will fail but we still have to return.
            }

            fn handle(&mut self, aid: Arc<ActorId>, message: &Arc<Message>) -> Status {
                if let Some(msg) = message.downcast_ref::<bool>() {
                    self.handle_bool(aid, msg)
                } else if let Some(msg) = message.downcast_ref::<i32>() {
                    self.handle_i32(aid, msg)
                } else {
                    assert!(false, "Failed to dispatch properly");
                    Status::Stop // This assertion will fail but we still have to return.
                }
            }
        }

        let data = Data { value: 0 };

        let aid = ActorSystem::spawn(&system, data, Data::handle);

        // Send some messages to the actor.
        ActorId::send(&aid, Arc::new(11));
        ActorId::send(&aid, Arc::new(true));
        ActorId::send(&aid, Arc::new(true));
        ActorId::send(&aid, Arc::new(false));

        // Wait for all of the messages to get there because this test is asynchronous.
        assert_await_received(&aid, 4, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_actor_returns_stop() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::create());

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types
        // manually but when that bug goes away this will be even simpler.
        let aid = ActorSystem::spawn(
            &system,
            0 as usize,
            |state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| {
                if let Some(_msg) = message.downcast_ref::<i32>() {
                    assert_eq!(0 as usize, *state);
                    *state += 1;
                    Status::Processed
                } else if let Some(msg) = message.downcast_ref::<SystemMsg>() {
                    assert_eq!(1 as usize, *state);
                    *state += 1;
                    match msg {
                        SystemMsg::Stop => Status::Stop,
                    }
                } else {
                    assert!(false, "Failed to dispatch properly");
                    Status::Processed // The assertion will fail but we still have to return.
                }
            },
        );

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11 as i32));
        ActorId::send(&aid, Arc::new(SystemMsg::Stop));

        // Wait for the message to get there because test is asynchronous.
        assert_await_received(&aid, 2, 1000);

        // Make sure that the actor is actually stopped and cant get more messages.
        assert!(true, aid.is_stopped());
        match ActorId::try_send(&aid, Arc::new(42 as i32)) {
            Err(ActorError::ActorStopped) => assert!(true), // all OK!
            Ok(_) => assert!(false, "Expected the actor to be shut down!"),
            Err(e) => assert!(false, "Unexpected error: {:?}", e),
        }
        assert_eq!(false, system.is_alive(&aid));

        // Verify the actor is NOT in the maps
        let sys_clone = system.clone();
        let actors_by_aid = sys_clone.actors_by_aid.read().unwrap();
        assert_eq!(false, actors_by_aid.contains_key(&aid));
        let aids_by_uuid = sys_clone.aids_by_uuid.read().unwrap();
        assert_eq!(false, aids_by_uuid.contains_key(&aid.uuid));

        // Shut down the system and clean up test.
        ActorSystem::shutdown(system);
    }

    #[test]
    fn test_actor_force_stop() {
        init_test_log();

        // This test verifies that the system does not panic if we schedule to an actor
        // that does not exist in the map. This can happen if the actor is stopped before
        // the system notifies the actor id that it is dead.
        let system = ActorSystem::create(ActorSystemConfig::create());
        let aid = ActorSystem::spawn(&system, 0, simple_handler);
        ActorId::send(&aid, Arc::new(11));
        assert_await_received(&aid, 1, 1000);

        // Now we force stop the actor.
        system.stop(aid.clone());

        // Make sure the actor is out of the maps and cant be sent to.
        assert!(true, aid.is_stopped());
        match ActorId::try_send(&aid, Arc::new(42)) {
            Err(ActorError::ActorStopped) => assert!(true), // all OK!
            Ok(_) => assert!(false, "Expected the actor to be shut down!"),
            Err(e) => assert!(false, "Unexpected error: {:?}", e),
        }
        assert_eq!(false, system.is_alive(&aid));

        // Verify the actor is NOT in the maps
        let sys_clone = system.clone();
        let actors_by_aid = sys_clone.actors_by_aid.read().unwrap();
        assert_eq!(false, actors_by_aid.contains_key(&aid));
        let aids_by_uuid = sys_clone.aids_by_uuid.read().unwrap();
        assert_eq!(false, aids_by_uuid.contains_key(&aid.uuid));

        // Wait for the message to get there because test is asynchronous.
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_actor_not_in_map() {
        init_test_log();

        // This test verifies that the system does not panic if we schedule to an actor
        // that does not exist in the map. This can happen if the actor is stopped before
        // the system notifies the actor id that it is dead.
        let system = ActorSystem::create(ActorSystemConfig::create());
        let aid = ActorSystem::spawn(&system, 0, simple_handler);

        // We force remove the actor from the system so now it cannot be scheduled.
        let sys_clone = system.clone();
        let mut actors_by_aid = (*sys_clone.actors_by_aid).write().unwrap();
        actors_by_aid.remove(&aid);
        drop(actors_by_aid); // Give up the lock.

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11));

        // Wait for the message to get there because test is asynchronous.
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_find_by_uuid() {
        init_test_log();

        // This test checks that we can look up an actor by the UUID of the actor. Note that the
        // UUIDs for the actors are generated using the version 4 UUID so the chance of collision
        // in the cluster is not worth considering.
        let system = ActorSystem::create(ActorSystemConfig::create());
        let aid = ActorSystem::spawn(&system, 0 as usize, simple_handler);

        // Send a message to the actor verifying it is up.
        ActorId::send(&aid, Arc::new(11));
        assert_await_received(&aid, 1, 1000);
        let found: &Arc<ActorId> = &system.find_aid_by_uuid(&aid.uuid).unwrap();
        assert!(Arc::ptr_eq(&aid, found));

        // Stop the actor and it should be out of the map.
        system.stop(aid.clone());
        assert_eq!(None, system.find_aid_by_uuid(&aid.uuid));

        // Verify attempting to find with unregistered name returns none.
        assert_eq!(None, system.find_aid_by_uuid(&Uuid::new_v4()));

        // Wait for the message to get there because test is asynchronous.
        ActorSystem::shutdown(system);
    }

    #[test]
    fn test_named_actors() {
        init_test_log();

        // This test checks that we can look up an actor by the UUID of the actor. Note that the
        // UUIDs for the actors are generated using the version 4 UUID so the chance of collision
        // in the cluster is not worth considering.
        let system = ActorSystem::create(ActorSystemConfig::create());

        let aid1 = ActorSystem::spawn_named(&system, "alpha", 0 as usize, simple_handler).unwrap();
        ActorId::send(&aid1, Arc::new(11));
        assert_await_received(&aid1, 1, 1000);
        let found1: &Arc<ActorId> = &system.find_aid_by_name("alpha").unwrap();
        assert!(Arc::ptr_eq(&aid1, found1));

        let aid2 = ActorSystem::spawn_named(&system, "beta", 0 as usize, simple_handler).unwrap();
        ActorId::send(&aid2, Arc::new(11));
        assert_await_received(&aid2, 1, 1000);
        let found2: &Arc<ActorId> = &system.find_aid_by_name("beta").unwrap();
        assert!(Arc::ptr_eq(&aid2, found2));

        // Spawn an actor to overwrite "alpha" in the names and make sure it did.
        let result = ActorSystem::spawn_named(&system, "alpha", 0 as usize, simple_handler);
        assert_eq!(
            Err(ActorError::NameAlreadyUsed("alpha".to_string())),
            result
        );

        // The same actor has "alpha" name and is still up.
        let found3: &Arc<ActorId> = &system.find_aid_by_name("alpha").unwrap();
        assert!(Arc::ptr_eq(&aid1, found3));
        ActorId::send(&aid1, Arc::new(11));
        assert_await_received(&aid1, 2, 1000);

        // Verify attempting to find with unregistered name returns none.
        assert_eq!(None, system.find_aid_by_name("charlie"));

        // Stop "beta" and they should and it should be out of the map.
        system.stop(aid2.clone());
        assert_eq!(None, system.find_aid_by_name("beta"));
        assert_eq!(None, system.find_aid_by_uuid(&aid2.uuid));

        // Now we should be able to crate a new actor with the name beta.
        let aid3 = ActorSystem::spawn_named(&system, "beta", 0 as usize, simple_handler).unwrap();
        ActorId::send(&aid3, Arc::new(11));
        assert_await_received(&aid3, 1, 1000);
        let found4: &Arc<ActorId> = &system.find_aid_by_name("beta").unwrap();
        assert!(Arc::ptr_eq(&aid3, found4));

        // Wait for the message to get there because test is asynchronous.
        ActorSystem::shutdown(system);
    }

    // ---------- Complete Example ----------

    #[derive(Debug)]
    enum Operation {
        Inc,
        Dec,
    }

    #[derive(Debug)]
    struct StructActor {
        count: usize,
    }

    impl StructActor {
        fn handle_op(&mut self, aid: Arc<ActorId>, msg: &Operation) -> Status {
            match msg {
                Operation::Inc => {
                    assert_eq!(0, aid.received());
                    self.count += 1;
                    assert_eq!(6 as usize, self.count);
                }
                Operation::Dec => {
                    assert_eq!(1, aid.received());
                    self.count -= 1;
                    assert_eq!(5 as usize, self.count);
                }
            }
            Status::Processed
        }

        fn handle_i32(&mut self, aid: Arc<ActorId>, msg: &i32) -> Status {
            assert_eq!(2, aid.received());
            assert_eq!(17 as i32, *msg);
            self.count += *msg as usize;
            assert_eq!(22 as usize, self.count);
            Status::Processed
        }

        fn handle(&mut self, aid: Arc<ActorId>, message: &Arc<Message>) -> Status {
            if let Some(msg) = message.downcast_ref::<i32>() {
                self.handle_i32(aid, msg)
            } else if let Some(msg) = message.downcast_ref::<Operation>() {
                self.handle_op(aid, msg)
            } else if let Some(msg) = message.downcast_ref::<u8>() {
                assert_eq!(3, aid.received());
                assert_eq!(7 as u8, *msg);
                self.count += *msg as usize;
                assert_eq!(29 as usize, self.count);
                Status::Processed
            } else {
                assert!(false, "Failed to dispatch properly");
                Status::Processed // This assertion will fail but we still have to return.
            }
        }
    }

    #[test]
    fn test_full_example() {
        init_test_log();

        // This test uses the actor struct declared above to demonstrate and test most of the
        // capabilities of actors. This is a fairly complete example.
        let system = ActorSystem::create(ActorSystemConfig::create());
        let starting_state: StructActor = StructActor { count: 5 as usize };

        let aid = ActorSystem::spawn(&system, starting_state, StructActor::handle);

        ActorId::send(&aid, Arc::new(Operation::Inc));
        assert_eq!(1, aid.sent());
        ActorId::send(&aid, Arc::new(Operation::Dec));
        assert_eq!(2, aid.sent());
        ActorId::send(&aid, Arc::new(17 as i32));
        assert_eq!(3, aid.sent());
        ActorId::send(&aid, Arc::new(7 as u8));
        assert_eq!(4, aid.sent());

        // Wait for the message to get there because test is asynch.
        assert_await_received(&aid, 4, 1000);
        ActorSystem::shutdown(system)
    }

}
