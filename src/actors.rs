//! Implements actors and the actor system which are the core of Axiom.
//!
//! These are the core components that make up the features of Axiom. The actor model is designed
//! to allow the user maximum flexibility. It makes use of [`axiom::secc`] as a channel for
//! messages to the actor as well as for the work channel for the worker threads. The actors
//! can skip messages if they choose, enabling them to work as a *finite state machine* without
//! having to move messages around as would be necessary with Akka's `stash` implementation.
//! When the actor system starts up, a number of worker threads will be spawned that will
//! constantly try to pull work from the work channel and process messages with the actor. The
//! actor will then be re-sent to the work channel if there are more messages for that actor to
//! process. This continues constantly until the actor system is shutdown and all actors are
//! stopped.
//!
//! The user should refer to test cases for examples as "how-to" guides for using Axiom.

use crate::message::*;
use crate::secc;
use crate::secc::*;
use log::{error, warn};
use once_cell::sync::OnceCell;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use uuid::Uuid;

// This holds the actor system in a threadlocal so that the user can obtain a clone of it
// if needed at any time.
std::thread_local! {
    static ACTOR_SYSTEM: OnceCell<ActorSystem> = OnceCell::new();
}

/// Status of the message and potentially the actor as a resulting from processing a message
/// with the actor.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Status {
    /// The message was processed and can be removed from the channel. Note that this doesn't
    /// necessarily mean that anything was done with the message, just that it can be removed.  
    /// It is up to the actor to decide what, if anything, to do with the message.
    Processed,

    /// The message was skipped and should remain in the queue. Once a message is skipped a skip
    /// cursor will be created in the actor's message channel that will act as the actual head
    /// of the channel until an [`axiom::actors::Status::ResetSkip`] is returned from an
    /// actor's processor. This enables an actor to skip messages while working on a process and
    /// then clear the skip cursor and resume normal processing. This functionality is critical
    /// for actors that act as a finite state machine and thus might temporarily change the
    /// implementation of the processor and then switch back to a state where the previously
    /// sent messages are delivered.
    Skipped,

    /// Clears the skip cursor on the channel. A skip cursor is present when a message has been
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
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SystemMsg {
    /// A message that is sent by the system and guaranteed to be the first message that the
    /// actor receives in its lifetime.
    Start,

    /// A message that instructs an actor to shut down. The actor receiving this message
    /// should shut down all open file handles and any other resources and return a
    /// [`axiom::actors::Status::Stop`] from the process call.
    Stop,

    /// A message sent to an actor when an actor it is monitoring is stopped and thus not
    /// able to process additional messages. The enum contains the `aid` of the actor that
    /// stopped.
    Stopped(ActorId),
}

/// Errors returned from actors and other parts of the actor system.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ActorError {
    /// Error sent when attempting to send to an actor that has already been stopped. A stopped
    /// actor cannot accept any more messages and is shut down. The holder of an
    /// [`axiom::actors::ActorId`] to a stopped actor should throw away the
    /// [`axiom::actors::ActorId`] as the actor can never be started again.
    ActorStopped,

    /// An error returned when an actor is already using a local name at the time the user tries
    /// to register that name for a new actor. The error contains the name that was attempted
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
/// then use an internal local sender to relay the message on the other node. Note that for
/// the purposes of Axiom, any actor system in another process is considered remote even if they
/// share the same hardware or even operating system. For example, if you start two instances of
/// an Axiom system on the same machine, the two instances are considered remote.
enum ActorSender {
    /// A sender used for sending messages to local actors.
    /// FIXME (Issue #43) The actor should actually live here instead of the hashtable. This would simplify
    /// dispatching greatly.
    Local(SeccSender<Message>),

    /// A sender that is used when an actor is remote to this actor system. The remote
    /// will use networking to send messages to the actor. Note that this doesnt care if the
    /// actor is actually on a different physical node. Two actors systems in even the same
    /// process are remote to each other.
    ///
    /// FIXME (Issue #9) Implement remote actors.
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

/// The inner data of an [`ActorId`].
///
/// This is kept separate to make serialization possible without duplicating all of the data
/// associated with the [`ActorId`]. This also makes the API a bit more friendly for users
/// because they dont have to worry about passing `ActorId` all over.
struct ActorIdData {
    /// See [`ActorId::uuid()`]
    uuid: Uuid,
    /// See [`ActorId::node_uuid()`]
    node_uuid: Uuid,
    /// See [`ActorId::name()`]
    name: Option<String>,
    /// Holds a boolean to indicate if the actor is stopped. A stopped actor will no longer
    /// accept further messages to be sent.
    /// TODO is_stopped should be in local sender only.
    is_stopped: AtomicBool,
    /// The handle to the sender side for the actor's message channel.
    sender: ActorSender,
}

// A helper type to make serialization cleaner. This contains the serialized form of the
// [`ActorId`].
#[derive(Serialize, Deserialize)]
struct ActorIdSerializedForm {
    uuid: Uuid,
    node_uuid: Uuid,
    name: Option<String>,
}

/// Encapsulates an ID to an actor and is often referred to as an `aid`.
///
/// This is a unique reference to the actor within the entire cluster and can be used to send
/// messages to the actor regardless of location. The [`axiom::actors::ActorId`] does the heavy
/// lifting of deciding where the actor is and sending the message. However it is important that
/// the user at least has some notion of where the actor is for developing an efficient actor
/// architecture. This id can also be serialized to a remote system transparently, enabling one
/// actor to send an id to another actor easily.
#[derive(Clone)]
pub struct ActorId {
    /// Holds the Actual data for the actor id.
    data: Arc<ActorIdData>,
}

impl Serialize for ActorId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let serialized_form = ActorIdSerializedForm {
            uuid: self.uuid(),
            node_uuid: self.node_uuid(),
            name: self.name(),
        };
        serialized_form.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ActorId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized_form = ActorIdSerializedForm::deserialize(deserializer)?;

        let system = ActorSystem::current();
        // We will look up the AID in the table to return to the user if it
        // exists otherwise it must be a remote AID.
        match system.find_aid_by_uuid(&serialized_form.uuid) {
            Some(aid) => Ok(aid.clone()),
            None => Ok(ActorId {
                data: Arc::new(ActorIdData {
                    uuid: serialized_form.uuid,
                    node_uuid: serialized_form.node_uuid,
                    name: serialized_form.name,
                    sender: ActorSender::Remote,
                    // TODO This should be inside the local sender.
                    is_stopped: AtomicBool::new(false),
                }),
            }),
        }
    }
}

impl ActorId {
    /// A helper to invoke [`axiom::actors::ActorId::try_send`] and simply panic if an error
    /// occurs. Note that if there is an error that is possible in the send, the user should
    /// call `try_send` instead and handle the error. Panics in Axiom have the capability of
    /// taking down the whole actor system and process. The standard practices of Rust are
    /// different than Erlang or other actor systems. In Rust the user is expected to handle
    /// and manage potential errors. For this reason an intentional decision was made to allow
    /// panics to take out the process. Rust developers, suspecting an error, should design
    /// around this.
    ///
    /// This should be used where the user doesn't expect an error to happen.
    ///
    /// FIXME Update examples before pushing release.
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use axiom::message::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    /// system.init_current();
    ///
    /// let aid = system.spawn(
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: ActorId, _message: &Message| Status::Processed,
    ///  );
    ///
    /// aid.send(Message::new(11));
    /// ```
    pub fn send(&self, message: Message) {
        match self.try_send(message) {
            Ok(_) => (),
            Err(e) => panic!("Error occurred sending to aid: {:?}", e),
        }
    }

    /// Attempts to send a message to the actor with the given [`axiom::actors::ActorId`]
    /// and returns [`std::Result::Ok`] when the send was successful or
    /// [`std::Result::Err<ActorError>`] error if something went wrong with the send.
    ///
    /// This is useful when the user wants to send a message and feels that there is a
    /// possibility of an error and that possibility has to be handled gracefully. Note that
    /// as with `send` if a user just calls `try_send` and unwraps, a panic could take down the
    /// whole process. See the documentation on `send` for a longer explanation.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use axiom::message::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    /// system.init_current();
    ///
    /// let aid = system.spawn(
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: ActorId, message: &Message| Status::Processed,
    ///  );
    ///
    /// match aid.try_send(Message::new(11)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    /// ```
    pub fn try_send(&self, message: Message) -> Result<(), ActorError> {
        if self.data.is_stopped.load(Ordering::Relaxed) {
            Err(ActorError::ActorStopped)
        } else {
            match &self.data.sender {
                ActorSender::Local(sender) => {
                    sender.send_await(message).unwrap();
                    // FIXME Investigate if this could race the dispatcher threads.
                    if sender.receivable() == 1 {
                        // Schedule on the actor system set as current for this thread.
                        ActorSystem::current().schedule(self.clone())
                    };

                    Ok(())
                }
                _ => Err(ActorError::RemoteNotImplemented),
            }
        }
    }

    /// The unique UUID for this actor within the entire cluster.
    /// FIXME Write a Test.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.data.uuid
    }

    /// The unique UUID for the node that this actor is on locally. This is where the actor
    /// actually lives but the user shouldn't need to worry about this under most circumstances
    /// because messages can be sent transparently across remote boundaries.
    /// FIXME Write a Test.
    #[inline]
    pub fn node_uuid(&self) -> Uuid {
        self.data.node_uuid
    }

    /// The name of the actor as assigned by the user at spawn time if any. Note that this name
    /// is local only, no guarantees are made that the name will be unique within the cluster.
    /// FIXME Write a Test.
    #[inline]
    pub fn name(&self) -> Option<String> {
        self.data.name.clone()
    }

    /// Determines if this actor is local to the current actor system on this node.
    /// FIXME Write a Test.
    #[inline]
    pub fn is_local(&self) -> bool {
        // FIXME Fix when remote actors are introduced.
        if let ActorSender::Local(_) = self.data.sender {
            true
        } else {
            false
        }
    }

    /// Returns the total number of messages that have been sent to the actor regardless of
    /// whether or not they have been received or processed by the actor.
    /// FIXME Move these metrics to be retreived by via a system message because this won't work remote.
    pub fn sent(&self) -> usize {
        match &self.data.sender {
            ActorSender::Local(sender) => sender.sent(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that have been received by the actor. Note that
    /// this doesn't mean that the actor did anything with the message, just that it was
    /// received and handled.
    /// FIXME Move these metrics to be retreived by via a system message because this won't work remote.
    pub fn received(&self) -> usize {
        match &self.data.sender {
            ActorSender::Local(sender) => sender.received(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the number of messages that are currently receivable by the actor. This count
    /// will not include any messages that have been skipped until the skip is reset.
    /// FIXME Move these metrics to be retreived by via a system message because this won't work remote.
    pub fn receivable(&self) -> usize {
        match &self.data.sender {
            ActorSender::Local(sender) => sender.receivable(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that are pending in the actor's channel. This
    /// should include messages that have been skipped by the actor as well as those that
    /// are receivable.
    /// FIXME Move these metrics to be retreived by via a system message because this won't work remote.
    pub fn pending(&self) -> usize {
        match &self.data.sender {
            ActorSender::Local(sender) => sender.pending(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Checks to see if the actor referenced by this [`axiom::actors::ActorId`] is stopped.
    /// Stopped actor ids are unable to send messages to the actor.
    /// FIXME Move these metrics to be retreived by via a system message because this won't work remote.
    pub fn is_stopped(&self) -> bool {
        self.data.is_stopped.load(Ordering::Relaxed)
    }

    /// Marks the actor referenced by the [`axiom::actors::ActorId`] as stopped and puts
    /// mechanisms in place to cause no more messages to be sent to the actor. Note that once
    /// stopped, an actor id can never be started again.
    fn stop(&self) {
        self.data.is_stopped.fetch_or(true, Ordering::AcqRel);
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ActorId{{id: {}, node_uuid: {}, name: {:?}, is_local: {}}}",
            self.data.uuid.to_string(),
            self.data.node_uuid.to_string(),
            self.data.name,
            self.is_local()
        )
    }
}

impl PartialEq for ActorId {
    fn eq(&self, other: &ActorId) -> bool {
        self.data.uuid == other.data.uuid && self.data.node_uuid == other.data.node_uuid
    }
}

impl Eq for ActorId {}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &'_ mut H) {
        self.data.uuid.hash(state);
        self.data.node_uuid.hash(state);
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
    (FnMut(&mut State, ActorId, &Message) -> Status) + Send + Sync
{
}

// Allows any function, static or closure, to be used as a processor.
impl<F, State> Processor<State> for F
where
    State: Send + Sync + 'static,
    F: (FnMut(&mut State, ActorId, &Message) -> Status) + Send + Sync + 'static,
{
}

/// This is the internal type for the handler that will manage the state for the actor using the
/// user-provided message processor.
trait Handler: (FnMut(ActorId, &Message) -> Status) + Send + Sync + 'static {}

impl<F> Handler for F where F: (FnMut(ActorId, &Message) -> Status) + Send + Sync + 'static {}

/// An actual actor in the system. Please see overview and library documentation for more detail.
struct Actor {
    /// Id of the associated actor.
    aid: ActorId,
    /// Receiver for the actor channel.
    receiver: SeccReceiver<Message>,
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
        node_uuid: Uuid,
        name: Option<String>,
        mut state: State,
        mut processor: F,
    ) -> Arc<Actor>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        // FIXME: Issue #33: Let the user pass the size of the channel queue when creating
        // the actor.  Create the channel for the actor.
        let (sender, receiver) = secc::create::<Message>(32, 10);

        // The sender will be put inside the actor id.
        let aid = ActorId {
            data: Arc::new(ActorIdData {
                uuid: Uuid::new_v4(),
                node_uuid,
                name,
                sender: ActorSender::Local(sender),
                is_stopped: AtomicBool::new(false),
            }),
        };

        // This handler will manage the state for the actor.
        let handler = Box::new({
            move |aid: ActorId, message: &Message| processor(&mut state, aid, message)
        });

        // This is the receiving side of the actor which holds the processor wrapped in the
        // handler type.
        // FIXME Get rid of the need to look up the actor object when enqueuing it for work!
        let actor = Actor {
            aid: aid.clone(),
            receiver,
            handler: Mutex::new(handler),
        };

        Arc::new(actor)
    }

    /// This method is called to finish up the procedure for processing a message
    fn post_message_process(actor: &Arc<Self>) {
        // We check to see if the actor still has pending messages and if so we re-schedule it
        // for work at the back of the work channel. This prevents actors that get tons of
        // messages from starving out actors that get few messages.
        // FIXME This should be converted to use a reductions system to process x number
        // of messages until a certain configurable time elapses to improve performance with
        // actors that get tons of super fast messages.
        if actor.receiver.receivable() > 0 {
            ActorSystem::current()
                .data
                .sender
                .send_await(actor.clone())
                .unwrap();
        }
    }

    /// Receive a message from the channel and processes it with the actor. This function is
    /// the core of the processing pipeline and what the thread pool will be calling to handle
    /// each message.
    fn receive(actor: Arc<Actor>) {
        match actor.receiver.peek() {
            Result::Err(err) => {
                // This happening should be very rare but it would mean that the thread pool
                // tried to process a message for an actor and was beaten to it by another
                // thread. In this case we will just ignore the error and write out a debug
                // message for purposes of later optimization.
                warn!("receive(): No Message to process: {:?}", err);
                ()
            }
            Result::Ok(message) => {
                // In this case there is a message in the channel that we have to process through
                // the actor. We process the message and then we may override the actor's returned
                // value if its a Stop message. This is not an error but an intentional act to
                // allow the user not to do anything special if they don't handle the Stop.
                let mut guard = actor.handler.lock().unwrap();
                let mut result = (&mut *guard)(actor.aid.clone(), message);
                if let Some(m) = message.content_as::<SystemMsg>() {
                    if let SystemMsg::Stop = *m {
                        // Stop the actor anyway.
                        result = Status::Stop
                    }
                };

                // Handle the result of the processing.
                match result {
                    Status::Processed => {
                        match actor.receiver.pop() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel pop(): {:?}.", e);
                                ActorSystem::current().stop(actor.aid.clone())
                            }
                        }
                        Actor::post_message_process(&actor);
                    }
                    Status::Skipped => {
                        match actor.receiver.skip() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel skip(): {:?}.", e);
                                ActorSystem::current().stop(actor.aid.clone())
                            }
                        }
                        Actor::post_message_process(&actor);
                    }
                    Status::ResetSkip => {
                        match actor.receiver.reset_skip() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel reset_skip(): {:?}.", e);
                                ActorSystem::current().stop(actor.aid.clone())
                            }
                        }
                        Actor::post_message_process(&actor);
                    }
                    Status::Stop => {
                        ActorSystem::current().stop(actor.aid.clone());
                        // Even though the actor is stopping we want to pop the message to make
                        // sure that the metrics on the actor's channel are correct. Then we will
                        // stop the actor in the actor system.
                        match actor.receiver.pop() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel pop(): {:?}.", e);
                                ActorSystem::current().stop(actor.aid.clone())
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
#[derive(Debug)]
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
    /// Create the configuration with the default values.
    pub fn create() -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: 100,
            thread_pool_size: 4,
            thread_wait_time: 10,
        }
    }

    /// Create a new configuration object based on the one passed with the new value
    /// for `work_channel_size`.
    pub fn work_channel_size(&self, value: u16) -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: value,
            thread_pool_size: self.thread_pool_size,
            thread_wait_time: self.thread_wait_time,
        }
    }

    /// Create a new configuration object based on the one passed with the new value
    /// for `thread_pool_size`.
    pub fn thread_pool_size(&self, value: u16) -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: self.work_channel_size,
            thread_pool_size: value,
            thread_wait_time: self.thread_wait_time,
        }
    }

    /// Create a new configuration object based on the one passed with the new value
    /// for `thread_wait_time`.
    pub fn thread_wait_time(&self, value: u16) -> ActorSystemConfig {
        ActorSystemConfig {
            work_channel_size: self.work_channel_size,
            thread_pool_size: self.thread_pool_size,
            thread_wait_time: value,
        }
    }
}

/// Contains the inner data used by the actor system.
struct ActorSystemData {
    /// Id for this actor system and node.
    node_uuid: Uuid,
    /// The configuration for the actor system which was passed to it when created.
    configuration: ActorSystemConfig,
    /// Sender side of the work channel. When an actor gets a message and its pending count
    /// goes from 0 to 1 it will put itself in the work channel via the sender. The actor will be
    /// resent to the channel by a thread after handling a message if it has more messages
    /// to process.
    sender: Arc<SeccSender<Arc<Actor>>>,
    /// Receiver side of the work channel. All threads in the pool will be grabbing actors
    /// from this receiver to process messages.
    receiver: Arc<SeccReceiver<Arc<Actor>>>,
    /// Holds handles to the pool of threads processing the work channel.
    thread_pool: Mutex<Vec<JoinHandle<()>>>,
    /// A flag holding whether or not the system is currently shutting down.
    shutdown_triggered: AtomicBool,
    // Stores the number of running threads with a Condvar that will be used to notify anyone
    // waiting on the condvar that all threads have exited.
    running_thread_count: Arc<(Mutex<u16>, Condvar)>,
    /// Holds the [`axiom::actors::Actor`] objects keyed by the [`axiom::actors::ActorId`]. The
    /// [`std::sync::RwLock`] will be locked for write only when a new actor is spawned but
    /// otherwise will be locked for read by the [`axiom::actors::ActorId`] instances when they
    /// attempt to send an actor to the work channel for processing.
    actors_by_aid: Arc<RwLock<HashMap<ActorId, Arc<Actor>>>>,
    /// Holds a map of the actor ids by the UUID in the actor id. UUIDs of actor ids are assigned
    /// when an actor is spawned using version 4 UUIDs.
    aids_by_uuid: Arc<RwLock<HashMap<Uuid, ActorId>>>,
    /// Holds a map of user assigned names to actor ids. The name is set when the actor is
    /// spawned.
    aids_by_name: Arc<RwLock<HashMap<String, ActorId>>>,
    /// Holds a map of monitors where the key is the `aid` of the actor being monitored and
    /// the value is a vector of `aid`s that are monitoring the actor.
    monitoring_by_monitored: Arc<RwLock<HashMap<ActorId, Vec<ActorId>>>>,
}

/// An actor system that contains and manages the actors spawned inside it.
///
/// The actors live inside the actor system and are managed by the actor system throughout
/// their lives. Although it would be very unusual to have two actor systems on the same hardware
/// and OS process, it would be possible. Nevertheless, each actor system has a unique node id
/// that can be used by other actor systems to connect and send messages.
#[derive(Clone)]
pub struct ActorSystem {
    /// This field means the user doesnt have to worry about declaring `Arc<ActorSystem>` all
    /// over the place but can just use `ActorSystem` instead. Wrapping the data also allows
    /// `&self` semantics on the methods which feels more natural.
    data: Arc<ActorSystemData>,
}

impl ActorSystem {
    /// Creates an actor system with the given configuration. The user should benchmark how
    /// many slots in the work channel, the number of threads they need and so on in order
    /// to satisfy the requirements of the system they are creating.
    pub fn create(config: ActorSystemConfig) -> ActorSystem {
        // Amount of time to wait in ms for between polling an empty work channel.
        let (sender, receiver) =
            secc::create_with_arcs::<Arc<Actor>>(config.work_channel_size, config.thread_wait_time);

        // Convert to usize for pre-allocating a vector.
        let thread_pool = Mutex::new(Vec::with_capacity(config.thread_pool_size as usize));
        let running_thread_count = Arc::new((Mutex::new(config.thread_pool_size), Condvar::new()));

        // Creates the actor system with the thread pools and actor map initialized.
        let system = ActorSystem {
            data: Arc::new(ActorSystemData {
                node_uuid: Uuid::new_v4(),
                configuration: config,
                sender,
                receiver,
                thread_pool,
                shutdown_triggered: AtomicBool::new(false),
                running_thread_count,
                actors_by_aid: Arc::new(RwLock::new(HashMap::new())),
                aids_by_uuid: Arc::new(RwLock::new(HashMap::new())),
                aids_by_name: Arc::new(RwLock::new(HashMap::new())),
                monitoring_by_monitored: Arc::new(RwLock::new(HashMap::new())),
            }),
        };

        // We have the thread pool in a mutex to avoid a chicken & egg situation with the actor
        // system not being created yet but needed by the thread. We put this code in a block to
        // get around rust borrow constraints without unnecessarily copying things.
        {
            let mut guard = system.data.thread_pool.lock().unwrap();
            for _ in 0..system.data.configuration.thread_pool_size {
                let thread = system.start_dispatcher_thread();
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
    /// one super busy actor from starving out other actors of that get messages only
    /// occasionally.
    fn start_dispatcher_thread(&self) -> JoinHandle<()> {
        // FIXME Issue #32: Add metrics to this to log warnings if the messages take to long to process.
        // FIXME Issue #32: Add metrics to this to log a warning if messages or actors are spending too
        // long in the channel.
        let system = self.clone();
        let receiver = self.data.receiver.clone();
        let thread_timeout = self.data.configuration.thread_wait_time;

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

    /// Initialises this actor system to use for the current thread which is necessary if the
    /// user wishes to call into the actor system from another thread. Note that this can be
    /// called only once.
    /// FIXME Use same technique as ActorId to enable self ergonomics.
    pub fn init_current(&self) {
        ACTOR_SYSTEM.with(|actor_system| {
            actor_system
                .set(self.clone())
                .expect("Unable to set ACTOR_SYSTEM.");
        });
    }

    /// Fetches a reference to the actor system for the current thread executing. The returned
    /// reference is a clone of the reference held internally.
    pub fn current() -> ActorSystem {
        ACTOR_SYSTEM.with(|actor_system| {
            actor_system
                .get()
                .expect("Thread local actor system not set!")
                .clone()
        })
    }

    /// Triggers a shutdown but doesnt wait for threads to stop. This function is non-blocking.
    pub fn trigger_shutdown(&self) {
        self.data.shutdown_triggered.store(true, Ordering::Relaxed);
    }

    /// Awaits for the actor system to be shutdown using a relatively CPU minimal condvar as
    /// a signalling mechanism. This function will block until all actor system threads have
    /// returned.
    pub fn await_shutdown(&self) {
        let &(ref mutex, ref condvar) = &*self.data.running_thread_count;
        let guard = mutex.lock().unwrap();
        let _condvar_guard = condvar.wait(guard).unwrap();
    }

    /// Triggers a shutdown of the system and returns only when all threads have joined.
    pub fn trigger_and_await_shutdown(&self) {
        self.trigger_shutdown();
        self.await_shutdown();
    }

    /// Returns the total number of times actors have been sent to the work channel.
    pub fn sent(&self) -> usize {
        self.data.receiver.sent()
    }

    /// Returns the total number of times actors have been processed from the work channel.
    pub fn received(&self) -> usize {
        self.data.receiver.received()
    }

    /// Returns the total number of actors that are currently pending in the work channel.
    pub fn pending(&self) -> usize {
        self.data.receiver.pending()
    }

    // A internal helper to register an actor in the actor system.
    fn register_actor(&self, actor: Arc<Actor>) -> Result<ActorId, ActorError> {
        let mut actors_by_aid = self.data.actors_by_aid.write().unwrap();
        let mut aids_by_uuid = self.data.aids_by_uuid.write().unwrap();
        let mut aids_by_name = self.data.aids_by_name.write().unwrap();
        let aid = actor.aid.clone();
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
    /// spawned actor will use default values for message channel size and other options and not
    /// be named.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use axiom::message::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    /// system.init_current();
    ///
    /// let aid = system.spawn(
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: ActorId, _message: &Message| Status::Processed,
    /// );
    /// aid.send(Message::new(11));
    /// ```
    pub fn spawn<F, State>(&self, state: State, processor: F) -> ActorId
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        let actor = Actor::new(self.data.node_uuid, None, state, processor);
        let result = self.register_actor(actor).unwrap();
        result.send(Message::new(SystemMsg::Start));
        result
    }

    /// Spawns a new named actor on the `system` using the given starting `state` for the actor
    /// and the given `processor` function that will be used to process actor messages.
    /// If the `name` is already registered then this function will return an [`std::Result::Err`]
    /// with the value [`axiom::actors::ActorError::NameAlreadyUsed`] containing the name
    /// attempted to be registered.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use axiom::message::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::create());
    /// system.init_current();
    ///
    /// let aid = system.spawn_named(
    ///     "alpha",
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: ActorId, message: &Message| Status::Processed,
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
        let actor = Actor::new(
            self.data.node_uuid,
            Some(name.to_string()),
            state,
            processor,
        );
        let result = self.register_actor(actor)?;
        result.send(Message::new(SystemMsg::Start));
        Ok(result)
    }

    /// Schedules the `aid` for work. Note that this is the only time that we have to use the
    /// lookup table. This function gets called when an actor goes from 0 receivable messages to
    /// 1 receivable message. If the actor has more receivable messages then this will not be
    /// needed to be called because the dispatcher threads will handle the process of resending
    /// the actor to the work channel.
    ///
    /// TODO Put tests verifying the resend on multiple messages.
    fn schedule(&self, aid: ActorId) {
        // Note that this is implemented here rather than calling the sender directly
        // from the send in order to allow internal optimization of the actor system.
        // FIXME Convert this to the actor itself being inside the AID.
        let actors_by_aid = self.data.actors_by_aid.read().unwrap();
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
                    self.data.node_uuid.to_string(),
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
    /// send the actor a [`axiom::actors::SystemMsg::Stop`] message and allow it to stop
    /// gracefully.
    pub fn stop(&self, aid: ActorId) {
        {
            let mut actors_by_aid = self.data.actors_by_aid.write().unwrap();
            let mut aids_by_uuid = self.data.aids_by_uuid.write().unwrap();
            let mut aids_by_name = self.data.aids_by_name.write().unwrap();
            actors_by_aid.remove(&aid);
            aids_by_uuid.remove(&aid.uuid());
            if let Some(name_string) = aid.name() {
                aids_by_name.remove(&name_string);
            }
            aid.stop();
        }

        // Notify all of the actors monitoring the actor that is stopped and remove the
        // actor from the map of monitors.
        if let Some(monitoring) = self
            .data
            .monitoring_by_monitored
            .write()
            .unwrap()
            .remove(&aid)
        {
            for m_aid in monitoring {
                ActorId::send(&m_aid, Message::new(SystemMsg::Stopped(aid.clone())));
            }
        }
    }

    /// Checks to see if the actor with the given [`axiom::actors::ActorId`] is alive within
    /// this actor system.
    pub fn is_alive(&self, aid: &ActorId) -> bool {
        let actors_by_aid = self.data.actors_by_aid.write().unwrap();
        actors_by_aid.contains_key(aid)
    }

    /// Look up an [`axiom::actors::ActorId`] by the unique UUID of the actor and either returns
    /// the located `aid` in a [`std::Option::Some`] or [`std::Option::None`] if not found.
    pub fn find_aid_by_uuid(&self, uuid: &Uuid) -> Option<ActorId> {
        let aids_by_uuid = self.data.aids_by_uuid.read().unwrap();
        aids_by_uuid.get(uuid).map(|aid| aid.clone())
    }

    /// Look up an [`axiom::actors::ActorId`] by the user assigned name of the actor and either
    /// returns the located `aid` in a [`std::Option::Some`] or [`std::Option::None`] if not
    /// found.
    pub fn find_aid_by_name(&self, name: &str) -> Option<ActorId> {
        let aids_by_name = self.data.aids_by_name.read().unwrap();
        aids_by_name.get(&name.to_string()).map(|aid| aid.clone())
    }

    /// Adds a monitor so that `monitoring` will be informed if `monitored` stops.
    pub fn monitor(&self, monitoring: &ActorId, monitored: &ActorId) {
        let mut monitoring_by_monitored = self.data.monitoring_by_monitored.write().unwrap();
        let monitoring_vec = monitoring_by_monitored
            .entry(monitored.clone())
            .or_insert(Vec::new());
        monitoring_vec.push(monitoring.clone());
    }
}

impl fmt::Debug for ActorSystem {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ActorSystem{{node_uuid: {}, configuration: {:?}}}",
            self.data.node_uuid.to_string(),
            self.data.configuration,
        )
    }
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use std::time::Duration;

    /// A test helper to assert that a certain number of messages arrived in a certain time.
    fn assert_await_received(aid: &ActorId, count: u8, timeout_ms: u64) {
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
    fn simple_handler(_state: &mut usize, _aid: ActorId, _message: &Message) -> Status {
        Status::Processed
    }

    #[test]
    fn test_actor_id_serialization() {
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();
        let aid = system.spawn(0 as usize, simple_handler);

        // forces the test to break here if someone changes this.
        if let ActorSender::Local(_) = aid.data.sender {
        } else {
            panic!("The sender should be `Local`")
        }

        let serialized = bincode::serialize(&aid).expect("Couldn't serialize.");
        let deserialized: ActorId =
            bincode::deserialize(&serialized).expect("Couldn't deserialize.");
        // In this case the resulting AID should be identical to the serialized one because
        // we have the same actor system in a threadlocal.
        assert!(Arc::ptr_eq(&aid.data, &deserialized.data));

        // If we deserialize on another actor system it should be a remote AID.
        let handle = thread::spawn(move || {
            let system = ActorSystem::create(ActorSystemConfig::create());
            system.init_current();

            let deserialized: ActorId =
                bincode::deserialize(&serialized).expect("Couldn't deserialize.");
            if let ActorSender::Remote = deserialized.data.sender {
                assert_eq!(aid.uuid(), deserialized.uuid());
                assert_eq!(aid.node_uuid(), deserialized.node_uuid());
                assert_eq!(aid.name(), deserialized.name());
                assert_eq!(false, deserialized.is_stopped());
            } else {
                panic!("The sender should be `Remote`")
            }
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_actor_id_as_message() {
        init_test_log();

        // This test verifies that an ActorId can be used as a message and inside other
        // objects used as a message.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        #[derive(Serialize, Deserialize)]
        enum Op {
            Aid(ActorId),
        }

        // The user will send our own ActorId to us.
        let aid = system.spawn(0, |_state: &mut i32, aid: ActorId, message: &Message| {
            if let Some(msg) = message.content_as::<ActorId>() {
                assert_eq!(aid.uuid(), msg.uuid());
            } else if let Some(msg) = message.content_as::<Op>() {
                match &*msg {
                    Op::Aid(a) => assert_eq!(aid.uuid(), a.uuid()),
                }
            }
            Status::Processed
        });

        // Send a message to the actor.
        aid.send(Message::new(aid.clone()));

        // Wait for the start and our message to get there because test is asynchronous.
        assert_await_received(&aid, 2, 1000);
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_simplest_actor() {
        init_test_log();

        // This test shows how the simplest actor can be built and used. This actor uses a closure
        // that simply returns that the message is processed.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let aid = system.spawn(
            0 as usize,
            |_state: &mut usize, _aid: ActorId, _message: &Message| Status::Processed,
        );

        // Send a message to the actor.
        aid.send(Message::new(11));

        // Wait for the message to get there because test is asynchronous.
        assert_await_received(&aid, 1, 1000);
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_simplest_struct_actor() {
        init_test_log();

        // This test shows how the simplest struct-based actor can be built and used. This actor
        // merely returns that the message was processed.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        // We declare a basic struct that has a handle method that does basically nothing.
        // Subsequently we will create that struct when we spawn the actor and then send the
        // actor a message.
        struct Data {}

        impl Data {
            fn handle(&mut self, _aid: ActorId, _message: &Message) -> Status {
                Status::Processed
            }
        }

        let aid = system.spawn(Data {}, Data::handle);

        // Send a message to the actor.
        aid.send(Message::new(11));

        // Wait for the message to get there because test is asynchronous.
        assert_await_received(&aid, 1, 1000);
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_dispatching_with_closure() {
        init_test_log();

        // This test shows how a closure based actor can be used and process different kinds of
        // messages and mutate the actor's state based upon the messages passed. Note that the
        // state of the actor is not available outside the actor itself.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let starting_state: usize = 0 as usize;
        let closure = |state: &mut usize, aid: ActorId, message: &Message| {
            // Expected messages in the expected order.
            let expected: Vec<i32> = vec![11, 13, 17];
            // Attempt to downcast to expected message.
            if let Some(_msg) = message.content_as::<SystemMsg>() {
                *state += 1;
                Status::Processed
            } else if let Some(msg) = message.content_as::<i32>() {
                assert_eq!(expected[*state - 1], *msg);
                assert_eq!(*state, aid.received());
                *state += 1;
                assert_eq!(aid.pending(), aid.sent() - aid.received());
                Status::Processed
            } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                // Note that we put this last because it only is ever received once, we
                // want the most frequently received messages first.
                Status::Processed
            } else {
                assert!(false, "Failed to dispatch properly");
                Status::Processed // This assertion will fail but we still have to return.
            }
        };

        let aid = system.spawn(starting_state, closure);

        // First message will always be the SystemMsg::Start
        assert_eq!(1, aid.sent());

        // Send some messages to the actor in the order required in the test. In a real actor
        // its unlikely any order restriction would be needed. However this test makes sure that
        // the messages are processed correctly.
        aid.send(Message::new(11 as i32));
        assert_eq!(2, aid.sent());
        aid.send(Message::new(13 as i32));
        assert_eq!(3, aid.sent());
        aid.send(Message::new(17 as i32));
        assert_eq!(4, aid.sent());

        // Wait for all of the messages to get there because test is asynchronous.
        assert_await_received(&aid, 4, 1000);
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_dispatching_with_struct() {
        init_test_log();

        // This test shows how a struct-based actor can be used and process different kinds of
        // messages and mutate the actor's state based upon the messages passed. Note that the
        // state of the actor is not available outside the actor itself.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        // We create a basic struct with a handler and use that handler to dispatch to other
        // inherent methods in the struct. Note that we don't have to implement any traits here
        // and there is nothing forcing the handler to be an inherent method.
        struct Data {
            value: i32,
        }

        impl Data {
            fn handle_bool(&mut self, _aid: ActorId, message: &bool) -> Status {
                if *message {
                    self.value += 1;
                } else {
                    self.value -= 1;
                }
                Status::Processed // This assertion will fail but we still have to return.
            }

            fn handle_i32(&mut self, _aid: ActorId, message: &i32) -> Status {
                self.value += *message;
                Status::Processed // This assertion will fail but we still have to return.
            }

            fn handle(&mut self, aid: ActorId, message: &Message) -> Status {
                if let Some(msg) = message.content_as::<bool>() {
                    self.handle_bool(aid, &*msg)
                } else if let Some(msg) = message.content_as::<i32>() {
                    self.handle_i32(aid, &*msg)
                } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                    // Note that we put this last because it only is ever received once, we
                    // want the most frequently received messages first.
                    Status::Processed
                } else {
                    assert!(false, "Failed to dispatch properly");
                    Status::Stop // This assertion will fail but we still have to return.
                }
            }
        }

        let data = Data { value: 0 };

        let aid = system.spawn(data, Data::handle);

        // Send some messages to the actor.
        aid.send(Message::new(11));
        aid.send(Message::new(true));
        aid.send(Message::new(true));
        aid.send(Message::new(false));

        // Wait for all of the messages to get there because this test is asynchronous.
        assert_await_received(&aid, 4, 1000);
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_actor_returns_stop() {
        init_test_log();

        // This test verifies functionality around stopping actors though means of the actor
        // returning a stop status.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types
        // manually but when that bug goes away this will be even simpler.
        let aid = system.spawn(
            0 as usize,
            |state: &mut usize, _aid: ActorId, message: &Message| {
                if let Some(_msg) = message.content_as::<i32>() {
                    assert_eq!(1 as usize, *state);
                    *state += 1;
                    Status::Processed
                } else if let Some(msg) = message.content_as::<SystemMsg>() {
                    match &*msg {
                        SystemMsg::Start => {
                            assert_eq!(0 as usize, *state);
                            *state += 1;
                            Status::Processed
                        }
                        SystemMsg::Stop => {
                            assert_eq!(2 as usize, *state);
                            *state += 1;
                            Status::Stop
                        }
                        m => panic!("unexpected message: {:?}", m),
                    }
                } else {
                    assert!(false, "Failed to dispatch properly");
                    Status::Processed // The assertion will fail but we still have to return.
                }
            },
        );

        // Send a message to the actor.
        aid.send(Message::new(11 as i32));
        aid.send(Message::new(SystemMsg::Stop));

        // Wait for the message to get there because test is asynchronous.
        assert_await_received(&aid, 3, 1000);

        // Make sure that the actor is actually stopped and cant get more messages.
        assert!(true, aid.is_stopped());
        match aid.try_send(Message::new(42 as i32)) {
            Err(ActorError::ActorStopped) => assert!(true), // all OK!
            Ok(_) => assert!(false, "Expected the actor to be shut down!"),
            Err(e) => assert!(false, "Unexpected error: {:?}", e),
        }
        assert_eq!(false, system.is_alive(&aid));

        // Verify the actor is NOT in the maps.
        let sys_clone = system.clone();
        let actors_by_aid = sys_clone.data.actors_by_aid.read().unwrap();
        assert_eq!(false, actors_by_aid.contains_key(&aid));
        let aids_by_uuid = sys_clone.data.aids_by_uuid.read().unwrap();
        assert_eq!(false, aids_by_uuid.contains_key(&aid.uuid()));

        // Shut down the system and clean up test.
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_actor_force_stop() {
        init_test_log();

        // This test verifies that the system does not panic if we schedule to an actor
        // that does not exist in the map. This can happen if the actor is stopped before
        // the system notifies the actor id that it is dead.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();
        let aid = system.spawn(0, simple_handler);
        aid.send(Message::new(11));
        assert_await_received(&aid, 2, 1000);

        // Now we force stop the actor.
        system.stop(aid.clone());

        // Make sure the actor is out of the maps and cant be sent to.
        assert!(true, aid.is_stopped());
        match aid.try_send(Message::new(42)) {
            Err(ActorError::ActorStopped) => assert!(true), // all OK!
            Ok(_) => assert!(false, "Expected the actor to be shut down!"),
            Err(e) => assert!(false, "Unexpected error: {:?}", e),
        }
        assert_eq!(false, system.is_alive(&aid));

        // Verify the actor is NOT in the maps.
        let sys_clone = system.clone();
        let actors_by_aid = sys_clone.data.actors_by_aid.read().unwrap();
        assert_eq!(false, actors_by_aid.contains_key(&aid));
        let aids_by_uuid = sys_clone.data.aids_by_uuid.read().unwrap();
        assert_eq!(false, aids_by_uuid.contains_key(&aid.uuid()));

        // Wait for the message to get there because test is asynchronous.
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_actor_not_in_map() {
        init_test_log();

        // This test verifies that the system does not panic if we schedule to an actor
        // that does not exist in the map. This can happen if the actor is stopped before
        // the system notifies the actor id that it is dead.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();
        let aid = system.spawn(0, simple_handler);

        // We force remove the actor from the system so now it cannot be scheduled.
        let sys_clone = system.clone();
        let mut actors_by_aid = (*sys_clone.data.actors_by_aid).write().unwrap();
        actors_by_aid.remove(&aid);
        drop(actors_by_aid); // Give up the lock.

        // Send a message to the actor.
        aid.send(Message::new(11));

        // Wait for the message to get there because test is asynchronous.
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_find_by_uuid() {
        init_test_log();

        // This test checks that we can look up an actor id by the UUID of the actor id.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();
        let aid = system.spawn(0 as usize, simple_handler);

        // Send a message to the actor verifying it is up.
        aid.send(Message::new(11));
        assert_await_received(&aid, 1, 1000);
        let found: &ActorId = &system.find_aid_by_uuid(&aid.uuid()).unwrap();
        assert!(Arc::ptr_eq(&aid.data, &found.data));

        // Stop the actor and it should be out of the map.
        system.stop(aid.clone());
        assert_eq!(None, system.find_aid_by_uuid(&aid.uuid()));

        // Verify attempting to find with unregistered name returns none.
        assert_eq!(None, system.find_aid_by_uuid(&Uuid::new_v4()));

        // Wait for the message to get there because test is asynchronous.
        system.trigger_and_await_shutdown();
    }

    #[test]
    fn test_named_actors() {
        init_test_log();

        // This test verifies that the concept of named actors works properly. When a user wants
        // to declare a named actor they cannot register the same name twice and when the actor
        // stops the name should be removed from the registered names and be available again.
        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();

        let aid1 = system
            .spawn_named("alpha", 0 as usize, simple_handler)
            .unwrap();
        ActorId::send(&aid1, Message::new(11));
        assert_await_received(&aid1, 1, 1000);
        let found1: &ActorId = &system.find_aid_by_name("alpha").unwrap();
        assert!(Arc::ptr_eq(&aid1.data, &found1.data));

        let aid2 = system
            .spawn_named("bravo", 0 as usize, simple_handler)
            .unwrap();
        ActorId::send(&aid2, Message::new(11));
        assert_await_received(&aid2, 1, 1000);
        let found2: &ActorId = &system.find_aid_by_name("bravo").unwrap();
        assert!(Arc::ptr_eq(&aid2.data, &found2.data));

        // Spawn an actor that attempts to overwrite "alpha" in the names and make sure the
        // attempt returns an error to be handled.
        let result = system.spawn_named("alpha", 0 as usize, simple_handler);
        assert_eq!(
            Err(ActorError::NameAlreadyUsed("alpha".to_string())),
            result
        );

        // Verify that the same actor has "alpha" name and is still up.
        let found3: &ActorId = &system.find_aid_by_name("alpha").unwrap();
        assert!(Arc::ptr_eq(&aid1.data, &found3.data));
        ActorId::send(&aid1, Message::new(11));
        assert_await_received(&aid1, 2, 1000);

        // Verify attempting to find with unregistered name returns none.
        assert_eq!(None, system.find_aid_by_name("charlie"));

        // Stop "bravo" and verify that the actor system's maps are cleaned up.
        system.stop(aid2.clone());
        assert_eq!(None, system.find_aid_by_name("bravo"));
        assert_eq!(None, system.find_aid_by_uuid(&aid2.data.uuid));

        // Now we should be able to crate a new actor with the name bravo.
        let aid3 = system
            .spawn_named("bravo", 0 as usize, simple_handler)
            .unwrap();
        ActorId::send(&aid3, Message::new(11));
        assert_await_received(&aid3, 1, 1000);
        let found4: &ActorId = &system.find_aid_by_name("bravo").unwrap();
        assert!(Arc::ptr_eq(&aid3.data, &found4.data));

        // Wait for the message to get there because test is asynchronous.
        system.trigger_and_await_shutdown();
    }

    /// A helper handler used by `test_monitors` that expects to get a stopped message for the
    /// `aid` that was being monitored.
    fn monitor_handler(state: &mut ActorId, _aid: ActorId, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<SystemMsg>() {
            match &*msg {
                SystemMsg::Stopped(aid) => {
                    assert!(Arc::ptr_eq(&state.data, &aid.data));
                    Status::Processed
                }
                SystemMsg::Start => Status::Processed,
                _ => {
                    assert!(false, "Received some other message!");
                    Status::Processed // This assertion will fail but we still have to return.
                }
            }
        } else {
            assert!(false, "Received some other message!");
            Status::Processed // This assertion will fail but we still have to return.
        }
    }

    #[test]
    fn test_monitors() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::create());
        system.init_current();
        let monitored = system.spawn(0 as usize, simple_handler);
        let not_monitoring = system.spawn(0 as usize, simple_handler);
        let monitoring1 = system.spawn(monitored.clone(), monitor_handler);
        let monitoring2 = system.spawn(monitored.clone(), monitor_handler);
        system.monitor(&monitoring1, &monitored);
        system.monitor(&monitoring2, &monitored);

        {
            // Validate the monitors are there in a block to release mutex afterwards.
            let monitoring_by_monitored = &system.data.monitoring_by_monitored.read().unwrap();
            let m_vec = monitoring_by_monitored.get(&monitored).unwrap();
            assert!(m_vec.contains(&monitoring1));
            assert!(m_vec.contains(&monitoring2));
        }

        // Stop the actor and it should be out of the map.
        system.stop(monitored.clone());
        assert_await_received(&monitoring1, 2, 1000);
        assert_await_received(&monitoring2, 2, 1000);
        assert_await_received(&not_monitoring, 1, 1000);

        // Wait for the message to get there because test is asynchronous.
        system.trigger_and_await_shutdown();
    }
}
