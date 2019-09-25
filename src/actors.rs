//! Implements actors and related types of Axiom.
//!
//! These are the core components that make up the features of Axiom. The actor model is designed
//! to allow the user maximum flexibility. The actors can skip messages if they choose, enabling
//! them to work as a *finite state machine* without having to move messages around. Actors are
//! created by calling `system::spawn().with()` with any kind of fuction or closure that
//! implements the `Processor` trait.

use crate::message::*;
use crate::system::*;
use crate::*;
use log::{error, warn};
use secc::*;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Status of the message and potentially the actor as a resulting from processing a message
/// with the actor.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Status {
    /// The message was processed and can be removed from the channel. Note that this doesn't
    /// necessarily mean that anything was done with the message, just that it can be removed.  
    /// It is up to the actor to decide what, if anything, to do with the message.
    Done,

    /// The message was skipped and should remain in the channel. Once a message is skipped a skip
    /// cursor will be created in the actor's message channel which will act as the actual head
    /// of the channel until an [`Status::Reset`] is returned from an actor's processor.
    /// This enables an actor to skip messages while working on a process and then clear the skip
    /// cursor and resume normal processing. This functionality is critical for actors that
    /// implement a finite state machine.
    Skip,

    /// Marks the message as processed and clears the skip cursor on the channel. A skip cursor
    /// is present when a message has been skipped by an actor returning [`Status::Skip`]
    /// from a call to the actor's message processor. If no skip cursor is set than this status
    /// is semantically the same as [`Status::Done`].
    Reset,

    /// Returned from an actor when the actor wants the system to stop the actor. When this status
    /// is returned the actor's [`ActorId`] will no longer send any messages and the actor
    /// instance itself will be removed from the actors table in the [`ActorSystem`]. The user is
    /// advised to do any cleanup needed before returning [`Status::Stop`].
    Stop,
}

/// An enum that holds a sender for an actor.
///
/// An [`ActorId`] uses the sender to send messages to the destination actor. Messages that are
/// sent to actors running on this actor system are wrapped in an Arc for efficiency.
enum ActorSender {
    /// A sender used for sending messages to actors running on the same actor system.
    Local {
        /// Holds a boolean to indicate if the actor is stopped. A stopped actor will no longer
        /// accept further messages to be sent.
        stopped: AtomicBool,
        /// The send side of the actor's message channel.
        sender: SeccSender<Message>,
        /// The reference to the local [`ActorSystem`] that the `aid` is on.
        system: ActorSystem,
    },

    /// A sender that is used when an actor is on another actor system. Messages are wrapped in a
    /// [`WireMessage`] struct and it will be up to the cluster implementation to get the messages
    /// to the remote system.
    Remote { sender: SeccSender<WireMessage> },
}

impl std::fmt::Debug for ActorSender {
    fn fmt(&self, formatter: &'_ mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "{}",
            match *self {
                ActorSender::Local { .. } => "ActorSender::Local",
                ActorSender::Remote { .. } => "ActorSender::Remote",
            }
        )
    }
}

/// The inner data of an [`ActorId`].
///
/// This is kept separate to make serialization possible without duplicating all of the data
/// associated with the [`ActorId`]. It also makes it easier when cloning and referring to an
/// `aid` as the user doesnt have to put `Arc<ActorId>` all over thier code.
struct ActorIdData {
    /// See [`ActorId::uuid()`]
    uuid: Uuid,
    /// See [`ActorId::system_uuid()`]
    system_uuid: Uuid,
    /// See [`ActorId::name()`]
    name: Option<String>,
    /// The handle to the sender side for the actor's message channel.
    sender: ActorSender,
}

/// A helper type to make [`ActorId`] serialization cleaner.
#[derive(Serialize, Deserialize)]
struct ActorIdSerializedForm {
    uuid: Uuid,
    system_uuid: Uuid,
    name: Option<String>,
}

/// Encapsulates an ID to an actor and is often referred to as an `aid`.
///
/// This is a unique reference to the actor within the entire cluster and can be used to send
/// messages to the actor regardless of location. The [`ActorId`] does the heavy lifting of
/// deciding where the actor is and sending the message. However it is important that the user at
/// least has some notion of where the actor is for developing an efficient actor architecture.
/// This `aid` can also be serialized to a remote system and then back to the system hosting the
/// actor without issue. Often `ActorId`s are passed around an actor system so this is a common
/// use case.
#[derive(Clone)]
pub struct ActorId {
    /// Holds the actual data for the [`ActorId`].
    data: Arc<ActorIdData>,
}

impl Serialize for ActorId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let serialized_form = ActorIdSerializedForm {
            uuid: self.uuid(),
            system_uuid: self.system_uuid(),
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
        // We will look up the aid in the table and return it to the caller if it exists otherwise
        // it must be a remote aid.
        // FIXME (Issue #67) Add error if the system_uuid is the same but the `ActorId` is not
        // found.
        match system.find_aid_by_uuid(&serialized_form.uuid) {
            Some(aid) => Ok(aid.clone()),
            None => {
                if let Some(sender) = system.remote_sender(&serialized_form.system_uuid) {
                    Ok(ActorId {
                        data: Arc::new(ActorIdData {
                            uuid: serialized_form.uuid,
                            system_uuid: serialized_form.system_uuid,
                            name: serialized_form.name,
                            sender: ActorSender::Remote { sender: sender },
                        }),
                    })
                } else {
                    let msg = format!("Unknown actor system {:?}", serialized_form.system_uuid);
                    Err(serde::de::Error::custom(msg))
                }
            }
        }
    }
}

impl std::cmp::PartialEq for ActorId {
    fn eq(&self, other: &Self) -> bool {
        self.data.uuid == other.data.uuid && self.data.system_uuid == other.data.system_uuid
    }
}

impl std::cmp::Eq for ActorId {}

impl std::cmp::PartialOrd for ActorId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        // Order by name, then by system, then by uuid.  Also, sort `None` names before others.
        match (&self.data.name, &other.data.name) {
            (None, Some(_)) => Some(Ordering::Less),
            (Some(_), None) => Some(Ordering::Greater),
            (Some(a), Some(b)) if a != b => Some(a.cmp(b)),
            (_, _) => {
                // Names are equal, either both `None` or `Some(thing)` where `thing1 == thing2`
                // so we impose a secondary order by system uuid.
                match self.data.system_uuid.cmp(&other.data.system_uuid) {
                    Ordering::Equal => Some(self.data.uuid.cmp(&other.data.uuid)),
                    x => Some(x),
                }
            }
        }
    }
}

impl std::cmp::Ord for ActorId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("ActorId::partial_cmp() returned None; can't happen")
    }
}

impl ActorId {
    /// Attempts to send a message to the actor with the given [`ActorId`] and returns
    /// `std::Result::Ok` when the send was successful or a `std::Result::Err<AxiomError>`
    /// if something went wrong with the send. Note that if a user just calls `send(msg).unwrap()`,
    /// a panic could take down the dispatcher thread and thus eventually hang the process.
    ///
    /// # Examples
    /// ```
    /// use axiom::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default());
    ///
    /// let aid = system.spawn().with(
    ///     0 as usize,
    ///     |_state: &mut usize, _context: &Context, message: &Message| Ok(Status::Done),
    ///  ).unwrap();
    ///
    /// match aid.send(Message::new(11)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    /// ```
    pub fn send(&self, message: Message) -> Result<(), AxiomError> {
        match &self.data.sender {
            ActorSender::Local {
                stopped,
                sender,
                system,
            } => {
                if stopped.load(Ordering::Relaxed) {
                    Err(AxiomError::ActorAlreadyStopped)
                } else {
                    match sender.send_await_timeout(message, system.config().send_timeout) {
                        Ok(_) => {
                            // FIXME (Issue #68) Investigate if this could race the dispatcher threads.
                            if sender.receivable() == 1 {
                                system.schedule(self.clone());
                            };
                            Ok(())
                        }
                        Err(_) => Err(AxiomError::SendTimedOut(self.clone())),
                    }
                }
            }
            ActorSender::Remote { sender } => {
                sender
                    .send_await(WireMessage::ActorMessage {
                        actor_uuid: self.data.uuid,
                        system_uuid: self.data.system_uuid,
                        message,
                    })
                    .unwrap();
                Ok(())
            }
        }
    }

    /// Shortcut for calling `send(Message::new(value))` This method will internally wrap
    /// whatever it is passed into a `Message` and try to send it. This method would not be
    /// appropriate if you want to re-send a message as it would wrap the message again with the
    /// same result as if the the code called `aid.send(Message::new(Message::new(value)))`.
    /// If the code wishes to resend a message it should just call just call `send(msg)`.
    ///
    /// # Examples
    /// ```
    /// use axiom::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default());
    ///
    /// let aid = system.spawn().with(
    ///     0 as usize,
    ///     |_state: &mut usize, _context: &Context, message: &Message| Ok(Status::Done),
    ///  ).unwrap();
    ///
    /// match aid.send_new(11) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    /// ```
    pub fn send_new<T>(&self, value: T) -> Result<(), AxiomError>
    where
        T: 'static + ActorMessage,
    {
        self.send(Message::new(value))
    }

    /// Schedules the given message to be sent after a minimum of the specified duration. Note
    /// that Axiom doesn't guarantee that the message will be sent on exactly now + duration but
    /// rather that _at least_ the duration will pass before the message is sent to the actor.
    /// Axiom will try to send as close as possible without going under the amount but precise
    /// timing should not be depended on.  This method will return an `Err` if the actor has been
    /// stopped or `Ok` if the message was scheduled to be sent. If the actor is stopped before
    /// the duration passes then the scheduled message will never get to the actor.
    pub fn send_after(&self, message: Message, duration: Duration) -> Result<(), AxiomError> {
        match &self.data.sender {
            ActorSender::Local {
                stopped, system, ..
            } => {
                if stopped.load(Ordering::Relaxed) {
                    Err(AxiomError::ActorAlreadyStopped)
                } else {
                    system.send_after(message, self.clone(), duration);
                    Ok(())
                }
            }
            ActorSender::Remote { sender } => {
                sender
                    .send_await(WireMessage::DelayedActorMessage {
                        duration: duration,
                        actor_uuid: self.data.uuid,
                        system_uuid: self.data.system_uuid,
                        message,
                    })
                    .unwrap(); // FIXME Get rid of this unwrap!
                Ok(())
            }
        }
    }

    /// Shortcut for calling `send_after(Message::new(value))` This method will internally wrap
    /// whatever it is passed into a `Message` and try to send it. This method would not be
    /// appropriate if you want to re-send a message as it would wrap the message again with the
    /// same result as if the the code called `aid.send_after(Message::new(Message::new(value)))`.
    /// If the code wishes to resend a message it should just call just call `send(msg)`.
    ///
    /// # Examples
    /// ```
    /// use axiom::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default());
    ///
    /// let aid = system.spawn().with(
    ///     0 as usize,
    ///     |_state: &mut usize, _context: &Context, message: &Message| Ok(Status::Done),
    ///  ).unwrap();
    ///
    /// match aid.send_new_after(11, Duration::from_millis(1)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    /// ```
    pub fn send_new_after<T>(&self, value: T, duration: Duration) -> Result<(), AxiomError>
    where
        T: 'static + ActorMessage,
    {
        self.send_after(Message::new(value), duration)
    }

    /// The unique UUID for this actor within the entire cluster. The UUID for an [`ActorId`]
    /// is generated with a v4 random UUID so the chances of collision are not worth considering.
    #[inline]
    pub fn uuid(&self) -> Uuid {
        self.data.uuid.clone()
    }

    /// The unique UUID for the actor system that this actor lives on. As with `uuid` this value
    /// is a v4 UUID and so the chances of two systems having the same uuid is inconsequential.
    #[inline]
    pub fn system_uuid(&self) -> Uuid {
        self.data.system_uuid.clone()
    }

    /// The name of the actor as assigned by the user at spawn time if any. Note that this name
    /// is guaranteed to be unique only within the actor system in which the actor was spawned;
    /// no guarantees are made that the name will be unique within a cluster of actor systems.
    #[inline]
    pub fn name(&self) -> Option<String> {
        self.data.name.clone()
    }

    /// Returns the name assigned to the ActorId if it is not a `None` and otherwise returns the
    /// uuid of the actor as a string.
    #[inline]
    pub fn name_or_uuid(&self) -> String {
        match &self.data.name {
            Some(value) => value.to_string(),
            None => self.data.uuid.to_string(),
        }
    }

    /// Determines if this actor lives on the local actor system or another system in the same
    /// process. Actors that are local to each other can exchange large amounts of data
    /// efficiently through passing [`Arc`]s.
    #[inline]
    pub fn is_local(&self) -> bool {
        if let ActorSender::Local { .. } = self.data.sender {
            true
        } else {
            false
        }
    }

    /// Determines how many messages the actor with the aid has been sent. This method works only
    /// for local `aid`s, remote `aid`s will return an error if this is called.
    pub fn sent(&self) -> Result<usize, AxiomError> {
        match &self.data.sender {
            ActorSender::Local { sender, .. } => Ok(sender.sent()),
            _ => Err(AxiomError::ActorIdNotLocal),
        }
    }

    /// Determines how many messages the actor with the `aid` has received. This method works only
    /// for local `aid`s, remote `aid`s will return an error if this is called.    
    pub fn received(&self) -> Result<usize, AxiomError> {
        match &self.data.sender {
            ActorSender::Local { sender, .. } => Ok(sender.received()),
            _ => Err(AxiomError::ActorIdNotLocal),
        }
    }

    /// Marks the actor referenced by the [`ActorId`] as stopped and puts mechanisms in place to
    /// cause no more messages to be sent to the actor. Note that once stopped, an [`ActorId`] can
    /// never be started again. Note that this is `pub(crate)` because the user should be sending
    /// `SystemMsg::Stop` to actors or, at worst, calling `ActorSystem::stop()` to stop an actor.
    pub(crate) fn stop(&self) -> Result<(), AxiomError> {
        match &self.data.sender {
            ActorSender::Local { stopped, .. } => {
                stopped.fetch_or(true, Ordering::AcqRel);
                Ok(())
            }
            _ => Err(AxiomError::ActorIdNotLocal),
        }
    }

    /// Checks to see if the left and right aid actually point at the exact same actor.
    pub fn ptr_eq(left: &ActorId, right: &ActorId) -> bool {
        Arc::ptr_eq(&left.data, &right.data)
    }
}

impl std::fmt::Debug for ActorId {
    fn fmt(&self, formatter: &'_ mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "ActorId{{id: {}, system_uuid: {}, name: {:?}, is_local: {}}}",
            self.data.uuid.to_string(),
            self.data.system_uuid.to_string(),
            self.data.name,
            self.is_local()
        )
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.data.name {
            Some(name) => write!(f, "{}:{}", name, self.data.uuid),
            None => write!(f, "{}", self.data.uuid),
        }
    }
}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &'_ mut H) {
        self.data.uuid.hash(state);
        self.data.system_uuid.hash(state);
    }
}

/// A context that is passed to the processor to give immutable access to elements of the
/// actor system to the implementor of an actor's processor.
#[derive(Debug)]
pub struct Context {
    pub aid: ActorId,
    pub system: ActorSystem,
}

impl std::fmt::Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Context{{aid: {}, system: {}}}",
            self.aid.uuid(),
            self.system.uuid()
        )
    }
}

/// A type for a function that processes messages for an actor.
///
/// This will be passed to a spawn function to specify the function used for managing the state of
/// the actor based on the messages passed to the actor. The result of a processor is used to
/// determine the status of an actor. If the actor returns an `AxiomError` then it will be stopped
/// as if the actor had returned `Stop`. The processor takes three arguments:
/// * `state`   - A mutable reference to the current state of the actor.
/// * `aid`     - The reference to the [`ActorId`] for this actor.
/// * `message` - The reference to the current message to process.
pub trait Processor<State: Send + Sync>:
    (FnMut(&mut State, &Context, &Message) -> AxiomResult) + Send + Sync
{
}

// Allows any static function or closure, to be used as a Processor.
impl<F, State> Processor<State> for F
where
    State: Send + Sync + 'static,
    F: (FnMut(&mut State, &Context, &Message) -> AxiomResult) + Send + Sync + 'static,
{
}

/// This is the internal type for the handler that will manage the state for the actor using the
/// user-provided message processor.
trait Handler: (FnMut(&Context, &Message) -> AxiomResult) + Send + Sync + 'static {}

// Allows any static function or closure, to be used as a Handler.
impl<F> Handler for F where F: (FnMut(&Context, &Message) -> AxiomResult) + Send + Sync + 'static {}

/// A builder that can be used to create and spawn an actor. To get a builder, the user would ask
/// the actor system to create one using `system.spawn()` and then to spawn the actor by means of
/// the the `with` method on the builder. See [`ActorSystem::actor`] for more information.
pub struct ActorBuilder {
    /// The System that the actor builder was created on.
    pub(crate) system: ActorSystem,
    /// The optional name of the actor which defaults to `None` meaning the actor will be unnamed.
    pub name: Option<String>,
    /// The size of the message channel for the actor which defaults to `None`; meaning the
    /// default for the actor system will be used for the message channel.
    pub channel_size: Option<u16>,
}

impl ActorBuilder {
    /// Completes the spawning of the the actor configured with this builder on the system,
    /// consuming the builder in the process and using the provided state and handler. See
    /// `ActorSystem::spawn` for more information and examples.
    ///
    /// FIXME Consider implementing `using` to spawn a stateless actor.
    pub fn with<F, State>(self, state: State, processor: F) -> Result<ActorId, AxiomError>
    where
        State: Send + Sync + 'static,
        F: (FnMut(&mut State, &Context, &Message) -> AxiomResult) + Send + Sync + 'static,
    {
        let actor = Actor::new(self.system.clone(), &self, state, processor);
        let result = self.system.register_actor(actor)?;
        Ok(result)
    }

    /// Set the name of the actor to the given string.
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the size of the channel to the given value instead of the default for the actor system
    /// that the actor is spawned on. Note that passing a value less than 1 will cause a panic and
    /// there would be little reason to do so anyway.
    pub fn channel_size(mut self, size: u16) -> Self {
        assert!(size > 0);
        self.channel_size = Some(size);
        self
    }
}

/// The implementation of the actor in the system. Please see overview and library documentation
/// for more detail.
pub(crate) struct Actor {
    /// The context data for the actor containg the `aid` as well as other immutable data.
    pub context: Context,
    /// Receiver for the actor's message channel.
    receiver: SeccReceiver<Message>,
    /// The function that processes messages that are sent to the actor wrapped in a closure to
    /// erase the state type that the actor is managing. Note that this is in a mutex because the
    /// handler itself is `FnMut` and we also don't want there to be any possibility of two
    /// threads calling the handler concurrently as that would break the actor model rules.
    handler: Mutex<Box<dyn Handler>>,
}

impl Actor {
    /// Creates a new actor on the given actor system with the given processor function. The user
    /// will pass the initial state of the actor as well as the processor that will be used to
    /// process messages sent to the actor.
    pub(crate) fn new<F, State>(
        system: ActorSystem,
        builder: &ActorBuilder,
        mut state: State,
        mut processor: F,
    ) -> Arc<Actor>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        let (sender, receiver) = secc::create::<Message>(
            builder
                .channel_size
                .unwrap_or(system.config().message_channel_size),
            Duration::from_millis(10),
        );

        // The sender will be put inside the actor id.
        let aid = ActorId {
            data: Arc::new(ActorIdData {
                uuid: Uuid::new_v4(),
                system_uuid: system.uuid(),
                name: builder.name.clone(),
                sender: ActorSender::Local {
                    system: system.clone(),
                    stopped: AtomicBool::new(false),
                    sender,
                },
            }),
        };

        // This handler will manage the state for the actor.
        let handler = Box::new({
            move |context: &Context, message: &Message| processor(&mut state, context, message)
        });

        // This is the receiving side of the actor which holds the processor wrapped in the
        // handler type.
        let context = Context { aid, system };

        let actor = Actor {
            context,
            receiver,
            handler: Mutex::new(handler),
        };

        Arc::new(actor)
    }

    /// Receive a message from the channel and process it with the actor. This function is the
    /// core of the processing pipeline. The function will process messages up until the actor has
    /// no more pending messages or the given time slice has expired. The time slice is
    /// configurable and should be tuned to allow the most possible messages to be processed
    /// without locking out other actors.
    pub(crate) fn receive(actor: Arc<Actor>) {
        let mut guard = actor.handler.lock().unwrap();
        let system = actor.context.system.clone();
        let start = Instant::now();
        // FIXME The time sliced batching of messages needs testing.
        while Instant::elapsed(&start) < system.config().time_slice {
            // If there isn't a message, another dispatcher thread beat us to it, no big deal.
            if let Ok(message) = actor.receiver.peek() {
                // In this case there is a message in the channel that we have to process. We
                // will time the result and log a warning if it took too long.
                let start_process = Instant::now();
                let mut result = (&mut *guard)(&actor.context, &message);
                let elapsed = Instant::elapsed(&start_process);
                if elapsed > system.config().warn_threshold {
                    warn!(
                        "[{}] Actor took {:?} to process a message, threshold is {:?}!",
                        actor.context.aid,
                        elapsed,
                        system.config().warn_threshold
                    );
                }

                // If the message was a system stop message then we override the actor returned
                // result with a 'Status::Stop'. The override means actors that don't do anything
                // special can essentially ignore processing stop.
                if let Some(m) = message.content_as::<SystemMsg>() {
                    if let SystemMsg::Stop = *m {
                        result = Ok(Status::Stop)
                    }
                };

                // Handle the result of the processing.
                match result {
                    Ok(Status::Done) => actor.receiver.pop().unwrap(),
                    Ok(Status::Skip) => actor.receiver.skip().unwrap(),
                    Ok(Status::Reset) => {
                        actor.receiver.pop().unwrap();
                        actor.receiver.reset_skip().unwrap();
                    }
                    Ok(Status::Stop) => {
                        actor.receiver.pop().unwrap();
                        actor.context.system.stop_actor(&actor.context.aid);
                        // Actor stopping, dont process more messages.
                        break;
                    }
                    Err(e) => {
                        actor.receiver.pop().unwrap();
                        actor.context.system.stop_actor(&actor.context.aid);
                        error!(
                            "[{}] Returned an error when processing: {:?}",
                            actor.context.aid, e
                        );
                        // Actor stopping, don't process more messages.
                        break;
                    }
                };
            } else {
                // No more messages so we just break out of the loop.
                break;
            }
        }

        // Reschedule the actor if it is still alive and has more messages.
        if system.is_actor_alive(&actor.context.aid) && actor.receiver.receivable() > 0 {
            actor.context.system.reschedule(actor.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use std::thread;

    /// This test verifies that an actor's functions that retrieve basic info are working for
    /// unnamed actors.
    #[test]
    fn test_basic_info_unnamed() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().with(0, simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        assert_eq!(system.uuid(), aid.data.system_uuid);
        assert_eq!(aid.data.system_uuid, aid.system_uuid());
        assert_eq!(aid.data.uuid, aid.uuid());
        assert_eq!(None, aid.data.name);
        assert_eq!(aid.data.name, aid.name());

        system.trigger_and_await_shutdown();
    }

    /// This test verifies that an actor's functions that retrieve basic info are working for
    /// named actors.
    #[test]
    fn test_basic_info_named() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().name("A").with(0, simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        assert_eq!(system.uuid(), aid.data.system_uuid);
        assert_eq!(aid.data.system_uuid, aid.system_uuid());
        assert_eq!(aid.data.uuid, aid.uuid());
        assert_eq!(Some("A".to_string()), aid.data.name);
        assert_eq!(aid.data.name, aid.name());

        system.trigger_and_await_shutdown();
    }

    /// Tests serialization and deserialization of `ActorId`s. This verifies that deserialized
    /// `aid`s on the same actor system should just be the same `aid` as well as the fact that
    /// when deserialized on other actor systems the `aid`'s sender should be a remote aid.
    ///
    /// FIXME (Issue #70) Return error when deserializing an ActorId if a remote is not connected
    /// instead of panic.
    #[test]
    fn test_actor_id_serialization() {
        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().with(0 as usize, simple_handler).unwrap();
        system.init_current(); // Required by ActorId serialization.

        // This check forces the test to break here if someone changes the default.
        match aid.data.sender {
            ActorSender::Local { .. } => (),
            _ => panic!("The sender should be `Local`"),
        }

        let serialized = bincode::serialize(&aid).unwrap();
        let deserialized: ActorId = bincode::deserialize(&serialized).unwrap();

        // In this case the resulting aid should be identical to the serialized one because
        // we have the same actor system in a thread-local.
        assert!(ActorId::ptr_eq(&aid, &deserialized));

        // If we deserialize on another actor system in another thread it should be a remote aid.
        let handle = thread::spawn(move || {
            let system2 = ActorSystem::create(ActorSystemConfig::default());
            system2.init_current();
            // Connect the systems so the remote channel can be used.
            ActorSystem::connect_with_channels(system, system2);

            let deserialized: ActorId = bincode::deserialize(&serialized).unwrap();
            match deserialized.data.sender {
                ActorSender::Remote { .. } => {
                    assert_eq!(aid.uuid(), deserialized.uuid());
                    assert_eq!(aid.system_uuid(), deserialized.system_uuid());
                    assert_eq!(aid.name(), deserialized.name());
                }
                _ => panic!(
                    "The sender should be `Remote` but was {:?}",
                    aid.data.sender
                ),
            }
        });

        handle.join().unwrap();
    }

    /// Tests that an ActorId can be used as a message alone and inside another value.
    #[test]
    fn test_actor_id_as_message() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default());

        #[derive(Serialize, Deserialize)]
        enum Op {
            Aid(ActorId),
        }

        let aid = system
            .spawn()
            .with(
                0,
                |_state: &mut i32, context: &Context, message: &Message| {
                    if let Some(msg) = message.content_as::<ActorId>() {
                        assert!(ActorId::ptr_eq(&context.aid, &msg));
                    } else if let Some(msg) = message.content_as::<Op>() {
                        match &*msg {
                            Op::Aid(a) => assert!(ActorId::ptr_eq(&context.aid, &a)),
                        }
                    }
                    Ok(Status::Done)
                },
            )
            .unwrap();

        // Send a message to the actor.
        aid.send_new(aid.clone()).unwrap();
        aid.send_new(Op::Aid(aid.clone())).unwrap();

        // Wait for the Start and our message to get there because test is asynchronous.
        await_received(&aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// Tests that messages cannot be sent to an `aid` for an actor that has been stopped.
    #[test]
    fn test_cant_send_to_stopped() {
        let system = ActorSystem::create(ActorSystemConfig::default());
        let aid = system.spawn().with(0 as usize, simple_handler).unwrap();
        system.stop_actor(&aid);
        assert_eq!(false, system.is_actor_alive(&aid));

        // Make sure that the actor is actually stopped and can't get more messages.
        match aid.send(Message::new(42 as i32)) {
            Err(AxiomError::ActorAlreadyStopped) => assert!(true), // all OK!
            Ok(_) => panic!("Expected the actor to be shut down!"),
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    /// Tests that an actor that returns stop is actually stopped by the system.
    #[test]
    fn test_actor_returns_stop() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default());

        let aid = system
            .spawn()
            .with((), |_: &mut (), _: &Context, message: &Message| {
                if let Some(_msg) = message.content_as::<i32>() {
                    Ok(Status::Stop)
                } else if let Some(msg) = message.content_as::<SystemMsg>() {
                    match &*msg {
                        SystemMsg::Start => Ok(Status::Done),
                        m => panic!("unexpected message: {:?}", m),
                    }
                } else {
                    panic!("Unknown Message received");
                }
            })
            .unwrap();

        // Send a message to the actor.
        assert_eq!(true, system.is_actor_alive(&aid));
        aid.send_new(11 as i32).unwrap();
        await_received(&aid, 2, 1000).unwrap(); // Remember they always get `Start` as well!

        let max = Duration::from_millis(200);
        let start = Instant::now();
        loop {
            if !system.is_actor_alive(&aid) {
                break;
            } else if max < Instant::elapsed(&start) {
                panic!("Timed out waiting for actor to stop!");
            }
            thread::sleep(Duration::from_millis(1));
        }

        system.trigger_and_await_shutdown();
    }

    /// Tests that an actor cannot override the processing of a `Stop` message by returning a
    /// different `Status` variant other than `Stop`.
    #[test]
    fn test_actor_cannot_override_stop() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default());

        // FIXME (Issue #63) Create a processor type that doesn't use state.
        let aid = system
            .spawn()
            .with((), |_: &mut (), _: &Context, message: &Message| {
                if let Some(msg) = message.content_as::<SystemMsg>() {
                    match &*msg {
                        SystemMsg::Start => Ok(Status::Done),
                        SystemMsg::Stop => Ok(Status::Done),
                        m => panic!("unexpected message: {:?}", m),
                    }
                } else {
                    panic!("Unknown Message received");
                }
            })
            .unwrap();

        // Send a message to the actor.
        assert_eq!(true, system.is_actor_alive(&aid));
        aid.send_new(SystemMsg::Stop).unwrap();
        await_received(&aid, 2, 1000).unwrap(); // Remember they always get `Start` as well!

        let max = Duration::from_millis(200);
        let start = Instant::now();
        loop {
            if !system.is_actor_alive(&aid) {
                break;
            } else if max < Instant::elapsed(&start) {
                panic!("Timed out waiting for actor to stop!");
            }
            thread::sleep(Duration::from_millis(1));
        }

        system.trigger_and_await_shutdown();
    }
}
