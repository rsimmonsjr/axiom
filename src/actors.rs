//! Implements actors and related types of Axiom.
//!
//! These are the core components that make up the features of Axiom. The actor model is designed
//! to allow the user maximum flexibility. The actors can skip messages if they choose, enabling
//! them to work as a *finite state machine* without having to move messages around. Actors are
//! created by calling `system::spawn().with()` with any kind of function or closure that
//! implements the `Processor` trait.

use crate::message::ActorMessage;
use crate::prelude::*;
use futures::{FutureExt, Stream};
use log::{debug, error, trace, warn};
use secc::*;
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync, PhantomData};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use uuid::Uuid;
use std::ops::DerefMut;

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
    /// is returned the actor's [`Aid`] will no longer send any messages and the actor
    /// instance itself will be removed from the actors table in the [`ActorSystem`]. The user is
    /// advised to do any cleanup needed before returning [`Status::Stop`].
    Stop,
}

impl Status {
    /// Ergonomic shortcut for writing `(state, Status::Done)`
    pub fn done<T>(state: T) -> (T, Status) {
        (state, Status::Done)
    }

    /// Ergonomic shortcut for writing `(state, Status::Skip)`
    pub fn skip<T>(state: T) -> (T, Status) {
        (state, Status::Skip)
    }

    /// Ergonomic shortcut for writing `(state, Status::Reset)`
    pub fn reset<T>(state: T) -> (T, Status) {
        (state, Status::Reset)
    }

    /// Ergonomic shortcut for writing `(state, Status::Stop)`
    pub fn stop<T>(state: T) -> (T, Status) {
        (state, Status::Stop)
    }
}

/// Errors returned by the Aid
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AidError {
    /// This error is returned when a message cannot be converted to bincode. This will happen if
    /// the message is not Serde serializable and the user has not implemented ActorMessage to
    /// provide the correct implementation.
    CantConvertToBincode,

    /// This error is returned when a message cannot be converted from bincode. This will happen
    /// if the message is not Serde serializable and the user has not implemented ActorMessage to
    /// provide the correct implementation.
    CantConvertFromBincode,

    /// Error sent when attempting to send to an actor that has already been stopped. A stopped
    /// actor cannot accept any more messages and is shut down. The holder of an [`Aid`] to
    /// a stopped actor should throw the [`Aid`] away as the actor can never be started again.
    ActorAlreadyStopped,

    /// Error returned when an Aid is not local and a user is trying to do operations that
    /// only work on local Aid instances.
    AidNotLocal,

    /// Used when unable to send to an actor's message channel within the scheduled timeout
    /// configured in the actor system. This could result from the actor's channel being too
    /// small to accommodate the message flow, the lack of thread count to process messages fast
    /// enough to keep up with the flow or something wrong with the actor itself that it is
    /// taking too long to clear the messages.
    SendTimedOut(Aid),

    /// Used when unable to schedule the actor for work in the work channel. This could be a
    /// result of having a work channel that is too small to accommodate the number of actors
    /// being concurrently scheduled, not enough threads to process actors in the channel fast
    /// enough or simply an actor that misbehaves, causing dispatcher threads to take a lot of
    /// time or not finish at all.
    UnableToSchedule,
}

impl std::fmt::Display for AidError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for AidError {}

/// An enum that holds a sender for an actor.
///
/// An [`Aid`] uses the sender to send messages to the destination actor. Messages that are
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

/// The inner data of an [`Aid`].
///
/// This is kept separate to make serialization possible without duplicating all of the data
/// associated with the [`Aid`]. It also makes it easier when cloning and referring to an
/// `aid` as the user doesnt have to put `Arc<Aid>` all over their code.
struct AidData {
    /// See [`Aid::uuid()`]
    uuid: Uuid,
    /// See [`Aid::system_uuid()`]
    system_uuid: Uuid,
    /// See [`Aid::name()`]
    name: Option<String>,
    /// The handle to the sender side for the actor's message channel.
    sender: ActorSender,
}

/// A helper type to make [`Aid`] serialization cleaner.
#[derive(Serialize, Deserialize)]
struct AidSerializedForm {
    uuid: Uuid,
    system_uuid: Uuid,
    name: Option<String>,
}

/// Encapsulates an Actor ID and is used to send messages to the actor.
///
/// This is a unique reference to the actor within the entire cluster and can be used to send
/// messages to the actor regardless of location. The [`Aid`] does the heavy lifting of
/// deciding where the actor is and sending the message. However it is important that the user at
/// least has some notion of where the actor is for developing an efficient actor architecture.
/// This `aid` can also be serialized to a remote system and then back to the system hosting the
/// actor without issue. Often `Aid`s are passed around an actor system so this is a common
/// use case.
#[derive(Clone)]
pub struct Aid {
    /// Holds the actual data for the [`Aid`].
    data: Arc<AidData>,
}

impl Serialize for Aid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let serialized_form = AidSerializedForm {
            uuid: self.uuid(),
            system_uuid: self.system_uuid(),
            name: self.name(),
        };
        serialized_form.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Aid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized_form = AidSerializedForm::deserialize(deserializer)?;

        let system = ActorSystem::current();
        // We will look up the aid in the actor system and return a clone to the caller if found;
        // otherwise the Aid must be a on a remote actor system.
        match system.find_aid_by_uuid(&serialized_form.uuid) {
            Some(aid) => Ok(aid.clone()),
            None => {
                if serialized_form.system_uuid == system.uuid() {
                    // This could happen if you get an Aid to an actor that has already been
                    // stopped and then attempt to deserialize it.
                    Err(serde::de::Error::custom(format!(
                        "{:?}:{} system uuid matches but the uuid was not found.",
                        serialized_form.name, serialized_form.uuid,
                    )))
                } else if let Some(sender) = system.remote_sender(&serialized_form.system_uuid) {
                    // This serialized Aid is on another actor system so we will create a remote
                    // sender for the Aid and return the result.
                    Ok(Aid {
                        data: Arc::new(AidData {
                            uuid: serialized_form.uuid,
                            system_uuid: serialized_form.system_uuid,
                            name: serialized_form.name,
                            sender: ActorSender::Remote { sender: sender },
                        }),
                    })
                } else {
                    // This can happen if you get an Aid to deserialize that is on another actor
                    // system but the other actor system has been disconnected.
                    Err(serde::de::Error::custom(format!(
                        "{:?}:{} Unable to find a connection for remote system.",
                        serialized_form.name, serialized_form.uuid,
                    )))
                }
            }
        }
    }
}

impl std::cmp::PartialEq for Aid {
    fn eq(&self, other: &Self) -> bool {
        self.data.uuid == other.data.uuid && self.data.system_uuid == other.data.system_uuid
    }
}

impl std::cmp::Eq for Aid {}

impl std::cmp::PartialOrd for Aid {
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

impl std::cmp::Ord for Aid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("Aid::partial_cmp() returned None; can't happen")
    }
}

impl Aid {
    /// Attempts to send a message to the actor with the given [`Aid`] and returns
    /// `std::Result::Ok` when the send was successful or a `std::Result::Err<AxiomError>`
    /// if something went wrong with the send. Note that if a user just calls `send(msg).unwrap()`,
    /// a panic could take down the dispatcher thread and thus eventually hang the process.
    ///
    /// # Examples
    /// ```
    /// use axiom::prelude::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// let aid = system
    ///     .spawn()
    ///     .with(
    ///         0 as usize,
    ///         |state: usize, context: Context, message: Message| async move {
    ///             if let Some(_) = message.content_as::<i32>() {
    ///                 context.system.trigger_shutdown();
    ///             }
    ///             Ok(Status::done(state))
    ///        },
    ///     )
    ///     .unwrap();
    ///
    /// match aid.send(Message::new(11)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    ///
    /// system.await_shutdown(None);
    /// ```
    pub fn send(&self, message: Message) -> Result<(), AidError> {
        match &self.data.sender {
            ActorSender::Local {
                stopped,
                sender,
                system,
            } => {
                if stopped.load(Ordering::Relaxed) {
                    Err(AidError::ActorAlreadyStopped)
                } else {
                    match sender.send_await_timeout(message, system.config().send_timeout) {
                        Ok(_) => {
                            if sender.receivable() == 1 {
                                system.schedule(self.clone());
                            };
                            Ok(())
                        }
                        Err(_) => Err(AidError::SendTimedOut(self.clone())),
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

    /// Shortcut for calling `send(Message::from_arc(arc))` This method will internally wrap the
    /// `Arc` passed into a `Message` and try to send it. Note that using this method is much
    /// more efficient than `send_new` if you want to send an `Arc` that you already have.
    /// The `Arc` sent will be transferred to the ownership of the `Aid`.
    ///
    /// ```
    /// use axiom::prelude::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// let aid = system
    ///     .spawn()
    ///     .with(
    ///         0 as usize,
    ///         |state: usize, context: Context, message: Message| async move {
    ///             if let Some(_) = message.content_as::<i32>() {
    ///                 context.system.trigger_shutdown();
    ///             }
    ///             Ok(Status::done(state))
    ///        },
    ///     )
    ///     .unwrap();
    ///
    /// let arc = Arc::new(11 as i32);
    /// match aid.send_arc(arc.clone()) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    ///
    /// system.await_shutdown(None);
    /// ```
    pub fn send_arc<T>(&self, value: Arc<T>) -> Result<(), AidError>
    where
        T: 'static + ActorMessage,
    {
        self.send(Message::from_arc(value))
    }

    /// Shortcut for calling `send(Message::new(value))` This method will internally wrap
    /// whatever it is passed into a `Message` and try to send it. This method would not be
    /// appropriate if you want to re-send a message as it would wrap the message again with the
    /// same result as if the the code called `aid.send(Message::new(Message::new(value)))`.
    /// If the code wishes to resend a message it should just call just call `send(msg)`.
    ///
    /// ```
    /// use axiom::prelude::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// let aid = system
    ///     .spawn()
    ///     .with(
    ///         0 as usize,
    ///         |state: usize, context: Context, message: Message| async move {
    ///             if let Some(_) = message.content_as::<i32>() {
    ///                 context.system.trigger_shutdown();
    ///             }
    ///             Ok(Status::done(state))
    ///        },
    ///     )
    ///     .unwrap();
    ///
    /// match aid.send_new(11) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    ///
    /// system.await_shutdown(None);
    /// ```
    pub fn send_new<T>(&self, value: T) -> Result<(), AidError>
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
    ///
    /// # Examples
    /// ```
    /// use axiom::prelude::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// let aid = system
    ///     .spawn()
    ///     .with(
    ///         0 as usize,
    ///         |state: usize, context: Context, message: Message| async move {
    ///             if let Some(_) = message.content_as::<i32>() {
    ///                 context.system.trigger_shutdown();
    ///             }
    ///             Ok(Status::done(state))
    ///        },
    ///     )
    ///     .unwrap();
    ///
    /// match aid.send_after(Message::new(11), Duration::from_millis(1)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    ///
    /// system.await_shutdown(None);
    /// ```
    pub fn send_after(&self, message: Message, duration: Duration) -> Result<(), AidError> {
        match &self.data.sender {
            ActorSender::Local {
                stopped, system, ..
            } => {
                if stopped.load(Ordering::Relaxed) {
                    Err(AidError::ActorAlreadyStopped)
                } else {
                    system.send_after(message, self.clone(), duration);
                    Ok(())
                }
            }
            ActorSender::Remote { sender } => {
                if let Err(err) = sender.send_await(WireMessage::DelayedActorMessage {
                    duration,
                    actor_uuid: self.data.uuid,
                    system_uuid: self.data.system_uuid,
                    message,
                }) {
                    // Right now, this is the full extent of errors, but if that should change, it
                    // should create a compiler error.
                    return match err {
                        SeccErrors::Full(_) | SeccErrors::Empty => Ok(()),
                    };
                }
                Ok(())
            }
        }
    }

    /// Shortcut for calling `send_after(Message::from_arc(arc))` This method will internally
    /// wrap the `Arc` passed into a `Message` and try to send it. Note that using this method is
    /// much more efficient than `send_new_after` if you want to send an `Arc` that you already
    /// have.
    ///
    /// # Examples
    /// ```
    /// use axiom::prelude::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// let aid = system
    ///     .spawn()
    ///     .with(
    ///         0 as usize,
    ///         |state: usize, context: Context, message: Message| async move {
    ///             if let Some(_) = message.content_as::<i32>() {
    ///                 context.system.trigger_shutdown();
    ///             }
    ///             Ok(Status::done(state))
    ///        },
    ///     )
    ///     .unwrap();
    ///
    /// let arc = Arc::new(11);
    /// match aid.send_arc_after(arc.clone(), Duration::from_millis(1)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    ///
    /// system.await_shutdown(None);
    /// ```
    pub fn send_arc_after<T>(&self, value: Arc<T>, duration: Duration) -> Result<(), AidError>
    where
        T: 'static + ActorMessage,
    {
        self.send_after(Message::from_arc(value), duration)
    }

    /// Shortcut for calling `send_after(Message::new(value))` This method will internally wrap
    /// whatever it is passed into a `Message` and try to send it. This method would not be
    /// appropriate if you want to re-send a message as it would wrap the message again with the
    /// same result as if the the code called `aid.send_after(Message::new(Message::new(value)))`.
    /// If the code wishes to resend a message it should just call just call `send(msg)`.
    ///
    /// # Examples
    /// ```
    /// use axiom::prelude::*;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
    ///
    /// let aid = system
    ///     .spawn()
    ///     .with(
    ///         0 as usize,
    ///         |state: usize, context: Context, message: Message| async move {
    ///             if let Some(_) = message.content_as::<i32>() {
    ///                 context.system.trigger_shutdown();
    ///             }
    ///             Ok(Status::done(state))
    ///        },
    ///     )
    ///     .unwrap();
    ///
    /// match aid.send_new_after(11, Duration::from_millis(1)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    ///
    /// system.await_shutdown(None);
    /// ```
    pub fn send_new_after<T>(&self, value: T, duration: Duration) -> Result<(), AidError>
    where
        T: 'static + ActorMessage,
    {
        self.send_after(Message::new(value), duration)
    }

    /// The unique UUID for this actor within the entire cluster. The UUID for an [`Aid`]
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

    /// Returns the name assigned to the Aid if it is not a `None` and otherwise returns the
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

    /// Determines how many messages the actor with the [`Aid`] has been sent. This method works only
    /// for local [`Aid`]s, remote [`Aid`]s will return an error if this is called.
    pub fn sent(&self) -> Result<usize, AidError> {
        match &self.data.sender {
            ActorSender::Local { sender, .. } => Ok(sender.sent()),
            _ => Err(AidError::AidNotLocal),
        }
    }

    /// Determines how many messages the actor with the [`Aid`] has received. This method works only
    /// for local [`Aid`]s, remote [`Aid`]s will return an error if this is called.
    pub fn received(&self) -> Result<usize, AidError> {
        match &self.data.sender {
            ActorSender::Local { sender, .. } => Ok(sender.received()),
            _ => Err(AidError::AidNotLocal),
        }
    }

    /// Marks the actor referenced by the [`Aid`] as stopped and puts mechanisms in place to
    /// cause no more messages to be sent to the actor. Note that once stopped, an [`Aid`] can
    /// never be started again. Note that this is `pub(crate)` because the user should be sending
    /// `SystemMsg::Stop` to actors or, at worst, calling `ActorSystem::stop()` to stop an actor.
    pub(crate) fn stop(&self) -> Result<(), AidError> {
        match &self.data.sender {
            ActorSender::Local { stopped, .. } => {
                trace!("Stopping local Actor");
                stopped.fetch_or(true, Ordering::AcqRel);
                Ok(())
            }
            _ => Err(AidError::AidNotLocal),
        }
    }

    /// Checks to see if the left and right aid actually point at the exact same actor.
    pub fn ptr_eq(left: &Aid, right: &Aid) -> bool {
        Arc::ptr_eq(&left.data, &right.data)
    }
}

impl std::fmt::Debug for Aid {
    fn fmt(&self, formatter: &'_ mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            formatter,
            "Aid{{id: {}, system_uuid: {}, name: {:?}, is_local: {}}}",
            self.data.uuid.to_string(),
            self.data.system_uuid.to_string(),
            self.data.name,
            self.is_local()
        )
    }
}

impl std::fmt::Display for Aid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.data.name {
            Some(name) => write!(f, "{}:{}", name, self.data.uuid),
            None => write!(f, "{}", self.data.uuid),
        }
    }
}

impl Hash for Aid {
    fn hash<H: Hasher>(&self, state: &'_ mut H) {
        self.data.uuid.hash(state);
        self.data.system_uuid.hash(state);
    }
}

/// A context that is passed to the processor to give immutable access to elements of the actor
/// system to the implementor of an actor's processor.
#[derive(Clone, Debug)]
pub struct Context {
    pub aid: Aid,
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
/// the actor based on the messages passed to the actor. The processor should return the status of
/// the actor, as well as the potentially modified state. If the actor returns `Err` then it will be
/// stopped as if the actor had returned `Stop`. The processor takes three arguments:
/// * `state`   - The current state of the actor.
/// * `context` - The immutable context for this actor and its system.
/// * `message` - The current message to process.
/// The actor must return the state on success as a `(State, Status)` tuple. See [`Status`] for
/// helper methods for returns.
pub trait Processor<S: Send + Sync, R: Future<Output = ActorResult<S>> + Send + 'static>:
    (FnMut(S, Context, Message) -> R) + Send + Sync
{
}

// Allows any static function or closure, to be used as a Processor.
impl<F, S, R> Processor<S, R> for F
where
    S: Send + Sync,
    R: Future<Output = ActorResult<S>> + Send + 'static,
    F: (FnMut(S, Context, Message) -> R) + Send + Sync + 'static,
{
}

pub(crate) type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<Status, StdError>> + Send + 'static>>;

/// This is the internal type for the handler that will manage the state for the actor using the
/// user-provided message processor.
pub(crate) trait Handler:
    (FnMut(Context, Message) -> HandlerFuture) + Send + Sync + 'static
{
}

// Allows any static function or closure, to be used as a Handler.
impl<F> Handler for F where F: (FnMut(Context, Message) -> HandlerFuture) + Send + Sync + 'static {}

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
    // FIXME Consider implementing `using` to spawn a stateless actor.
    pub fn with<F, S, R>(self, state: S, processor: F) -> Result<Aid, SystemError>
    where
        S: Send + Sync + 'static,
        R: Future<Output = ActorResult<S>> + Send + 'static,
        F: Processor<S, R> + 'static,
    {
        let (actor, stream) = Actor::new(self.system.clone(), &self, state, processor);
        debug!("Actor created: {}", actor.context.aid.uuid());
        self.system.register_actor(actor, stream)
    }

    /// Set the name of the actor to the given string.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
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

pub(crate) struct ActorStream {
    /// The context data for the actor containing the [`Aid`] as well as other immutable data.
    pub context: Context,
    /// Receiver for the actor's message channel.
    receiver: SeccReceiver<Message>,
    /// An async function processing a message sent to the actor, wrapped in a closure to
    /// erase the state type that the actor is managing. The inner state is Arc<Mutex>'d to
    /// ensure the Actor is synchronous in relation to itself.
    handler: Box<dyn Handler>,
    /// The pending result of the current handler invocation.
    pending: Option<HandlerFuture>,
    /// Set to true when the stream receives SystemMsg::Stop
    stopping: bool,
}

/// The implementation of the actor in the system. Please see overview and library documentation
/// for more detail.
pub(crate) struct Actor {
    /// The context data for the actor containing the `aid` as well as other immutable data.
    pub context: Context,
}

/// This is exclusively used in contexts we can be more than confident are safe.
/// This is required for holding onto the Actor State.
#[repr(transparent)]
struct SendSyncPointer<T>(*mut T);

/// This is exclusively used in contexts we can be more than confident are safe.
/// This is required for holding onto the Actor State.
#[repr(transparent)]
struct SendSyncUnsafeCell<T>(UnsafeCell<T>);

unsafe impl<T> Send for SendSyncPointer<T> {}
unsafe impl<T> Sync for SendSyncPointer<T> {}
unsafe impl<T> Send for SendSyncUnsafeCell<T> {}
unsafe impl<T> Sync for SendSyncUnsafeCell<T> {}

impl Actor {
    /// Creates a new actor on the given actor system with the given processor function. The user
    /// will pass the initial state of the actor as well as the processor that will be used to
    /// process messages sent to the actor.
    pub(crate) fn new<F, S, R>(
        system: ActorSystem,
        builder: &ActorBuilder,
        state: S,
        mut processor: F,
    ) -> (Arc<Actor>, ActorStream)
    where
        S: Send + Sync + 'static,
        R: Future<Output = ActorResult<S>> + Send + 'static,
        F: Processor<S, R> + 'static,
    {
        let (sender, receiver) = secc::create::<Message>(
            builder
                .channel_size
                .unwrap_or(system.config().message_channel_size),
            Duration::from_millis(10),
        );

        // The sender will be put inside the actor id.
        let aid = Aid {
            data: Arc::new(AidData {
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
        // Here we wrap the state in an UnsafeCell so we can do more performant retention of state.
        // While it might normally be prudent to have the unsafe block encompass the code that keeps
        // the safe guarantees, that isn't possible here as the guarantees are the same that keep
        // the Actor Model sound, and are stretched across multiple parts of the infrastructure. If
        // the unsoundness of this were to leak, there would be problems beyond this one UnsafeCell.
        let state_box = SendSyncUnsafeCell(UnsafeCell::new(Some(state)));

        let handler = Box::new(move |ctx: Context, msg: Message| {
            let state = SendSyncPointer(state_box.0.get());
            let s = unsafe { (*state.0).take() }.expect("State cell was empty");
            let future = catch_unwind(AssertUnwindSafe(|| (processor)(s, ctx, msg)));
            async move {
                let result = match future {
                    Ok(future) => {
                        let pending = PanicSafeFuture {
                            future: Box::pin(future),
                            __state_phantom_data: PhantomData::<S>::default(),
                        };
                        pending.await
                    },
                    Err(err) => {
                        warn!("Actor panicked! Catching as error");
                        Err(Panic::from(err).into())
                    }
                };
                result.map(|(s, status)| {
                    unsafe { ptr::write(state.0, Some(s)) };
                    status
                })
            }
                .boxed()
        });

        // This is the receiving side of the actor which holds the processor wrapped in the
        // handler type.
        let context = Context { aid, system };

        let actor = Actor {
            context: context.clone(),
        };

        let stream = ActorStream {
            context,
            receiver,
            handler,
            pending: None,
            stopping: false,
        };

        (Arc::new(actor), stream)
    }
}

struct PanicSafeFuture<S, F>
where
    F: Future<Output=ActorResult<S>>,
{
    future: Pin<Box<F>>,
    __state_phantom_data: PhantomData<S>,
}

impl<S, F: Future<Output=ActorResult<S>>> Future for PanicSafeFuture<S, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        let x = self.future.as_mut();
        match catch_unwind(AssertUnwindSafe(|| x.poll(cx))) {
            Ok(result) => result,
            Err(p) => Poll::Ready(Err(Panic::from(p).into())),
        }
    }
}

impl ActorStream {
    /// This takes the result and executes the subsequent steps in respect to the result. Namely,
    /// handling the Actor's message channel and informing the ActorSystem of errors. Returns
    /// whether the Actor is stopping or not.
    pub(crate) fn handle_result(&self, result: Result<Status, StdError>) -> bool {
        let mut stopping = false;

        match result {
            Ok(Status::Done) => {
                trace!(
                    "Actor {} finished processing a message",
                    self.context.aid.uuid()
                );
                self.receiver.pop().unwrap()
            }
            Ok(Status::Skip) => {
                trace!(
                    "Actor {} skipped processing a message",
                    self.context.aid.uuid()
                );
                self.receiver.skip().unwrap()
            }
            Ok(Status::Reset) => {
                trace!(
                    "Actor {} finished processing a message and reset the cursor",
                    self.context.aid.uuid()
                );
                self.receiver.pop().unwrap();
                self.receiver.reset_skip().unwrap();
            }
            Ok(Status::Stop) => {
                debug!("Actor \"{}\" stopping", self.context.aid.name_or_uuid());
                self.receiver.pop().unwrap();
                self.context
                    .system
                    .internal_stop_actor(&self.context.aid, None);
                stopping = true;
            }
            Err(e) => {
                self.receiver.pop().unwrap();
                error!(
                    "[{}] returned an error when processing: {}",
                    self.context.aid, &e
                );
                self.context
                    .system
                    .internal_stop_actor(&self.context.aid, e);
                stopping = true;
            }
        }

        stopping
    }

    fn overwrite_on_stop(&self, result: Result<Status, StdError>) -> Result<Status, StdError> {
        match self.stopping {
            true => result.map(|_| Status::Stop),
            false => result,
        }
    }
}

/// The meat of the Actor's handling
impl Stream for ActorStream {
    type Item = Result<Status, StdError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        trace!("Actor {} is being polled", self.context.aid.name_or_uuid());
        // If we have a pending future, that's what we poll.
        if let Some(mut pending) = self.pending.take() {
            // Poll, ensure we respect stopping condition.
            pending
                .as_mut()
                .poll(cx)
                .map(|r| Some(self.overwrite_on_stop(r)))
        } else {
            // Are we stopped? If so, we should not have been polled, panic. This is only acceptable
            // because it means a bug in the Executor or Reactor.
            if self.stopping {
                panic!("Stopped ActorStream was polled after stopping. Please open a bug report.")
            }
            // Else, we go for another.
            match self.receiver.peek() {
                Ok(msg) => {
                    // We're stopping after this future, mark as such
                    if let Some(m) = msg.content_as::<SystemMsg>() {
                        if let SystemMsg::Stop = *m {
                            trace!("Actor {} received stop message", self.context.aid.uuid());
                            self.stopping = true;
                        }
                    }

                    // Get the next future
                    let ctx = self.context.clone();
                    let mut future = (&mut self.handler)(ctx, msg.clone());
                    // Just. give it a ~~wave~~ poll!!
                    match future.as_mut().poll(cx) {
                        Poll::Ready(r) => Poll::Ready(Some(self.overwrite_on_stop(r))),
                        Poll::Pending => {
                            trace!("Actor {} is pending", self.context.aid.uuid());
                            self.pending = Some(future);
                            Poll::Pending
                        }
                    }
                }
                Err(err) => match err {
                    // Ready(None) is standard for "Stream is depleted". The stream is effectively
                    // monadic around the message queue, so if the channel is depleted, the stream
                    // is as well. `Full` is non-contextual.
                    //
                    // While this is exhaustive, we're avoiding a catchall to in anticipation of
                    // future Secc errors we would *want* to handle.
                    SeccErrors::Empty | SeccErrors::Full(_) => Poll::Ready(None),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use log::*;
    use std::thread;
    use std::time::Instant;

    /// This is identical to the documentation but here so that its formatted by rust and we can
    /// copy paste this into the docs. It's also easier to debug here.
    #[test]
    fn test_send_examples() {
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        let aid = system
            .spawn()
            .with((), |_: (), context: Context, message: Message| {
                async move {
                    if let Some(_) = message.content_as::<i32>() {
                        context.system.trigger_shutdown();
                    }
                    Ok(Status::done(()))
                }
            })
            .unwrap();

        match aid.send(Message::new(11)) {
            Ok(_) => info!("OK Then!"),
            Err(e) => info!("Ooops {:?}", e),
        }

        system.await_shutdown(None);
    }

    /// Tests that unserializable messages can be sent locally.
    #[test]
    fn test_send_unserializable() {
        use std::time::Duration;

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        // We declare a message type that we know is unserializable and then we implement the
        // `ActorMessage` with the default methods which error on attempting to serialize. Note
        // that this could be used for sending any unserialized type in other libs by simply
        // wrapping that value in a user-made struct.
        struct Foo {}
        impl ActorMessage for Foo {}
        assert!(Foo {}.to_bincode().is_err());
        assert!(Foo::from_bincode(&vec![1, 2, 3]).is_err());

        let aid = system
            .spawn()
            .with((), move |_state: (), context: Context, message: Message| {
                async move {
                    if let Some(_) = message.content_as::<Foo>() {
                        context.system.trigger_shutdown();
                    }
                    Ok(Status::done(()))
                }
            })
            .unwrap();

        aid.send(Message::new(Foo {})).unwrap();
        await_received(&aid, 2, 1000).unwrap();

        system.await_shutdown(Duration::from_millis(1000));
    }

    /// This test verifies that an actor's functions that retrieve basic info are working for
    /// unnamed actors.
    #[test]
    fn test_basic_info_unnamed() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().with((), simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        assert_eq!(system.uuid(), aid.data.system_uuid);
        assert_eq!(aid.data.system_uuid, aid.system_uuid());
        assert_eq!(aid.data.uuid, aid.uuid());
        assert_eq!(None, aid.data.name);
        assert_eq!(aid.data.name, aid.name());

        system.trigger_and_await_shutdown(None);
    }

    /// This test verifies that an actor's functions that retrieve basic info are working for
    /// named actors.
    #[test]
    fn test_basic_info_named() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().name("A").with((), simple_handler).unwrap();
        await_received(&aid, 1, 1000).unwrap();
        assert_eq!(system.uuid(), aid.data.system_uuid);
        assert_eq!(aid.data.system_uuid, aid.system_uuid());
        assert_eq!(aid.data.uuid, aid.uuid());
        assert_eq!(Some("A".to_string()), aid.data.name);
        assert_eq!(aid.data.name, aid.name());

        system.trigger_and_await_shutdown(None);
    }

    /// Tests serialization and deserialization of `Aid`s. This verifies that deserialized
    /// `aid`s on the same actor system should just be the same `aid` as well as the fact that
    /// when deserialized on other actor systems the `aid`'s sender should be a remote aid.
    ///
    /// FIXME (Issue #70) Return error when deserializing an Aid if a remote is not connected
    /// instead of panic.
    #[test]
    fn test_aid_serialization() {
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid1 = system.spawn().with((), simple_handler).unwrap();
        system.init_current(); // Required by Aid serialization.

        // This check forces the test to break here if someone changes the default.
        match aid1.data.sender {
            ActorSender::Local { .. } => (),
            _ => panic!("The sender should be `Local`"),
        }

        let aid1_serialized = bincode::serialize(&aid1).unwrap();
        let aid1_deserialized: Aid = bincode::deserialize(&aid1_serialized).unwrap();

        // In this case the resulting Aid should be identical to the serialized one because
        // we have the same actor system in a thread-local.
        assert!(Aid::ptr_eq(&aid1, &aid1_deserialized));

        // Spawn an actor and serialize the value but then stop the actor and try and deserialize
        // and we should get an error.
        let aid2 = system.spawn().with((), simple_handler).unwrap();
        let aid2_serialized = bincode::serialize(&aid2).unwrap();
        system.stop_actor(&aid2);
        let aid2_deserialized = bincode::deserialize::<Aid>(&aid2_serialized);
        assert!(aid2_deserialized.is_err());

        // If we deserialize on another actor system in another thread it should be a remote aid.
        let handle = thread::spawn(move || {
            let system2 = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
            system2.init_current();
            // Connect the systems so the remote channel can be used.
            ActorSystem::connect_with_channels(&system, &system2);

            let deserialized: Aid = bincode::deserialize(&aid1_serialized).unwrap();
            match deserialized.data.sender {
                ActorSender::Remote { .. } => {
                    assert_eq!(aid1.uuid(), deserialized.uuid());
                    assert_eq!(aid1.system_uuid(), deserialized.system_uuid());
                    assert_eq!(aid1.name(), deserialized.name());
                }
                _ => panic!(
                    "The sender should be `Remote` but was {:?}",
                    aid1.data.sender
                ),
            }

            // Disconnecting the remote then attempting to deserialize the Aid should result in a
            // deserialization error.
            system2.disconnect(aid1.system_uuid()).unwrap();
            let aid1_deserialized = bincode::deserialize::<Aid>(&aid1_serialized);
            assert!(aid1_deserialized.is_err());
        });

        handle.join().unwrap();
    }

    /// Tests that an Aid can be used as a message alone and inside another value.
    #[test]
    fn test_aid_as_message() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let tracker = AssertCollect::new();
        let t = tracker.clone();

        #[derive(Serialize, Deserialize)]
        enum Op {
            Aid(Aid),
        }

        let aid = system
            .spawn()
            .with(t, |t: AssertCollect, context: Context, message: Message| {
                async move {
                    if let Some(msg) = message.content_as::<Aid>() {
                        t.assert(Aid::ptr_eq(&context.aid, &msg), "Aid mutated in transit");
                    } else if let Some(msg) = message.content_as::<Op>() {
                        match &*msg {
                            Op::Aid(a) => {
                                t.assert(Aid::ptr_eq(&context.aid, &a), "Aid mutated in transit")
                            }
                        }
                    }
                    Ok(Status::done(t))
                }
            })
            .unwrap();

        // Send a message to the actor.
        aid.send_new(aid.clone()).unwrap();
        aid.send_new(Op::Aid(aid.clone())).unwrap();

        // Wait for the Start and our message to get there because test is asynchronous.
        await_received(&aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown(None);
        tracker.collect();
    }

    /// Tests that messages cannot be sent to an `aid` for an actor that has been stopped.
    #[test]
    fn test_cant_send_to_stopped() {
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let aid = system.spawn().with((), simple_handler).unwrap();
        system.stop_actor(&aid);
        assert_eq!(false, system.is_actor_alive(&aid));

        // Make sure that the actor is actually stopped and can't get more messages.
        match aid.send(Message::new(42 as i32)) {
            Err(AidError::ActorAlreadyStopped) => assert!(true), // all OK!
            Ok(_) => panic!("Expected the actor to be shut down!"),
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    /// Tests that an actor that returns stop is actually stopped by the system.
    #[test]
    fn test_actor_returns_stop() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let tracker = AssertCollect::new();
        let t = tracker.clone();

        let aid = system
            .spawn()
            .with(t, |t: AssertCollect, _: Context, message: Message| {
                async move {
                    if let Some(_msg) = message.content_as::<i32>() {
                        Ok(Status::stop(t))
                    } else if let Some(msg) = message.content_as::<SystemMsg>() {
                        match &*msg {
                            SystemMsg::Start => Ok(Status::done(t)),
                            m => t.panic(format!("unexpected message: {:?}", m)),
                        }
                    } else {
                        t.panic("Unknown Message received")
                    }
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
            sleep(1);
        }

        system.trigger_and_await_shutdown(None);
        tracker.collect();
    }

    /// Tests that an actor cannot override the processing of a `Stop` message by returning a
    /// different `Status` variant other than `Stop`.
    #[test]
    fn test_actor_cannot_override_stop() {
        init_test_log();
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let tracker = AssertCollect::new();
        let t = tracker.clone();

        // FIXME (Issue #63) Create a processor type that doesn't use state.
        let aid = system
            .spawn()
            .with(t, |t: AssertCollect, _: Context, message: Message| {
                async move {
                    if let Some(msg) = message.content_as::<SystemMsg>() {
                        match &*msg {
                            SystemMsg::Start => Ok(Status::done(t)),
                            SystemMsg::Stop => Ok(Status::done(t)),
                            m => t.panic(format!("unexpected message: {:?}", m)),
                        }
                    } else {
                        t.panic("Unknown Message received")
                    }
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
            sleep(1);
        }
        system.trigger_and_await_shutdown(None);
        tracker.collect();
    }
}
