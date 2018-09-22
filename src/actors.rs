/// Implements actors and the actor system.
use secc;
use secc::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// This is a type used by the system for sending a message through a channel to the actor.
/// All messages are sent as this type and it is up to the message handler to cast the message
/// properly and deal with it. It is recommended that the user make use of the [`dispatch`]
/// utility function to help in the casting and calling operation.
pub type Message = dyn Any + Sync + Send;

/// A result returned by the dequeue function that indicates the disposition of the message.
#[derive(Debug, Eq, PartialEq)]
pub enum DequeueResult {
    /// The message was processed and can be removed from the channel. Note that this doesn't
    /// necessarily mean that anything was done with the message, just that it can be removed.
    /// It is up to the message handler to decide what if anything to do with the message.
    Processed,
    /// The message was skipped and should remain in the queue and the dequeue should loop
    /// to fetch the next pending message; once a message is skipped then a skip tail will
    /// be created in the channel that will act as the actual tail until the [`SkipCleared']
    /// result is returned from a message handler. This enables an actor to skip messages while
    /// working on a process and then clear the skip buffer and resume normal processing.
    Skipped,
    /// Clears the skip tail on the channel. A skip tail is present when a message has been
    /// skipped by returning [`Skipped`] If no skip tail is set than this result is semantically
    /// the same as [`Processed`].
    SkipCleared,
    /// The message generated an error of some kind and a panic should occur.
    Panic,
}

/// Attempts to downcast the Arc<Message> to the specific arc type of the handler and then call
/// that handler with the message. If the dispatch is successful it will return the result of the
/// call to the caller, otherwise it will return none back to the user. Note that the user of this
/// function should clone the message or likely there will be borrow issues.
///
/// ## Examples
///
/// Dispatches the message to one of 3 different functions that handle different types or returns
/// the Panic result if the code cannot dispatch the message.
pub fn dispatch<T: 'static, S, R>(
    state: &mut S,
    message: Arc<Message>,
    mut func: impl FnMut(&mut S, &T) -> R,
) -> Option<R> {
    match message.downcast_ref::<T>() {
        Some(x) => Some(func(state, x)),
        None => None,
    }
}

/// An enum that holds a sender for an actor. This is usually wrapped in an actor id.
pub enum ActorSender {
    /// A Sender used for sending messages to local actors.
    Local(SeccSender<Arc<Message>>),
    Remote,
    // FIXME Create a remote sender.
}

/// Encapsulates an ID to an actor.
pub struct ActorId {
    /// The unique id for this actor on this node.
    id: Uuid,
    /// The id for the node that this actor is on.
    node_id: Uuid,
    /// The handle to the sender side for the actor's message channel. Note that this
    /// value will be [None] if this actor id refers to an actor on another node. In that
    /// case a tell will serialize the object to the remote node and then use the [ActorId]
    /// there instead to send the message.
    sender: ActorSender,
}

impl ActorId {
    /// Tells the actor a message and immediately returns. This method takes the message
    /// to send and returns the number of messages currently in the actor's channel.
    pub fn send(&self, message: Arc<Message>) -> Result<usize, SeccErrors<Arc<Message>>> {
        match &self.sender {
            ActorSender::Local(sender) => sender.send_await(message),
            _ => unimplemented!("Remote actors not implemented currently."),
        }
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, f: &'_ mut Formatter) -> fmt::Result {
        write!(
            f,
            "ActorId{{ id: {}, node_id: {} }}",
            self.id.to_string(),
            self.node_id.to_string()
        )
    }
}

impl PartialEq for ActorId {
    fn eq(&self, other: &ActorId) -> bool {
        self.id == other.id && self.node_id == other.node_id
    }
}

impl Eq for ActorId {}

impl Hash for ActorId {
    fn hash<H: Hasher>(&self, state: &'_ mut H) {
        self.id.hash(state);
        self.node_id.hash(state);
    }
}

/// Context for an actor storing the information core to the actor and its management.
pub struct ActorContext {
    /// The id of this actor.
    aid: Arc<ActorId>,
    /// The receiver part of the channel being sent to the actor.
    receiver: SeccReceiver<Arc<Message>>,
}

impl ActorContext {
    /// Creates a new actor context with the given actor id.
    pub fn new(node_id: Uuid) -> ActorContext {
        // FIXME Allow user to pass a mailbox size and potentially a grow function.
        let (tx, receiver) = secc::create::<Arc<Message>>(32);
        let id = Uuid::new_v4();
        let sender = ActorSender::Local(tx);
        let aid = Arc::new(ActorId {
            id,
            node_id,
            sender,
        });
        ActorContext { aid, receiver }
    }

    /// Receives the message on the actor and calls the handler for that actor. This function
    /// will be typically called by the scheduler to process messages in the actor's channel.
    fn receive(&self) -> Result<Arc<Message>, SeccErrors<Arc<Message>>> {
        self.receiver.receive()
    }

}

/// This is the core Actor type in the actor system. The user should see the `README.md` for
/// a detailed description of an actor and the Actor model. Callers communicate with the actor
/// by means of a channel which holds the messages in the order received. The messages are
/// then processed by the message handler and the appropriate state of the message is returned.
pub trait Actor: Send + Sync {
    /// Fetches the context for this actor.
    fn context(&self) -> &ActorContext;

    /// Fetches the mutable context for this actor.
    fn context_mut(&mut self) -> &mut ActorContext;

    /// Process the the message and return the result of the dispatching.
    fn handle_message(&mut self, message: Arc<Message>) -> DequeueResult;

    /// fetches the actor id for this actor.
    fn aid(&self) -> Arc<ActorId> {
        self.context().aid.clone()
    }

    /// Returns the total number of messages that have been sent to this actor.
    fn sent(&self) -> usize {
        self.context().receiver.enqueued()
    }

    /// Returns the total number of messages that have been received by this actor.
    fn received(&self) -> usize {
        self.context().receiver.dequeued()
    }

    /// Returns the total number of messages that are currently pending in the actor's channel.
    fn pending(&self) -> usize {
        self.context().receiver.length()
    }
}

/// An actor system that contains and manages the actors spawned inside it.
pub struct ActorSystem {
    /// Id for this actor system and node.
    node_id: Uuid,
    /// Holds a table of actors in the system keyed by their actor id (aid).
    actors_by_aid: HashMap<Arc<ActorId>, Arc<dyn Actor>>,
}

impl ActorSystem {
    pub fn new() -> ActorSystem {
        ActorSystem {
            node_id: Uuid::new_v4(),
            actors_by_aid: HashMap::new(),
        }
    }

    /// Spawns a new actor using the given function to create the actor from the passed context.
    /// This is seaprated from the raw spawn method to facilitate various means of tracking actors
    /// as well as to make testing easier.
    fn spawn_internal<T: Actor + 'static, F: Fn(ActorContext) -> T>(&mut self, f: F) -> T {
        let context = ActorContext::new(self.node_id);
        f(context)
    }

    /// Spawns a new actor using the given function to create the actor from the passed context.
    pub fn spawn<T: Actor + 'static, F: Fn(ActorContext) -> T>(&mut self, f: F) -> Arc<ActorId> {
        let actor = self.spawn_internal(f);
        let aid = actor.aid();
        self.actors_by_aid.insert(aid.clone(), Arc::new(actor));
        aid
    }
}

/// Holds an instance to the default actor system. Note that although a user could theoretically create another actor
/// system, there owuld be almost no reason to do so.
lazy_static! {
    static ref ACTOR_SYSTEM: Mutex<ActorSystem> = Mutex::new(ActorSystem::new());
}

/// Spawn an actor on the default actor system using the given function to create the actor
/// from the passed context.
pub fn spawn<T: Actor + 'static, F: Fn(ActorContext) -> T>(f: F) -> Arc<ActorId> {
    let mut system = ACTOR_SYSTEM.lock().unwrap();
    system.spawn(f)
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Operation {
        Inc,
        Dec,
    }

    /// A struct of a counter.
    struct Counter {
        context: ActorContext,
        count: i32,
    }

    impl Counter {
        /// Handles a message that is just an i32.
        fn handle_i32(&mut self, message: &i32) -> DequeueResult {
            self.count += *message;
            DequeueResult::Processed
        }

        /// Handles a message that is just an i32.
        fn handle_f32(&mut self, _message: &f32) -> DequeueResult {
            DequeueResult::Skipped
        }

        /// Handles a message that is just an boolean.
        fn handle_bool(&mut self, _message: &bool) -> DequeueResult {
            DequeueResult::SkipCleared
        }

        /// Handles an enum message.
        fn handle_op(&mut self, message: &Operation) -> DequeueResult {
            match *message {
                Operation::Inc => self.count += 1,
                Operation::Dec => self.count -= 1,
            }
            DequeueResult::Processed
        }
    }

    impl Actor for Counter {
        fn context(&self) -> &ActorContext {
            &self.context
        }

        fn context_mut(&mut self) -> &mut ActorContext {
            &mut self.context
        }

        fn handle_message(&mut self, msg: Arc<Message>) -> DequeueResult {
            dispatch(self, msg.clone(), Counter::handle_i32)
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_op))
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_bool))
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_f32))
                .unwrap_or(DequeueResult::Panic)
        }
    }

    #[test]
    fn test_dispatch() {
        let node_id = Uuid::new_v4();
        let mut state = Counter {
            context: ActorContext::new(node_id),
            count: 0,
        };
        assert_eq!(
            DequeueResult::Processed,
            state.handle_message(Arc::new(Operation::Inc))
        );
        assert_eq!(1, state.count);
        assert_eq!(
            DequeueResult::Processed,
            state.handle_message(Arc::new(Operation::Inc))
        );
        assert_eq!(2, state.count);
        assert_eq!(
            DequeueResult::Processed,
            state.handle_message(Arc::new(Operation::Dec))
        );
        assert_eq!(1, state.count);
        assert_eq!(
            DequeueResult::Processed,
            state.handle_message(Arc::new(10 as i32))
        );
        assert_eq!(11, state.count);
        assert_eq!(
            DequeueResult::SkipCleared,
            state.handle_message(Arc::new(true))
        );
        assert_eq!(11, state.count);
        assert_eq!(
            DequeueResult::Skipped,
            state.handle_message(Arc::new(2.5 as f32))
        );
        assert_eq!(11, state.count);
        assert_eq!(
            DequeueResult::Panic,
            state.handle_message(Arc::new(String::from("oh no")))
        );
        assert_eq!(11, state.count);
    }

    #[test]
    fn test_create_struct_actor() {
        let mut actor_system = ActorSystem::new();
        let actor = actor_system.spawn_internal(|context| Counter { context, count: 0 });
        let aid = actor.aid();
        let context = actor.context();
        // Send the actor we created some messages.
        assert_eq!(actor.pending(), 0);
        assert_eq!(actor.sent(), 0);
        assert_eq!(actor.sent(), 0);
        assert_eq!(actor.received(), 0);
        aid.send(Arc::new(Operation::Inc)).unwrap();
        assert_eq!(actor.pending(), 1);
        assert_eq!(actor.sent(), 1);
        assert_eq!(actor.received(), 0);
        aid.send(Arc::new(Operation::Inc)).unwrap();
        assert_eq!(actor.pending(), 2);
        assert_eq!(actor.sent(), 2);
        assert_eq!(actor.received(), 0);
        aid.send(Arc::new(Operation::Dec)).unwrap();
        assert_eq!(actor.pending(), 3);
        assert_eq!(actor.sent(), 3);
        assert_eq!(actor.received(), 0);
        aid.send(Arc::new(10 as i32)).unwrap();
        assert_eq!(actor.pending(), 4);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 0);

        // Receive messages on the actor.
        context.receive().unwrap();
        assert_eq!(actor.pending(), 3);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 1);
        context.receive().unwrap();
        assert_eq!(actor.pending(), 2);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 2);
        context.receive().unwrap();
        assert_eq!(actor.pending(), 1);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 3);
        context.receive().unwrap();
        assert_eq!(actor.pending(), 0);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 4);
    }
}
