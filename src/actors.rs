use mailbox::{DequeueResult, Message};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

/// Attempts to downcast the Arc<Message> to the specific arc type of the handler and then call
/// that handler with the message. If the dispatch is successful it will return the result of the
/// call to the caller, otherwise it will return none back to the user. Note that the user of this
/// function should clone the message or likely there will be borrow issues.
///
/// ## Examples
///
/// Dispatches the message to one of 3 different funtions thath andle different types or returns
/// the Panic result if the code cannot dispatch the message.
///
/// ```rust
/// dispatch(self, msg.clone(), Counter::handle_i32)
///      .or_else(|| dispatch(self, msg.clone(), Counter::handle_op))
///      .or_else(|| dispatch(self, msg.clone(), Counter::handle_bool))
///      .or_else(|| dispatch(self, msg.clone(), Counter::handle_f32))
///      .unwrap_or(DispatchResult::Panic)
/// ```
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

/// Encapsulates an ID to an actor.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct ActorId {
    // TODO DO We want to make these UUIDs?
    /// The node for the actor which is relative to the node referring to the actor. Nodes
    /// are not assigned unique IDs by the system.
    node: i16,
    /// The unique id for this actor on this node.
    id: usize,
}

/// Context for an actor storing the information core to the actor and its management.
pub struct ActorContext {
    /// The id of this actor.
    aid: ActorId,
    /// The transmit side of the channel to use to send messages to the actor.
    tx: Sender<Arc<Message>>,
    /// The receiver part of the channel being sent to the actor.
    rx: Mutex<Receiver<Arc<Message>>>,
    /// The count of the number of messages that have been sent to the actor.
    sent: AtomicUsize,
    /// The count of the number of messages that have been received by the actor.
    received: AtomicUsize,
    /// The count of the number of messages currently pending in the actor's channel.
    pending: AtomicUsize,
    // TODO Add the ability to track execution time in min/mean/max/std_deviation
}

impl ActorContext {
    /// Creates a new actor context with the given actor id.
    fn new(aid: ActorId) -> ActorContext {
        let (tx, rx) = mpsc::channel::<Arc<Message>>();
        ActorContext {
            aid,
            tx,
            rx: Mutex::new(rx),
            sent: AtomicUsize::new(0),
            received: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
        }
    }

    /// Sends the actor a message contained in an arc. Since this function moves the `Arc`,
    /// if the user wants to retain the message they should clone the `Arc` before calling this
    /// function.
    fn send(&mut self, message: Arc<Message>) {
        // FIXME this should be part of the private API.
        self.tx.send(message).unwrap();
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    /// Receives the message on the actor and calls the handler for that actor. This function
    /// will be typically called by the scheduler to process messages in the actor's channel.
    fn receive(&mut self) -> Arc<Message> {
        // FIXME move this to private api.
        // FIXME We have to change this to enable message skipping according to the dispatcher.
        let rx = self.rx.lock().unwrap();
        let message = rx.recv().unwrap();
        self.received.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_sub(1, Ordering::Relaxed);
        message
    }
}

/// This is the core Actor type in the actor system. The user should see the `README.md` for
/// a detailed description of an actor and the Actor model. Callers communicate with the actor
/// by means of a channel which holds the messages in the order received. The messages are
/// then processed by the message handler and the appropriate state of the message is returned.
pub trait Actor {
    /// Fetches the context for this actor.
    fn context(&self) -> &ActorContext;

    /// Fetches the mutable context for this actor.
    fn context_mut(&mut self) -> &mut ActorContext;

    /// Process the the message and return the result of the dispatching.
    fn handle_message(&mut self, message: Arc<Message>) -> DequeueResult;

    /// fetches the actor id for this actor.
    fn aid(&self) -> &ActorId {
        &self.context().aid
    }

    /// Returns the total number of messages that have been sent to this actor.
    fn sent(&self) -> usize {
        self.context().sent.load(Ordering::Relaxed)
    }

    /// Returns the total number of messages that have been received by this actor.
    fn received(&self) -> usize {
        self.context().received.load(Ordering::Relaxed)
    }

    /// Returns the total number of messages that are currently pending in the actor's channel.
    fn pending(&self) -> usize {
        self.context().pending.load(Ordering::Relaxed)
    }

    /// Sends the actor a message contained in an arc. Since this function moves the `Arc`,
    /// if the user wants to retain the message they should clone the `Arc` before calling this
    /// function.
    fn send(&mut self, message: Arc<Message>) {
        // FIXME this should be part of the private API.
        self.context_mut().send(message)
    }

    /// Receives the message on the actor and calls the dispatcher for that actor. This function
    /// will be typically called by the scheduler to process messages in the actor's channel.
    fn receive(&mut self) -> DequeueResult {
        // FIXME this should be part of the private API.
        let message: Arc<Message> = self.context_mut().receive();
        self.handle_message(message)
    }
}

pub struct ActorSystem {
    /// Holds a table of actors in the system keyed by their actor id (aid).
    actors_by_aid: HashMap<ActorId, Arc<Mutex<Actor>>>,
    /// Holds an [`AtomicUsize`] that is incremented when creating each new actor to assign
    /// a unique id within this actor system to the actor.
    aid_sequence: AtomicUsize,
}

impl ActorSystem {
    pub fn new() -> ActorSystem {
        ActorSystem {
            // TODO do I need to give size hints for the table?
            actors_by_aid: HashMap::new(),
            aid_sequence: AtomicUsize::new(1),
        }
    }

    pub fn spawn<T: Actor + 'static, F: Fn(ActorContext) -> T>(
        &mut self,
        f: F,
    ) -> Arc<Mutex<Actor>> {
        // FIXME This should probably be hidden so a actor table can track the actor instead.
        // FIXME Implement a spawn() function in the ActorSystem to call this API
        // FIXME The actor needs to implement an ID assigned by the ActorSystem.
        // FIXME the spawn() function should move the dispatcher or have some means of making sure the user has no more access.
        // FIXME A new kind of channel is needed to support skipping messages and should be integrated into the actor.
        let aid = ActorId {
            node: 0,
            id: self.aid_sequence.fetch_add(1, Ordering::Relaxed),
        };
        let context = ActorContext::new(aid.clone());
        let actor = Arc::new(Mutex::new(f(context)));
        self.actors_by_aid.insert(aid, actor.clone());
        actor
    }
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
        let aid = ActorId { node: 0, id: 0 };
        let mut state = Counter {
            context: ActorContext::new(aid),
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
        let mut system = ActorSystem::new();
        let arc = system.spawn(|context| Counter { context, count: 0 });
        let mut actor = arc.lock().unwrap();
        // Send the actor we created some messages.
        assert_eq!(actor.pending(), 0);
        assert_eq!(actor.sent(), 0);
        assert_eq!(actor.sent(), 0);
        assert_eq!(actor.received(), 0);
        actor.send(Arc::new(Operation::Inc));
        assert_eq!(actor.pending(), 1);
        assert_eq!(actor.sent(), 1);
        assert_eq!(actor.received(), 0);
        actor.send(Arc::new(Operation::Inc));
        assert_eq!(actor.pending(), 2);
        assert_eq!(actor.sent(), 2);
        assert_eq!(actor.received(), 0);
        actor.send(Arc::new(Operation::Dec));
        assert_eq!(actor.pending(), 3);
        assert_eq!(actor.sent(), 3);
        assert_eq!(actor.received(), 0);
        actor.send(Arc::new(10 as i32));
        assert_eq!(actor.pending(), 4);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 0);

        // Receive messages on the actor.
        actor.receive();
        assert_eq!(actor.pending(), 3);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 1);
        actor.receive();
        assert_eq!(actor.pending(), 2);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 2);
        actor.receive();
        assert_eq!(actor.pending(), 1);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 3);
        actor.receive();
        assert_eq!(actor.pending(), 0);
        assert_eq!(actor.sent(), 4);
        assert_eq!(actor.received(), 4);
    }
}
