use std::any::Any;
use std::marker::Send;
use std::marker::Sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;

/// This is a type used by the system for sending a message through a channel to the actor.
/// All messages are sent as this type and it is up to the dispatcher to cast the message
/// properly and deal with it. It is recommended that the user make use of the [`dispatch`]
/// utility function to help in the casting and calling operation.
pub type Message = dyn Any + Sync + Send;

/// A trait for anything that can dispatch actor messages. Implementing this trait for a
/// structure will allow that structure to be used as a dispatcher for an actor.
pub trait Dispatcher {
    /// Process the the message and return the result of the dispatching.
    fn dispatch(&mut self, message: Arc<Message>) -> DispatchResult;
}

/// Implements the Dispatcher trait for all functions with the proper signature. This allows
/// a user to create an actor with a simple closure.
///
/// ## Examples
/// ```rust
/// let mut state = 0;
/// let dispatcher = |_m: Arc<Message>| {
///     state += 1;
///     DispatchResult::Processed
/// };
/// ```
impl<F: FnMut(Arc<Message>) -> DispatchResult> Dispatcher for F {
    fn dispatch(&mut self, message: Arc<Message>) -> DispatchResult {
        self(message)
    }
}

/// A result returned by the dispatch function that indicates the disposition of the message.
#[derive(Debug, Eq, PartialEq)]
pub enum DispatchResult {
    /// The message was processed and can be removed from the channel. Note that this doesn't
    /// necessarily mean that anything was done with the message, just that it can be removed.
    /// It is up to the dispatcher to decide what if anything to do with the message.
    Processed,
    /// The message was skipped and should remain in the queue and the dequeue should loop
    /// to fetch the next pending message; once a message is skipped then a skip tail will
    /// be created in the channel that will act as the actual tail until the [`SkipCleared']
    /// result is returned from a dispatcher. This enables an actor to skip messages while
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

/// This is the core Actor type in the actor system. The user should see the `README.md` for
/// a detailed description of an actor and the Actor model. Callers communicate with the actor
/// by means of a channel which holds the messages in the order received. The messages are
/// then processed by the dispatcher and the appropriate state of the message is returned.
pub struct Actor<T: Dispatcher> {
    // FIXME turn the docs into something we can gen with cargo
    /// The implementation that will be processing the messages to this actor..
    dispatcher: T,
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

impl<T: Dispatcher> Actor<T> {
    /// Creates a new Actor using the given initial state and the handler for processing
    /// messages sent to that actor.
    pub fn new(dispatcher: T) -> Actor<T> {
        let (tx, rx) = mpsc::channel::<Arc<Message>>();
        // FIXME This should probably be hidden so a actor table can track the actor instead.
        // FIXME Implement a spawn() function in the ActorSystem to call this API
        // FIXME The actor needs to implement an ID assigned by the ActorSystem.
        // FIXME the spawn() function should move the dispatcher or have some means of making sure the user has no more access.
        // FIXME A new kind of channel is needed to support skipping messages and should be integrated into the actor.
        Actor {
            tx,
            rx: Mutex::new(rx),
            dispatcher,
            sent: AtomicUsize::new(0),
            received: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
        }
    }

    /// Sends the actor a message contained in an arc. Since this function moves the `Arc`,
    /// if the user wants to retain the message they should clone the `Arc` before calling this
    /// function.
    pub fn send(&mut self, message: Arc<Message>) {
        &self.tx.send(message).unwrap();
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    /// Receives the message on the actor and calls the dispatcher for that actor. This function
    /// will be typically called by the scheduler to process messages in the actor's channel.
    pub fn receive(&mut self) {
        // FIXME We have to change this to enable message skipping according to the dispatcher.
        // first we get the lock so that no other thread can receive at the same time.
        let rx = self.rx.lock().unwrap();
        // now fetch the message from the channel.
        let message = rx.recv().unwrap();
        // dispatch the message to the handler
        self.dispatcher.dispatch(message.clone());
        // reduce the pending count and increase the received count.
        self.received.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the total number of messages that have been sent to this actor.
    pub fn sent(&self) -> usize {
        self.sent.load(Ordering::Relaxed)
    }

    /// Returns the total number of messages that have been received by this actor.
    pub fn received(&self) -> usize {
        self.received.load(Ordering::Relaxed)
    }

    /// Returns the total number of messages that are currently pending in the actor's channel.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
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
        count: i32,
    }

    impl Counter {
        /// Handles a message that is just an i32.
        fn handle_i32(&mut self, message: &i32) -> DispatchResult {
            self.count += *message;
            DispatchResult::Processed
        }

        /// Handles a message that is just an i32.
        fn handle_f32(&mut self, _message: &f32) -> DispatchResult {
            DispatchResult::Skipped
        }

        /// Handles a message that is just an boolean.
        fn handle_bool(&mut self, _message: &bool) -> DispatchResult {
            DispatchResult::SkipCleared
        }

        /// Handles an enum message.
        fn handle_op(&mut self, message: &Operation) -> DispatchResult {
            match *message {
                Operation::Inc => self.count += 1,
                Operation::Dec => self.count -= 1,
            }
            DispatchResult::Processed
        }
    }

    impl Dispatcher for Counter {
        /// Dispatcher function.
        fn dispatch(&mut self, msg: Arc<Message>) -> DispatchResult {
            dispatch(self, msg.clone(), Counter::handle_i32)
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_op))
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_bool))
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_f32))
                .unwrap_or(DispatchResult::Panic)
        }
    }

    #[test]
    fn test_dispatch() {
        let mut state = Counter { count: 0 };
        assert_eq!(DispatchResult::Processed, state.dispatch(Arc::new(Operation::Inc)));
        assert_eq!(1, state.count);
        assert_eq!(DispatchResult::Processed, state.dispatch(Arc::new(Operation::Inc)));
        assert_eq!(2, state.count);
        assert_eq!(DispatchResult::Processed, state.dispatch(Arc::new(Operation::Dec)));
        assert_eq!(1, state.count);
        assert_eq!(DispatchResult::Processed, state.dispatch(Arc::new(10 as i32)));
        assert_eq!(10, state.count);
        assert_eq!(DispatchResult::SkipCleared, state.dispatch(Arc::new(true)));
        assert_eq!(10, state.count);
        assert_eq!(DispatchResult::Skipped, state.dispatch(Arc::new(2.5 as f32)));
        assert_eq!(10, state.count);
        assert_eq!(DispatchResult::Panic, state.dispatch(Arc::new(String::from("oh no"))));
        assert_eq!(10, state.count);
    }

    /// Helper that checks that the send and recive methods work for an actor.
    fn assert_send_receive<T: Dispatcher>(actor: &mut Actor<T>) {
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

    #[test]
    fn test_create_struct_actor() {
        // FIXME I am not sure the actor has to be mutable.
        let counter = Counter { count: 0 };
        let mut actor = Actor::new(counter);
        assert_send_receive(&mut actor);
    }

    #[test]
    fn test_create_closure_actor() {
        let mut state = 0;
        let dispatcher = |_m: Arc<Message>| {
            state += 1;
            DispatchResult::Processed
        };
        let mut actor = Actor::new(dispatcher);
        assert_send_receive(&mut actor);
    }
}
