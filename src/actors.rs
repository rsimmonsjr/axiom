use std::any::Any;
use std::marker::Send;
use std::marker::Sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
//use uuid::Uuid;

/// This is a type used by the system for sending a message through a channel to the actor.
type Message = dyn Any + Sync + Send;

/// A kind of function used to dispatch a message to a processor.
pub trait Dispatcher: FnMut(Arc<Message>) -> DispatchResult {}

/// Implements the dispatcher trait for all variants of functions with the right signature.
impl<F: FnMut(Arc<Message>) -> DispatchResult> Dispatcher for F {}

/// A result returned by the dispatch function that indicates the disposition of the message.
pub enum DispatchResult {
    /// The message was processed and can be removed from the queue.
    Processed,
    /// The message was not processed and can be removed from the queue.
    Ignore,
    /// The message was skipped and should remain in the queue and the dequeue should loop
    /// to fetch the next pending message.
    Skip,
    /// The message generated an error of some kind and a panic should occur.
    Panic,
}

/// Attempts to downcast the Arc<Message> to the specific arc type of the handler and then call
/// that handler with the message. If the dispatch is successful it will return the result of the
/// call to the caller, otherwise it will return none back to the user. Note that the user of this
/// function should clone the message or likely there will be borrow issues.
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

/// This is a macro that makes it easier to write a Dispatcher function.
// FIXME integrate default into this macro to return the dispatch result type.
#[macro_export]
macro_rules! dispatch {
    ($state:expr, $message:expr, $default:expr, $($func:expr),*) => {
        None
        $(.or_else(|| dispatch($state, $message.clone(), $func)))*
        .unwrap()
    }
}

/// A dispatcher that ignores all messages.
pub fn ignore_dispatcher(_msg: Arc<Message>) -> DispatchResult {
    DispatchResult::Ignore
}

/// A dispatcher that panics on any message.
pub fn panic_dispatcher(_msg: Arc<Message>) -> DispatchResult {
    DispatchResult::Panic
}

/// This is the core Actor type in the actor system. The user should see the `README.md` for
/// a detailed description of an actor and the Actor model.
pub struct Actor<F: Dispatcher> {
    // FIXME turn the docs into something we can gen with cargo
    /// The transmit side of the channel to use to send messages to the actor.
    tx: Sender<Arc<Message>>,
    /// The receiver part of the channel being sent to the actor.
    rx: Mutex<Receiver<Arc<Message>>>,
    /// The dispatcher function to be used to dispatch messages.
    dispatcher: F,
    /// The count of the number of messages that have been sent to the actor.
    sent: AtomicUsize,
    /// The count of the number of messages that have been received by the actor.
    received: AtomicUsize,
    /// The count of the number of messages currently pending in the actor's channel.
    pending: AtomicUsize,
    // TODO Add the ability to track execution time in min/mean/max/std_deviation
}

impl<F: Dispatcher> Actor<F> {
    /// Creates a new Actor using the given initial state and the handler for processing
    /// messages sent to that actor.
    // FIXME A new kind of channel is needed to support skipping messages and should be integrated into the actor.
    pub fn new(dispatcher: F) -> Actor<F> {
        let (tx, rx) = mpsc::channel::<Arc<Message>>();
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
        (self.dispatcher)(message.clone());
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

    /// Returns the total number of messages that have been received by this actor.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}

/// An enum that defines an actor message. This enum contains system messages as well as
/// a container for any other message that the user wishes to pass.
#[derive(Debug)]
pub enum SystemMessage {
    /// UA system message used to tell an actor to stop itself.
    Stop,
}

/// Messages sent by a cluster manager.
#[derive(Debug)]
pub enum ClusterActorMessage {
    /// Indicates that a node in the cluster has come up.
    NodeUp,
    /// Indicates that a node in the cluster has gone down.
    NodeDown,
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

        /// Handles an enum message.
        fn handle_op(&mut self, message: &Operation) -> DispatchResult {
            match *message {
                Operation::Inc => self.count += 1,
                Operation::Dec => self.count -= 1,
            }
            DispatchResult::Processed
        }

        /// Dispatcher function.
        pub fn dispatch(&mut self, msg: Arc<Message>) -> DispatchResult {
            dispatch(self, msg.clone(), Counter::handle_i32)
                .or_else(|| dispatch(self, msg.clone(), Counter::handle_op))
                .unwrap_or(DispatchResult::Panic)
        }
    }

    #[test]
    fn test_actor_dispatch() {
        // FIXME I am not sure the actor has to be mutable.
        let state = Arc::new(Mutex::new(Counter { count: 0 }));

        let state2 = state.clone();
        let mut actor = Actor::new({
            |msg: Arc<Message>| {
                // dispatch to the handlers or panic.
                let mut v = state2.lock().unwrap();
                dispatch(&mut *v, msg.clone(), Counter::handle_i32)
                    .or_else(|| dispatch(&mut *v, msg.clone(), Counter::handle_op))
                    .unwrap_or(DispatchResult::Panic)
            }
        });

        // Send the actor we created some messages.
        assert!(actor.pending() == 0 && actor.sent() == 0 && actor.received() == 0 && state.lock().unwrap().count == 0);
        actor.send(Arc::new(Operation::Inc));
        assert!(actor.pending() == 1 && actor.sent() == 1 && actor.received() == 0 && state.lock().unwrap().count == 0);
        actor.send(Arc::new(Operation::Inc));
        assert!(actor.pending() == 2 && actor.sent() == 2 && actor.received() == 0 && state.lock().unwrap().count == 0);
        actor.send(Arc::new(Operation::Dec));
        assert!(actor.pending() == 3 && actor.sent() == 3 && actor.received() == 0 && state.lock().unwrap().count == 0);
        actor.send(Arc::new(10 as i32));
        assert!(actor.pending() == 4 && actor.sent() == 4 && actor.received() == 0 && state.lock().unwrap().count == 0);

        // Receive messages on the actor.
        actor.receive();
        assert!(actor.pending() == 3 && actor.sent() == 4 && actor.received() == 1 && state.lock().unwrap().count == 1);
        actor.receive();
        assert!(actor.pending() == 2 && actor.sent() == 4 && actor.received() == 2 && state.lock().unwrap().count == 2);
        actor.receive();
        assert!(actor.pending() == 1 && actor.sent() == 4 && actor.received() == 3 && state.lock().unwrap().count == 1);
        actor.receive();
        assert!(actor.pending() == 0 && actor.sent() == 4 && actor.received() == 4 && state.lock().unwrap().count == 11);
    }
}
