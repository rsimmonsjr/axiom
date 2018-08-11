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

/// This is a macro that makes it easier to write a dispatch function.
#[macro_export]
macro_rules! dispatch {
    ($state:expr, $message:expr, $($func:expr),*) => {
        None
        $(.or_else(|| dispatch($state, $message.clone(), $func)))*
        .unwrap()
    }
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

/// An actor.
pub struct Actor<F: FnMut(Arc<Message>)> {
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
}

impl<F: FnMut(Arc<Message>)> Actor<F> {
    /// Creates a new Actor using the given initial state and the handler for processing
    /// messages sent to that actor.
    // FIXME A new kind of channel is needed to support skipping messages for only messages?
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

    /// Tell the actor a message.
    pub fn send(&mut self, message: Arc<Message>) {
        &self.tx.send(message).unwrap();
        // fixme put these two swaps in a loop for efficiency.
        loop {
            let old = self.sent.load(Ordering::Relaxed);
            if old == self.sent.compare_and_swap(old, old + 1, Ordering::Relaxed) {
                break;
            }
        }
        loop {
            let old = self.pending.load(Ordering::Relaxed);
            if old == self.pending.compare_and_swap(old, old + 1, Ordering::Relaxed) {
                break;
            }
        }
    }

    /// Receives the message on the actor and calls the dispatcher for that actor.
    pub fn receive(&mut self) {
        // first we get the lock so that no other thread can receive at the same time.
        let rx = self.rx.lock().unwrap();
        // now fetch the message from the channel.
        let message = rx.recv().unwrap();
        // dispatch the message to the handler
        (self.dispatcher)(message);
        // reduce the pending count and increase the received count.
        loop {
            let old = self.received.load(Ordering::Relaxed);
            if old == self.received.compare_and_swap(old, old + 1, Ordering::Relaxed) {
                break;
            }
        }
        loop {
            let old = self.pending.load(Ordering::Relaxed);
            if old == self.pending.compare_and_swap(old, old - 1, Ordering::Relaxed) {
                break;
            }
        }
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
        fn handle_i32(&mut self, message: &i32) {
            self.count += *message;
        }

        /// Handles an enum message.
        fn handle_op(&mut self, message: &Operation) {
            match *message {
                Operation::Inc => self.count += 1,
                Operation::Dec => self.count -= 1,
            }
        }
    }

    #[test]
    fn test_actor_dispatch() {
        // FIXME I am not sure the actor has to be mutable.
        let mut actor = Actor::new({
            let mut state = Counter { count: 0 };
            let mut msg_count = 0;

            move |msg| {
                // dispatch to the handlers or panic.
                dispatch(&mut state, msg.clone(), Counter::handle_i32)
                    .or_else(|| dispatch(&mut state, msg.clone(), Counter::handle_op))
                    .unwrap();
                msg_count += 1;
                println!("{}: {}", msg_count, state.count);
            }
        });

        // Send the actor we created some messages.
        actor.send(Arc::new(Operation::Inc));
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
        actor.send(Arc::new(Operation::Inc));
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
        actor.send(Arc::new(Operation::Dec));
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
        actor.send(Arc::new(10 as i32));
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());

        // The scheduler would normally do this but we want to test just the dispatch paradigm.
        actor.receive();
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
        actor.receive();
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
        actor.receive();
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
        actor.receive();
        println!("pending: {}, sent: {}, received: {}", actor.pending(), actor.sent(), actor.received());
    }
}
