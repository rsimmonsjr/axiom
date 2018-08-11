use std::any::Any;
use std::marker::Send;
use std::marker::Sync;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
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

/// Processes messages received by an actor.
pub struct ActorReceiver<F: FnMut(Arc<Message>)> {
    /// The receiver part of the channel being sent to the actor.
    rx: Receiver<Arc<Message>>,
    /// The dispatcher function to be used to dispatch messages.
    dispatcher: F,
}

impl<F: FnMut(Arc<Message>)> ActorReceiver<F> {
    /// Receives the message on the actor and calls the dispatcher for that actor.
    pub fn receive(&mut self) {
        let message = self.rx.recv().unwrap();
        (self.dispatcher)(message);
    }
}

/// An actor.
pub struct Actor<F: FnMut(Arc<Message>)> {
    tx: Sender<Arc<Message>>,
    #[allow(dead_code)] // fixme remove after implementing dispatcher
    processor: Mutex<ActorReceiver<F>>,
}

impl<F: FnMut(Arc<Message>)> Actor<F> {
    /// Creates a new Actor using the given initial state and the handler for processing
    /// messages sent to that actor.
    // FIXME A new kind of channel is needed to support skipping messages for only messages?
    pub fn new(dispatcher: F) -> Actor<F> {
        let (tx, rx) = mpsc::channel::<Arc<Message>>();
        let processor = ActorReceiver { rx, dispatcher };
        Actor {
            tx,
            processor: Mutex::new(processor),
        }
    }

    /// Tell the actor a message.
    pub fn tell(&self, message: Arc<Message>) {
        &self.tx.send(message).unwrap();
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
        // Create a new actor that holds its state by handing it a handler.
        let actor = Actor::new({
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
        actor.tell(Arc::new(Operation::Inc));
        actor.tell(Arc::new(Operation::Inc));
        actor.tell(Arc::new(Operation::Dec));
        actor.tell(Arc::new(10 as i32));

        // The scheduler would normally do this but we want to test just the dispatch paradigm.
        let mut processor = actor.processor.lock().unwrap();
        processor.receive();
        processor.receive();
        processor.receive();
        processor.receive();
    }
}
