/// Implements actors and the actor system.
use secc;
use secc::HalfUsize;
use secc::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
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
    /// be created in the channel that will act as the actual tail until the [`ResetSkip']
    /// result is returned from a message handler. This enables an actor to skip messages while
    /// working on a process and then clear the skip buffer and resume normal processing.
    Skipped,
    /// Clears the skip tail on the channel. A skip tail is present when a message has been
    /// skipped by returning [`Skipped`] If no skip tail is set than this result is semantically
    /// the same as [`Processed`].
    ResetSkip,
}

/// Attempts to downcast the Arc<Message> to the specific arc type of the handler and then call
/// that handler with the message. If the dispatch is successful it will return the result of the
/// call to the caller, otherwise it will return None back to the caller.
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
    /// The remote sender.
    Remote, // FIXME not implemented.
}

/// Encapsulates an ID to an actor.
pub struct ActorId {
    /// The unique id for this actor on this node.
    id: Uuid,
    /// The id for the node that this actor is on.
    node_id: Uuid,
    /// The handle to the sender side for the actor's message channel.
    sender: ActorSender,
    /// Holds a reference to the local actor system that the [ActorId] lives at.
    system: Arc<ActorSystem>,
    /// Holds an Arc to the actual actor if the actor is local to this node.
    actor: Option<Arc<Mutex<Actor>>>,
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

/// A type for a user function that processes messages for an actor.
pub type Processor<State> = Fn(Arc<ActorId>, State, Message) -> (DequeueResult, State);

/// A type that is used by an actor to hold the handler for messages.
type Handler = Fn(Message) -> DequeueResult + Send + Sync;

/// An actual actor in the system that manages the message processing.
pub struct Actor {
    /// Id of the associated actor.
    aid: Arc<ActorId>,
    /// Receiver for the actor channel.
    receiver: SeccReceiver<Arc<Message>>,
    /// Function processes messages sent to the actor wrapped in a closure to erase the
    /// state type that the actor is maanaging.
    handler: Box<Handler>,
}

impl Actor {
    /// Creates a new actor context with the given actor id. The user will pass the
    /// initial state of the actor as well as the processor that will be used to process
    /// messages sent to the actor. The system and node id are passed separately because
    /// of restrictions on mutex guards not being re-entrant in rust.
    pub fn new<State>(
        system: Arc<Mutex<ActorSystem>>,
        node_id: Uuid,
        starting_state: State,
        processor: Processor<State>,
    ) -> Arc<ActorId> {
        // TODO Let the user pass the size of the channel queue when creating the actor.
        // Create the channel for the actor.
        let (tx, receiver) = secc::create::<Arc<Message>>(32);
        // The sender will be put inside the actor id.
        let mut aid = ActorId {
            id: Uuid::new_v4(),
            node_id: node_id,
            sender: ActorSender::Local(tx),
            system: system.clone(),
            actor: None,
        };

        // This handler will manage the state for the actor.
        let handler = Box::new({
            // Store the state in a temporary container that can be empty so we can pass the
            // state by value to the processing function.
            let mut current_state = Some(starting_state);
            move |aid, message| {
                let (res, new_state) = processor(aid, current_state.take().unwrap(), message);
                // Now we have the state back so lets stuff it in the some again.
                current_state = Some(new_state);
                res
            }
        });

        // The actor context will be the holder of the actual actor.
        let actor = Actor {
            aid: Arc::new(aid),
            receiver,
            handler,
        };

        // We will copy the reference to the actor in the actor id which means when handling
        // messages we will not have to look up the actor in a map once we have the actor id.
        aid.actor = Some(Mutex::new(Arc::new(actor)));

        actor.aid
    }

    /// fetches the actor id for this actor.
    fn aid(&self) -> Arc<ActorId> {
        self.aid.clone()
    }

    /// Returns the total number of messages that have been sent to this actor.
    fn sent(&self) -> usize {
        self.receiver.enqueued()
    }

    /// Returns the total number of messages that have been received by this actor.
    fn received(&self) -> usize {
        self.receiver.dequeued()
    }

    /// Returns the total number of messages that are currently pending in the actor's channel.
    fn pending(&self) -> usize {
        self.receiver.length()
    }

    /// Receive as message off the channel and processes it with the actor.
    fn receive(&mut self) {
        match self.receiver.peek() {
            Result::Err(err) => {
                // TODO Log the error from the channel.
                return;
            }
            Result::Ok(message) => {
                let result = self.actor.handle_msg(self, message);
                let queue_result = match result {
                    DequeueResult::Processed => self.receiver.pop(),
                    DequeueResult::Skipped => self.receiver.skip(),
                    DequeueResult::ResetSkip => self.receiver.reset_skip(),
                };
                match queue_result {
                    Ok(_) => (),
                    Err(e) => println!("Error Occurred {:?}", e), // TODO Log this
                };
            }
        }
    }
}

/// An actor system that contains and manages the actors spawned inside it. The actor system
/// embodies principally two main components, a AID table that maps [ActorId]s to the actors
/// and a run channel that the actors are sent to when they have pending messages.
pub struct ActorSystem {
    /// Id for this actor system and node.
    pub node_id: Uuid,
    /// Holds the actor id objects keyed by Uuid of the actor.
    actors_by_aid: HashMap<Uuid, Arc<ActorId>>,
    /// Holds handles to the pool of threads processing the run queue.
    thread_pool: Vec<JoinHandle<()>>,
    /// Sender side of the dispatcher channel. When an actor gets a message and its pending
    /// count goes from 0 to 1 it will put itself in the channel via the sender.
    dispatch_sender: Arc<SeccSender<Arc<ActorId>>>,
    /// Receiver side of the dispatcher channel. All threads in the pool will be grabbing
    /// actor ids from this receiver. When a thread gets an actor it will lock the
    /// mutex of the actor inside the id and then will process the message. If the
    /// actor has pending messages after the processing is done then the actor will
    /// be put back in the channel to be re-queued.
    dispatch_receiver: Arc<SeccReceiver<Arc<ActorId>>>,
}

impl ActorSystem {
    /// Creates a new actor system with the given size for the dispatcher channel and thread
    /// pool. The user should benchmark how many slots in the run channel and the number of
    /// threads they need in order to satisfy the requirements of the system they are creating.
    pub fn new(run_channel_size: HalfUsize, thread_pool_size: HalfUsize) -> ActorSystem {
        let (sender, receiver) = secc::create::<Arc<ActorId>>(run_channel_size);
        let thread_pool: Vec<JoinHandle<()>> = (1..thread_pool_size)
            .map(|_i| ActorSystem::start_dispatcher_thread(sender.clone(), receiver.clone()))
            .collect();
        ActorSystem {
            node_id: Uuid::new_v4(),
            actors_by_aid: HashMap::new(),
            run_channel_sender: sender,
            run_channel_receiver: receiver,
            thread_pool,
        }
    }

    /// Starts a thread for the dispatcher that will process actor messages.
    fn start_dispatcher_thread(
        dispatch_sender: Arc<SeccSender<Arc<ActorId>>>,
        dispatch_receiver: Arc<SeccReceiver<Arc<ActorId>>>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            match dispatch_receiver.receive_await() {
                Err(_) => println!("Error"), // FIXME turn this into logging instead.
                Ok(aid) => {
                    // FIXME Actor panic shouldnt take down the whole code
                    let mut mutex = aid.actor.lock().unwrap();
                    let mut actor = *mutex;
                    actor.receive();
                    if actor.receiver.readable() > 0 {
                        // if there are additional messages pending in the actor,
                        // re-enqueue the message at the back of the queue.
                        dispatch_receiver.send_await(mutex.clone());
                    }
                }
            }
        })
    }

    /// Spawns a new actor on the actor system using the given `starting_state` for the actor
    /// and the given `processor` function that will be used to process actor messages.
    pub fn spawn<State>(
        system: Arc<Mutex<ActorSystem>>,
        starting_state: State,
        processor: Processor<State>,
    ) -> Arc<ActorId> {
        let mut guard = *system.lock().unwrap();
        let aid = Actor::new(system, guard.node_id, starting_state, processor);
        guard.actors_by_aid.insert(aid.id, aid.clone());
        aid
    }
}

pub fn send(aid: Arc<ActorId>, message: Arc<Message>) -> Result<usize, SeccErrors<Arc<Message>>> {
    match aid.sender {
        ActorSender::Local(sender) => {
            let readable = sender.send_await(message).unwrap();
            // From 0 to 1 message readable triggers schedule.
            if readable == 1 {
                aid.system.dispatch_sender.send(aid);
            }
            Ok(readable)
        }
        _ => unimplemented!("Remote actors not implemented currently."),
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
            DequeueResult::ResetSkip
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
            DequeueResult::ResetSkip,
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
