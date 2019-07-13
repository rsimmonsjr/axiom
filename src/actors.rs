///
/// Implements actors and the actor system.
use secc;
use secc::*;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::{Send, Sync};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use uuid::Uuid;

/// This is a type used by the system for sending a message through a channel to the actor.
/// All messages are sent as this type and it is up to the message handler to cast the message
/// properly and deal with it. It is recommended that the user make use of the [`dispatch`]
/// utility function to help in the casting and calling operation.
pub type Message = dyn Any + Sync + Send;

/// Status of the message resulting from processing a message with an actor.
#[derive(Debug, Eq, PartialEq)]
pub enum Status {
    /// The message was processed and can be removed from the channel. Note that this doesn't
    /// necessarily mean that anything was done with the message, just that it can be removed.
    /// It is up to the mctor's essage processor to decide what if anything to do with the message.
    Processed,
    /// The message was skipped and should remain in the queue. Once a message is skipped then
    /// a skip tail will be created in the channel that will act as the actual tail until the
    /// [`ResetSkip'] status is returned from an actor's processor. This enables an actor to
    /// skip messages while working on a process and then clear the skip buffer and resume normal
    /// processing. This functionality is critical for actors that act as a finite state machine
    /// and thus might temporarily change the implementation of the processor and then switch
    /// back to a state where the previously sent messages are delivered.
    Skipped,
    /// Clears the skip tail on the channel. A skip tail is present when a message has been
    /// skipped by returning [`Skipped`] If no skip tail is set than this result is semantically
    /// the same as [`Processed`].
    ResetSkip,
}

/// An enum that holds a sender for an actor. This is usually wrapped in an actor id.
pub enum ActorSender {
    /// A Sender used for sending messages to local actors.
    Local(SeccSender<Arc<Message>>),
    /// The remote sender.
    Remote, // FIXME (Issue #9) not implemented.
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
}

impl ActorId {
    /// Returns the total number of messages that have been sent to the actor.
    pub fn sent(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.sent(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that have been recieived and processed by the actor.
    pub fn received(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.received(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that are pending to be received by the actor.
    pub fn pending(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.pending(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
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

/// A type for a user function that processes messages for an actor. This will be passed to a
/// spawn funtion to specify the handler used for managing the state of the actor based
/// on the messages passed to the actor. The rewulting state passed will be used in the
/// next message call. The processor takes three arguments. First, the `state` is a mutable
/// reference to the current state of the actor. Second, is the [`ActorId`] enclosed in
/// an [`Arc`] to allow access to the actor system for spawning, sending to self and so on.
/// Third is the current message to process in an [`Arc`].
pub trait Processor<State: Send + Sync>:
    (FnMut(&mut State, Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync
{
}

// Allows any function, static or closure, to be used as a processor.
impl<F, State> Processor<State> for F
where
    State: Send + Sync + 'static,
    F: (FnMut(&mut State, Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static,
{
}

/// This is the type for the handler that will manage the state for the actor using
/// the user-provided message processor.
pub trait Handler: (FnMut(Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static {}

impl<F> Handler for F where F: (FnMut(Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static
{}

/// An actual actor in the system. An actor is a worker that allows the asynchronous
/// processing of messages. The actor will obey a certain set of rules, namely.
/// 1. An actor can be interracted with only by means of messages.
/// 2. An actor processes ONLY one message at a time.
/// 3. An actor will process a message only once.
/// 4. An actor can send a message to any other actor without knowledge of that actor's internals
/// 5. Actors send only immutable data as messages, though they may have mutable internal state.
/// 6. Actors are location agnostic. An actor can be sent a message from anywhere in the cluster.
pub struct Actor {
    /// Id of the associated actor.
    aid: Arc<ActorId>,
    /// Receiver for the actor channel.
    receiver: SeccReceiver<Arc<Message>>,
    /// Function processes messages sent to the actor wrapped in a closure to erase the
    /// state type that the actor is maanaging.
    handler: Mutex<Box<dyn Handler>>,
}

impl Actor {
    /// Creates a new actor context with the given actor id. The user will pass the initial state
    /// of the actor as well as the processor that will be used to process messages sent to the
    /// actor. The system and node id are passed separately because of restrictions on mutex
    /// guards not being re-entrant in rust.
    pub fn new<F, State>(
        system: Arc<ActorSystem>,
        node_id: Uuid,
        mut state: State,
        mut processor: F,
    ) -> Arc<Actor>
    where
        State: Send + Sync + 'static,
        F: Processor<State> + 'static,
    {
        // TODO Let the user pass the size of the channel queue when creating the actor.
        // Create the channel for the actor.
        let (tx, receiver) = secc::create::<Arc<Message>>(32);
        // The sender will be put inside the actor id.
        let aid = ActorId {
            id: Uuid::new_v4(),
            node_id: node_id,
            sender: ActorSender::Local(tx),
            system: system.clone(),
        };

        // This handler will manage the state for the actor.
        let handler = Box::new({
            move |aid: Arc<ActorId>, message: &Arc<Message>| processor(&mut state, aid, message)
        });

        // The actor context will be the holder of the actual actor.
        let actor = Actor {
            aid: Arc::new(aid),
            receiver,
            handler: Mutex::new(handler),
        };

        Arc::new(actor)
    }

    /// Returns the total number of messages that have been sent to this actor.
    pub fn sent(&self) -> usize {
        self.receiver.sent()
    }

    /// Returns the total number of messages that have been received by this actor.
    pub fn received(&self) -> usize {
        self.receiver.received()
    }

    /// Returns the total number of messages that are currently pending in the actor's channel.
    pub fn pending(&self) -> usize {
        self.receiver.pending()
    }

    /// Receive as message off the channel and processes it with the actor.
    fn receive(&self) {
        match self.receiver.peek() {
            Result::Err(err) => {
                // TODO Log the error from the channel.
                println!("Error Occurred {:?}", err); // TODO Log this
                return;
            }
            Result::Ok(message) => {
                let mut guard: std::sync::MutexGuard<Box<Handler>> = self.handler.lock().unwrap();
                let result = (&mut *guard)(self.aid.clone(), message);
                let queue_result = match result {
                    Status::Processed => self.receiver.pop(),
                    Status::Skipped => self.receiver.skip(),
                    Status::ResetSkip => self.receiver.reset_skip(),
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
    /// Sender side of the work channel. When an actor gets a message and its pending count goes
    /// from 0 to 1 it will put itself in the channel via the sender.
    sender: Arc<SeccSender<Arc<Actor>>>,
    /// Receiver side of the work channel. All threads in the pool will be grabbing actors
    /// from this receiver to process messages.
    receiver: Arc<SeccReceiver<Arc<Actor>>>,
    /// Holds the [Actor] objects keyed by the [ActorId]. The [RwLock] will be locked for write
    /// only when a new actor is spawned but otherwise will be locked for read by the threads
    /// in the threadbool as they try to look up actors to process.
    actors_by_aid: Arc<RwLock<HashMap<Arc<ActorId>, Arc<Actor>>>>,
    /// Holds handles to the pool of threads processing the run queue.
    thread_pool: Mutex<Vec<JoinHandle<()>>>,
    /// A flag holding whether or not the system is currently shutting down.
    shutdown_triggered: AtomicBool,
    /// Duration for threads to wait for new messages before cycling and checking again. Defaults
    /// to 10 milliseconds.
    /// TODO Allow user to pass this as config.
    thread_wait_time: Option<Duration>,
}

impl ActorSystem {
    /// Creates an actor system with the given size for the dispatcher channel and thread pool.
    /// The user should benchmark how many slots in the run channel and the number of threads
    /// they need in order to satisfy the requirements of the system they are creating.
    pub fn create(run_channel_size: u16, thread_pool_size: u16) -> Arc<ActorSystem> {
        // We will use arcs for the sender and receivers because the they will surf threads.
        let (sender, receiver) = secc::create_with_arcs::<Arc<Actor>>(run_channel_size);

        // Creates the actor system with the thread pools and actor map initialized.
        let system = Arc::new(ActorSystem {
            node_id: Uuid::new_v4(),
            sender,
            receiver,
            actors_by_aid: Arc::new(RwLock::new(HashMap::new())),
            thread_pool: Mutex::new(Vec::with_capacity(thread_pool_size as usize)),
            shutdown_triggered: AtomicBool::new(false),
            thread_wait_time: Some(Duration::from_millis(10)),
        });

        // We have the thread pool in a mutex to avoid a chicken - egg situation with the actor
        // system not being created but needed by the thread.
        {
            let mut guard = system.thread_pool.lock().unwrap();

            // Allocate the thread pool of threads grabbing for work in the channel.
            for _ in 0..thread_pool_size {
                let thread = ActorSystem::start_dispatcher_thread(system.clone());
                guard.push(thread);
            }
        }

        system
    }

    /// Starts a thread for the dispatcher that will process actor messages.
    fn start_dispatcher_thread(system: Arc<ActorSystem>) -> JoinHandle<()> {
        let sender = system.sender.clone();
        let receiver = system.receiver.clone();

        thread::spawn(move || {
            while !system.shutdown_triggered.load(Ordering::Relaxed) {
                match receiver.receive_await_timeout(system.thread_wait_time) {
                    Err(_) => (),
                    Ok(actor) => {
                        // FIXME (Issue #5) Actor panic shouldnt take down the whole code
                        actor.receive();
                        if actor.receiver.receivable() > 0 {
                            // if there are additional messages pending in the actor,
                            // re-enqueue the message at the back of the queue.
                            sender.send_await(actor).unwrap();
                        }
                    }
                }
            }
        })
    }

    /// Triggers a shutdown of the system and returns only when all threads have joined.
    pub fn shutdown(&self) {
        self.shutdown_triggered.store(true, Ordering::Relaxed);
        let mut guard = self.thread_pool.lock().unwrap();
        let vec = std::mem::replace(&mut *guard, vec![]);
        for handle in vec {
            handle.join().unwrap();
        }
    }

    /// Returns the total number of times actors have been sent to work channels.
    pub fn sent(&self) -> usize {
        self.receiver.sent()
    }

    /// Returns the total number of times actors have been processed from the work channels.
    pub fn received(&self) -> usize {
        self.receiver.received()
    }

    /// Returns the total number of actors that are currently pending in the work channel.
    pub fn pending(&self) -> usize {
        self.receiver.pending()
    }

    /// Schedules the `actor_id` for work on the given actor system.
    fn schedule(aid: Arc<ActorId>) {
        // Note that this is implemented here rather than calling the sender directly
        // from the send in order :to allow internal optimization of the actor system.
        // FIXME (Issue #10) Harden this against the actor being out of the map.
        let guard = aid.system.actors_by_aid.read().unwrap();
        // FIXME (Issue #11) if the actor is not able to schedule, this will panic.
        (aid.system.sender.send(guard.get(&aid).unwrap().clone())).unwrap();
    }
}

/// Spawns a new actor on the actor `system` using the given `starting_state` for the actor and
/// the given `processor` function that will be used to process actor messages.
pub fn spawn<F, State>(system: Arc<ActorSystem>, state: State, processor: F) -> Arc<ActorId>
where
    State: Send + Sync + 'static,
    F: Processor<State> + 'static,
{
    let mut guard = system.actors_by_aid.write().unwrap();
    let actor = Actor::new(system.clone(), system.node_id, state, processor);
    let aid = actor.aid.clone();
    guard.insert(aid.clone(), actor);
    aid
}

/// Sends the `message` to the actor identified by the `aid` passed.
pub fn send(aid: &Arc<ActorId>, message: Arc<Message>) {
    match &aid.sender {
        ActorSender::Local(sender) => {
            let readable = sender.send_await(message).unwrap();
            // From 0 to 1 message readable triggers schedule.
            if readable == 1 {
                ActorSystem::schedule(aid.clone())
            }
        }
        _ => unimplemented!("Remote actors not implemented currently."),
    }
}

/// Attempts to downcast the Arc<Message> to the specific arc type of the handler and then call
/// that handler with the message. If the dispatch is successful it will return the result of the
/// call to the caller, otherwise it will return None back to the caller.
pub fn dispatch<T: 'static, S>(
    state: &mut S,
    aid: Arc<ActorId>,
    message: Arc<Message>,
    mut func: impl FnMut(&mut S, Arc<ActorId>, &T) -> Status,
) -> Option<Status> {
    match message.downcast_ref::<T>() {
        Some(x) => Some(func(state, aid.clone(), x)),
        None => None,
    }
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_with_closure() {
        let system = ActorSystem::create(10, 1);
        let starting_state: usize = 0 as usize;
        // Note we have to tediously define closure types because of a bug in rust compiler's
        // type inferrence. They can be removed when that bug is fixed.
        let closure = |message_count: &mut usize, aid: Arc<ActorId>, msg: &Arc<Message>| {
            // Expected messages in the expected order.
            let expected: Vec<i32> = vec![11, 13, 17];
            // Attempt to downcast to expected message.
            match msg.downcast_ref::<i32>() {
                Some(value) => {
                    assert_eq!(expected[*message_count], *value);
                    assert_eq!(*message_count, aid.received());
                    *message_count += 1;
                    assert_eq!(aid.pending(), aid.sent() - aid.received());
                }
                None => assert!(false, "Unknown message type received."),
            }
            Status::Processed
        };

        let aid = spawn(system.clone(), starting_state, closure);

        // We will send three messages and we have to wait on pending in order to make sure the
        // test doesn't race the actor system.

        send(&aid, Arc::new(11 as i32));
        assert_eq!(1, aid.sent());
        send(&aid, Arc::new(13 as i32));
        assert_eq!(2, aid.sent());
        send(&aid, Arc::new(17 as i32));
        assert_eq!(3, aid.sent());

        thread::sleep(Duration::from_millis(10));

        assert_eq!(3, aid.received());
        system.shutdown()
    }

    // ---------- Test an actor made from a Struct

    #[derive(Debug)]
    enum Operation {
        Inc,
        Dec,
    }

    #[derive(Debug)]
    struct StructActor {
        count: usize,
    }

    impl StructActor {
        fn handle_op(&mut self, aid: Arc<ActorId>, msg: &Operation) -> Status {
            match msg {
                Operation::Inc => {
                    assert_eq!(0, aid.received());
                    self.count += 1;
                    assert_eq!(6 as usize, self.count);
                }
                Operation::Dec => {
                    assert_eq!(1, aid.received());
                    self.count -= 1;
                    assert_eq!(5 as usize, self.count);
                }
            }
            Status::Processed
        }

        fn handle_i32(&mut self, aid: Arc<ActorId>, msg: &i32) -> Status {
            assert_eq!(2, aid.received());
            assert_eq!(17 as i32, *msg);
            self.count += *msg as usize;
            assert_eq!(22 as usize, self.count);
            Status::Processed
        }

        fn handle(&mut self, aid: Arc<ActorId>, msg: &Arc<Message>) -> Status {
            dispatch(self, aid.clone(), msg.clone(), &StructActor::handle_op)
                .or_else(|| dispatch(self, aid.clone(), msg.clone(), &StructActor::handle_i32))
                .or_else(|| {
                    dispatch(
                        self,
                        aid.clone(),
                        msg.clone(),
                        move |state: &mut StructActor, aid: Arc<ActorId>, msg: &u8| -> Status {
                            assert_eq!(3, aid.received());
                            assert_eq!(7 as u8, *msg);
                            state.count += *msg as usize;
                            assert_eq!(29 as usize, state.count);
                            Status::Processed
                        },
                    )
                })
                .unwrap()
        }
    }

    #[test]
    fn test_spawn_with_struct() {
        let system = ActorSystem::create(10, 1);
        let starting_state: StructActor = StructActor { count: 5 as usize };

        let aid = spawn(system.clone(), starting_state, StructActor::handle);

        send(&aid, Arc::new(Operation::Inc));
        assert_eq!(1, aid.sent());
        send(&aid, Arc::new(Operation::Dec));
        assert_eq!(2, aid.sent());
        send(&aid, Arc::new(17 as i32));
        assert_eq!(3, aid.sent());
        send(&aid, Arc::new(7 as u8));
        assert_eq!(4, aid.sent());

        thread::sleep(Duration::from_millis(10));

        assert_eq!(4, aid.received());
        system.shutdown()
    }

}
