//! Implements actors and the actor system.

use crate::secc;
use crate::secc::*;
use log::error;
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

/// A type used by the system for sending a message through a channel to the actor.
///
/// All messages are sent as this type and it is up to the message handler to cast the message
/// properly and deal with it. For help in creating an effective dispatching strategy see the
/// test cases.
pub type Message = dyn Any + Sync + Send;

/// Status of the message resulting from processing a message with an actor.
#[derive(Debug, Eq, PartialEq)]
pub enum Status {
    /// The message was processed and can be removed from the channel.
    ///
    /// Note that this doesn't necessarily mean that anything was done with the message, just
    /// that it can be removed.  It is up to the mctor's essage processor to decide what if
    /// anything to do with the message.
    Processed,

    /// The message was skipped and should remain in the queue.
    ///
    /// Once a message is skipped then a skip tail will be created in the channel that will act
    /// as the actual tail until the [ResetSkip] status is returned from an actor's processor.
    /// This enables an actor to skip messages while working on a process and then clear the skip
    /// buffer and resume normal processing. This functionality is critical for actors that act
    /// as a finite state machine and thus might temporarily change the implementation of the
    /// processor and then switch back to a state where the previously sent messages are delivered.
    Skipped,

    /// Clears the skip tail on the channel.
    ///
    /// A skip tail is present when a message has been skipped by returning [Skipped] If no
    /// skip tail is set than this result is semantically the same as [Processed].
    ResetSkip,

    /// Returned from an actor when the actor wants the system to stop the actor.
    ///
    /// When this is returned the actor's [ActorId] will no longer send any messages and the
    /// actor instance itself will be removed from the actors table in the [ActorSystem]. The
    /// user is advised to do any cleanup needed before returning [Status::Stop].
    Stop,
}

/// An enum containing messages that are sent to actors by the actor system itself and other
/// messages that have special content and are universal to all actors.
pub enum SystemMsg {
    /// A message that instructs an actor to shut down.
    ///
    /// The actor receiving this message should shut down all open file handles and any other
    /// resources and return a [Status::Stop] as a result from the call. This is an attempt for
    /// the caller to shut down the actor nicely rather than just calling [ActorSystem::stop].
    Stop,
}

/// Errors returned from actors and other parts of the actor system.
#[derive(Debug)]
pub enum ActorError {
    /// Message sent when attempting to send to an actor that has already been stopped.
    ///
    /// A stopped actor cannot accept any more messages and is shut down. The holder of an
    /// [ActorId] to a stopped actor should throw away the [ActorId] as the actor can never
    /// be startd again.
    ActorStopped,

    /// Error Used for when an attempt is made to send a message to a remote actor. *This
    /// error will be removed when remote actors are implemented.*
    RemoteNotImplemented,
}

/// An enum that holds a sender for an actor.
///
/// An [ActorId] uses the sender to send messages to the destination actor. Messages that are
/// sent locally with [ActorSender::Local] are sent by reference, sharing the memory. Those
/// that are sent to a node with [ActorSender::Remote] are sent via serializing the message to
/// the remote system which will then use an internal [ActorSender::Local] to forward the
/// message to the proper actor.
pub enum ActorSender {
    /// A Sender used for sending messages to local actors.
    Local(SeccSender<Arc<Message>>),
    /// The remote sender.
    Remote, // FIXME (Issue #9) not implemented.
}

impl fmt::Debug for ActorSender {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "{}",
            match *self {
                ActorSender::Local(_) => "ActorSender::Local",
                ActorSender::Remote => "ActorSender::Remote",
            }
        )
    }
}
/// Encapsulates an ID to an actor.
///
/// This is a unique reference to the actor within the entire cluster and can be used to send
/// messages to the actor regardless of location. The [ActorId] does the heavy lifting of
/// deciding where the actor is and sending the message. However it is important that the user
/// of the [ActorId] at least has some notion of where the actor is for developing an efficient
/// actor architecture.
pub struct ActorId {
    /// The unique id for this actor on this node.
    id: Uuid,
    /// The id for the node that this actor is on.
    node_id: Uuid,
    /// The handle to the sender side for the actor's message channel.
    sender: ActorSender,
    /// Holds a reference to the local actor system that the [ActorId] lives at.
    system: Arc<ActorSystem>,
    /// Holds a boolean to indicate if the actor is stopped. A stopped actor will no longer
    /// accept further messages to be sent.
    is_stopped: AtomicBool,
}

impl ActorId {
    /// A helper to invoke [ActorId::try_send] and simply panic if an error occurrs.
    ///
    /// This should be used where the user doesnt expect an error to happen.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(10, 1);
    ///
    /// let aid = ActorSystem::spawn(&system,
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, _message: &Arc<Message>| Status::Processed,
    ///  );
    ///
    /// ActorId::send(&aid, Arc::new(11));
    /// ```
    pub fn send(aid: &Arc<ActorId>, message: Arc<Message>) {
        match ActorId::try_send(aid, message) {
            Ok(_) => (),
            Err(e) => panic!("Error occurred sending to aid: {:?}", e),
        }
    }

    /// Attempts to send a message to the actor with the given [Arc<ActorId>] and returns
    /// [Result::Ok] when the send was successful or an [Result::Err<ActorError>] error if
    /// something went wrong with the send.
    ///
    /// This is useful when the user wants to send and feels that there is a possibility of an
    /// error and that possibility has to be handled gracefully.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(10, 1);
    ///
    /// let aid = ActorSystem::spawn(&system,
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| Status::Processed,
    ///  );
    ///
    /// match ActorId::try_send(&aid, Arc::new(11)) {
    ///     Ok(_) => println!("OK Then!"),
    ///     Err(e) => println!("Ooops {:?}", e),
    /// }
    /// ```
    pub fn try_send(aid: &Arc<ActorId>, message: Arc<Message>) -> Result<(), ActorError> {
        if aid.is_stopped.load(Ordering::Relaxed) {
            Err(ActorError::ActorStopped)
        } else {
            match &aid.sender {
                ActorSender::Local(sender) => {
                    let receivable = sender.receivable();
                    sender.send_await(message).unwrap();
                    // The worst that happens here is the actor gets sent to the work channel
                    // more than once if several callers record receivable as 0 and then
                    // send the actor id. This is preferable to missiing a message.
                    if receivable == 0 {
                        ActorSystem::schedule(aid.clone())
                    };

                    Ok(())
                }
                _ => Err(ActorError::RemoteNotImplemented),
            }
        }
    }

    /// Returns the total number of messages that have been sent to the actor regardless of
    /// whether or not they have been received or processed by the actor.
    pub fn sent(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.sent(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that have been recieived by the actor.
    ///
    /// Note that this doesn't mean that the actor did anything with the message, just that it
    /// was received and handled.
    pub fn received(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.received(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the number of messages that are receivable by the actor.
    ///
    /// This will not include any messages that have been skipped until the skip is reset.
    pub fn receivable(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.receivable(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Returns the total number of messages that are pending to be received by the actor.
    ///
    /// This should include messages that have been skipped by the actor as well as those that are
    /// receivable.
    pub fn pending(&self) -> usize {
        match &self.sender {
            ActorSender::Local(sender) => sender.pending(),
            _ => panic!("Only implemented for Local sender!"),
        }
    }

    /// Checks to see if the actor referenced by this [ActorId] is actually stopped already.
    pub fn is_stopped(&self) -> bool {
        self.is_stopped.load(Ordering::AcqRel)
    }

    /// Marks the actor referenced by the [ActorId] as stopped and puts mechanisms in place to
    /// cause no more messages to be sent to the actor.
    ///
    /// Note that once stopped, the [ActorId] can never be started again.
    fn stop(&self) {
        self.is_stopped.fetch_or(true, Ordering::AcqRel);
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &'_ mut fmt::Formatter) -> fmt::Result {
        write!(
            formatter,
            "ActorId{{id: {}, node_id: {}, is_local: {}}}",
            self.id.to_string(),
            self.node_id.to_string(),
            if let ActorSender::Local(_) = self.sender {
                true
            } else {
                false
            }
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
///
/// This will be passed to a spawn funtion to specify the handler used for managing the state of
/// the actor based on the messages passed to the actor. The resulting state passed will be used
/// in the next message call. The processor takes three arguments. First, the `state` is a mutable
/// reference to the current state of the actor. Second, is the [ActorId] enclosed in an [Arc] to
/// allow access to the actor system for spawning, sending to self and so on.  Third is the
/// current message to process in a reference to an [Arc].
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

/// This is the type for the handler that will manage the state for the actor using the
/// user-provided message processor.
///
/// This is an internal closure that wraps the mutable state.
trait Handler: (FnMut(Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static {}

impl<F> Handler for F where F: (FnMut(Arc<ActorId>, &Arc<Message>) -> Status) + Send + Sync + 'static
{}

/// An actual actor in the system. An actor is a worker that allows the asynchronous
/// processing of messages.
///
///
/// The actor will obey a certain set of rules, namely.
/// 1. An actor can be interracted with only by means of messages.
/// 2. An actor processes only one message at a time.
/// 3. An actor will process a message only once.
/// 4. An actor can send a message to any other actor without knowledge of that actor's internals.
/// 5. Actors send only immutable data as messages, though they may have mutable internal state.
/// 6. Actors are location agnostic. An actor can be sent a message from anywhere in the cluster.
struct Actor {
    /// Id of the associated actor.
    aid: Arc<ActorId>,
    /// Receiver for the actor channel.
    receiver: SeccReceiver<Arc<Message>>,
    /// Function processes messages sent to the actor wrapped in a closure to erase the
    /// state type that the actor is maanaging.
    handler: Mutex<Box<dyn Handler>>,
}

impl Actor {
    /// Creates a new actor context with the given actor id.
    ///
    /// The user will pass the initial state of the actor as well as the processor that will be
    /// used to process messages sent to the actor. The system and node id are passed separately
    /// because of restrictions on mutex guards not being re-entrant in rust.
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
            is_stopped: AtomicBool::new(false),
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

    /// Receive a message from the channel and processes it with the actor.
    ///
    /// This function is the core of the processing pipeline and what the thread pool will be
    /// calling to handle each message.
    fn receive(actor: Arc<Actor>) {
        match actor.receiver.peek() {
            Result::Err(err) => {
                // This happening should be very rare but it would mean that the thread pool
                // tried to process a message for an actor and was beaten to it by another
                // thread. In this case we will just ignore the error and write out a debug
                // message for purposes of later optimization.
                error!("Error Occurred {:?}", err);
                ()
            }
            Result::Ok(message) => {
                // In this case there is a message in the channel that we have to process through
                // the actor.
                let mut guard = actor.handler.lock().unwrap();
                // FIXME (Issue #5) An actor panic shouldn't kill the whole system.
                let result = (&mut *guard)(actor.aid.clone(), message);
                match result {
                    Status::Processed => match actor.receiver.pop() {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error on Work Channel pop(): {:?}.", e);
                            actor.aid.system.stop(actor.aid.clone())
                        }
                    },
                    Status::Skipped => match actor.receiver.skip() {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error on Work Channel skip(): {:?}.", e);
                            actor.aid.system.stop(actor.aid.clone())
                        }
                    },
                    Status::ResetSkip => match actor.receiver.reset_skip() {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error on Work Channel reset_skip(): {:?}.", e);
                            actor.aid.system.stop(actor.aid.clone())
                        }
                    },
                    Status::Stop => {
                        actor.aid.system.stop(actor.aid.clone());
                        // Even though the actor is stopping we want to pop the message to make
                        // sure that the metrics on the actor's channel are correct. Then we will
                        // stop the actor in the actor system.
                        match actor.receiver.pop() {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error on Work Channel pop(): {:?}.", e);
                                actor.aid.system.stop(actor.aid.clone())
                            }
                        }
                    }
                };
                // If the actor still has receivable messages we will send it to the work queue
                // again to process the other messages, otherwise we just drop it.
                if actor.receiver.receivable() > 0 {
                    actor.aid.system.sender.send_await(actor.clone()).unwrap();
                }
            }
        }
    }
}

/// An actor system that contains and manages the actors spawned inside it.
///
/// Note that the actors live inside the system and are managed by the actor system throughout
/// their lives. Although it would be very unusual to have two actor systems on the same hardware
/// and OS process, it would be possible. Nevertheless, each actor stystem has a unique node id
/// that can be used by other actor systems to connect and send messages.
pub struct ActorSystem {
    /// Id for this actor system and node.
    pub node_id: Uuid,
    /// Sender side of the work channel.
    ///
    /// When an actor gets a message and its pending count goes from 0 to 1 it will put itself
    /// in the channel via the sender.
    sender: Arc<SeccSender<Arc<Actor>>>,
    /// Receiver side of the work channel. All threads in the pool will be grabbing actors
    /// from this receiver to process messages.
    receiver: Arc<SeccReceiver<Arc<Actor>>>,
    /// Holds the [Actor] objects keyed by the [ActorId].
    ///
    /// The [RwLock] will be locked for write only when a new actor is spawned but otherwise will
    /// be locked for read by the threads in the threadbool as they try to look up actors to process.
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
    ///
    /// The user should benchmark how many slots in the run channel and the number of threads
    /// they need in order to satisfy the requirements of the system they are creating.
    pub fn create(run_channel_size: u16, thread_pool_size: u16) -> Arc<ActorSystem> {
        // TODO Instead of two params we should pass a config struct.
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
        // system not being created but needed by the thread. We put this in a block to get around
        // rust borrow constraints without unnecessarily copying things.
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
    ///
    /// The dispatcher threads constantly grab at the work channel trying to get the next actor
    /// off the channel.  When they get an actor they will process the message using the actor
    /// and then check to see if the actor has more receivable messages. If it does then the actor
    /// will be re-sent to the work channel to process the next message. This process allows
    /// thousands of actors to run and not take up resources if they have no messages to process
    /// but also prevents one super busy actor from starving out actors of that get messages only
    /// occasionally.
    fn start_dispatcher_thread(system: Arc<ActorSystem>) -> JoinHandle<()> {
        // FIXME Add metrics to this to log warnings if the messages take to long to process.
        // FIXME Add metrics to this to log a warning if messages or actors are spending too
        // long in the channel.
        let receiver = system.receiver.clone();

        thread::spawn(move || {
            while !system.shutdown_triggered.load(Ordering::Relaxed) {
                match receiver.receive_await_timeout(system.thread_wait_time) {
                    Err(_) => (), // not an error, just loop and try again.
                    Ok(actor) => Actor::receive(actor),
                }
            }
        })
    }

    /// Triggers a shutdown of the system and returns only when all threads have joined.
    ///
    /// This allows a graceful shutdown of the actor system but any messages pending in actor
    /// channels will get discarded.
    pub fn shutdown(system: Arc<ActorSystem>) {
        system.shutdown_triggered.store(true, Ordering::Relaxed);
        let mut guard = system.thread_pool.lock().unwrap();
        let vec = std::mem::replace(&mut *guard, vec![]);
        for handle in vec {
            handle.join().unwrap();
        }
    }

    /// Returns the total number of times actors have been sent to the work channel.
    pub fn sent(&self) -> usize {
        self.receiver.sent()
    }

    /// Returns the total number of times actors have been processed from the work channel.
    pub fn received(&self) -> usize {
        self.receiver.received()
    }

    /// Returns the total number of actors that are currently pending in the work channel.
    pub fn pending(&self) -> usize {
        self.receiver.pending()
    }

    /// Spawns a new actor on the actor `system` using the given `starting_state` for the actors
    /// and the given `processor` function that will be used to process actor messages.
    ///
    /// The returned [ActorId] can be used to send messages to the actor.
    ///
    /// # Examples
    /// ```
    /// use axiom::actors::*;
    /// use std::sync::Arc;
    ///
    /// let system = ActorSystem::create(10, 1);
    ///
    /// let aid = ActorSystem::spawn(&system,
    ///     0 as usize,
    ///     |_state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| Status::Processed,
    /// );
    /// ```
    pub fn spawn<F, State>(system: &Arc<Self>, state: State, processor: F) -> Arc<ActorId>
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

    /// Schedules the `actor_id` for work on the given actor system. Note that this is the only
    /// time that we have to use the lookup table.
    ///
    /// It gets called when an actor goes from 0 receivable messages to 1 receivable messages. If
    /// the actor has more receivable messages then this will not be needed to be called because
    /// the dispatcher threads will handle the process of resending the actor to the work channel.
    fn schedule(aid: Arc<ActorId>) {
        // Note that this is implemented here rather than calling the sender directly
        // from the send in order :to allow internal optimization of the actor system.
        // FIXME (Issue #10) Harden this against the actor being out of the map.
        let guard = aid.system.actors_by_aid.read().unwrap();
        // FIXME (Issue #11) if the actor is not able to schedule, this will panic.
        (aid.system.sender.send(guard.get(&aid).unwrap().clone())).unwrap();
    }

    /// Stops an actor by shutting down its channels and removing it from the actors list and
    /// telling the actor id to not allow send to the actor since the receiving side of the
    /// actor is gone.
    ///
    /// This is something that should rarely be called from the outside as it is much better to
    /// send the actor a [SystemMsg::Shutdown] message.
    fn stop(&self, aid: Arc<ActorId>) {
        let mut guard = self.actors_by_aid.write().unwrap();
        guard.remove(&aid);
        aid.stop();
    }

    /// Checks to see if the actor with the given [ActorId] is alive within this actor system.
    pub fn is_alive(&self, aid: &Arc<ActorId>) -> bool {
        let guard = self.actors_by_aid.write().unwrap();
        guard.contains_key(aid)
    }
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;

    /// A test helper to assert that a certain number of messages arrived in a certain time.
    fn assert_await_received(aid: &Arc<ActorId>, count: u8, timeout_ms: u64) {
        use std::time::Instant;
        let start = Instant::now();
        let duration = Duration::from_millis(timeout_ms);
        while aid.received() < count as usize {
            if Instant::elapsed(&start) > duration {
                assert!(
                    false,
                    "Timed out! count: {} timeout_ms: {}",
                    count, timeout_ms
                )
            }
        }
    }

    #[test]
    fn test_simplest_actor() {
        init_test_log();

        // This test shows how the simplest actor can be built and used. This actor uses a closure
        // that simply returns that the message is processed.
        let system = ActorSystem::create(10, 1);

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inferrence we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let aid = ActorSystem::spawn(
            &system,
            0 as usize,
            |_state: &mut usize, _aid: Arc<ActorId>, _message: &Arc<Message>| Status::Processed,
        );

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11));

        // Wait for the message to get there because test is asynch.
        assert_await_received(&aid, 1, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_simplest_struct_actor() {
        init_test_log();

        // This test shows how the simplest struct-based actor can be built and used. This actor
        // merely returns that the message was processed.
        let system = ActorSystem::create(10, 1);

        // We create a basic struct that has a handle method that does basically nothing
        // and then we will create that struct when we spawn the actor and that is all.
        struct Data {}

        impl Data {
            fn handle(&mut self, _aid: Arc<ActorId>, _message: &Arc<Message>) -> Status {
                Status::Processed
            }
        }

        let aid = ActorSystem::spawn(&system, Data {}, Data::handle);

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11));

        // Wait for the message to get there because test is asynch.
        assert_await_received(&aid, 1, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_dispatching_with_closure() {
        init_test_log();

        // This test shows how a closure-based actor can be used and process different kinds of
        // messages and mutate its state based upon the messages passed. Note that the state of
        // the actor is not available outside the actor itself. There is no way to get access to
        // the state without going through the actor.
        let system = ActorSystem::create(10, 1);

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inferrence we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let starting_state: usize = 0 as usize;
        let closure = |state: &mut usize, aid: Arc<ActorId>, message: &Arc<Message>| {
            // Expected messages in the expected order.
            let expected: Vec<i32> = vec![11, 13, 17];
            // Attempt to downcast to expected message.
            if let Some(msg) = message.downcast_ref::<i32>() {
                assert_eq!(expected[*state], *msg);
                assert_eq!(*state, aid.received());
                *state += 1;
                assert_eq!(aid.pending(), aid.sent() - aid.received());
                Status::Processed
            } else {
                assert!(false, "Failed to dispatch properly");
                Status::Processed // assertion will fail but we still have to return.
            }
        };

        let aid = ActorSystem::spawn(&system, starting_state, closure);

        // Send some messages to the actor in the order required in the test. In a real actor
        // its unlikely any order restriction would be needed. However this test makes sure that
        // the messages are processed correctly.
        ActorId::send(&aid, Arc::new(11 as i32));
        assert_eq!(1, aid.sent());
        ActorId::send(&aid, Arc::new(13 as i32));
        assert_eq!(2, aid.sent());
        ActorId::send(&aid, Arc::new(17 as i32));
        assert_eq!(3, aid.sent());

        // Wait for all of the messages to get there because this test is asynch.
        assert_await_received(&aid, 3, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_dispatching_with_struct() {
        init_test_log();

        // This test shows how a struct-based actor can be used and process different kinds of
        // messages and mutate its state based upon the messages passed. Note that the state of
        // the actor is not available outside the actor itself. There is no way to get access to
        let system = ActorSystem::create(10, 1);

        // We create a basic struct with a handler and use that handler to dispatch to other
        // inherent methods in the struct. Note that we dont have to implement any traits here
        // and there is nothing forcing the handler to be an inherent method if it doesn't want
        // to. It could be any method, even one not on the struct.
        struct Data {
            value: i32,
        }

        impl Data {
            fn handle_bool(&mut self, _aid: Arc<ActorId>, message: &bool) -> Status {
                if *message {
                    self.value += 1;
                } else {
                    self.value -= 1;
                }
                Status::Processed // assertion will fail but we still have to return.
            }

            fn handle_i32(&mut self, _aid: Arc<ActorId>, message: &i32) -> Status {
                self.value += *message;
                Status::Processed // assertion will fail but we still have to return.
            }

            fn handle(&mut self, aid: Arc<ActorId>, message: &Arc<Message>) -> Status {
                if let Some(msg) = message.downcast_ref::<bool>() {
                    self.handle_bool(aid, msg)
                } else if let Some(msg) = message.downcast_ref::<i32>() {
                    self.handle_i32(aid, msg)
                } else {
                    assert!(false, "Failed to dispatch properly");
                    Status::Stop // assertion will fail but we still have to return.
                }
            }
        }

        let data = Data { value: 0 };

        let aid = ActorSystem::spawn(&system, data, Data::handle);

        // Send some messages to the actor.
        ActorId::send(&aid, Arc::new(11));
        ActorId::send(&aid, Arc::new(true));
        ActorId::send(&aid, Arc::new(true));
        ActorId::send(&aid, Arc::new(false));

        // Wait for all of the messages to get there because this test is asynch.
        assert_await_received(&aid, 4, 1000);
        ActorSystem::shutdown(system)
    }

    #[test]
    fn test_actor_stop() {
        init_test_log();

        let system = ActorSystem::create(10, 1);

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inferrence we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let aid = ActorSystem::spawn(
            &system,
            0 as usize,
            |state: &mut usize, _aid: Arc<ActorId>, message: &Arc<Message>| {
                if let Some(_msg) = message.downcast_ref::<i32>() {
                    assert_eq!(0 as usize, *state);
                    *state += 1;
                    Status::Processed
                } else if let Some(msg) = message.downcast_ref::<SystemMsg>() {
                    assert_eq!(1 as usize, *state);
                    *state += 1;
                    match msg {
                        SystemMsg::Stop => Status::Stop,
                    }
                } else {
                    assert!(false, "Failed to dispatch properly");
                    Status::Processed // assertion will fail but we still have to return.
                }
            },
        );

        // Send a message to the actor.
        ActorId::send(&aid, Arc::new(11 as i32));
        ActorId::send(&aid, Arc::new(SystemMsg::Stop));

        // Wait for the message to get there because test is asynch.
        assert_await_received(&aid, 2, 1000);

        // Make sure that the actor is actually stopped and cant get more messages.
        assert!(true, aid.is_stopped());
        match ActorId::try_send(&aid, Arc::new(42 as i32)) {
            Err(ActorError::ActorStopped) => assert!(true), // all ok!
            Ok(_) => assert!(false, "Expected the actor to be shut down!"),
            Err(e) => assert!(false, "Unexpected error: {:?}", e),
        }
        assert_eq!(false, system.is_alive(&aid));

        // Shut down the system and clean up test.
        ActorSystem::shutdown(system);
    }

    // ---------- Complete Example ----------

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

        fn handle(&mut self, aid: Arc<ActorId>, message: &Arc<Message>) -> Status {
            if let Some(msg) = message.downcast_ref::<i32>() {
                self.handle_i32(aid, msg)
            } else if let Some(msg) = message.downcast_ref::<Operation>() {
                self.handle_op(aid, msg)
            } else if let Some(msg) = message.downcast_ref::<u8>() {
                assert_eq!(3, aid.received());
                assert_eq!(7 as u8, *msg);
                self.count += *msg as usize;
                assert_eq!(29 as usize, self.count);
                Status::Processed
            } else {
                assert!(false, "Failed to dispatch properly");
                Status::Processed // assertion will fail but we still have to return.
            }
        }
    }

    #[test]
    fn test_full_example() {
        init_test_log();

        // This test uses the actor struct declared above to demonstrate and test most of the
        // capabilities of actors. This is a fairly complete example.
        let system = ActorSystem::create(10, 1);
        let starting_state: StructActor = StructActor { count: 5 as usize };

        let aid = ActorSystem::spawn(&system, starting_state, StructActor::handle);

        ActorId::send(&aid, Arc::new(Operation::Inc));
        assert_eq!(1, aid.sent());
        ActorId::send(&aid, Arc::new(Operation::Dec));
        assert_eq!(2, aid.sent());
        ActorId::send(&aid, Arc::new(17 as i32));
        assert_eq!(3, aid.sent());
        ActorId::send(&aid, Arc::new(7 as u8));
        assert_eq!(4, aid.sent());

        // Wait for the message to get there because test is asynch.
        assert_await_received(&aid, 4, 1000);
        ActorSystem::shutdown(system)
    }

}
