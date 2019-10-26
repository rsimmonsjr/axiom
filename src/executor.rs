use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::thread::JoinHandle;
use std::time::Instant;

use dashmap::DashMap;
use futures::task::ArcWake;
use futures::Stream;
use log::error;
use uuid::Uuid;

use crate::actors::PinnedActorRef;
use crate::{ActorSystemConfig, AxiomError, Status};

/// The Executor is responsible for the starting and high-level scheduling of Actors. When an
/// Actor is registered, it is wrapped in a Task and added to the sleep queue. When the Actor is
/// woken by a sent message, the Executor will check its scheduling data and queue it in the
/// appropriate Reactor.
#[derive(Clone)]
pub(crate) struct AxiomExecutor {
    /// The Config of the owning ActorSystem.
    config: ActorSystemConfig,
    /// The system's "is shutting down" flag.
    shutdown_triggered: Arc<AtomicBool>,
    /// Actors that have no messages available.
    sleeping: Arc<DashMap<Uuid, Task>>,
    /// All Reactors owned by this Executor.
    reactors: Arc<DashMap<u16, AxiomReactor>>,
    /// Counting actors per reactor for even distribution of Actors.
    actors_per_reactor: Arc<DashMap<u16, u32>>,
}

impl AxiomExecutor {
    /// Creates a new Executor with the given actor system configuration. This will govern the
    /// configuration of the executor.
    pub(crate) fn new(config: ActorSystemConfig, shutdown_triggered: Arc<AtomicBool>) -> Self {
        Self {
            config,
            shutdown_triggered,
            sleeping: Arc::new(Default::default()),
            reactors: Arc::new(Default::default()),
            actors_per_reactor: Default::default(),
        }
    }

    /// Initializes the executor and starts the AxiomReactor instances based on the count of the
    /// number of threads configured in the actor system. This must be called before any work can
    /// be performed with the actor system.
    pub(crate) fn init(&self) {
        for i in 0..self.config.thread_pool_size {
            self.reactors.insert(i, AxiomReactor::new(self.clone(), i));
            self.actors_per_reactor.insert(i, 0);
        }
    }

    /// This gives the Actor to the Executor to manage. This must be ran before any messages are
    /// sent to the Actor, else it will fail to be woken until after its registered.
    pub(crate) fn register_actor(&self, actor: PinnedActorRef) {
        let id = actor.read().expect("Poisoned Actor").context.aid.uuid();
        self.sleeping.insert(id, Task { id, actor });
    }

    /// This wakes an Actor in the Executor which will cause its future to be polled. The Aid,
    /// through the ActorSystem, will call this on Message Send.
    pub(crate) fn wake(&self, id: Uuid) {
        // Pull the Task
        let task = match self.sleeping.remove(&id) {
            Some((_, task)) => task,
            None => return, // The Actor is already awake.
        };
        // Get the optimal Reactor
        let destination = self.get_reactor_with_least_actors();
        // Increment the Reactor's Actor count
        *self.actors_per_reactor.get_mut(&destination).unwrap() += 1;
        // Insert in the Reactor
        self.reactors.get(&destination).unwrap().insert(task);
    }

    /// Iterates over the actors-per-reactor collection, and finds the Reactor with the least number
    /// of Actors.
    fn get_reactor_with_least_actors(&self) -> u16 {
        let mut iter_state = (0u16, u32::max_value());
        for i in self.actors_per_reactor.iter() {
            if i.value() < &iter_state.1 {
                iter_state = (*i.key(), *i.value());
            }
        }
        iter_state.0
    }

    /// When a Reactor is done with an Actor, it will be sent here.  If it is sent with a `Some`
    /// `new_reactor`, it's been reassigned to a different reactor and will be sent to that
    /// reactor immediately.  Otherwise, it's been depleted of messages to process and should be
    /// stored as sleeping.
    fn return_task(&self, task: Task, reactor: u16) {
        // Put the Task back.
        self.sleeping.insert(task.id, task);
        // Decrement the Reactor's Actor count.
        *self.actors_per_reactor.get_mut(&reactor).unwrap() -= 1;
    }

    pub(crate) fn shutdown(&self) {
        self.reactors
            .iter()
            .filter_map(|reactor| reactor.join())
            .for_each(|j| match j.join() {
                Err(_) => error!("Panic in Reactor thread"),
                _ => {}
            });
    }
}

/// The Reactor is a single-threaded loop that will poll the woken Actors. It will run scheduling
/// checks for `Ready(Some)`, either re-running or returning to the Executor for schedule
/// adjustments. It will return the Task to the Executor on `Ready(None)`, until the Actor wakes
/// again. It will return the Task to the wait_queue on `Pending`.
#[derive(Clone)]
pub(crate) struct AxiomReactor {
    /// The ID of the Reactor
    id: u16,
    /// The Executor that owns this Reactor.
    executor: AxiomExecutor,
    /// The queue of Actors that are ready to be polled.
    run_queue: Arc<RwLock<VecDeque<Wakeup>>>,
    /// The queue of Actors this Reactor is responsible for.
    wait_queue: Arc<RwLock<BTreeMap<Uuid, Task>>>,
    /// This is used as a semaphore to ensure the Reactor has only a single active thread.
    thread_join: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl AxiomReactor {
    /// Creates a new Reactor
    fn new(executor: AxiomExecutor, id: u16) -> AxiomReactor {
        AxiomReactor {
            id,
            executor,
            run_queue: Arc::new(RwLock::new(Default::default())),
            wait_queue: Arc::new(RwLock::new(BTreeMap::new())),
            thread_join: Arc::new(RwLock::new(None)),
        }
    }

    /// Moves an Actor from the executor into a reactor.
    fn insert(&self, task: Task) {
        let token = Token {
            id: task.id,
            reactor: self.clone(),
        };
        let waker = futures::task::waker(Arc::new(token));
        let wakeup = Wakeup { id: task.id, waker };
        self.wait(task);
        self.wake(wakeup);
        self.ensure_running();
    }

    /// This is the logic for the core loop that drives the Reactor. It MUST be invoked inside a
    /// dedicated thread, else it will block the executor.
    fn thread(&self) {
        loop {
            // If we're shutting down, quit.
            if self.executor.shutdown_triggered.load(Ordering::Relaxed) {
                break;
            }

            // If there's no Actors woken, the Reactor thread can end until it's needed again.
            // If there's an Wakeup without an Actor (which might happen due to an acceptable race
            // condition), we can continue to the next woken Actor, and drop this Wakeup. Otherwise,
            // we have the Wakeup and Task we need, and can continue.
            //
            // While this arrangement is a little dense, it saves a level of indentation, and early
            // returns are easier to read.
            let (w, mut task) = if let Some(w) = self.get_woken() {
                if let Some(task) = self.remove_waiting(&w.id) {
                    (w, task)
                } else {
                    continue;
                }
            } else {
                break;
            };

            let end = Instant::now() + self.executor.config.time_slice;
            loop {
                // This polls the Actor as a Stream.
                match task.poll(&w.waker) {
                    Poll::Ready(result) => {
                        // Ready(None) indicates an empty message queue. Time to sleep.
                        if let None = result {
                            self.executor.return_task(task, self.id);
                            break;
                        }
                        let result = result.unwrap();
                        // The Actor should handle its own internal modifications in response to the
                        // result.
                        task.actor
                            .read()
                            .expect("Poisoned Actor")
                            .handle_result(&result);
                        match result {
                            // Intentional or error'd stop means drop. It's dead, Jim.
                            Ok(Status::Stop) | Err(_) => break,
                            _ => {
                                // If we're past this timeslice, add back into the queues and move
                                // to the next woken Actor. Else, keep going.
                                if Instant::now() >= end {
                                    self.wait(task);
                                    self.wake(w);
                                    break;
                                } else {
                                    continue;
                                }
                            }
                        }
                    }
                    // Still pending, return to wait_queue. Drop the wakeup, because the futures
                    // will re-add it later through their wakers.
                    Poll::Pending => {
                        self.wait(task);
                        break;
                    }
                }
            }
        }
    }

    /// This function ensures the Reactor is running when it is needed to be.
    /// By allowing it to conclude and simply restart as necessary, we avoid
    /// CPU spin locking. We should call this function as much as we need.
    // TODO: Check if the thread has panicked.
    fn ensure_running(&self) {
        // Ensure running? Nah, we're shutting down.
        if self.executor.shutdown_triggered.load(Ordering::Relaxed) {
            return;
        }
        let mut join = self.thread_join.write().expect("Poisoned thread_join");
        let r = self.clone();
        join.get_or_insert_with(|| {
            self.thread_builder()
                .spawn(move || {
                    r.thread();
                    r.thread_join.write().expect("Poisoned thread_join").take();
                })
                .unwrap_or_else(|e| panic!("Error creating Reactor thread: {}", e))
        });
    }

    #[inline]
    fn thread_builder(&self) -> thread::Builder {
        thread::Builder::new().name(format!("ActorReactor-{}", self.id))
    }

    /// Takes the JoinHandle from the Reactor. If called before shutting down, the Reactor may start
    /// back up if ensure_running is called before the ActorSystem is shutdown.
    fn join(&self) -> Option<JoinHandle<()>> {
        self.thread_join
            .write()
            .expect("Poisoned thread_join")
            .take()
    }

    /// Add an Actor's Wakeup to the run_queue.
    fn wake(&self, wakeup: Wakeup) {
        self.run_queue
            .write()
            .expect("Poisoned run_queue")
            .push_back(wakeup);
    }

    /// Pop the next Wakeup.
    fn get_woken(&self) -> Option<Wakeup> {
        self.run_queue
            .write()
            .expect("Poisoned run_queue")
            .pop_front()
    }

    /// Add a Task to the Reactor's wait_queue.
    fn wait(&self, task: Task) {
        self.wait_queue
            .write()
            .expect("Poisoned wait_queue")
            .insert(task.id, task);
    }

    /// Remove a Task from the Reactor's wait_queue.
    fn remove_waiting(&self, id: &Uuid) -> Option<Task> {
        self.wait_queue
            .write()
            .expect("Poisoned wait_queue")
            .remove(id)
    }
}

/// Tasks represent the unit of work that an Executor-Reactor system is responsible for.
struct Task {
    id: Uuid,
    actor: PinnedActorRef,
}

impl Task {
    fn poll(&mut self, waker: &Waker) -> Poll<Option<Result<Status, AxiomError>>> {
        let mut ctx = Context::from_waker(waker);

        self.actor
            .write()
            .expect("Poisoned Actor")
            .as_mut()
            .poll_next(&mut ctx)
    }
}

/// Object used for generating our wakers.
struct Token {
    id: Uuid,
    reactor: AxiomReactor,
}

impl ArcWake for Token {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let id = arc_self.id;

        let wakeup = Wakeup {
            id,
            waker: futures::task::waker(arc_self.clone()),
        };

        (arc_self.reactor).wake(wakeup);
    }
}

/// Object representing the need to wake an Actor, to be enqueued for waking.
struct Wakeup {
    id: Uuid,
    waker: Waker,
}
