//! The Executor is responsible for the high-level scheduling of Actors.

use crate::actors::ActorStream;
use crate::{ActorSystem, Status, StdError, Aid};
use dashmap::DashMap;
use futures::task::ArcWake;
use futures::Stream;
use log::{debug, trace};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};
use std::pin::Pin;

/// The Executor is responsible for the high-level scheduling of Actors. When an Actor is
/// registered, it is wrapped in a Task and added to the sleep queue. When the Actor is
/// woken by a sent message, the Executor will check its scheduling data and queue it in the
/// appropriate Reactor.
#[derive(Clone)]
pub(crate) struct AxiomExecutor {
    /// The system's "is shutting down" flag.
    shutdown_triggered: Arc<(Mutex<bool>, Condvar)>,
    /// Barrier to await shutdown on.
    shutdown_semaphore: Arc<DrainAwait>,
    /// Actors that have no messages available.
    sleeping: Arc<DashMap<Aid, Task>>,
    /// All Reactors owned by this Executor.
    reactors: Arc<DashMap<u16, AxiomReactor>>,
    /// Counting actors per reactor for even distribution of Actors.
    actors_per_reactor: Arc<DashMap<u16, u32>>,
}

impl AxiomExecutor {
    /// Creates a new Executor with the given actor system configuration. This will govern the
    /// configuration of the executor.
    pub(crate) fn new(shutdown_triggered: Arc<(Mutex<bool>, Condvar)>) -> Self {
        Self {
            shutdown_triggered,
            shutdown_semaphore: Arc::new(DrainAwait::new()),
            sleeping: Arc::new(Default::default()),
            reactors: Arc::new(Default::default()),
            actors_per_reactor: Default::default(),
        }
    }

    /// Initializes the executor and starts the AxiomReactor instances based on the count of the
    /// number of threads configured in the actor system. This must be called before any work can
    /// be performed with the actor system.
    pub(crate) fn init(&self, system: ActorSystem) {
        for i in 0..system.data.config.thread_pool_size {
            self.reactors
                .insert(i, AxiomReactor::new(self.clone(), system.clone(), i));
            self.actors_per_reactor.insert(i, 0);
        }
    }

    /// This gives the Actor to the Executor to manage. This must be ran before any messages are
    /// sent to the Actor, else it will fail to be woken until after its registered.
    pub(crate) fn register_actor(&self, actor: ActorStream) {
        let id = actor.context.aid.clone();
        let actor = Mutex::new(Box::pin(actor));

        self.sleeping.insert(id.clone(), Task { id, actor });
    }

    /// This wakes an Actor in the Executor which will cause its future to be polled. The Aid,
    /// through the ActorSystem, will call this on Message Send.
    pub(crate) fn wake(&self, id: Aid) {
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

    /// When a Reactor is done with an Actor, it will be sent here, and the Executor will decrement
    /// the Actor count for that Reactor.
    fn return_task(&self, task: Task, reactor: u16) {
        // Put the Task back.
        self.sleeping.insert(task.id.clone(), task);
        // Decrement the Reactor's Actor count.
        *self.actors_per_reactor.get_mut(&reactor).unwrap() -= 1;
    }

    pub(crate) fn await_shutdown(&self, timeout: impl Into<Option<Duration>>) -> ShutdownResult {
        match timeout.into() {
            Some(timeout) => self.shutdown_semaphore.wait_timeout(timeout),
            None => self.shutdown_semaphore.wait(),
        }
    }
}

/// A semaphore of sorts that unblocks when its internal counter hits 0
struct DrainAwait {
    /// Mutex for blocking on
    mutex: Mutex<u16>,
    /// Condvar for waiting on
    condvar: Condvar,
}

impl DrainAwait {
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(0),
            condvar: Condvar::new(),
        }
    }

    /// Increment the counter
    pub fn increment(&self) {
        *self.mutex.lock().expect("DrainAwait poisoned") += 1;
    }

    /// Decrement the counter, notify condvar if it hits 0
    pub fn decrement(&self) {
        let mut guard = self.mutex.lock().expect("DrainAwait poisoned");
        *guard -= 1;
        trace!("Decrementing DrainAwait to {}", *guard);
        if *guard == 0 {
            debug!("Notifying blocked threads");
            self.condvar.notify_all();
        }
    }

    /// Block on the condvar
    pub fn wait(&self) -> ShutdownResult {
        let mut guard = match self.mutex.lock() {
            Ok(g) => g,
            Err(_) => return ShutdownResult::Panicked,
        };

        while *guard != 0 {
            guard = match self.condvar.wait(guard) {
                Ok(g) => g,
                Err(_) => return ShutdownResult::Panicked,
            };
        }
        ShutdownResult::Ok
    }

    /// Block on the condvar until it times out
    pub fn wait_timeout(&self, timeout: Duration) -> ShutdownResult {
        let mut guard = match self.mutex.lock() {
            Ok(g) => g,
            Err(_) => return ShutdownResult::Panicked,
        };

        while *guard != 0 {
            let (new_guard, timeout) = match self.condvar.wait_timeout(guard, timeout) {
                Ok(ret) => (ret.0, ret.1),
                Err(_) => return ShutdownResult::Panicked,
            };

            if timeout.timed_out() {
                return ShutdownResult::TimedOut;
            }
            guard = new_guard;
        }
        ShutdownResult::Ok
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum ShutdownResult {
    Ok,
    TimedOut,
    Panicked,
}

/// The Reactor is a single-threaded loop that will poll the woken Actors. It will run scheduling
/// checks for `Ready(Some)`, either re-running or returning to the Executor for schedule
/// adjustments. It will return the Task to the Executor on `Ready(None)`, until the Actor wakes
/// again. It will return the Task to the wait_queue on `Pending`.
#[derive(Clone)]
pub(crate) struct AxiomReactor {
    /// The ID of the Reactor
    id: u16,
    /// The diagnostic ID of this Reactor.
    name: String,
    /// The Executor that owns this Reactor.
    executor: AxiomExecutor,
    /// The System that owns the Executor that owns this Reactor.
    system: ActorSystem,
    /// The queue of Actors that are ready to be polled.
    run_queue: Arc<RwLock<VecDeque<Wakeup>>>,
    /// The queue of Actors this Reactor is responsible for.
    wait_queue: Arc<RwLock<BTreeMap<Aid, Task>>>,
    /// This is used to pause/resume threads that run out of work.
    thread_condvar: Arc<RwLock<(Mutex<()>, Condvar)>>,
    /// This is used as a semaphore to ensure the Reactor has only a single active thread.
    thread_state: Arc<RwLock<ThreadState>>,
    /// The current Actor being processed by the Reactor.
    current_actor: Arc<Mutex<Option<Aid>>>,
}

enum LoopResult<T> {
    Ok(T),
    Continue,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ThreadState {
    Running,
    Stopped,
    Panicked,
}

struct ThreadLease(Arc<RwLock<ThreadState>>);

impl Drop for ThreadLease {
    fn drop(&mut self) {
        // Get the current state of the thread. A poisoned lock would indicate something VERY wrong
        let current_state = { match self.0.read() {
            Ok(state) => *state,
            Err(err) => *err.into_inner()
        }};

        // If it was dropped before it was set to Stopped, then it panicked.
        if let ThreadState::Running = current_state {
            *self.0.write().unwrap() = ThreadState::Panicked;
        }
    }
}

impl AxiomReactor {
    /// Creates a new Reactor
    fn new(executor: AxiomExecutor, system: ActorSystem, id: u16) -> AxiomReactor {
        let name = format!("{:08x?}-{}", system.data.uuid.as_fields().0, id);

        AxiomReactor {
            id,
            name,
            executor,
            system,
            run_queue: Arc::new(RwLock::new(Default::default())),
            wait_queue: Arc::new(RwLock::new(BTreeMap::new())),
            thread_condvar: Arc::new(RwLock::new((Mutex::new(()), Condvar::new()))),
            thread_state: Arc::new(RwLock::new(ThreadState::Stopped)),
            current_actor: Arc::new(Mutex::new(None))
        }
    }

    /// Moves an Actor from the executor into a reactor.
    fn insert(&self, task: Task) {
        let token = Token {
            id: task.id.clone(),
            reactor: self.clone(),
        };
        let waker = futures::task::waker(Arc::new(token));
        let wakeup = Wakeup { id: task.id.clone(), waker };
        self.wait(task);
        self.wake(wakeup);
    }

    /// This is the logic for the core loop that drives the Reactor. It MUST be invoked inside a
    /// dedicated thread, else it will block the executor.
    fn thread(&self) {
        debug!("Reactor-{} thread started", self.name);
        loop {
            // If we're shutting down, quit.
            {
                if *self.executor.shutdown_triggered.0.lock()
                    .expect("Poisoned shutdown_triggered condvar") {
                    debug!("Reactor-{} acknowledging shutdown", self.name);
                    break;
                }
            }

            let (w, mut task) = match self.get_work() {
                LoopResult::Ok(v) => v,
                LoopResult::Continue => continue,
            };

            let end = Instant::now() + self.system.data.config.time_slice;
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
                        let is_stopping = match &result {
                            Ok(Status::Stop) | Err(_) => true,
                            _ => false,
                        };
                        // The Actor should handle its own internal modifications in response to the
                        // result.
                        {
                            task.actor
                                .lock()
                                .expect("Poisoned Actor")
                                .handle_result(result);
                        }
                        // Intentional or error'd stop means drop. It's dead, Jim.
                        if is_stopping {
                            break;
                        }
                        // If we're past this timeslice, add back into the queues and move
                        // to the next woken Actor. Else, poll it again.
                        if Instant::now() >= end {
                            self.wait(task);
                            self.wake(w);
                            break;
                        } else {
                            continue;
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
        debug!("Reactor-{} thread ended", self.name);
    }

    // If there's no Actors woken, the Reactor thread will block on the condvar. If there's a Wakeup
    // without an Actor (which might happen due to an acceptable race condition), we can continue to
    // the next woken Actor, and drop this Wakeup. Otherwise, we have the Wakeup and Task we need,
    // and can continue.
    //
    // While this arrangement is a little dense, it saves a level of indentation.
    #[inline]
    fn get_work(&self) -> LoopResult<(Wakeup, Task)> {
        if let Some(w) = self.get_woken() {
            if let Some(task) = self.remove_waiting(&w.id) {
                trace!("Reactor-{} Got work", self.name);
                { *self.current_actor.lock().expect("Poisoned current_actor") = Some(w.id.clone()) }
                LoopResult::Ok((w, task))
            } else {
                trace!("Reactor-{} dropping futile WakeUp", self.name);
                LoopResult::Continue
            }
        } else {
            let (mutex, condvar) = &*self
                .thread_condvar
                .read()
                .expect("Poisoned Reactor condvar");

            trace!("Reactor-{} waiting on condvar", self.name);
            let g = mutex.lock().expect("Poisoned Reactor condvar");
            let _ = condvar
                .wait_timeout(g, self.system.data.config.thread_wait_time)
                .expect("Poisoned Reactor condvar");
            trace!("Reactor-{} resuming", self.name);
            LoopResult::Continue
        }
    }

    /// This function ensures the Reactor is running when it is needed to be, recovering from a
    /// panic if necessary. We should call this function as much as we need.
    fn ensure_running(&self) {
        // Ensure running? Nah, we're shutting down.
        {if *self.executor.shutdown_triggered.0.lock().unwrap() {
            return;
        }}

        match { *self.thread_state.read().expect("Poisoned thread_state") } {
            ThreadState::Running => {}
            ThreadState::Panicked => { self.recover_from_panic(); self.start_thread() }
            // This is the coldest path in the current impl, because we don't let the thread die
            // unless shutdown is triggered.
            ThreadState::Stopped => self.start_thread(),
        }
        // We've ensured it's running, let's make sure it's not waiting
        self.thread_condvar
            .read()
            .expect("Poisoned Reactor condvar")
            .1
            .notify_one();
    }

    /// Start a new thread. This should only be called when the state is Stopped or Panicked.
    #[cold]
    fn start_thread(&self) {
        let reactor = self.clone();
        thread::Builder::new().name(format!("Reactor-{}", self.name))
            .spawn(move || {
                { *reactor.thread_state.write()
                    .expect("thread_state poisoned at thread start")
                = ThreadState::Running; }
                reactor.executor.shutdown_semaphore.increment();
                reactor.system.init_current();
                reactor.thread();
                reactor.executor.shutdown_semaphore.decrement();
                *reactor.thread_state.write()
                    .expect("thread_state poisoned at thread end")
                = ThreadState::Stopped;
            })
            .unwrap_or_else(|e| panic!("Error creating Reactor thread: {}", e));
    }

    /// Goes over all the locks in the Reactor and ensures they aren't poisoned. It holds onto the
    /// locks so it can release them all at once. Additionally, informs the ActorSystem of the Actor
    /// that poisoned it.
    fn recover_from_panic(&self) {
        let _run_queue_guard = match self.run_queue.write() {
            Ok(g) => g,
            Err(psn) => psn.into_inner(),
        };

        let _wait_queue_guard = match self.wait_queue.write() {
            Ok(g) => g,
            Err(psn) => psn.into_inner(),
        };

        let _thread_condvar_guard = match self.thread_condvar.write() {
            Ok(g) => { let _ = g.0.lock().map_err(|psn| psn.into_inner()); g },
            Err(psn) => psn.into_inner(),
        };

        let _thread_state_guard = match self.thread_state.write() {
            Ok(g) => g,
            Err(psn) => psn.into_inner(),
        };

        let perpetrator = match self.current_actor.lock() {
            Ok(id) => (*id).clone().unwrap(),
            Err(psn) => (*psn.into_inner()).clone().unwrap(),
        };
        self.system.stop_actor(&perpetrator);
    }

    /// Add an Actor's Wakeup to the run_queue.
    fn wake(&self, wakeup: Wakeup) {
        self.run_queue
            .write()
            .expect("Poisoned run_queue")
            .push_back(wakeup);
        self.ensure_running();
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
            .insert(task.id.clone(), task);
    }

    /// Remove a Task from the Reactor's wait_queue.
    fn remove_waiting(&self, id: &Aid) -> Option<Task> {
        self.wait_queue
            .write()
            .expect("Poisoned wait_queue")
            .remove(id)
    }
}

/// Tasks represent the unit of work that an Executor-Reactor system is responsible for.
struct Task {
    id: Aid,
    actor: Mutex<Pin<Box<ActorStream>>>,
}

impl Task {
    fn poll(&mut self, waker: &Waker) -> Poll<Option<Result<Status, Box<StdError>>>> {
        let mut ctx = Context::from_waker(waker);

        self.actor
            .lock()
            .expect("Poisoned ActorStream")
            .as_mut()
            .poll_next(&mut ctx)
    }
}

/// Object used for generating our wakers.
struct Token {
    id: Aid,
    reactor: AxiomReactor,
}

impl ArcWake for Token {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let id = arc_self.id.clone();

        let wakeup = Wakeup {
            id,
            waker: futures::task::waker(arc_self.clone()),
        };

        (arc_self.reactor).wake(wakeup);
    }
}

/// Object representing the need to wake an Actor, to be enqueued for waking.
struct Wakeup {
    id: Aid,
    waker: Waker,
}
