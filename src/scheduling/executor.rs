use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::thread::JoinHandle;

use dashmap::DashMap;
use futures::task::ArcWake;
use uuid::Uuid;

use crate::{ActorSystemConfig, AxiomError, Status};
use crate::actors::Actor;
use crate::scheduling::{Schedule, Scheduler, ScheduleResult};
use futures::Stream;

/// The Executor is responsible for the starting and high-level scheduling of
/// Actors. When an Actor is registered, it is wrapped in a Task and added to
/// the sleep queue. When the Actor is woken by a sent message, the Executor
/// will check its scheduling data and queue it in the appropriate Reactor.
#[derive(Clone)]
pub(crate) struct AxiomExecutor {
    config: ActorSystemConfig,
    sleeping: Arc<DashMap<Uuid, Task>>,
    scheduler: Arc<dyn Scheduler + Send + Sync>,
    reactors: Arc<DashMap<u16, AxiomReactor>>,
}

impl AxiomExecutor {
    pub(crate) fn new<S: Scheduler + Send + Sync + 'static>(config: &ActorSystemConfig) -> Self {
        Self {
            config: config.clone(),
            sleeping: Arc::new(Default::default()),
            scheduler: Arc::new(S::new(config)),
            reactors: Arc::new(Default::default()),
        }
    }

    pub(crate) fn init(&self) {
        for i in 0..self.config.thread_pool_size {
            self.reactors.insert(i, AxiomReactor::new(self.clone()));
        }
    }

    /// This gives the Actor to the Executor to manage. This must be ran
    /// before any messages are sent to the Actor, else it will fail to be
    /// woken until after its registered.
    pub(crate) fn register_actor(&self, actor: Arc<RwLock<Pin<Box<Actor>>>>) {
        let id = actor.read().expect("Poisoned Actor").context.aid.uuid();
        let mut schedule = Schedule::new(id, self.scheduler.clone());
        schedule.update();
        schedule.log_start();
        self.sleeping.insert(
            id,
            Task {
                id,
                schedule,
                actor,
            },
        );
    }

    /// The Aid, through the ActorSystem, should call this on Message Send.
    pub(crate) fn wake(&self, id: Uuid) {
        let mut task = match self.sleeping.remove(&id) {
            Some((_, task)) => task,
            None => return, // The Actor is already awake.
        };

        task.schedule.log_wake();
        task.schedule.update();

        let reactor = self
            .reactors
            .get(&task.schedule.get_reactor())
            .expect("Uninitialized Reactor");

        reactor.insert(task);
    }

    /// When a Reactor is done with an Actor, it should be sent here.
    /// If it is sent with new_reactor: Some, it's been reassigned to
    /// a different Reactor and should be sent there immediately.
    /// Otherwise, it's been depleted and should be stored as Sleeping.
    fn return_task(&self, task: Task, new_reactor: Option<u16>) {
        match new_reactor {
            Some(id) => {
                let r = self
                    .reactors
                    .get(&id)
                    .expect("Scheduler provided invalid Reactor ID");
                r.insert(task);
            }
            None => {
                task.schedule.log_sleep();
                self.sleeping.insert(task.id, task);
            }
        }
    }
}

/// The Reactor is a single-threaded loop that will poll the woken Actors. It
/// will run scheduling checks for `Ready(Some)`, either re-running or returning
/// to the Executor for schedule adjustments. It will return the Task to the
/// Executor on `Ready(None)`, until the Actor wakes again. It will return the
/// Task to the wait_queue on `Pending`.
#[derive(Clone)]
pub(crate) struct AxiomReactor {
    executor: AxiomExecutor,
    /// The queue of Actors that are ready to be polled
    run_queue: Arc<RwLock<VecDeque<Wakeup>>>,
    /// The queue of Actors this Reactor is responsible for
    wait_queue: Arc<RwLock<BTreeMap<Uuid, Task>>>,
    /// This is used as a semaphore to ensure the reactor has
    /// only a single active thread at a time.
    thread_join: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl AxiomReactor {
    /// This is how the Executor moves an Actor from itself into a Reactor
    fn insert(&self, task: Task) {
        let token = Token {
            id: task.id,
            reactor: self.clone(),
        };
        let waker = futures::task::waker(Arc::new(token));
        let wakeup = Wakeup { id: task.id, waker };

        self.wait_queue
            .write()
            .expect("Poisoned wait_queue")
            .insert(task.id, task);
        self.run_queue
            .write()
            .expect("Poisoned run_queue")
            .push_back(wakeup);
        self.ensure_running();
    }

    // This is the logic for the core loop that drives the Reactor. It MUST be
    // invoked inside a dedicated thread, else it will block the executor.
    fn thread(&self) {
        'wakeup: while let Some(w) = self
            .run_queue
            .write()
            .expect("Poisoned run_queue")
            .pop_front()
        {
            if let Some(mut task) = self
                .wait_queue
                .write()
                .expect("Poisoned wait_queue")
                .remove(&w.id)
            {
                task.schedule.update();
                task.schedule.reset_stretch_data();
                'stretch: loop {
                    task.schedule.log_poll_actor();
                    match task.poll(&w.waker) {
                        Poll::Ready(result) => {
                            if let None = result {
                                self.executor.return_task(task, None);
                                continue 'wakeup;
                            }

                            task.schedule.log_stop_message();

                            let result = result.unwrap();
                            task.actor
                                .read()
                                .expect("Poisoned Actor")
                                .handle_result(&result);

                            match result {
                                // Intentional or error'd stop means drop. It's dead, Jim.
                                Ok(Status::Stop) | Err(_) => task.schedule.log_stop(),
                                _ => {
                                    // Ready(Some) where the Actor is still alive
                                    // Need to make sure this execution stretch is
                                    // still scheduled.
                                    match task.schedule.check_schedule() {
                                        ScheduleResult::Continue => continue 'stretch,
                                        ScheduleResult::Next => {
                                            self.wait_queue
                                                .write()
                                                .expect("Poisoned wait_queue")
                                                .insert(w.id, task);
                                            self.run_queue
                                                .write()
                                                .expect("Poisoned run_queue")
                                                .push_back(w);
                                        }
                                        ScheduleResult::Reassigned(new_reactor) => {
                                            self.executor.return_task(task, Some(new_reactor));
                                        }
                                        ScheduleResult::Stopped => {
                                            task.schedule.log_stop();
                                        }
                                    }
                                }
                            }

                            continue 'wakeup;
                        }
                        Poll::Pending => {
                            task.schedule.log_pending_message();
                            self.wait_queue
                                .write()
                                .expect("Poisoned wait_queue")
                                .insert(w.id, task);
                            continue 'wakeup;
                        }
                    }
                }
            }
        }
    }

    // This function ensures the Reactor is running when it is needed to be.
    // By allowing it to conclude and simply restart as necessary, we avoid
    // CPU spin locking. We should call this function as much as we need.
    fn ensure_running(&self) {
        let mut join = self.thread_join.write().expect("Poisoned thread_join");

        let r = self.clone();
        join.get_or_insert(thread::spawn(move || {
            r.thread();
            r.thread_join.write().expect("Poisoned thread_join").take();
        }));
    }

    fn new(executor: AxiomExecutor) -> AxiomReactor {
        AxiomReactor {
            executor,
            run_queue: Arc::new(RwLock::new(Default::default())),
            wait_queue: Arc::new(RwLock::new(BTreeMap::new())),
            thread_join: Arc::new(RwLock::new(None)),
        }
    }

    fn wake(&self, wakeup: Wakeup) {
        self.run_queue
            .write()
            .expect("Poisoned run_queue")
            .push_back(wakeup);
    }

    fn len(&self) -> usize {
        self.wait_queue.read().expect("Poisoned wait_queue").len()
    }
}

struct Task {
    id: Uuid,
    schedule: Schedule,
    actor: Arc<RwLock<Pin<Box<Actor>>>>,
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

struct Wakeup {
    id: Uuid,
    waker: Waker,
}
