use crate::executor::ShutdownResult;
use log::{debug, error, trace};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Axiom's core ThreadPool, custom so we can await the shutdown of the threads.
#[derive(Default)]
pub(crate) struct AxiomThreadPool {
    drain: Arc<DrainAwait>,
}

impl AxiomThreadPool {
    /// Primary functionality of the ThreadPool. Spawns a thread, adds a little tracking before and
    /// after execution of the given function.
    pub fn spawn<F: FnMut() + Send + 'static>(&self, name: String, f: F) -> Arc<ThreadDeed> {
        let deed = Arc::new(ThreadDeed {
            name,
            state: Mutex::new(ThreadState::Stopped),
            drain: self.drain.clone(),
        });
        self.thread(f, deed.clone());
        deed
    }

    /// The function that actually spawns the thread with tracking.
    fn thread<F>(&self, mut f: F, deed: Arc<ThreadDeed>)
    where
        F: FnMut() + Send + 'static,
    {
        thread::Builder::new()
            .name(deed.name.clone())
            .spawn(move || {
                let lease = ThreadLease::new(deed);
                lease.deed.drain.increment();
                debug!("Thread {} has started", lease.deed.name);
                lease.deed.set_running();
                f();
                lease.deed.set_stopped();
            })
            .expect("Failed to spawn thread");
    }

    /// Blocks until all threads have stopped, or the timeout has been reached.
    pub fn await_shutdown(&self, timeout: impl Into<Option<Duration>>) -> ShutdownResult {
        match timeout.into() {
            Some(t) => self.drain.wait_timeout(t),
            None => self.drain.wait(),
        }
    }
}

/// The record that accompanies the thread. This should have all the data associated with the
/// thread.
pub(crate) struct ThreadDeed {
    pub name: String,
    pub state: Mutex<ThreadState>,
    drain: Arc<DrainAwait>,
}

impl ThreadDeed {
    /// Helper to set the ThreadState as Running.
    fn set_running(&self) {
        *self.state.lock().unwrap() = ThreadState::Running;
    }

    /// Helper to set the ThreadState as Stopped.
    fn set_stopped(&self) {
        *self.state.lock().unwrap() = ThreadState::Stopped;
    }
}

/// A wrapper for the ThreadDeed that checks for premature thread termination. The lifetime of the
/// Lease defines the thread lifetime. As the only thing that ends a thread prematurely at this
/// level is a panic, the thread dying before we can set it as Stopped indicates a panic without the
/// need to join on the JoinHandle.
struct ThreadLease {
    deed: Arc<ThreadDeed>,
}

impl ThreadLease {
    pub fn new(deed: Arc<ThreadDeed>) -> Self {
        Self { deed }
    }
}

impl Drop for ThreadLease {
    fn drop(&mut self) {
        // Don't you tell me it's poisoned!! I want to find out for myself!!
        let mut g = match self.deed.state.lock() {
            Ok(g) => g,
            Err(psn) => psn.into_inner(),
        };
        // If the Lease dropped while Running, it Panicked.
        if let ThreadState::Running = *g {
            *g = ThreadState::Panicked;
            error!("Thread {} panicked!", self.deed.name)
        } else {
            debug!("Thread {} has stopped", self.deed.name)
        }
        // Either way, it's dead, let's decrement the thread counter.
        self.deed.drain.decrement();
    }
}

/// As it's named, the state of the thread. Should be self-explanatory.
pub enum ThreadState {
    Running,
    Stopped,
    Panicked,
}

/// A semaphore of sorts that unblocks when its internal counter hits 0
#[derive(Default)]
struct DrainAwait {
    /// Mutex for blocking on
    mutex: Mutex<u16>,
    /// Condvar for waiting on
    condvar: Condvar,
}

impl DrainAwait {
    /// Increment the drain counter
    pub fn increment(&self) {
        let mut g = self.mutex.lock().expect("DrainAwait poisoned");
        let new = *g + 1;
        trace!("Incrementing DrainAwait to {}", new);
        *g += 1;
    }

    /// Decrement the drain counter, and notify condvar if it hits 0
    pub fn decrement(&self) {
        let mut guard = self.mutex.lock().expect("DrainAwait poisoned");
        *guard -= 1;
        trace!("Decrementing DrainAwait to {}", *guard);
        if *guard == 0 {
            debug!("DrainAwait is depleted, notifying blocked threads");
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
