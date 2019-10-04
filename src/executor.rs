use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Future;
use futures::task::{ArcWake, Waker};

thread_local! {
    static REACTOR: Rc<AxiomReactor> = Rc::new(AxiomReactor::new());
}

pub(crate) struct AxiomReactor {
    counter: Cell<usize>,
    wait_queue: RefCell<BTreeMap<usize, Task>>,
    run_queue: RefCell<VecDeque<Wakeup>>,
}

impl AxiomReactor {
    pub fn spawn<F: Future<Output=()> + 'static>(f: F) {
        REACTOR.with(|reactor| {
            let (id, waker) = reactor.next_task();
            let mut task = Task {
                future: Box::pin(f)
            };

            // if the task is ready immediately, don't add it to wait_queue
            if let Poll::Ready(_) = task.poll(waker) {
                return;
            }

            reactor.wait_queue.borrow_mut().insert(id, task);
        })
    }

    fn next_task(&self) -> (usize, Waker) {
        let counter = self.counter.get();
        let w = Arc::new(Token(counter));
        self.counter.set(counter + 1);
        (counter, futures::task::waker(w))
    }

    pub fn single_iter() {
        REACTOR.with(|reactor| {
            while let Some(w) = reactor.run_queue.borrow_mut().pop_front() {
                if let Some(mut task) = reactor.wait_queue.borrow_mut().remove(&w.id) {
                    if let Poll::Pending = task.poll(w.waker) {
                        reactor.wait_queue.borrow_mut().insert(w.id, task);
                    }
                }
            }
        });
    }

    fn new() -> AxiomReactor {
        AxiomReactor {
            counter: Cell::new(0),
            wait_queue: RefCell::new(BTreeMap::new()),
            run_queue: RefCell::new(Default::default()),
        }
    }

    fn wake(&self, wakeup: Wakeup) {
        self.run_queue.borrow_mut().push_back(wakeup);
    }

    pub fn len() -> usize {
        REACTOR.with(|reactor| reactor.wait_queue.borrow().len())
    }
}

struct Task {
    future: Pin<Box<dyn Future<Output=()>>>,
}

impl Task {
    fn poll(&mut self, waker: Waker) -> Poll<()> {
        let mut ctx = Context::from_waker(&waker);

        match self.future.as_mut().poll(&mut ctx) {
            Poll::Ready(_) => {
                Poll::Ready(())
            }
            Poll::Pending => {
                Poll::Pending
            }
        }
    }
}

struct Token(usize);

impl ArcWake for Token {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let Token(id) = **arc_self;

        // get access to the reactor by way of TLS and call wake
        REACTOR.with(|reactor| {
            let wakeup = Wakeup {
                id,
                waker: futures::task::waker(arc_self.clone()),
            };
            (**reactor).wake(wakeup);
        });
    }
}

struct Wakeup {
    id: usize,
    waker: Waker,
}
