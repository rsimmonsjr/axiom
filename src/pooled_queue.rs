//! Implements a bounded sized queue that uses a pair of pooled linked lists and
//! provides concurrent read from the queue and write to the queue using mutexes.

use std::cell::UnsafeCell;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Error values that can be returned as a result of methods on the PooledQueue.
#[derive(Debug, Eq, PartialEq)]
pub enum PooledQueueError {
    QueueFull,
    QueueEmpty,
}

/// A node in a LinkedNodeList
struct Node<T: Sync + Send> {
    /// Value that the node is holding or None if the node is empty.
    value: Option<T>,
    /// The pointer to the next node in the list.
    next: AtomicPtr<Node<T>>,
}

/// Common data shared by both the enqueue and dequeue side of the data structure.
pub struct Common<T: Sync + Send> {
    /// Capacity of the list.
    capacity: usize,
    /// Node storage of the nodes.
    nodes: UnsafeCell<Vec<Box<[Node<T>]>>>,
    /// Number of values currently in the list.
    length: AtomicUsize,
    /// Total number of values that have been enqueued.
    enqueued: AtomicUsize,
    /// Total number of values that have been dequeued.
    dequeued: AtomicUsize,
}

pub trait PooledQueueCommon<T: Sync + Send> {
    fn common(&self) -> &Arc<Common<T>>;

    /// Returns the capacity of the list.
    fn capacity(&self) -> usize {
        self.common().capacity
    }

    /// Returns the length indicating how many total items are in the queue.
    fn length(&self) -> usize {
        self.common().length.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been enqueued to the list.
    fn enqueued(&self) -> usize {
        self.common().enqueued.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been dequeued from the list.
    fn dequeued(&self) -> usize {
        self.common().dequeued.load(Ordering::Relaxed)
    }
}

/// The enqueue side of the data structure.
pub struct Enqueue<T: Sync + Send> {
    /// Reference to data common to enqueue and dequeue side of the data structure.
    common: Arc<Common<T>>,
    /// Reference to the internal lock used.
    lock: Mutex<bool>,
    /// Head of the nodes in the pool list
    pool_head: *mut Node<T>,
    /// Tail of the nodes in the queue list
    queue_tail: *mut Node<T>,
}

impl<T: Sync + Send> Enqueue<T> {
    /// Enqueues the value in the queue.
    pub fn enqueue(&mut self, value: T) -> Result<usize, PooledQueueError> {
        // The pool head will become the new queue tail and the value will be put in the
        // current queue tai.
        let _lock = self.lock.lock().unwrap();
        unsafe {
            let nil = null_mut();
            let pool_head = &mut (*self.pool_head);
            let queue_tail = &mut (*self.queue_tail);
            let next_pool_head = pool_head.next.load(Ordering::Relaxed);
            if next_pool_head == nil {
                return Err(PooledQueueError::QueueFull);
            }
            pool_head.next.store(nil, Ordering::Relaxed);
            queue_tail.next.load(Ordering::Acquire);
            queue_tail.value = Some(value);
            queue_tail.next.store(self.pool_head, Ordering::Release);
            self.queue_tail = self.pool_head;
            self.pool_head = next_pool_head;
            self.common.enqueued.fetch_add(1, Ordering::Relaxed);
            let old_lenth = self.common.length.fetch_add(1, Ordering::Relaxed);
            Ok(old_lenth + 1)
        }
    }
}

impl<T: Sync + Send> PooledQueueCommon<T> for Enqueue<T> {
    fn common(&self) -> &Arc<Common<T>> {
        &self.common
    }
}

/// The dequeue side of the data structure.
pub struct Dequeue<T: Sync + Send> {
    // Reference to data common to enqueue and dequeue side of the data structure.
    common: Arc<Common<T>>,
    /// Reference to the internal lock used.
    lock: Mutex<bool>,
    /// Tail of the nodes in the pool list
    pool_tail: *mut Node<T>,
    /// Head of the nodes in the queue list
    queue_head: *mut Node<T>,
}

impl<T: Sync + Send> Dequeue<T> {
    /// Dequeue the next pending value in the queue and return it to the user.
    pub fn dequeue(&mut self) -> Result<T, PooledQueueError> {
        // The value will be pulled off the queue head and the node for the queue head
        // will now be the new pool tail.
        let _lock = self.lock.lock().unwrap();
        unsafe {
            let nil = null_mut();
            let queue_head = &mut (*self.queue_head);
            let pool_tail = &mut (*self.pool_tail);
            let next_queue_head = queue_head.next.load(Ordering::Acquire);
            if nil == next_queue_head {
                return Err(PooledQueueError::QueueEmpty);
            }
            let result = queue_head.value.take().unwrap();
            queue_head.next.store(nil, Ordering::Relaxed);
            pool_tail.next.store(self.queue_head, Ordering::Relaxed);
            self.pool_tail = self.queue_head;
            self.queue_head = next_queue_head;
            self.common.dequeued.fetch_add(1, Ordering::Relaxed);
            self.common.length.fetch_sub(1, Ordering::Relaxed);
            Ok(result)
        }
    }
}

impl<T: Sync + Send> PooledQueueCommon<T> for Dequeue<T> {
    fn common(&self) -> &Arc<Common<T>> {
        &self.common
    }
}

/// Creates a pooled queue enqueue and dequeue mechanisms.
pub fn create<T: Sync + Send>(capacity: usize) -> (Enqueue<T>, Dequeue<T>) {
    if capacity < 1 {
        panic!("capacity cannot be smaller than 1");
    }

    // we add two to the allocated capacity to account for the mandatory nodes on each list.
    let mut nodes_vec = Vec::<Node<T>>::with_capacity((capacity + 2) as usize);
    // The queue just gets one initial node with no data and the queue_tail is just
    // the same as the queue_head.
    let nil = null_mut();
    nodes_vec.push(Node {
        value: None,
        next: AtomicPtr::new(nil),
    });
    let queue_head: *mut _ = nodes_vec.last_mut().unwrap();
    let queue_tail = queue_head;

    // Allocate the pool of nodes that will be used for the list using a cons like
    // operation order with H -> N0 -> N1 -> N2 -> Nn <- T
    nodes_vec.push(Node {
        value: None,
        next: AtomicPtr::new(nil),
    });
    let mut pool_head: *mut _ = nodes_vec.last_mut().unwrap();
    let pool_tail = pool_head;
    for _ in 0..capacity {
        nodes_vec.push(Node {
            value: None,
            next: AtomicPtr::new(pool_head),
        });
        pool_head = nodes_vec.last_mut().unwrap();
    }

    let common = Arc::new(Common {
        capacity,
        nodes: UnsafeCell::new(vec![nodes_vec.into_boxed_slice()]),
        length: AtomicUsize::new(0),
        enqueued: AtomicUsize::new(0),
        dequeued: AtomicUsize::new(0),
    });

    let enqueue = Enqueue {
        common: common.clone(),
        lock: Mutex::new(true),
        pool_head,
        queue_tail,
    };

    let dequeue = Dequeue {
        common,
        lock: Mutex::new(true),
        pool_tail,
        queue_head,
    };

    (enqueue, dequeue)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A macro to assert that pointers point to the right nodes.
    macro_rules! assert_pointer_nodes {
        (
            $pointers:expr,
            $enqueue:expr,
            $dequeue:expr,
            $queue_head:expr,
            $queue_tail:expr,
            $pool_head:expr,
            $pool_tail:expr
        ) => {{
            assert_eq!(
                $pointers[$queue_head], $dequeue.queue_head,
                "<== queue_head mismatch\n"
            );
            assert_eq!(
                $pointers[$queue_tail], $enqueue.queue_tail,
                "<== queue_tail mismatch\n"
            );
            assert_eq!(
                $pointers[$pool_head], $enqueue.pool_head,
                "<== pool_head mismatch\n"
            );
            assert_eq!(
                $pointers[$pool_tail], $dequeue.pool_tail,
                "<== pool_tail mismatch\n"
            );
        }};
    }

    /// Asserts that the given node in the queue has the expected next pointer.
    macro_rules! assert_node_next {
        ($pointers:expr, $node:expr, $next:expr) => {
            unsafe {
                assert_eq!(
                    (*$pointers[$node]).next.load(Ordering::Relaxed),
                    $pointers[$next]
                )
            }
        };
    }

    /// Asserts that the given node in the queue has the expected next pointing to null_mut().
    macro_rules! assert_node_next_nil {
        ($pointers:expr, $node:expr) => {
            unsafe {
                assert_eq!(
                    (*$pointers[$node]).next.load(Ordering::Relaxed),
                    null_mut() as *mut _
                )
            }
        };
    }

    // Items that will be put in the list
    #[derive(Debug, Eq, PartialEq)]
    enum Items {
        A,
        B,
        C,
        D,
        E,
        F,
    }

    fn pointers_vec<T: Sync + Send>(common: &Common<T>) -> Vec<*mut Node<T>> {
        unsafe {
            let mut results = Vec::<*mut _>::new();
            let nodes_vec = &*common.nodes.get();
            for i in 0..nodes_vec.len() {
                let nodes = &nodes_vec[i];
                for j in 0..nodes.len() {
                    results.push(&nodes[j] as *const _ as *mut _)
                }
            }
            results
        }
    }

    /// Tests the basics of the queue.
    #[test]
    fn test_queue_dequeue() {
        let pooled_queue = create::<Items>(5);
        let (mut enqueue, mut dequeue) = pooled_queue;

        // fetch the pointers for easy checking of the nodes.
        let pointers = pointers_vec(&*enqueue.common);

        assert_eq!(7, pointers.len());
        assert_eq!(5, enqueue.common.capacity);
        assert_eq!(5, enqueue.capacity());
        assert_eq!(5, dequeue.capacity());

        // Write out the nodes list to facilitate testing
        println!("queue nodes list is:");
        for i in 0..pointers.len() {
            println!("[{}] -> {:?}", i, &pointers[i]);
        }
        println!();

        // Check the initial structure.
        assert_eq!(0, enqueue.length());
        assert_eq!(0, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());
        assert_node_next_nil!(pointers, 0);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 0, 0, 6, 1); // ( qh, qt, ph, pt)

        // Check that enqueueing removes pool head and appends to queue tail and changes
        // nothing else in the node structure.
        assert_eq!(Ok(1), enqueue.enqueue(Items::A));
        assert_eq!(1, enqueue.length());
        assert_eq!(1, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next_nil!(pointers, 6);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 0, 6, 5, 1);

        // Second enqueue should also move the pool_head node.
        assert_eq!(Ok(2), enqueue.enqueue(Items::B));
        assert_eq!(2, enqueue.length());
        assert_eq!(2, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next_nil!(pointers, 5);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 0, 5, 4, 1);

        assert_eq!(Ok(3), enqueue.enqueue(Items::C));
        assert_eq!(3, enqueue.length());
        assert_eq!(3, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next_nil!(pointers, 4);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 0, 4, 3, 1);

        assert_eq!(Ok(4), enqueue.enqueue(Items::D));
        assert_eq!(4, enqueue.length());
        assert_eq!(4, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next_nil!(pointers, 3);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 0, 3, 2, 1);

        assert_eq!(Ok(5), enqueue.enqueue(Items::E));
        assert_eq!(5, enqueue.length());
        assert_eq!(5, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 0, 2, 1, 1);

        assert_eq!(Err(PooledQueueError::QueueFull), enqueue.enqueue(Items::F));
        assert_eq!(5, enqueue.length());
        assert_eq!(5, enqueue.enqueued());
        assert_eq!(0, enqueue.dequeued());

        assert_eq!(Ok(Items::A), dequeue.dequeue());
        assert_eq!(4, dequeue.length());
        assert_eq!(5, dequeue.enqueued());
        assert_eq!(1, dequeue.dequeued());
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next_nil!(pointers, 0);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 6, 2, 1, 0);

        assert_eq!(Ok(Items::B), dequeue.dequeue());
        assert_eq!(3, dequeue.length());
        assert_eq!(5, dequeue.enqueued());
        assert_eq!(2, dequeue.dequeued());
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next_nil!(pointers, 6);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 5, 2, 1, 6);

        assert_eq!(Ok(Items::C), dequeue.dequeue());
        assert_eq!(2, dequeue.length());
        assert_eq!(5, dequeue.enqueued());
        assert_eq!(3, dequeue.dequeued());
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next_nil!(pointers, 5);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 4, 2, 1, 5);

        assert_eq!(Ok(Items::D), dequeue.dequeue());
        assert_eq!(1, dequeue.length());
        assert_eq!(5, dequeue.enqueued());
        assert_eq!(4, dequeue.dequeued());
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next_nil!(pointers, 4);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 3, 2, 1, 4);

        assert_eq!(Ok(Items::E), dequeue.dequeue());
        assert_eq!(0, dequeue.length());
        assert_eq!(5, dequeue.enqueued());
        assert_eq!(5, dequeue.dequeued());
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next_nil!(pointers, 3);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 2, 2, 1, 3);

        assert_eq!(Err(PooledQueueError::QueueEmpty), dequeue.dequeue());
        assert_eq!(0, dequeue.length());
        assert_eq!(5, dequeue.enqueued());
        assert_eq!(5, dequeue.dequeued());

        assert_eq!(Ok(1), enqueue.enqueue(Items::F));
        assert_eq!(1, dequeue.length());
        assert_eq!(6, dequeue.enqueued());
        assert_eq!(5, dequeue.dequeued());
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next_nil!(pointers, 3);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 2, 1, 0, 3);

        assert_eq!(Ok(Items::F), dequeue.dequeue());
        assert_eq!(0, dequeue.length());
        assert_eq!(6, dequeue.enqueued());
        assert_eq!(6, dequeue.dequeued());
        assert_node_next_nil!(pointers, 1);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_pointer_nodes!(pointers, enqueue, dequeue, 1, 1, 0, 2);
    }
}
