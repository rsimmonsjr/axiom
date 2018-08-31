//! Implements a bounded sized queue that uses a pair of pooled linked lists and
//! provides concurrent read from the queue and write to the queue using mutexes.

use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

/// A node in a LinkedNodeList
struct Node<T: Sync + Send> {
    /// Value that the node is holding or None if the node is empty.
    value: Option<T>,
    /// The pointer to the next node in the list.
    next: AtomicPtr<Node<T>>,
}

/// Error values that can be returned as a result of methods on the PooledQueue.
#[derive(Debug, Eq, PartialEq)]
pub enum PooledQueueError {
    QueueFull,
    QueueEmpty,
}

/// A Linked List of Node<T> objects where enqueue and dequeue take the node with the
/// T already in it rather than the T itself.
pub struct PooledQueue<T: Sync + Send> {
    /// Capacity of the list.
    capacity: usize,
    /// Node storage of the nodes.
    nodes: Box<[Node<T>]>,
    /// Enqueue pointers containing (pool_head, queue_tail) in a single lockable mutex.
    enqueue: Mutex<(*mut Node<T>, *mut Node<T>)>,
    /// Dequeue pointers containing (pool_tail, queue_head) in a single locable mutex.
    dequeue: Mutex<(*mut Node<T>, *mut Node<T>)>,
    /// Number of values currently in the list.
    length: AtomicUsize,
    /// Total number of values that have been enqueued.
    enqueued: AtomicUsize,
    /// Total number of values that have been dequeued.
    dequeued: AtomicUsize,
}

impl<T: Sync + Send> PooledQueue<T> {
    pub fn new(capacity: usize) -> PooledQueue<T> {
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
        let enqueue = Mutex::new((pool_head, queue_tail));
        let dequeue = Mutex::new((pool_tail, queue_head));
        PooledQueue {
            capacity,
            nodes: nodes_vec.into_boxed_slice(),
            enqueue,
            dequeue,
            length: AtomicUsize::new(0),
            enqueued: AtomicUsize::new(0),
            dequeued: AtomicUsize::new(0),
        }
    }

    /// Enqueues the value in the queue.
    pub fn enqueue(&mut self, value: T) -> Result<usize, PooledQueueError> {
        let mut lock = self.enqueue.lock().unwrap();
        let (pool_head, queue_tail) = *lock;
        // Pull the node off the pool and use it for the new data.
        unsafe {
            let nil = null_mut();
            // The pool_head.next will now be the new pool head unless the next pointer is
            // a null in which case we error as we cannot remove the last node from the pool
            // because both the queue and the pool always have at least one node.
            let next_pool_head = (*pool_head).next.load(Ordering::Relaxed);
            if next_pool_head == nil {
                return Err(PooledQueueError::QueueFull);
            }
            (*pool_head).next.store(nil, Ordering::Relaxed);
            (*queue_tail).next.load(Ordering::Acquire);
            // fixme if old isn't null we didn't acquire it? Loop ?
            (*queue_tail).value = Some(value);
            (*queue_tail).next.store(pool_head, Ordering::Release);
            *lock = (next_pool_head, pool_head);
            self.enqueued.fetch_add(1, Ordering::Relaxed);
            let old_lenth = self.length.fetch_add(1, Ordering::Relaxed);
            Ok(old_lenth + 1)
        }
    }

    /// Dequeue the next pending value in the queue and return it to the user.
    pub fn dequeue(&mut self) -> Result<T, PooledQueueError> {
        let mut lock = self.dequeue.lock().unwrap();
        let (pool_tail, queue_head) = *lock;
        // Pull the node off the pool and use it for the new data.
        unsafe {
            let nil = null_mut();
            let next_queue_head = (*queue_head).next.load(Ordering::Acquire);
            if nil == next_queue_head {
                return Err(PooledQueueError::QueueEmpty);
            }
            let result = (*queue_head).value.take().unwrap();
            (*queue_head).next.store(nil, Ordering::Relaxed);
            (*pool_tail).next.store(queue_head, Ordering::Relaxed);
            *lock = (queue_head, next_queue_head);
            self.dequeued.fetch_add(1, Ordering::Relaxed);
            self.length.fetch_sub(1, Ordering::Relaxed);
            Ok(result)
        }
    }

    /// Returns the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the length indicating how many total items are in the queue.
    pub fn length(&self) -> usize {
        self.length.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been enqueued to the list.
    pub fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been dequeued from the list.
    pub fn dequeued(&self) -> usize {
        self.dequeued.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A macro to assert that pointers point to the right nodes.
    macro_rules! assert_pointer_nodes {
        ($queue:expr, $queue_head:expr, $queue_tail:expr, $pool_head:expr, $pool_tail:expr) => {{
            let queue_head = (
                &mut $queue.nodes[$queue_head] as *mut _,
                $queue.dequeue.lock().unwrap().1,
            );
            assert_eq!(queue_head.0, queue_head.1, "<== queue_head mismatch\n");

            let queue_tail = (
                &mut $queue.nodes[$queue_tail] as *mut _,
                $queue.enqueue.lock().unwrap().1,
            );
            assert_eq!(queue_tail.0, queue_tail.1, "<== queue_tail mismatch\n");

            let pool_head = (
                &mut $queue.nodes[$pool_head] as *mut _,
                $queue.enqueue.lock().unwrap().0,
            );

            assert_eq!(pool_head.0, pool_head.1, "<== pool_head mismatch\n");
            let pool_tail = (
                &mut $queue.nodes[$pool_tail] as *mut _,
                $queue.dequeue.lock().unwrap().0,
            );
            assert_eq!(pool_tail.0, pool_tail.1, "<== pool_tail mismatch\n");
        }};
    }

    /// Asserts that the given node in the queue has the expected next pointer.
    macro_rules! assert_node_next {
        ($queue:expr, $node:expr, $next:expr) => {
            assert_eq!(
                $queue.nodes[$node].next.load(Ordering::Relaxed),
                &mut $queue.nodes[$next] as *mut _
            )
        };
    }

    /// Asserts that the given node in the queue has the expected next pointing to null_mut().
    macro_rules! assert_node_next_nil {
        ($queue:expr, $node:expr) => {
            assert_eq!(
                $queue.nodes[$node].next.load(Ordering::Relaxed),
                null_mut() as *mut _
            )
        };
    }

    /// Tests the basics of the queue.
    #[test]
    fn test_queue_dequeue() {
        let mut queue = PooledQueue::new(5);
        assert_eq!(7, queue.nodes.len());
        assert_eq!(5, queue.capacity());

        // Write out the nodes list to facilitate testing
        println!("queue nodes list is:");
        for i in 0..queue.nodes.len() {
            println!("[{}] -> {:?}", i, &mut queue.nodes[i] as *mut _);
        }
        println!();

        // Check the initial structure.
        assert_eq!(0, queue.length());
        assert_eq!(0, queue.enqueued());
        assert_eq!(0, queue.dequeued());
        assert_pointer_nodes!(queue, 0, 0, 6, 1); // (q, qh, qt, ph, pt)
        assert_node_next_nil!(queue, 0);
        assert_node_next!(queue, 6, 5);
        assert_node_next!(queue, 5, 4);
        assert_node_next!(queue, 4, 3);
        assert_node_next!(queue, 3, 2);
        assert_node_next!(queue, 2, 1);
        assert_node_next_nil!(queue, 1);

        // Check that enqueueing removes pool head and appends to queue tail and changes
        // nothing else in the node structure.
        let a = "A".to_string();
        assert_eq!(1, queue.enqueue(a).unwrap());
        assert_eq!(1, queue.length());
        assert_eq!(1, queue.enqueued());
        assert_eq!(0, queue.dequeued());
        assert_pointer_nodes!(queue, 0, 6, 5, 1);
        assert_node_next!(queue, 0, 6);
        assert_node_next_nil!(queue, 6);
        assert_node_next!(queue, 5, 4);
        assert_node_next!(queue, 4, 3);
        assert_node_next!(queue, 3, 2);
        assert_node_next!(queue, 2, 1);
        assert_node_next_nil!(queue, 1);

        // Second enqueue should also move the pool_head node.
        let b = "B".to_string();
        assert_eq!(2, queue.enqueue(b).unwrap());
        assert_eq!(2, queue.length());
        assert_eq!(2, queue.enqueued());
        assert_eq!(0, queue.dequeued());
        assert_pointer_nodes!(queue, 0, 5, 4, 1);
        assert_node_next!(queue, 0, 6);
        assert_node_next!(queue, 6, 5);
        assert_node_next_nil!(queue, 5);
        assert_node_next!(queue, 4, 3);
        assert_node_next!(queue, 3, 2);
        assert_node_next!(queue, 2, 1);
        assert_node_next_nil!(queue, 1);

        let c = "C".to_string();
        assert_eq!(3, queue.enqueue(c).unwrap());
        assert_eq!(3, queue.length());
        assert_eq!(3, queue.enqueued());
        assert_eq!(0, queue.dequeued());
        assert_pointer_nodes!(queue, 0, 4, 3, 1);
        assert_node_next!(queue, 0, 6);
        assert_node_next!(queue, 6, 5);
        assert_node_next!(queue, 5, 4);
        assert_node_next_nil!(queue, 4);
        assert_node_next!(queue, 3, 2);
        assert_node_next!(queue, 2, 1);
        assert_node_next_nil!(queue, 1);

        let d = "D".to_string();
        assert_eq!(4, queue.enqueue(d).unwrap());
        assert_eq!(4, queue.length());
        assert_eq!(4, queue.enqueued());
        assert_eq!(0, queue.dequeued());
        assert_pointer_nodes!(queue, 0, 3, 2, 1);
        assert_node_next!(queue, 0, 6);
        assert_node_next!(queue, 6, 5);
        assert_node_next!(queue, 5, 4);
        assert_node_next!(queue, 4, 3);
        assert_node_next_nil!(queue, 3);
        assert_node_next!(queue, 2, 1);
        assert_node_next_nil!(queue, 1);

        let e = "E".to_string();
        assert_eq!(5, queue.enqueue(e).unwrap());
        assert_eq!(5, queue.length());
        assert_eq!(5, queue.enqueued());
        assert_eq!(0, queue.dequeued());
        assert_pointer_nodes!(queue, 0, 2, 1, 1);
        assert_node_next!(queue, 0, 6);
        assert_node_next!(queue, 6, 5);
        assert_node_next!(queue, 5, 4);
        assert_node_next!(queue, 4, 3);
        assert_node_next!(queue, 3, 2);
        assert_node_next_nil!(queue, 2);
        assert_node_next_nil!(queue, 1);

        let f = "F".to_string();
        assert_eq!(Err(PooledQueueError::QueueFull), queue.enqueue(f));
        assert_eq!(5, queue.length());
        assert_eq!(5, queue.enqueued());
        assert_eq!(0, queue.dequeued());

        //        assert_eq!("A".to_string(), queue.dequeue().unwrap());
        //        assert_eq!(1, queue.length());
        //
        //        assert_eq!("B".to_string(), queue.dequeue().unwrap());
        //        assert_eq!(0, queue.length());
    }
}
