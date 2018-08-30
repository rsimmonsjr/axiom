//! Implements a bounded sized queue that uses a pair of pooled linked lists and
//! provides concurrent read from the queue and write to the queue using mutexes.

use std::ptr::null_mut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// A node in a LinkedNodeList
struct Node<T: Sync + Send> {
    /// Value that the node is holding or None if the node is empty.
    value: Option<T>,
    /// The pointer to the next node in the list.
    next: *mut Node<T>,
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
        // we add one to the allocated capacity to account for queue dummy node.
        let mut nodes_vec = Vec::<Node<T>>::with_capacity((capacity + 1) as usize);
        // The queue just gets one initial node with no data and the queue_tail is just
        // the same as the queue_head.
        let nil = null_mut();
        nodes_vec.push(Node {
            value: None,
            next: nil,
        });
        let queue_head: *mut _ = nodes_vec.last_mut().unwrap();
        let queue_tail = queue_head;

        // Allocate the pool of nodes that will be used for the list using a cons like
        // operation order with H -> N0 -> N1 -> N2 -> Nn <- T
        nodes_vec.push(Node {
            value: None,
            next: nil,
        });
        let mut pool_head: *mut _ = nodes_vec.last_mut().unwrap();
        let pool_tail = pool_head;
        for _ in 1..capacity {
            nodes_vec.push(Node {
                value: None,
                next: pool_head,
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
    pub fn enqueue(&mut self, value: T) -> Result<usize, String> {
        let mut lock = self.enqueue.lock().unwrap();
        let (pool_head, queue_tail) = *lock;
        // Pull the node off the pool and use it for the new data.
        unsafe {
            let nil = null_mut();
            // The pool_head.next will now be the new pool head unless the next pointer is
            // a null in which case we error as we cannot remove the last node from the pool
            // because both the queue and the pool always have at least one node.
            let next_pool_head = (*pool_head).next;
            if next_pool_head == nil {
                return Err("Queue Full".to_string());
            }
            (*pool_head).next = nil;
            (*queue_tail).next = pool_head;
            (*queue_tail).value = Some(value);
            *lock = (next_pool_head, pool_head);
            self.enqueued.fetch_add(1, Ordering::Relaxed);
            let old_lenth = self.length.fetch_add(1, Ordering::Relaxed);
            Ok(old_lenth + 1)
        }
    }

    /// Dequeue the next pending value in the queue and return it to the user.
    pub fn dequeue(&mut self) -> Result<T, String> {
        let mut lock = self.dequeue.lock().unwrap();
        let (pool_tail, queue_head) = *lock;
        // Pull the node off the pool and use it for the new data.
        unsafe {
            let nil = null_mut();
            if nil == (*queue_head).next {
                return Err("Queue Empty".to_string());
            }
            let result = (*queue_head).value.take().unwrap();
            let next_queue_head = (*queue_head).next;
            (*queue_head).next = nil;
            (*pool_tail).next = queue_head;
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

    /// Returns the total number of objects that have been enqueued to the list.
    pub fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been dequeued from the list.
    pub fn dequeued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }
}
