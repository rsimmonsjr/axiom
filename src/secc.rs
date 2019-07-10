//! An Skip Enabled Concurrent Channel (SECC) is a channel that allows users to send and receive
//! data from multiple threads and allows the user to skip reading messages if they choose and
//! then reset the skip later to read the messages. In the purest sense the channel is FIFO
//! unless the user intends to skip one or more messages in which case a message could be read
//! in a different order. The channel does guarantee that the messages will remain in the same
//! order as inserted and unless skipped will be processed in order.
//!
//! The module is implemented using two linked lists where one list acts as a pool and the
//! other list acts as the queue holding the messages. This allows us to move data in and out
//! of the list and even skip a message with O(1) efficiency. If there are 1000 messages and
//! the user desires to skip one in the middle they will incur virtually the exact same performance
//! as a normal read operation. There are only a couple more pointer operations to dequeue a
//! node out of the middle of the linked list that is the queue. When a node is dequeued the
//! node is removed from the queue and appended to the tail of the pool and when a message is
//! enqueued the node moves from the head of the pool to the tail of the queue. In this manner
//! nodes are constantly cycled in and out of the queue and we only need to allocate them once
//! when the channel is created.
//!
//! Although the channel is currently bounded, adding the ability for the channel to grow would
//! be trivial.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;

/// Type alias for half of a usize on 64 bit platform.
pub type HalfUsize = u32;

/// Value used to indicate that a position index points to no other node.
const NIL_NODE: usize = 1 << 63 as usize;

/// Errors potentially returned from run queue operations.
#[derive(Debug, Eq, PartialEq)]
pub enum SeccErrors<T: Sync + Send> {
    /// Channel is full, no more messages can be enqueued, contains the last message attempted
    /// to be enqueued.
    Full(T),
    /// Channel is empty so no more messages can be dequeued. This can also be returned if there
    /// is an active cursor and there are no messages to dequeue after the cursor even though
    /// there ar skipped messages.
    Empty,
}

/// A single node in the run queue's ring buffer.
struct SeccNode<T: Sync + Send> {
    /// Contains a value set in the node in a Some or a None if empty. Note that this is unsafe
    /// in order to get around Rust mutability locks so that this data structure can be passed
    /// around immutably but also still be able to enqueue and dequeue.
    cell: UnsafeCell<Option<Arc<T>>>,
    /// The pointer to the next node in the channel.
    next: AtomicUsize,
    // FIXME Add tracking of time in channel by milliseconds.
}

impl<T: Sync + Send> SeccNode<T> {
    /// Creates a new node where the next index is set to the nil node.
    pub fn new() -> SeccNode<T> {
        SeccNode {
            cell: UnsafeCell::new(None),
            next: AtomicUsize::new(NIL_NODE),
        }
    }

    /// Creates a new node where the next index is the given value.
    pub fn with_next(next: usize) -> SeccNode<T> {
        SeccNode {
            cell: UnsafeCell::new(None),
            next: AtomicUsize::new(next),
        }
    }
}

/// Operations that work on the core of the channel.
pub trait SeccCoreOps<T: Sync + Send> {
    /// Fetch the core of the channel.
    fn core(&self) -> &SeccCore<T>;

    /// Returns the capacity of the channel.
    fn capacity(&self) -> usize {
        self.core().capacity
    }

    /// Count of the number of times receivers of this channel called awaiting messages.
    fn awaited_messages(&self) -> usize {
        self.core().awaited_messages.load(Ordering::Relaxed)
    }

    /// Count of the number of times a sender was called and awaited capacity.
    fn awaited_capacity(&self) -> usize {
        self.core().awaited_capacity.load(Ordering::Relaxed)
    }

    /// Returns the length indicating how many total items are in the queue currently.
    fn length(&self) -> usize {
        self.core().length.load(Ordering::Relaxed)
    }

    /// Number of values in the channel that are available for read. This will normally be the
    /// length unless there is a skip cursor active then it may be smaller than length or even 0.
    fn readable(&self) -> usize {
        self.core().readable.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been enqueued to the channel.
    fn enqueued(&self) -> usize {
        self.core().enqueued.load(Ordering::Relaxed)
    }

    /// Returns the total number of objects that have been dequeued from the channel.
    fn dequeued(&self) -> usize {
        self.core().dequeued.load(Ordering::Relaxed)
    }
}

/// Data structure that contains the core of the channel including tracking fo statistics
/// and data storage.
pub struct SeccCore<T: Sync + Send> {
    /// Capacity of the channel, which is the total number of items that can be stored.
    /// Note that there are 2 more nodes than the capacity because neither the queue nor pool
    /// should ever be empty.
    capacity: usize,
    /// Node storage of the nodes. These nodes are never read directly except during
    /// allocation and tests. Therefore they can be stored in an [UnsafeCell]. It is critical
    /// that the nodes don't change memory location so they are in a `Box<[Node<T>]>` slice
    /// and the surrounding [Vec] allows for expanding the storage without moving existing.
    _nodes: UnsafeCell<Vec<Box<[SeccNode<T>]>>>,
    /// Pointers to the nodes in the channel. It is critical that these pointers never change
    /// order during the operations of the channel. If the channel has to be resized it should
    /// push the new pointers into the [Vec] at the back and never remove a pointer. Note that
    /// resizing the channel is not currently supported.
    node_ptrs: UnsafeCell<Vec<*mut SeccNode<T>>>,
    /// Cond var that is set when the channel transitions from no messages available to some
    /// messages available. All threads are notified only when the length goes from 0 to 1.
    has_messages: Arc<(Mutex<bool>, Condvar)>,
    /// Count of the number of times receivers of this channel called awaiting messages.
    awaited_messages: AtomicUsize,
    /// Cond var that is set when the channel transitions from being full to having space in
    /// which a new message can be enqueued. All threads are notified when length goes from
    /// capacity to capacity - 1.
    has_capacity: Arc<(Mutex<bool>, Condvar)>,
    /// Count of the number of times a sender was called and awaited capacity.
    awaited_capacity: AtomicUsize,
    /// Number of values currently in the channel.
    length: AtomicUsize,
    /// Number of values in the channel that are available for read. This will normally be the
    /// length unless there is a skip cursor active then it may be smaller than length or even 0.
    readable: AtomicUsize,
    /// Total number of values that have been enqueued.
    enqueued: AtomicUsize,
    /// Total number of values that have been dequeued.
    dequeued: AtomicUsize,
}

/// Sender side of the channel.
pub struct SeccSender<T: Sync + Send> {
    /// The core of the channel.
    core: Arc<SeccCore<T>>,
    /// Indexes in the node_ptrs used for enqueue of elements in the channel wrapped in
    /// a mutex that acts as a lock.
    queue_tail_pool_head: Mutex<(usize, usize)>,
}

impl<T: Sync + Send> SeccSender<T> {
    /// Sends a value into the channel, the value will be moved into the mailbox and it will take
    /// ownership of the value. This function will either return the count of readable messages
    /// in an [Ok] or an [Err] if something went wrong.
    pub fn send(&self, value: T) -> Result<usize, SeccErrors<T>> {
        self.send_arc(Arc::new(value))
    }

    /// Sends a value which is an [`Arc`] into the channel, the value will be moved into the
    /// mailbox and it will take ownership of the value. This function will either return
    /// the count of readable messages in an [Ok] or an [Err] if something went wrong.
    pub fn send_arc(&self, value: Arc<T>) -> Result<usize, SeccErrors<T>> {
        unsafe {
            // Retrieve send pointers and the encoded indexes inside them.
            let mut send_ptrs = self.queue_tail_pool_head.lock().unwrap();
            let (queue_tail, pool_head) = *send_ptrs;

            // Get a pointer to the current pool_head and see if we have space to send.
            let pool_head_ptr = (*self.core().node_ptrs.get())[pool_head];
            let next_pool_head = (*pool_head_ptr).next.load(Ordering::Acquire);
            if NIL_NODE == next_pool_head {
                return Err(SeccErrors::Full(value));
            }

            // Pool head moves to become the queue tail or else loop and try again!
            *send_ptrs = (pool_head, next_pool_head);
            let queue_tail_ptr = (*self.core().node_ptrs.get())[queue_tail];
            (*(*queue_tail_ptr).cell.get()) = Some(value);
            (*pool_head_ptr).next.store(NIL_NODE, Ordering::Release);
            (*queue_tail_ptr).next.store(pool_head, Ordering::Release);

            // Once we complete the write we have to adjust the channel statistics.
            self.core.enqueued.fetch_add(1, Ordering::Relaxed);
            self.core.length.fetch_add(1, Ordering::Release);
            let old_readable = self.core.readable.fetch_add(1, Ordering::Relaxed);
            // If we enqueued a new message into an channel that had no readable messages previously
            // so we notify waiters that there is content to read.
            if old_readable == 0 {
                let (ref mutex, ref condvar) = &*self.core.has_messages;
                let _guard = mutex.lock().unwrap();
                condvar.notify_all();
            }
            return Ok(old_readable + 1);
        }
    }

    /// Send to the channel, awaiting capacity if necessary with an optional timeout. This
    /// function will either return the count of readable messages /// in an [Ok] or an [Err]
    /// if something went wrong. This function is semantically identical to [send] but
    /// simply waits for there to be space in the channel to send before sending.
    pub fn send_await_timeout(
        &self,
        mut value: T,
        timeout: Option<Duration>,
    ) -> Result<usize, SeccErrors<T>> {
        self.send_arc_await_timeout(Arc::new(value), timeout)
    }

    /// Send to the channel, awaiting capacity if necessary with an optional timeout. This
    /// function will either return the count of readable messages /// in an [Ok] or an [Err]
    /// if something went wrong. This function is semantically identical to [send_arc] but
    /// simply waits for there to be space in the channel to send before sending.
    pub fn send_arc_await_timeout(
        &self,
        mut value: Arc<T>,
        timeout: Option<Duration>,
    ) -> Result<usize, SeccErrors<T>> {
        loop {
            match self.send_arc(value) {
                Err(SeccErrors::Full(v)) => {
                    value = v;
                    let (ref mutex, ref condvar) = &*self.core.has_capacity;
                    let guard = mutex.lock().unwrap();
                    if self.core.readable.load(Ordering::Acquire) < self.core.capacity {
                        // race occurred, there is space to send now, loop and try again.
                        continue;
                    }
                    // nope, still no capacity, wait.
                    self.core.awaited_capacity.fetch_add(1, Ordering::Relaxed);
                    match timeout {
                        Some(dur) => {
                            let _condvar_guard = condvar.wait_timeout(guard, dur).unwrap();
                        }
                        None => {
                            let _condvar_guard = condvar.wait(guard).unwrap();
                        }
                    };
                    // loop and try again.
                }
                v => return v,
            }
        }
    }

    /// Helper to call [send_await_with_timeout] using a None for the timeout. This function
    /// will either return the count of readable messages in an [Ok] or an [Err] if
    /// something went wrong.
    pub fn send_await(&self, value: T) -> Result<usize, SeccErrors<T>> {
        self.send_await_timeout(value, None)
    }

    /// Helper to call [send_arc_await_with_timeout] using a None for the timeout. This function
    /// will either return the count of readable messages in an [Ok] or an [Err] if
    /// something went wrong.
    pub fn send_arc_await(&self, value: T) -> Result<usize, SeccErrors<T>> {
        self.send_arc_await_timeout(value, None)
    }
}

impl<T: Sync + Send> SeccCoreOps<T> for SeccSender<T> {
    fn core(&self) -> &SeccCore<T> {
        &self.core
    }
}

unsafe impl<T: Send + Sync> Send for SeccSender<T> {}

unsafe impl<T: Send + Sync> Sync for SeccSender<T> {}

/// Receiver side of the channel.
pub struct SeccReceiver<T: Sync + Send> {
    /// The core of the channel.
    core: Arc<SeccCore<T>>,
    /// Position in the buffer where the nodes can be dequeued from the queue and put back
    /// on the pool. The queue head is where we can dequeue the next message and the pool
    /// tail is where to put nodes back into the pool.
    queue_head_pool_tail_precursor_cursor: Mutex<(usize, usize, usize, usize)>,
}

impl<T: Sync + Send> SeccReceiver<T> {
    /// Receives the message at the head of the channel.
    pub fn peek(&self) -> Result<Arc<T>, SeccErrors<T>> {
        unsafe {
            // Retrieve receive pointers and the encoded indexes inside them.
            let receive_ptrs = self.queue_head_pool_tail_precursor_cursor.lock().unwrap();
            let (queue_head, _pool_tail, _precursor, cursor) = *receive_ptrs;

            // Get a pointer to the current queue_head or cursor and see if there is anything to read.
            let read_ptr = if cursor == NIL_NODE {
                (*self.core().node_ptrs.get())[queue_head]
            } else {
                (*self.core().node_ptrs.get())[cursor]
            };
            let next_read_pos = (*read_ptr).next.load(Ordering::Acquire);
            if NIL_NODE == next_read_pos {
                return Err(SeccErrors::Empty);
            }
            let value: Arc<T> = (*(*read_ptr).cell.get()).take().unwrap();
            (*(*read_ptr).cell.get()) = Some(value.clone());
            Ok(value)
        }
    }

    /// Receives the message at the head of the channel.
    pub fn receive(&self) -> Result<Arc<T>, SeccErrors<T>> {
        unsafe {
            // Retrieve receive pointers and the encoded indexes inside them.
            let mut receive_ptrs = self.queue_head_pool_tail_precursor_cursor.lock().unwrap();
            let (queue_head, pool_tail, precursor, cursor) = *receive_ptrs;

            // Get a pointer to the current queue_head or cursor and see if there is anything to read.
            let read_ptr = if cursor == NIL_NODE {
                (*self.core().node_ptrs.get())[queue_head]
            } else {
                (*self.core().node_ptrs.get())[cursor]
            };
            let next_read_pos = (*read_ptr).next.load(Ordering::Acquire);
            if NIL_NODE == next_read_pos {
                return Err(SeccErrors::Empty);
            }

            let pool_tail_ptr = (*self.core().node_ptrs.get())[pool_tail];
            let value: Arc<T> = (*(*read_ptr).cell.get()).take().unwrap();
            if cursor == NIL_NODE {
                // If we aren't using a cursor then the queue_head moves to become the pool tail or else loop and try again.
                (*pool_tail_ptr).next.store(queue_head, Ordering::Release);
                (*read_ptr).next.store(NIL_NODE, Ordering::Release);
                *receive_ptrs = (next_read_pos, queue_head, precursor, cursor);
            } else {
                // if the cursor is set we have to dequeue in the middle of the list and fix the node chain and then move
                // the node that the cursor was point to to the pool tail. Note that the precursor will never be nip because
                // that would mean that there is no skip going on. Precursor is only ever set to a skipped node that could be
                // read.
                let precursor_ptr = (*self.core().node_ptrs.get())[precursor];
                (*precursor_ptr)
                    .next
                    .store(next_read_pos, Ordering::Release);
                (*pool_tail_ptr).next.store(cursor, Ordering::Release);
                (*read_ptr).next.store(NIL_NODE, Ordering::Release);
                *receive_ptrs = (queue_head, queue_head, precursor, next_read_pos);
            }
            (*read_ptr).next.store(NIL_NODE, Ordering::Release);

            // Once we complete the write we have to adjust the channel statistics.
            self.core.dequeued.fetch_add(1, Ordering::Relaxed);
            self.core.readable.fetch_sub(1, Ordering::Release);
            let old_length = self.core.length.fetch_sub(1, Ordering::Release);
            // If we dequeued a message from a full buffer notify any waiters.
            if old_length == self.core.capacity {
                let (ref mutex, ref condvar) = &*self.core.has_capacity;
                let _guard = mutex.lock().unwrap();
                condvar.notify_all();
            }
            return Ok(value);
        }
    }

    /// messages in the channel or an error if the channel was empty.
    pub fn pop(&self) -> Result<usize, SeccErrors<T>> {
        self.receive()?;
        Ok(self.core.length.load(Ordering::Relaxed))
    }

    /// Send to the channel, awaiting capacity if necessary.
    pub fn receive_await_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Arc<T>, SeccErrors<T>> {
        loop {
            match self.receive() {
                Err(SeccErrors::Empty) => {
                    let (ref mutex, ref condvar) = &*self.core.has_messages;
                    let guard = mutex.lock().unwrap();
                    if self.core.readable.load(Ordering::Acquire) > 0 {
                        // there was some race and now data is available so we just loop and try again.
                        continue;
                    }
                    // nope, still no messages, wait.
                    self.core.awaited_messages.fetch_add(1, Ordering::Relaxed);
                    match timeout {
                        Some(dur) => {
                            let _condvar_guard = condvar.wait_timeout(guard, dur).unwrap();
                        }
                        None => {
                            let _condvar_guard = condvar.wait(guard).unwrap();
                        }
                    };
                    // loop and try again because even if data was added and is now readable, some
                    // other thread might beat us to it so we have to loop again.
                }
                v => return v,
            }
        }
    }

    /// A helper to call [receive_await_timeout] with [None] for a timeout.
    pub fn receive_await(&self) -> Result<Arc<T>, SeccErrors<T>> {
        self.receive_await_timeout(None)
    }

    /// A helper used for skipping messages in the channel. If the user passed [true] for
    /// to_end then the skip mechanism will skip to the end of the channel inside a single
    /// lock. This function returns the total number of readable messages or an error.
    fn skip_helper(&self, to_end: bool) -> Result<usize, SeccErrors<T>> {
        // FIXME Need tests!
        // FIXME Track Metrics.
        unsafe {
            let mut count = 0; // count the number skipped in this call
                               // Retrieve receive pointers and the encoded indexes inside them.
            let mut receive_ptrs = self.queue_head_pool_tail_precursor_cursor.lock().unwrap();
            loop {
                let (queue_head, pool_tail, _precursor, cursor) = *receive_ptrs;
                let read_ptr = if cursor == NIL_NODE {
                    (*self.core().node_ptrs.get())[queue_head]
                } else {
                    (*self.core().node_ptrs.get())[cursor]
                };
                let next_read_pos = (*read_ptr).next.load(Ordering::Acquire);
                // if there is a single node in the queue then there is no data in the channel
                // and therefore nothing to skip. If we already skipped some nodes we will
                // just return the total number readable, otherwise we will return an error.
                if NIL_NODE == next_read_pos {
                    if count == 0 {
                        return Err(SeccErrors::Empty);
                    } else {
                        return Ok(self.core.readable.load(Ordering::Relaxed));
                    }
                }
                if cursor == NIL_NODE {
                    // no current cursor, establish one,
                    *receive_ptrs = (queue_head, pool_tail, queue_head, next_read_pos);
                } else {
                    // There is a cursor already so make sure we increment cursor and precursor.
                    *receive_ptrs = (queue_head, pool_tail, cursor, next_read_pos);
                }
                let old_readable = self.core.readable.fetch_sub(1, Ordering::Relaxed);
                count += 1;
                if !to_end {
                    return Ok(old_readable + 1);
                }
                // otherwise we will loop around
            }
        }
    }

    /// Skips the next message to be read in the channel and either returns the number of total
    /// readable messages in the channel or an error if the skip fails. If the skip succeeds than
    /// the number of readable messages will drop by one because message is skipped. To read the
    /// message again the user will need to call [reset_skip] in order to reset the skip pointer
    /// back to the head of the channel.
    pub fn skip(&self) -> Result<usize, SeccErrors<T>> {
        self.skip_helper(false)
    }

    /// Skips the channel to the current end of the channel. and either returns the number of total
    /// readable messages in the channel or an error if the skip fails. This has an O(n) efficiency
    /// as the channel needs to traverse all messages to get to the end and there could be a race
    /// to get to the end before new data is sent to the channel so the user should be aware that
    /// the channel may not completely be at the end of the channel.
    pub fn skip_to_end(&self) -> Result<usize, SeccErrors<T>> {
        // FIXME Need tests!
        self.skip_helper(true)
    }

    /// Cancels skipping messages in the channel and resets the pointers of the channel back to
    /// the head returning the current number of messages readable in the channel.
    pub fn reset_skip(&self) -> Result<usize, SeccErrors<T>> {
        // FIXME Need tests!
        // FIXME Track Metrics.
        // Retrieve receive pointers and the encoded indexes inside them.
        let mut receive_ptrs = self.queue_head_pool_tail_precursor_cursor.lock().unwrap();
        let (queue_head, pool_tail, _precursor, cursor) = *receive_ptrs;
        if cursor == NIL_NODE {
            return Err(SeccErrors::Empty); // nothing to do.
        };
        // no current cursor, establish one,
        let length = self.core.length.load(Ordering::Acquire);
        self.core.readable.store(length, Ordering::Release);
        *receive_ptrs = (queue_head, pool_tail, NIL_NODE, NIL_NODE);
        Ok(length)
    }
}

impl<T: Sync + Send> SeccCoreOps<T> for SeccReceiver<T> {
    fn core(&self) -> &SeccCore<T> {
        &self.core
    }
}

unsafe impl<T: Send + Sync> Send for SeccReceiver<T> {}

unsafe impl<T: Send + Sync> Sync for SeccReceiver<T> {}

/// Creates the sender and receiver sides of this channel.
pub fn create<T: Sync + Send>(capacity: HalfUsize) -> (SeccSender<T>, SeccReceiver<T>) {
    // FIXME support reallocation of size ?
    if capacity < 1 {
        panic!("capacity cannot be smaller than 1");
    }

    // We add two to the allocated capacity to account for the mandatory two placeholder nodes
    // that guarantee that both queue and pool are never empty.
    let alloc_capacity = (capacity + 2) as usize;
    let mut nodes = Vec::<SeccNode<T>>::with_capacity(alloc_capacity);
    let mut node_ptrs = Vec::<*mut SeccNode<T>>::with_capacity(alloc_capacity);

    // The queue just gets one initial node with no data and the queue_tail is just
    // the same as the queue_head.
    nodes.push(SeccNode::<T>::new());
    node_ptrs.push(nodes.last_mut().unwrap() as *mut SeccNode<T>);
    let queue_head = nodes.len() - 1;
    let queue_tail = queue_head;

    // Allocate the tail in the pool of nodes that will be added to in order to form
    // the pool. Note that although this is expensive, it only has to be done once.
    nodes.push(SeccNode::<T>::new());
    node_ptrs.push(nodes.last_mut().unwrap() as *mut SeccNode<T>);
    let mut pool_head = nodes.len() - 1;
    let pool_tail = pool_head;

    // Allocate the rest of the pool pushing each node onto the previous node.
    for _ in 0..capacity {
        nodes.push(SeccNode::<T>::with_next(pool_head));
        node_ptrs.push(nodes.last_mut().unwrap() as *mut SeccNode<T>);
        pool_head = nodes.len() - 1;
    }

    // Create the channel structures
    let core = Arc::new(SeccCore {
        capacity: capacity as usize,
        _nodes: UnsafeCell::new(vec![nodes.into_boxed_slice()]),
        node_ptrs: UnsafeCell::new(node_ptrs),
        has_messages: Arc::new((Mutex::new(true), Condvar::new())),
        awaited_messages: AtomicUsize::new(0),
        has_capacity: Arc::new((Mutex::new(true), Condvar::new())),
        awaited_capacity: AtomicUsize::new(0),
        length: AtomicUsize::new(0),
        readable: AtomicUsize::new(0),
        enqueued: AtomicUsize::new(0),
        dequeued: AtomicUsize::new(0),
    });

    let sender = SeccSender {
        core: core.clone(),
        queue_tail_pool_head: Mutex::new((queue_tail, pool_head)),
    };

    let receiver = SeccReceiver {
        core,
        queue_head_pool_tail_precursor_cursor: Mutex::new((
            queue_head, pool_tail, NIL_NODE, NIL_NODE,
        )),
    };

    (sender, receiver)
}

/// Creates the sender and receiver sides of the channel for multiple producers and
/// multiple consumers by returning sender and receiver each wrapped in [Arc] instances.
pub fn create_with_arcs<T: Sync + Send>(
    capacity: HalfUsize,
) -> (Arc<SeccSender<T>>, Arc<SeccReceiver<T>>) {
    let (sender, receiver) = create(capacity);
    (Arc::new(sender), Arc::new(receiver))
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    /// A macro to assert that pointers point to the right nodes.
    macro_rules! assert_pointer_nodes {
        (
            $sender:expr,
            $receiver:expr,
            $queue_head:expr,
            $queue_tail:expr,
            $pool_head:expr,
            $pool_tail:expr,
            $precursor:expr,
            $cursor:expr
        ) => {{
            let send_ptrs = $sender.queue_tail_pool_head.lock().unwrap();
            let (queue_tail, pool_head) = *send_ptrs;
            let receive_ptrs = $receiver
                .queue_head_pool_tail_precursor_cursor
                .lock()
                .unwrap();
            let (queue_head, pool_tail, precursor, cursor) = *receive_ptrs;

            assert_eq!($queue_head, queue_head, " <== queue_head mismatch\n");
            assert_eq!($queue_tail, queue_tail, "<== queue_tail mismatch\n");
            assert_eq!($pool_head, pool_head, "<== pool_head mismatch\n");
            assert_eq!($pool_tail, pool_tail, " <== pool_tail mismatch\n");
            assert_eq!($precursor, precursor, " <== precursor mismatch\n");
            assert_eq!($cursor, cursor, " <== pool_tail mismatch\n");
        }};
    }

    /// Asserts that the given node in the queue has the expected next pointer.
    macro_rules! assert_node_next {
        ($pointers:expr, $node:expr, $next:expr) => {
            unsafe { assert_eq!((*$pointers[$node]).next.load(Ordering::Relaxed), $next) }
        };
    }

    /// Asserts that the given node in the queue has the expected next pointing to null_mut().
    macro_rules! assert_node_next_nil {
        ($pointers:expr, $node:expr) => {
            unsafe { assert_eq!((*$pointers[$node]).next.load(Ordering::Relaxed), NIL_NODE) }
        };
    }

    #[derive(Debug, Eq, PartialEq)]
    enum Items {
        A,
        B,
        C,
        D,
        E,
        F,
    }

    /// Tests the basics of the queue.
    #[test]
    fn test_queue_dequeue() {
        let channel = create::<Items>(5);
        let (sender, receiver) = channel;

        // fetch the pointers for easy checking of the nodes.
        let pointers = unsafe { &*sender.core.node_ptrs.get() };

        assert_eq!(7, pointers.len());
        assert_eq!(5, sender.core.capacity);
        assert_eq!(5, sender.capacity());
        assert_eq!(5, receiver.capacity());

        // Check the initial structure.
        assert_eq!(0, sender.length());
        assert_eq!(0, sender.enqueued());
        assert_eq!(0, sender.dequeued());
        assert_node_next_nil!(pointers, 0);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(sender, receiver, 0, 0, 6, 1, NIL_NODE, NIL_NODE); // ( qh, qt, ph, pt)

        // Check that enqueueing removes pool head and appends to queue tail and changes
        // nothing else in the node structure.
        assert_eq!(Ok(1), sender.send(Items::A));
        assert_eq!(1, sender.length());
        assert_eq!(1, sender.enqueued());
        assert_eq!(0, sender.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next_nil!(pointers, 6);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(sender, receiver, 0, 6, 5, 1, NIL_NODE, NIL_NODE);

        // Second sender should also move the pool_head node.
        assert_eq!(Ok(2), sender.send(Items::B));
        assert_eq!(2, sender.length());
        assert_eq!(2, sender.enqueued());
        assert_eq!(0, sender.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next_nil!(pointers, 5);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(sender, receiver, 0, 5, 4, 1, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(3), sender.send(Items::C));
        assert_eq!(3, sender.length());
        assert_eq!(3, sender.enqueued());
        assert_eq!(0, sender.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next_nil!(pointers, 4);
        assert_node_next!(pointers, 3, 2);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(sender, receiver, 0, 4, 3, 1, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(4), sender.send(Items::D));
        assert_eq!(4, sender.length());
        assert_eq!(4, sender.enqueued());
        assert_eq!(0, sender.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next_nil!(pointers, 3);
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(sender, receiver, 0, 3, 2, 1, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(5), sender.send(Items::E));
        assert_eq!(5, sender.length());
        assert_eq!(5, sender.enqueued());
        assert_eq!(0, sender.dequeued());
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next_nil!(pointers, 1);
        assert_pointer_nodes!(sender, receiver, 0, 2, 1, 1, NIL_NODE, NIL_NODE);

        assert_eq!(Err(SeccErrors::Full(Items::F)), sender.send(Items::F));
        assert_eq!(5, sender.length());
        assert_eq!(5, sender.enqueued());
        assert_eq!(0, sender.dequeued());

        assert_eq!(Ok(Items::A), receiver.receive());
        assert_eq!(4, receiver.length());
        assert_eq!(5, receiver.enqueued());
        assert_eq!(1, receiver.dequeued());
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next_nil!(pointers, 0);
        assert_pointer_nodes!(sender, receiver, 6, 2, 1, 0, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(Items::B), receiver.receive());
        assert_eq!(3, receiver.length());
        assert_eq!(5, receiver.enqueued());
        assert_eq!(2, receiver.dequeued());
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next_nil!(pointers, 6);
        assert_pointer_nodes!(sender, receiver, 5, 2, 1, 6, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(Items::C), receiver.receive());
        assert_eq!(2, receiver.length());
        assert_eq!(5, receiver.enqueued());
        assert_eq!(3, receiver.dequeued());
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next_nil!(pointers, 5);
        assert_pointer_nodes!(sender, receiver, 4, 2, 1, 5, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(Items::D), receiver.receive());
        assert_eq!(1, receiver.length());
        assert_eq!(5, receiver.enqueued());
        assert_eq!(4, receiver.dequeued());
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next_nil!(pointers, 4);
        assert_pointer_nodes!(sender, receiver, 3, 2, 1, 4, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(Items::E), receiver.receive());
        assert_eq!(0, receiver.length());
        assert_eq!(5, receiver.enqueued());
        assert_eq!(5, receiver.dequeued());
        assert_node_next_nil!(pointers, 2);
        assert_node_next!(pointers, 1, 0);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next_nil!(pointers, 3);
        assert_pointer_nodes!(sender, receiver, 2, 2, 1, 3, NIL_NODE, NIL_NODE);

        assert_eq!(Err(SeccErrors::Empty), receiver.receive());
        assert_eq!(0, receiver.length());
        assert_eq!(5, receiver.enqueued());
        assert_eq!(5, receiver.dequeued());

        assert_eq!(Ok(1), sender.send(Items::F));
        assert_eq!(1, receiver.length());
        assert_eq!(6, receiver.enqueued());
        assert_eq!(5, receiver.dequeued());
        assert_node_next!(pointers, 2, 1);
        assert_node_next_nil!(pointers, 1);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next_nil!(pointers, 3);
        assert_pointer_nodes!(sender, receiver, 2, 1, 0, 3, NIL_NODE, NIL_NODE);

        assert_eq!(Ok(Items::F), receiver.receive());
        assert_eq!(0, receiver.length());
        assert_eq!(6, receiver.enqueued());
        assert_eq!(6, receiver.dequeued());
        assert_node_next_nil!(pointers, 1);
        assert_node_next!(pointers, 0, 6);
        assert_node_next!(pointers, 6, 5);
        assert_node_next!(pointers, 5, 4);
        assert_node_next!(pointers, 4, 3);
        assert_node_next!(pointers, 3, 2);
        assert_node_next_nil!(pointers, 2);
        assert_pointer_nodes!(sender, receiver, 1, 1, 0, 2, NIL_NODE, NIL_NODE);
    }

    #[test]
    fn test_single_producer_single_receiver() {
        let message_count = 200;
        let capacity = 32;
        let (sender, receiver) = create_with_arcs::<u32>(capacity);

        let rx = thread::spawn(move || {
            let mut count = 0;
            while count < message_count {
                match receiver.receive() {
                    Ok(_v) => count += 1,
                    _ => (),
                };
            }
        });

        let tx = thread::spawn(move || {
            for i in 0..message_count {
                sender.send(i).unwrap();
                thread::sleep(Duration::from_millis(1));
            }
        });

        tx.join().unwrap();
        rx.join().unwrap();
    }

    #[test]
    fn test_multiple_producer_single_receiver() {
        let message_count = 1000;
        let capacity = 100;
        let (sender, receiver) = create_with_arcs::<u32>(capacity);

        let receiver1 = receiver.clone();
        let rx = thread::spawn(move || {
            let mut count = 0;
            while count < message_count {
                match receiver1.receive_await() {
                    Ok(_) => count += 1,
                    _ => (),
                };
            }
        });

        let sender1 = sender.clone();
        let tx = thread::spawn(move || {
            for i in 0..(message_count / 3) {
                match sender1.send_await(i) {
                    Ok(_c) => (),
                    Err(e) => assert!(false, "----> Error while sending: {}:{:?}", i, e),
                }
            }
        });

        let sender2 = sender.clone();
        let tx2 = thread::spawn(move || {
            for i in (message_count / 3)..((message_count / 3) * 2) {
                match sender2.send_await(i) {
                    Ok(_c) => (),
                    Err(e) => assert!(false, "----> Error while sending: {}:{:?}", i, e),
                }
            }
        });

        let sender3 = sender.clone();
        let tx3 = thread::spawn(move || {
            for i in ((message_count / 3) * 2)..(message_count) {
                match sender3.send_await(i) {
                    Ok(_c) => (),
                    Err(e) => assert!(false, "----> Error while sending: {}:{:?}", i, e),
                }
            }
        });

        tx.join().unwrap();
        tx2.join().unwrap();
        tx3.join().unwrap();
        rx.join().unwrap();

        println!(
            "All messages complete: awaited_messages: {}, awaited_capacity: {}",
            receiver.awaited_messages(),
            sender.awaited_capacity()
        );
    }
}
