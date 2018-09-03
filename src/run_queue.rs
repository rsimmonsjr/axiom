//! Implements a run queue that allows actors to be enqueued from multiple threads and
//! dequeued from multiple threads as well using a fixed concurrent ring buffer.
//!
//! Positions values in the run queue buffer are made up of two components. The upper half
//! contains a lap variable that signals that a node is enabled for writing (on even laps)
//! and reading (on odd laps). The lower half is the actual index of the position in the buffer.
//! We need this combination because we need to swap out the whole position atomically.
//!
//! ## Detailed Explanation
//!
//! The following shows some examples of the run queue with 5 node buffer. D is the Dequeue
//! position and points to the first possible node to be dequeued. For all pointers the value
//! in the parentheses is the "lap" of the pointer which not only indicates how many trips around
//! the buffer the pointer has had but also is used to signal if a node can be read or written. E
//! starts at 0 whereas D starts at 1. When the pointer reaches the end of the allocated buffer
//! the value wraps around and the lap is increased by 2. This means E will always be even and D
//! will always be odd. A pointer can operate on a node when in the buffer when the lap
//! in the node matches the pointer. In the following structures an N indicates the node has
//! no value and a S indicates there is a value in the node. The second element per node is
//! the lap for that node which is always incremented by 1 so that it alternates between
//! writable and readable.
//!
//! The starting structure looks like this for a 5 node allocated buffer. Note that there is
//! nothing to dequeue because D points at a node where the lap is less than the lap in D.
//! ```text
//! 0: [ N, 0] <-- E(0) <-- D(1)
//! 1: [ N, 0]
//! 2: [ N, 0]
//! 3: [ N, 0]
//! 4: [ N, 0]
//! ```
//!
//! After enqueueing a few messages the D value stays the same but E has moved and the
//! lap on the nodes written have been incremented by one indicating they can be read.
//! ```text
//! 0: [ S, 1] <-- D(1)
//! 1: [ S, 1]
//! 2: [ S, 1]
//! 3: [ S, 1]
//! 4: [ N, 0] <-- E(0)
//! ```
//!
//! After dequeueing 2 nodes the lap has been incremented again and D has moved.
//! ```text
//! 0: [ N, 2]
//! 1: [ N, 2]
//! 2: [ S, 1] <-- D(1)
//! 3: [ S, 1]
//! 4: [ N, 0] <-- E(0)
//! ```
//!
//! Enqueueing 1 more node means E has to lap around the buffer and this its lap is incremented
//! by 2 and thus remains even and points at a node with a matching lap so the node can be
//! written to in the queue.
//! ```text
//! 0: [ N, 2] <-- E(2)
//! 1: [ N, 2]
//! 2: [ S, 1] <-- D(1)
//! 3: [ S, 1]
//! 4: [ S, 1]
//! ```
//!
//! If we enqueue a couple more messages E now points at a node with an odd numbered lap and
//! the pending messages are the size of the queue so there is no way another message can
//! be enqueued to the buffer and the buffer is full. D will have to increment the node lap
//! in order to allow the node to be an even number again and thus writable.
//! ```text
//! 0: [ S, 3]
//! 1: [ S, 3]
//! 2: [ S, 1] <-- D(1) <-- E(2)
//! 3: [ S, 1]
//! 4: [ S, 1]
//! ```
//!
//! Now we enqueue a few more nodes and dequeue more and D has to wrap around, again being
//! Incremented by two.
//! ```text
//! 0: [ S, 3] <-- D(3)
//! 1: [ S, 3]
//! 2: [ S, 3]
//! 3: [ S, 3]
//! 4: [ N, 2] <-- E(2)
//! ```

//! ## Credits:
//! Based on a modified version of (Go channels on steroids)
//! [https://docs.google.com/document/d/1yIAYmbvL3JxOKOjuCyon7JhW4cSv1wy5hC0ApeGMV9s/pub]

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Type alias for half of a usize on 64 bit platform.
type HalfUsize = u32;

/// Number of bits in half of a usize on 64 bit platform.
const HALF_USIZE_BITS: u8 = 32;

/// Bitmask used to extract an index from a position on 64 bit platform.
const INDEX_BITMASK: usize = 0xFFFFFFFF;

/// Errors potentially returned from run queue operations.
#[derive(Debug, Eq, PartialEq)]
pub enum RunQueueErrors {
    Full,
    Empty,
}

/// A single node in the run queue's ring buffer.
pub struct RunQueueNode<T: Sync + Send> {
    /// Contains a value set in the node in a Some or a None if empty. Note that this is unsafe
    /// in order to get around Rust mutability locks so that this data structure can be passed
    /// around immutably but also still be able to enqueue and dequeue.
    cell: UnsafeCell<Option<T>>,
    /// This is the "lap" variable that provides an indication of whether the node is ready to
    /// read or write and encompasses the semantics that all even numbers indicate ready for
    /// writing while all odd numbers indicate ready for reading. This enables us to avoid
    /// comparing portions all the time.
    lap: AtomicUsize,
}

pub struct RunQueue<T: Sync + Send> {
    /// Holds the nodes that make up the buffer backing up the run queue.
    buffer: Vec<RunQueueNode<T>>,
    /// Position in the buffer at which to enqueue the next value.
    enqueue_pos: AtomicUsize,
    /// Position in the buffer at which to the last message available to be dequeued occupies..
    dequeue_pos: AtomicUsize,
    /// The total number of messages that have been sent to this mailbox. When the mailbox
    /// pending is the same as the capacity then the mailbox is full.
    pending: AtomicUsize,
    /// The total number of messages that have been sent to this mailbox (enqueued).
    enqueued: AtomicUsize,
    /// The total number of messages that have been received from this mailbox (dequeued)
    dequeued: AtomicUsize,
}

impl<T: Sync + Send> RunQueue<T> {
    /// Creates a new run queue with the given capacity.
    pub fn new(capacity: HalfUsize) -> RunQueue<T> {
        let mut buffer = Vec::<RunQueueNode<T>>::with_capacity(capacity as usize);
        for _ in 0..capacity {
            buffer.push(RunQueueNode {
                cell: UnsafeCell::new(None),
                lap: AtomicUsize::new(0),
            })
        }
        RunQueue {
            buffer,
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(1 << HALF_USIZE_BITS as usize),
            pending: AtomicUsize::new(0),
            enqueued: AtomicUsize::new(0),
            dequeued: AtomicUsize::new(0),
        }
    }

    /// The total number of messages that are currently pending in the run queue.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// The total number of messages that have been enqueued to this run queue.
    pub fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }

    /// The total number of messages that have been dequeued from this run queue.
    pub fn dequeued(&self) -> usize {
        self.dequeued.load(Ordering::Relaxed)
    }

    /// Finds a node using the given position reference and either returns the node,
    /// incrementing the position as needed or returns a None, indicating no node was
    /// available.
    fn find_next_node<'a>(
        buffer: &'a Vec<RunQueueNode<T>>,
        position: &AtomicUsize,
    ) -> Option<&'a RunQueueNode<T>> {
        let capacity = buffer.len();
        loop {
            // First decode the lap and idx from the position passed.
            let pos = position.load(Ordering::Relaxed);
            let idx = pos & INDEX_BITMASK; // slice the lap off
            let lap = pos >> HALF_USIZE_BITS;
            let node_lap = buffer[idx].lap.load(Ordering::Acquire);
            if lap == node_lap {
                // We know this node is ready on the lap so try to increment the pointer
                // so that no one else can access this node.
                let new_pos = if capacity > idx + 1 {
                    // simple increment of the position will suffice
                    pos + 1
                } else {
                    // Reached the end of the buffer so loop and add 2 to the lap.
                    (lap + 2) << HALF_USIZE_BITS
                };
                // If this atomic swap works then we have access to the node and can do
                //our operation without fear that other threads will do the same.
                if pos == position.compare_and_swap(pos, new_pos, Ordering::Relaxed) {
                    return Some(&buffer[idx]);
                }
            } else {
                return None;
            }
            // The node lap was less than the position lap so we cannot operate on this node.
        }
    }

    /// Enqueues a message into the run queue and returns either a result with the number
    /// of values now pending or an error if there was a problem enqueuing.
    pub fn enqueue(&self, message: T) -> Result<usize, RunQueueErrors> {
        match RunQueue::find_next_node(&self.buffer, &self.enqueue_pos) {
            None => Err(RunQueueErrors::Full),
            Some(node) => {
                unsafe {
                    *node.cell.get() = Some(message);
                    node.lap.fetch_add(1, Ordering::Release);
                }
                let old_pending = self.pending.fetch_add(1, Ordering::Relaxed);
                self.enqueued.fetch_add(1, Ordering::Relaxed);
                Ok(old_pending + 1)
            }
        }
    }

    /// Dequeue the result, from the run queue or returns an error if no message is available.
    pub fn dequeue(&self) -> Result<T, RunQueueErrors> {
        match RunQueue::find_next_node(&self.buffer, &self.dequeue_pos) {
            None => return Err(RunQueueErrors::Empty),
            Some(node) => {
                let message = unsafe {
                    let value = (*node.cell.get()).take().unwrap();
                    node.lap.fetch_add(1, Ordering::Release);
                    value
                };
                self.dequeued.fetch_add(1, Ordering::Relaxed);
                self.pending.fetch_sub(1, Ordering::Relaxed);
                return Ok(message);
            }
        }
    }
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    enum Items {
        One,
        Two,
        Three,
        Four
    }

    /// Checks that the counters in the run queue are correct.
    macro_rules! assert_counters {
        ($mailbox:expr, $pending:expr, $enqueued:expr, $dequeued:expr) => {{
            assert_eq!($pending, $mailbox.pending());
            assert_eq!($enqueued, $mailbox.enqueued());
            assert_eq!($dequeued, $mailbox.dequeued());
        }};
    }

    #[test]
    fn test_basic_operations() {
        let run_queue = RunQueue::<Items>::new(3);
        assert_eq!(3, run_queue.buffer.len());
        assert_counters!(run_queue, 0, 0, 0);

        assert_eq!(Err(RunQueueErrors::Empty), run_queue.dequeue());
        assert_counters!(run_queue, 0, 0, 0);

        assert_eq!(Ok(1), run_queue.enqueue(Items::One));
        assert_counters!(run_queue, 1, 1, 0);

        assert_eq!(Ok(2), run_queue.enqueue(Items::Two));
        assert_counters!(run_queue, 2, 2, 0);

        assert_eq!(Ok(3), run_queue.enqueue(Items::Three));
        assert_counters!(run_queue, 3, 3, 0);

        assert_eq!(Err(RunQueueErrors::Full), run_queue.enqueue(Items::Four));
        assert_counters!(run_queue, 3, 3, 0);

        assert_eq!(Ok(Items::One), run_queue.dequeue());
        assert_counters!(run_queue, 2, 3, 1);

        assert_eq!(Ok(Items::Two), run_queue.dequeue());
        assert_counters!(run_queue, 1, 3, 2);

        assert_eq!(Ok(Items::Three), run_queue.dequeue());
        assert_counters!(run_queue, 0, 3, 3);

        assert_eq!(Err(RunQueueErrors::Empty), run_queue.dequeue());
        assert_counters!(run_queue, 0, 3, 3);
    }
}
