//! Implements a run queue that allows entries to be enqueued from multiple threads and
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
//! and C will always be odd. A pointer can operate on a node when in the buffer when the lap
//! in the node matches the pointer. In the following structures an N indicates the node has
//! no value and a S indicates there is a value in the node. The second element per node is
//! the lap for that node which is always incremented by 1 so that it alternates between
//! writable and readable.
//!
//! The starting structure looks like this for a 5 node allocated buffer. Note that there is
//! nothing to dequeue because the D and C point at a node where the lap is less than the
//! lap in D and C.
//! ```text
//! 0: [ N, 0] <-- E(0) <-- D(1)
//! 1: [ N, 0]
//! 2: [ N, 0]
//! 3: [ N, 0]
//! 4: [ N, 0]
//! ```
//!
//! After enqueueing a few messages the D and C value stay the same but E has moved and the
//! lap on the nodes written have been incremented by one indicating they can be read.
//! ```text
//! 0: [ S, 1] <-- D(1)
//! 1: [ S, 1]
//! 2: [ S, 1]
//! 3: [ S, 1]
//! 4: [ N, 0] <-- E(0)
//! ```
//!
//! After dequeueing 2 nodes the lap has been incremented again and  the D and C have moved.
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
//! be enqueued to the buffer and the buffer is full. C or D will have to increment the node lap
//! in order to allow the node to be an even number again and thus writable.
//! ```text
//! 0: [ S, 3]
//! 1: [ S, 3]
//! 2: [ S, 1] <-- D(1) <-- E(2)
//! 3: [ S, 1]
//! 4: [ S, 1]
//! ```
//!
//! Now we enqueue a few more nodes and dequeue more and D and C have to wrap around, again being
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
//! [https://docs.google.com/document/d/1yIAYmbvL3JxOKOjuCyon7JhW4cSv1wy5hC0ApeGMV9s/pub

use actors::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

type HalfUsize = u32;

const HALF_USIZE_BITS: i8 = 32;

//#[cfg(target_pointer_width = "64")]
//type HalfUsize = u32;

//#[cfg(target_pointer_width = "32")]
//const HALF_USIZE_BITS: i8 = 32;

//#[cfg(target_pointer_width = "32")]
//type HalfUsize = u16;

//#[cfg(target_pointer_width = "32")]
//const HALF_USIZE_BITS: i8 = 16;

/// A single node in the ring buffer.
pub struct Node {
    /// Could be empty or contains a value set in the node.
    message: Option<Arc<Message>>,
    /// This is the "lap" variable that provides an indication of whether the node is ready to
    /// read or write and encompasses the semantics that all even numbers indicate ready for
    /// writing while all odd numbers indicate ready for reading. This enables us to avoid
    /// comparing portions all the time.
    lap: AtomicUsize,
}

struct Core {}

/// Holds the mailbox for an Actor that manages the messages being sent to the actor. This
/// is implemented as a ring buffer with a bounded size that allows the reader to skip messages
/// rather than currently process the messages in the queue. This enables an actor to perform
/// some process that requires other messages to be skipped for the time being. Note that all
/// counters are kept as atomics and thus are O(1) to access. In this case we get more perfomance
/// by tracking the counters rather than iterating the structure.
pub struct RunQueue {
    /// The buffer holding the nodes that make up the mailbox. Actors should usually need
    /// only small buffers unless they expect that there will be a lot of backlogged messages
    /// from slow actors or they will be doing extensive skipping. Usually if you are creating
    /// huge ring buffers you may want to rethink your actor topology.
    buffer: Vec<Node>,
    /// Position in the buffer at which to enqueue the next message.
    enqueue_pos: AtomicUsize,
    /// Position in the buffer at which to the last message available to be dequeued occupies.
    /// It is worth noting that the cursor which represents where the queue is actually
    /// dequeueing may be lagging behind if some messages are being skipped.
    dequeue_pos: AtomicUsize,
    /// The number of messages that have been skipped in the mailbox at the current time.
    /// Note that this will mostly be 0 unless a mailbox has skipped some messages while
    /// waiting for some other message.
    skipped: AtomicUsize,
    /// The total number of messages that have been sent to this mailbox. When the mailbox
    /// pending is the same as the capacity then the mailbox is full.
    pending: AtomicUsize,
    /// The total number of messages that have been sent to this mailbox (enqueued).
    enqueued: AtomicUsize,
    /// The total number of messages that have been received from this mailbox (dequeued)
    dequeued: AtomicUsize,
}

impl RunQueue {
    /// Creates a new mailbox with the size of the given capacity.  The most possible mistakes
    /// when calling this method are either failing to assign a buffer big enough to
    /// accommodate the message flow or assigning a buffer that is too big and wasting space.
    /// The developer should pay close attention to load testing
    /// their actor and assigning it a buffer that is appropriate for the amount of messages it
    /// will receive and the amount of skipping being done.
    pub fn new(capacity: HalfUsize) -> RunQueue {
        let mut buffer = Vec::<Node>::with_capacity(capacity as usize);
        for _ in 0..capacity {
            buffer.push(Node {
                message: None,
                lap: AtomicUsize::new(0),
            })
        }
        RunQueue {
            buffer,
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(1 << HALF_USIZE_BITS as usize),
            skipped: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            enqueued: AtomicUsize::new(0),
            dequeued: AtomicUsize::new(0),
        }
    }

    /// The total number of messages that are currently pending in the mailbox.
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// The total number of messages that have been enqueued to this mailbox.
    pub fn enqueued(&self) -> usize {
        self.enqueued.load(Ordering::Relaxed)
    }

    /// The total number of messages that have been dequeued from this mailbox.
    pub fn dequeued(&self) -> usize {
        self.dequeued.load(Ordering::Relaxed)
    }

    /// The total number of messages that are currently skipped in the mailbox.
    pub fn skipped(&self) -> usize {
        self.skipped.load(Ordering::Relaxed)
    }

    /// Starts the channel skipping messages at the current back of the channel the
    /// rationale here is that in this case, no message could possibly match the message
    /// we are looking for if the message was sent before this time. This process is
    /// important in the case where the actor that owns this mailbox is waiting for
    /// one or more responses before it goes back into normal state.
    pub fn start_skip() {}

    /// Finds a node using the given position reference and either returns the node,
    /// incrementing the position as needed or returns a None, indicating no node was
    /// available.
    fn find_next_node<'a>(
        buffer: &'a mut Vec<Node>,
        position: &mut AtomicUsize,
    ) -> Option<&'a mut Node> {
        let capacity = buffer.len();
        loop {
            // First decode the lap and idx from the position passed.
            let pos = position.load(Ordering::Relaxed);
            let idx = pos & 0xFFFFFFFF; // slice the lap off
            let lap = pos >> HALF_USIZE_BITS;
            let node_lap = buffer[idx].lap.load(Ordering::Relaxed);
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
                    return Some(&mut buffer[idx]);
                }
            } else {
                return None;
            }
            // THe node lap was less than the position lap so we cannot operate on this node.
        }
    }

    /// Enqueues a message into the mailbox and returns either a result with the number
    /// of messages now pending or an error if there was a problem enququing.
    pub fn enqueue(&mut self, message: Arc<Message>) -> Result<usize, String> {
        match RunQueue::find_next_node(&mut self.buffer, &mut self.enqueue_pos) {
            None => Err("Mailbox Full".to_string()),
            Some(node) => {
                node.message = Some(message.clone());
                node.lap.fetch_add(1, Ordering::Relaxed);
                let old_pending = self.pending.fetch_add(1, Ordering::Relaxed);
                self.enqueued.fetch_add(1, Ordering::Relaxed);
                // Manually yield after the enqueue.
                thread::yield_now();
                Ok(old_pending + 1)
            }
        }
    }

    /// Dequeue the result, passing it to the provided function that will handle the
    /// message and then return the status off the message after being handled. The first
    /// parameter is this mailbox, the next parameter is a type T and a function that takes
    /// a type T and a message and returns a [`DequeueResult`].
    pub fn dequeue<T, F: FnMut(&mut T, Arc<Message>) -> DequeueResult>(
        &mut self,
        t: &mut T,
        mut f: F,
    ) -> Result<usize, String> {
        loop {
            match RunQueue::find_next_node(&mut self.buffer, &mut self.dequeue_pos) {
                None => return Err("Mailbox Empty".to_string()), // fixme turn into enums
                Some(node) => {
                    // If the message is a None, it might have been dequeued by a cursor so we
                    // just loop and try to get the next node, otherwise we fetch the message
                    // for processing.
                    if let Some(ref mut message) = node.message {
                        // FIXME implement message skipping based upon the processing function.
                        let _result = f(t, message.clone());
                        self.dequeued.fetch_add(1, Ordering::Relaxed);
                        let old_pending = self.pending.fetch_sub(1, Ordering::Relaxed);
                        // Manually yield after the dequeue.
                        thread::yield_now();
                        return Ok(old_pending - 1);
                    }
                }
            }
        }
    }
}

// --------------------- Test Cases ---------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// An enum used for testing message passing.
    enum TestMsg {
        MsgOne,
    }

    fn assert_counters(
        mailbox: &RunQueue,
        pending: usize,
        enqueued: usize,
        dequeued: usize,
        skipped: usize,
    ) {
        assert_eq!(pending, mailbox.pending());
        assert_eq!(enqueued, mailbox.enqueued());
        assert_eq!(dequeued, mailbox.dequeued());
        assert_eq!(skipped, mailbox.skipped());
    }

    #[test]
    fn test_basic_operations() {
        let mut mailbox = RunQueue::new(3);
        assert_eq!(3, mailbox.buffer.len());
        assert_counters(&mailbox, 0, 0, 0, 0);

        let m1 = Arc::new(10 as u32);
        let result = mailbox.enqueue(m1.clone());
        assert_eq!(Ok(1), result);
        assert_counters(&mailbox, 1, 1, 0, 0);
        assert_eq!(0, mailbox.skipped());

        let m2 = Arc::new("hello".to_string());
        let result = mailbox.enqueue(m2.clone());
        assert_eq!(Ok(2), result);
        assert_counters(&mailbox, 2, 2, 0, 0);

        let m3 = Arc::new(TestMsg::MsgOne);
        let result = mailbox.enqueue(m3.clone());
        assert_eq!(Ok(3), result);
        assert_counters(&mailbox, 3, 3, 0, 0);
    }
}
