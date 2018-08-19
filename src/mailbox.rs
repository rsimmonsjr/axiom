//! Implements a mailbox with specific skip semantics allowing the mailbox to skip messages
//! without removing them from the mailbox. If a skip operation occurs then the mailbox is
//! incremented to read the next message until the skip operation is cleared and processing
//! resumes normally. A mailbox in this case will react to the result of the dequeue request
//! before decding how to handle the message.
//!
//! Credits:
//! Based on a modified version of (Go channels on steroids)
//! [https://docs.google.com/document/d/1yIAYmbvL3JxOKOjuCyon7JhW4cSv1wy5hC0ApeGMV9s/pub

use std::any::Any;
use std::marker::{Send, Sync};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// This is a type used by the system for sending a message through a channel to the actor.
/// All messages are sent as this type and it is up to the message handler to cast the message
/// properly and deal with it. It is recommended that the user make use of the [`dispatch`]
/// utility function to help in the casting and calling operation.
pub type Message = dyn Any + Sync + Send;

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

pub struct Mailbox {
    capacity: HalfUsize,
    /// The total number of messages that have been sent to this mailbox.
    pending: AtomicUsize,
    /// The total number of messages that have been sent to this mailbox (enqueued).
    enqueued: AtomicUsize,
    /// The total number of messages that have been received from this mailbox (dequeued)
    dequeued: AtomicUsize,
    /// Position at which to enqueue the message.
    enqueue_pos: AtomicUsize,
    /// Position at which to dequeue the message.
    dequeue_pos: AtomicUsize,
    /// Position at which to dequeue the message when the mailbox is skipping.
    skip_pos: AtomicUsize,
    /// Whether or not the mailbox is skipping messages.
    is_skipping: bool,
    data: Vec<Node>,
}

impl Mailbox {
    pub fn new(capacity: HalfUsize) -> Mailbox {
        Mailbox {
            capacity,
            pending: AtomicUsize::new(0),
            enqueued: AtomicUsize::new(0),
            dequeued: AtomicUsize::new(0),
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
            skip_pos: AtomicUsize::new(0),
            is_skipping: false,
            data: Vec::with_capacity(capacity as usize),
        }
        // FIXME allocate the nodes in the buffer
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

    pub fn enqueue(&mut self, message: Arc<Message>) -> Result<(), String> {
        loop { // We will iterate until we succeed.
            let pos = self.enqueue_pos.load(Ordering::Relaxed);
            let idx = pos as HalfUsize;  // Index in the buffer is the lower half bits
            let lap = (pos >> HALF_USIZE_BITS) as HalfUsize; // Lap is upper half bits.
            let node = &mut self.data[idx as usize]; // Fetches the proposed storage node.
            let node_lap = node.lap.load(Ordering::Relaxed) as HalfUsize;
            if lap == node_lap { // Element is ready for writing on this lap.
                let new_pos = if idx + 1 < self.capacity {
                    pos + 1
                } else { // Reached the end of the buffer so loop and add 1 to the lap.
                    ((lap + 1) << HALF_USIZE_BITS) as usize
                };
                // If this atomic swap works then we have access to the node and can write to
                // the node without fear that other threads will do the same.
                if pos == self.enqueue_pos.compare_and_swap(pos, new_pos, Ordering::Relaxed) {
                    node.message = Some(message.clone());
                    node.lap.fetch_add(1, Ordering::Relaxed);
                    self.pending.fetch_add(1, Ordering::Relaxed);
                    self.enqueued.fetch_add(1, Ordering::Relaxed);
                    // Manually yield after the enqueue.
                    thread::yield_now();
                    return Ok(());
                }
            } else if lap - node_lap > 0 {
                // This will only happen if the mailbox is full.
                return Err("Mailbox Full".to_string());
            }
            // We lost the data race and the element has been written already. Try again.
        }
    }
}