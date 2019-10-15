use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::ops::Add;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::{Future, Stream};
use futures::task::{ArcWake, Waker};
use uuid::Uuid;

use crate::{ActorSystemConfig, AxiomResult, Status};
use crate::actors::Actor;

pub mod executor;
pub mod schedulers;

/// This trait defines the behaviour of being able to track the timing of Actors
/// actions, and being able to determine how long to let it run and how many messages
/// to let it process, at a time. The actual enforcement is up to the produced
/// Schedules.
pub(crate) trait Scheduler {
    fn new(config: &ActorSystemConfig) -> Self;

    fn log_start(&self, id: Uuid);
    fn log_stop(&self, id: Uuid);
    fn log_wake(&self, id: Uuid);
    fn log_sleep(&self, id: Uuid);
    fn log_poll(&self, id: Uuid);
    fn log_pending_message(&self, id: Uuid);
    fn log_stop_message(&self, id: Uuid);

    fn stop_actor(&self, id: Uuid);

    fn get_schedule_data(&self, id: Uuid) -> ScheduleData;
}

pub(crate) struct ScheduleData {
    reassigned: bool,
    stopped: bool,
    message_limit: Option<u64>,
    reactor_id: u16,
    timeslice: Option<Duration>,
}

pub(crate) struct Schedule {
    id: Uuid,
    scheduler: Arc<dyn Scheduler>,
    schedule_data: ScheduleData,

    // This is data specific to the current stretch of the Actor
    // This data should be reset on before each stretch.
    messages_left: Option<u64>,
    yield_at: Option<Instant>,
    current_reactor: u16,
}

impl Schedule {
    fn new(id: Uuid, scheduler: Arc<dyn Scheduler>) -> Self {
        Self {
            id,
            scheduler,
            schedule_data: scheduler.get_schedule_data(id),
            messages_left: None,
            yield_at: None,
            current_reactor: 0,
        }
    }

    fn log_start(&self) { self.scheduler.log_start(self.id) }
    fn log_stop(&self) { self.scheduler.log_stop(self.id) }
    fn log_wake(&self) { self.scheduler.log_wake(self.id) }
    fn log_sleep(&self) { self.scheduler.log_sleep(self.id) }
    fn log_poll_actor(&self) { self.scheduler.log_poll(self.id) }
    fn log_pending_message(&self) { self.scheduler.log_pending_message(self.id) }
    fn log_stop_message(&self) { self.scheduler.log_stop_message(self.id) }

    fn get_reactor(&self) -> u16 { self.current_reactor }

    /// This function needs to be ran every time the Actor returns Ready(Some)
    /// When ran, it decrements how many messages it has left to handle, then
    /// performing checks. If messages_left is 0 or yield_at has passed, then
    /// this will return Next. If the Scheduler has changed which Reactor the
    /// Actor is assigned to, then this will return Reassigned(new_reactor_id).
    /// Otherwise, this will return Continue.
    pub(crate) fn check_schedule(&mut self) -> ScheduleResult {
        self.update();

        if self.schedule_data.stopped {
            return ScheduleResult::Stopped
        }

        if let Some(msgs_left) = &mut self.messages_left {
            if *msgs_left < 0 {
                msgs_left -= 1;
            }

            if msgs_left == 0 {
                return ScheduleResult::Next
            }
        }

        if let Some(yield_at) = self.yield_at {
            if Instant::now() >= yield_at {
                return ScheduleResult::Next
            }
        }

        if self.current_reactor != self.schedule_data.reactor_id {
            ScheduleResult::Reassigned(self.schedule_data.reactor_id)
        } else { ScheduleResult::Continue }
    }

    pub(crate) fn update(&mut self) {
        self.schedule_data = self.scheduler.get_schedule_data(self.id);
    }

    pub(crate) fn reset_stretch_data(&mut self) {
        self.current_reactor = self.schedule_data.reactor_id;
        self.messages_left = self.schedule_data.message_limit;
        self.yield_at = self.schedule_data.timeslice.map(|t| Instant::now() + t);
    }
}

enum ScheduleResult {
    /// The Actor is still scheduled.
    Continue,
    /// The Actor needs to yield to the next Actor.
    Next,
    /// The Actor has been re-assigned.
    Reassigned(u16),
    /// The Actor has been stopped.
    Stopped,
}
