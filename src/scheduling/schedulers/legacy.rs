use std::time::{Duration, Instant};

use dashmap::{DashMap, DashMapRefMut};
use uuid::Uuid;

use crate::ActorSystemConfig;
use crate::scheduling::{ScheduleData, Scheduler};

/// This scheduler is the old scheduling rules using the new system. It could
/// very well be very bad with the async refactor, but for now, we're preserving
/// it.
#[derive(Default)]
pub struct LegacyScheduling {
    system_config: ActorSystemConfig,
    actors_per_reactor: DashMap<u16, u32>,
    actor_trackers: DashMap<Uuid, RawTrackingData>,
}

#[derive(Clone)]
struct RawTrackingData {
    stopping: bool,
    reassigning: bool,
    reactor_id: u16,
    current_start_time: Option<Instant>,
    total_run_time: Duration,
    total_messages_processed: u64,
}

impl Scheduler for LegacyScheduling {
    fn new(config: &ActorSystemConfig) -> Self {
        let actors_per_reactor = DashMap::default();
        for i in 0..config.thread_pool_size {
            actors_per_reactor.insert(i, 0);
        }

        Self {
            system_config: config.clone(),
            actors_per_reactor,
            actor_trackers: Default::default(),
        }
    }

    fn log_start(&self, _id: Uuid) {}

    fn log_stop(&self, _id: Uuid) {}

    fn log_wake(&self, _id: Uuid) {}

    fn log_sleep(&self, id: Uuid) {
        let data = self.get_raw_data(&id);
        (*self.actors_per_reactor.get_mut(&data.reactor_id).unwrap()) -= 1;
    }

    /// This is effectively a resume or start of time spent on an Actor,
    /// therefore we want to mark this as the start of a duration to be
    /// added to the current work time.
    fn log_poll(&self, id: Uuid) {
        let mut data = self.get_raw_data(&id);

        data.current_start_time = Some(Instant::now());
    }

    /// This is is called when an Actor has returned pending, therefore
    /// is no longer doing work. This is the end of a stretch, and can
    /// be used to calculate a timespan to be added to the total run time
    fn log_pending_message(&self, id: Uuid) {
        let mut data = self.get_raw_data(&id);

        log_end_stretch_segment(&mut data);
    }

    /// This is is called when an Actor has finished a message, therefore
    /// is no longer doing work. This is the end of a stretch, and can
    /// be used to calculate a timespan to be added to the total run time
    fn log_stop_message(&self, id: Uuid) {
        let mut data = self.get_raw_data(&id);

        data.total_messages_processed += 1;

        log_end_stretch_segment(&mut data);
    }

    fn stop_actor(&self, id: Uuid) {
        let mut data = self.get_raw_data(&id);

        data.stopping = true;
    }

    fn get_schedule_data(&self, id: Uuid) -> ScheduleData {
        let mut data = self.get_raw_data(&id);

        if data.reactor_id == u16::max_value() {
            let mut state = (0u16, u32::max_value());
            for x in self.actors_per_reactor.iter() {
                if x.value() > &state.1 {
                    state = (*x.key(), *x.value());
                }
            }

            data.reactor_id = state.0;
        }

        ScheduleData {
            reassigned: data.reassigning,
            stopped: data.stopping,
            message_limit: None,
            reactor_id: 0,
            timeslice: Some(self.system_config.time_slice),
        }
    }
}

impl LegacyScheduling {
    fn get_raw_data(&self, key: &Uuid) -> DashMapRefMut<Uuid, RawTrackingData> {
        match self.actor_trackers.get_mut(&key) {
            Some(t) => t,
            None => {
                let new = RawTrackingData::default();
                self.actor_trackers.insert(*key, new);
                self.actor_trackers.get_mut(&key).unwrap()
            }
        }
    }
}

fn log_end_stretch_segment(data: &mut RawTrackingData) {
    let end_time = match data.current_start_time {
        Some(at) => at,
        _ => return,
    };

    let time_to_add = Instant::now() - end_time;

    data.total_run_time += time_to_add;
    data.current_start_time = None;
}

impl Default for RawTrackingData {
    fn default() -> Self {
        Self {
            stopping: false,
            reassigning: false,
            reactor_id: u16::max_value(),
            current_start_time: None,
            total_run_time: Duration::new(0, 0),
            total_messages_processed: 0,
        }
    }
}
