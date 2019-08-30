//! An implementation of the classic finite state machine (FSM) [Dining Philosophers]
//! (https://en.wikipedia.org/wiki/Dining_philosophers_problem) problem using Axiom.
use axiom::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

pub struct Data {}

/// The state of a fork.
pub enum ForkState {
    /// The fork is held by a philosopher with the given `ActorId`.
    Held(ActorId),
    /// The fork is available.
    Available,
}

#[derive(Serialize, Deserialize)]
enum ForkCommand {
    /// Request a fork for the given aid.
    Request(ActorId),
    /// Return the fork from the given aid.
    Return(ActorId),
}

/// The fork actor will be handled by a single method. If the fork is held by a philosopher then
/// the actor will skip all messages that request the fork until the philosopher returns the fork.
/// If the fork is available it will grant it to the first philosopher that sends a message
/// requesting the fork.
///
/// Note that this implementation is complete and very strict. If a fork gets out of synch in
/// state then the actor will panic.
///
/// FIXME There should be some way to abort the whole program other than panicing one actor.
pub fn fork(state: &mut ForkState, context: &Context, message: &Message) -> Status {
    if let Some(msg) = message.content_as::<ForkCommand>() {
        match &*msg {
            ForkCommand::Return(philosopher) => match state {
                ForkState::Available => panic!(
                    "{} Return from {} but not owned by anyone",
                    context.aid, philosopher
                ),
                ForkState::Held(owner) => {
                    if owner.uuid() == philosopher.uuid() {
                        *state = ForkState::Available;
                        Status::ResetSkip
                    } else {
                        panic!(
                            "{} Return from {} but owned by {}",
                            context.aid, philosopher, owner
                        )
                    }
                }
            },
            ForkCommand::Request(philosopher) => match state {
                ForkState::Available => {
                    *state = ForkState::Held(philosopher.clone());
                    Status::Processed
                }
                ForkState::Held(_) => Status::Skipped,
            },
        }
    } else {
        // Ignore any other message
        Status::Processed
    }
}

/// The state of a philosopher at the table.
enum PhilosopherState {
    /// Has no forks and is thinking.
    Thinking,
    /// Has both forks and is eating.
    Eating,
    /// Is waiting for both forks and ready to eat.
    WaitingForForks,
    /// Has the left fork but not the right.
    HasLeftFork,
    /// Has the right fork but not the left.
    HasRightFork,
}

#[derive(Serialize, Deserialize)]
pub enum PhilosopherCommand {
    /// Command to start eating.
    StopEating,
    /// Command to stop eating.
    StartEating,
    /// Notifies the philosopher that they got the fork.
    GotFork(ActorId),
}

/// The structure holding the data for the philosopher. This will be used to make an actor
/// from the structure itself.
struct Philosopher {
    state: PhilosopherState,
    left_fork: ActorId,
    right_fork: ActorId,
    last_state_change: Instant,
    time_thinking: Duration,
    time_eating: Duration,
    time_waiting_for_forks: Duration,
    failed_to_eat: u16,
    already_eating: u16,
    already_thinking: u16,
}

impl Philosopher {
    pub fn new(left_fork: ActorId, right_fork: ActorId) -> Philosopher {
        Philosopher {
            state: PhilosopherState::Thinking,
            left_fork,
            right_fork,
            last_state_change: Instant::now(),
            time_thinking: Duration::from_micros(0),
            time_eating: Duration::from_micros(0),
            time_waiting_for_forks: Duration::from_micros(0),
            failed_to_eat: 0,
            already_eating: 0,
            already_thinking: 0,
        }
    }

    /// In this method we only care about 1 message, the others are ignored, so we use
    /// a nested `if let`
    fn start_eating(&mut self, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Thinking => {
                self.left_fork
                    .send_new(ForkCommand::Request(context.aid.clone()));
                self.right_fork
                    .send_new(ForkCommand::Request(context.aid.clone()));
                self.time_thinking += Instant::elapsed(&self.last_state_change);
                self.last_state_change = Instant::now();
                self.last_state_change = Instant::now();
                self.state = PhilosopherState::WaitingForForks;
                Status::Processed
            }
            PhilosopherState::Eating => {
                self.already_eating += 1;
                Status::Processed
            }
            PhilosopherState::WaitingForForks => {
                self.already_eating += 1;
                Status::Processed
            }
            PhilosopherState::HasLeftFork => {
                self.already_eating += 1;
                Status::Processed
            }
            PhilosopherState::HasRightFork => {
                self.already_eating += 1;
                Status::Processed
            }
        }
    }

    fn stop_eating(&mut self, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Eating => {
                self.state = PhilosopherState::Thinking;
                self.left_fork
                    .send_new(ForkCommand::Return(context.aid.clone()));
                self.right_fork
                    .send_new(ForkCommand::Return(context.aid.clone()));
                self.time_eating += Instant::elapsed(&self.last_state_change);
                self.last_state_change = Instant::now();
                Status::Processed
            }
            PhilosopherState::Thinking => {
                self.already_thinking += 1;
                Status::Processed
            }
            PhilosopherState::WaitingForForks => {
                self.failed_to_eat += 1;
                self.state = PhilosopherState::Thinking;
                Status::Processed
            }
            PhilosopherState::HasLeftFork => {
                self.left_fork
                    .send_new(ForkCommand::Return(context.aid.clone()));
                self.failed_to_eat += 1;
                self.state = PhilosopherState::Thinking;
                Status::Processed
            }
            PhilosopherState::HasRightFork => {
                self.right_fork
                    .send_new(ForkCommand::Return(context.aid.clone()));
                self.failed_to_eat += 1;
                self.state = PhilosopherState::Thinking;
                Status::Processed
            }
        }
    }

    fn got_fork(&mut self, fork: &ActorId, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Eating => {
                panic!("{} Already eating when got a new fork!", &context.aid);
            }
            PhilosopherState::Thinking => {
                panic!("{} Got a fork while thinking!", &context.aid);
            }
            PhilosopherState::WaitingForForks => {
                if fork.uuid() == self.left_fork.uuid() {
                    self.state = PhilosopherState::HasLeftFork
                } else if fork.uuid() == self.right_fork.uuid() {
                    self.state = PhilosopherState::HasRightFork
                } else {
                    panic!("{} Got an unknown fork! {}", &context.aid, fork);
                }
                Status::Processed
            }
            PhilosopherState::HasLeftFork => {
                if fork.uuid() == self.left_fork.uuid() {
                    panic!("{} Got a left fork when already owned!", &context.aid);
                } else if fork.uuid() == self.right_fork.uuid() {
                    self.time_waiting_for_forks += Instant::elapsed(&self.last_state_change);
                    self.last_state_change = Instant::now();
                    self.state = PhilosopherState::Eating
                } else {
                    panic!("{} Got an unknown fork! {}", &context.aid, fork);
                }
                Status::Processed
            }
            PhilosopherState::HasRightFork => {
                if fork.uuid() == self.left_fork.uuid() {
                    self.time_waiting_for_forks += Instant::elapsed(&self.last_state_change);
                    self.last_state_change = Instant::now();
                    self.state = PhilosopherState::Eating
                } else if fork.uuid() == self.right_fork.uuid() {
                    panic!("{} Got a right fork when already owned!", &context.aid);
                } else {
                    panic!("{} Got an unknown fork! {}", &context.aid, fork);
                }
                Status::Processed
            }
        }
    }

    pub fn handle(&mut self, context: &Context, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<PhilosopherCommand>() {
            match &*msg {
                PhilosopherCommand::StartEating => self.start_eating(context),
                PhilosopherCommand::StopEating => self.stop_eating(context),
                PhilosopherCommand::GotFork(fork) => self.got_fork(fork, context),
            }
        } else {
            Status::Processed
        }
    }
}

pub fn main() {
    // First we initialize the actor system using the default config.
    let config = ActorSystemConfig::default();
    let system = ActorSystem::create(config);

    // Spawn the fork actors clockwise from top of table.
    let fork1 = system
        .spawn_named("Fork1", ForkState::Available, fork)
        .unwrap();
    let fork2 = system
        .spawn_named("Fork2", ForkState::Available, fork)
        .unwrap();
    let fork3 = system
        .spawn_named("Fork3", ForkState::Available, fork)
        .unwrap();
    let fork4 = system
        .spawn_named("Fork4", ForkState::Available, fork)
        .unwrap();
    let fork5 = system
        .spawn_named("Fork5", ForkState::Available, fork)
        .unwrap();

    // Spawn the philosopher actors clockwise from top of table.
    let _philosopher1 = system
        .spawn_named(
            "Confucius",
            Philosopher::new(fork5.clone(), fork1.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher2 = system
        .spawn_named(
            "Laozi",
            Philosopher::new(fork1.clone(), fork2.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher3 = system
        .spawn_named(
            "Descartes",
            Philosopher::new(fork2.clone(), fork3.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher4 = system
        .spawn_named(
            "Ben Franklin",
            Philosopher::new(fork3.clone(), fork4.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher5 = system
        .spawn_named(
            "Thomas Jefferson",
            Philosopher::new(fork4.clone(), fork5.clone()),
            Philosopher::handle,
        )
        .unwrap();

    // Have to do this since we want to call from outside actor system.
    system.init_current();

    // Spawn the actor and send the message.
    // let aid = system.spawn(true, hello);
    // aid.send(Message::new(HelloMessages::Greet));

    // The actor will trigger shutdown, we just wait for it.
    // system.await_shutdown();
}
