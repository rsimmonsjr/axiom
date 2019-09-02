//! An implementation of the Chandy & Misra solution to the classic finite state machine (FSM)
//! concurrency problem known as [Dining Philosophers]
//! (https://en.wikipedia.org/wiki/Dining_philosophers_problem) problem using Axiom.
//!
//! # Demonstrated in this Example:
//!   * Basic usage of Actors to solve a classic problem in concurrency.
//!   * Communication with Enumeration based messages.
//!   * Skipping messages in the channel to defer processing.
//!   * Implementation of finite state machine semantics with differential processing.
//!   * Concurrent, Independent processing.
//!   * Ability to send messages to self; stopping eating and getting hungry send to self.
//!   * Ability to send messages after a specified time frame. (FIXME IMPLEMENT!)
//!  
//! This example is extremely strict. If the FSM at any time gets out of synch with expectations
//! panics ensue. Some FSM implementations might be quite a bit more lose, preferring to ignore
//! badly timeds messages. This is largely up to the user.
//!
//! FIXME There should be some way to abort the whole program other than panicing one actor.

use axiom::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// A command sent to a fork actor.
#[derive(Debug, Serialize, Deserialize)]
pub enum ForkCommand {
    /// A command sent when a fork is requested.
    RequestFork(ActorId),
    /// Mark the fork being used which will mark it dirty.
    UsingFork(ActorId),
    /// Sent to a fork to indicate that it was put down and no longer is in use. This will
    /// allow the fork to be sent to the next user. The `ActorId` is the id of the current
    /// holder of the fork.
    ForkPutDown(ActorId),
}

/// A fork for use in the problem. This represents a single resource that other resources need
/// in order to solve problems in a distributed sytem.
#[derive(Eq, PartialEq)]
struct Fork {
    /// A flag to indicate if the fork is clean or not.
    clean: bool,
    /// The actor that owns the fork.
    owned_by: Option<ActorId>,
}

impl Fork {
    /// Creates a new fork structure, defaulting `clean` to false as per the Chandy problem
    /// solution to 'Dining Philosophers'.
    fn new() -> Fork {
        Fork {
            clean: false,
            owned_by: None,
        }
    }

    /// Request that a fork be sent to a philosopher.
    fn fork_requested(&mut self, context: &Context, requester: &ActorId) -> Status {
        match &self.owned_by {
            Some(owner) => {
                if self.clean {
                    Status::Skipped
                } else {
                    owner.send_new(Command::GiveUpFork(context.aid.clone()));
                    Status::Skipped
                }
            }
            None => {
                self.owned_by = Some(requester.clone());
                requester.send_new(Command::ReceiveFork(context.aid.clone()));
                Status::Processed
            }
        }
    }

    /// The philosopher that is the current owner of the fork has put it down, making it available
    /// for other philosophers to pick up.
    fn fork_put_down(&mut self, context: &Context, sender: &ActorId) -> Status {
        match &self.owned_by {
            Some(owner) => {
                if owner == sender {
                    self.owned_by = None;
                    // Resetting the skip allows fork requests to be processed.
                    Status::ResetSkip
                } else {
                    println!(
                        "[{}] Got PutDownFork from non-owner: {}",
                        context.aid, sender
                    );
                    Status::Processed
                }
            }
            None => {
                println!(
                    "[{}] Got PutDownFork from non-owner: {}",
                    context.aid, sender
                );
                Status::Processed
            }
        }
    }

    /// The owner of the fork is notifying the fork that they are going to use the fork. This
    /// will mark the fork as dirty and make it available to be sent to another philosopher if
    /// they request the fork.
    fn using_fork(&mut self, context: &Context, sender: &ActorId) -> Status {
        match self.owned_by {
            Some(ref owner) => {
                if *owner == *sender {
                    self.clean = false;
                    // Resetting the skip allows fork requests to be processed now that the fork
                    // has been marked as being dirty.
                    Status::ResetSkip
                } else {
                    println!("[{}] Got UsingFork from non-owner: {}", context.aid, sender);
                    Status::Processed
                }
            }
            _ => {
                println!("[{}] Got UsingFork from non-owner: {}", context.aid, sender);
                Status::Processed
            }
        }
    }

    /// Handles actor messages, downcasting them to the proper types and then sends the messages
    /// to the other functions to handle the details.
    pub fn handle(&mut self, context: &Context, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<ForkCommand>() {
            match &*msg {
                ForkCommand::RequestFork(requester) => self.fork_requested(context, &requester),
                ForkCommand::UsingFork(owner) => self.using_fork(context, &owner),
                ForkCommand::ForkPutDown(owner) => self.fork_put_down(context, &owner),
            }
        } else {
            Status::Processed
        }
    }
}

/// The state of a philosopher at the table.
enum PhilosopherState {
    /// Has no forks and is thinking.
    Thinking,
    /// Philosopher is hungry and waiting for forks.
    Hungry,
    /// Has both forks and is eating.
    Eating,
}

/// A command sent to an actor.
#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    /// Command to start eating.
    StopEating,
    /// Command to stop eating.
    BecomeHungry,
    /// Instructs an actor to receive give up a fork with the given `aid`.
    GiveUpFork(ActorId),
    /// Instructs an actor to receive a fork.
    ReceiveFork(ActorId),
}

/// The structure holding the data for the philosopher. This will be used to make an actor
/// from the structure itself.
struct Philosopher {
    /// The current state that the philosopher is in.
    state: PhilosopherState,
    /// The `ActorId` of the philosopher's left fork.
    left_fork_aid: ActorId,
    /// Whether the philosopher has the left fork.
    has_left_fork: bool,
    /// The `ActorId` of the philosopher's right fork.
    right_fork_aid: ActorId,
    /// Whether the philosopher has the right fork.
    has_right_fork: bool,
    /// The last time the philosopher's state changed. This is used for tracking time eating, etc.
    last_state_change: Instant,
    /// The time that the Philosopher spent thinking.
    time_thinking: Duration,
    /// The time that the Philosopher spent hungry.
    time_hungry: Duration,
    /// The time that the Philosopher spent eating.
    time_eating: Duration,
}

impl Philosopher {
    /// Creates a new dining philosopher that starts hungry by default. The passed fork aids
    /// are used to request forks for eating.
    pub fn new(left_fork_aid: ActorId, right_fork_aid: ActorId) -> Philosopher {
        Philosopher {
            state: PhilosopherState::Hungry,
            left_fork_aid,
            has_left_fork: false,
            right_fork_aid,
            has_right_fork: false,
            last_state_change: Instant::now(),
            time_thinking: Duration::from_micros(0),
            time_hungry: Duration::from_micros(0),
            time_eating: Duration::from_micros(0),
        }
    }

    /// The philosopher received a fork. Once they have both forks they can start eating.
    /// Otherwise they have to wait for the other fork to begin eating.
    fn fork_received(&mut self, context: &Context, fork_aid: &ActorId) -> Status {
        match &self.state {
            PhilosopherState::Hungry => {
                if self.left_fork_aid == *fork_aid {
                    println!("[{}] Got Left Fork {:?}", context.aid, fork_aid);
                    self.has_left_fork = true;
                } else if self.left_fork_aid == *fork_aid {
                    println!("[{}] Got Left Fork {:?}", context.aid, fork_aid);
                    self.has_right_fork = true;
                } else {
                    println!("[{}] Unknown Fork Received: {}", context.aid, *fork_aid);
                }
                // If we have both forks then we can start eating.
                if self.has_left_fork && self.has_right_fork {
                    self.time_hungry += Instant::elapsed(&self.last_state_change);
                    self.last_state_change = Instant::now();
                    self.state = PhilosopherState::Eating;
                    // Now that we are eating we will tell the fork that we are using it,
                    // thus marking the fork as dirty.
                    self.left_fork_aid
                        .send_new(ForkCommand::UsingFork(context.aid.clone()));
                    self.right_fork_aid
                        .send_new(ForkCommand::UsingFork(context.aid.clone()));
                    Status::ResetSkip
                } else {
                    Status::Processed
                }
            }
            PhilosopherState::Thinking => {
                panic!("[{}] Got Fork while thinking! {:?}", context.aid, fork_aid);
            }
            PhilosopherState::Eating => {
                panic!("[{}] Got Fork while eating! {:?}", context.aid, fork_aid);
            }
        }
    }

    /// The philosopher is being instructed to get hungry which will cause them to ask for the
    /// forks to eat. Note that the philosopher will have to have both forks to start eating.
    /// Once a philosopher starts eating, the forks will be sent a message indicating they are
    /// being used.
    fn become_hungry(&mut self, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Thinking => {
                self.time_thinking += Instant::elapsed(&self.last_state_change);
                self.last_state_change = Instant::now();
                self.state = PhilosopherState::Hungry;
                // Now that we are hungry we need both forks if we don't already have them.
                // or just start eating if we have both.
                match (self.has_left_fork, self.has_right_fork) {
                    (false, false) => {
                        self.left_fork_aid
                            .send_new(ForkCommand::RequestFork(context.aid.clone()));
                        self.right_fork_aid
                            .send_new(ForkCommand::RequestFork(context.aid.clone()));
                        self.state = PhilosopherState::Hungry
                    }
                    (true, false) => {
                        self.right_fork_aid
                            .send_new(ForkCommand::RequestFork(context.aid.clone()));
                        self.state = PhilosopherState::Hungry
                    }
                    (false, true) => {
                        self.left_fork_aid
                            .send_new(ForkCommand::RequestFork(context.aid.clone()));
                        self.state = PhilosopherState::Hungry
                    }
                    _ => {
                        self.state = PhilosopherState::Eating;
                        // Now that we are eating we will tell the fork that we are using it,
                        // thus marking the fork as dirty.
                        self.left_fork_aid
                            .send_new(ForkCommand::UsingFork(context.aid.clone()));
                        self.right_fork_aid
                            .send_new(ForkCommand::UsingFork(context.aid.clone()));
                    }
                }
                Status::Processed
            }
            PhilosopherState::Hungry => {
                panic!("[{}] Got BecomeHungry while eating!", context.aid);
            }
            PhilosopherState::Eating => {
                panic!("[{}] Got BecomeHungry while eating!", context.aid);
            }
        }
    }

    /// Instructs a philosopher to stop eating and put down both forks.
    fn stop_eating(&mut self, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Eating => {
                self.state = PhilosopherState::Thinking;
                self.time_eating += Instant::elapsed(&self.last_state_change);
                self.last_state_change = Instant::now();
                self.left_fork_aid
                    .send_new(ForkCommand::ForkPutDown(context.aid.clone()));
                self.has_left_fork = false;
                self.right_fork_aid
                    .send_new(ForkCommand::ForkPutDown(context.aid.clone()));
                self.has_right_fork = false;
                Status::Processed
            }
            PhilosopherState::Thinking => {
                println!("[{}] Got StopEating while thinking!", context.aid);
                Status::Processed
            }
            PhilosopherState::Hungry => {
                println!("[{}] Got StopEating while eating!", context.aid);
                Status::Processed
            }
        }
    }

    /// Instructs a philosopher to give up a fork.
    fn give_up_fork(&mut self, context: &Context, fork_aid: &ActorId) -> Status {
        match &self.state {
            PhilosopherState::Eating => {
                self.state = PhilosopherState::Thinking;
                self.time_eating += Instant::elapsed(&self.last_state_change);
                self.last_state_change = Instant::now();

                if self.left_fork_aid == *fork_aid {
                    self.has_left_fork = false;
                } else if self.left_fork_aid == *fork_aid {
                    self.has_right_fork = false;
                } else {
                    println!("[{}] Unknown Fork Received: {}", context.aid, *fork_aid);
                }

                // Tell the fork that we put it down.
                fork_aid.send_new(ForkCommand::ForkPutDown(context.aid.clone()));
                Status::Processed
            }
            PhilosopherState::Hungry => {
                // All of our forks should be clean, we didn't get a chance to even eat
                // so we just skip the message.
                Status::Skipped
            }
            PhilosopherState::Thinking => {
                panic!("[{}] Got Fork while thinking! {:?}", context.aid, *fork_aid);
            }
        }
    }

    /// Handle a message for a dining philosopher, mostly dispatching to another method to
    /// manage the details of handling the message. The only exception being the `Start`
    /// system message which is handled inline.
    pub fn handle(&mut self, context: &Context, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<Command>() {
            match &*msg {
                Command::StopEating => self.stop_eating(context),
                Command::BecomeHungry => self.become_hungry(context),
                Command::ReceiveFork(fork_aid) => self.fork_received(context, fork_aid),
                Command::GiveUpFork(fork_aid) => self.give_up_fork(context, fork_aid),
            }
        } else if let Some(msg) = message.content_as::<SystemMsg>() {
            match &*msg {
                // Note that we generally want to make handling this message last as we know that
                // this message will be recieved only once so we want to put the most used
                // message types first.
                SystemMsg::Start => {
                    self.left_fork_aid
                        .send_new(ForkCommand::RequestFork(context.aid.clone()));
                    Status::Processed
                }
                _ => Status::Processed,
            }
        } else {
            Status::Processed
        }
    }
}

/// Main method of the dining philosophers problem. This sets up the solution and starts the
/// actors.
pub fn main() {
    // First we initialize the actor system using the default config.
    let config = ActorSystemConfig::default();
    let system = ActorSystem::create(config);

    // Spawn the fork resources clockwise from top of table.
    let fork1 = system
        .spawn_named("Fork1", Fork::new(), Fork::handle)
        .unwrap();
    let fork2 = system
        .spawn_named("Fork2", Fork::new(), Fork::handle)
        .unwrap();
    let fork3 = system
        .spawn_named("Fork3", Fork::new(), Fork::handle)
        .unwrap();
    let fork4 = system
        .spawn_named("Fork4", Fork::new(), Fork::handle)
        .unwrap();
    let fork5 = system
        .spawn_named("Fork5", Fork::new(), Fork::handle)
        .unwrap();

    // Spawn the philosopher actors clockwise from top of table.
    let _philosopher1 = system
        .spawn_named(
            "Confucius",
            Philosopher::new(fork1.clone(), fork5.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher2 = system
        .spawn_named(
            "Laozi",
            Philosopher::new(fork2.clone(), fork1.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher3 = system
        .spawn_named(
            "Descartes",
            Philosopher::new(fork3.clone(), fork2.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher4 = system
        .spawn_named(
            "Ben Franklin",
            Philosopher::new(fork4.clone(), fork3.clone()),
            Philosopher::handle,
        )
        .unwrap();
    let _philosopher5 = system
        .spawn_named(
            "Thomas Jefferson",
            Philosopher::new(fork5.clone(), fork4.clone()),
            Philosopher::handle,
        )
        .unwrap();

    system.await_shutdown();
}
