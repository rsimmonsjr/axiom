//! An implementation of the classic finite state machine (FSM) [Dining Philosophers]
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
//! FIXME There should be some way to abort the whole program other than panicing one actor.

use axiom::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// A fork for use in the problem.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Fork {
    /// A unique UUID for the fork.
    uuid: Uuid,
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

/// A command sent to a philosopher.
#[derive(Serialize, Deserialize)]
pub enum Command {
    /// Sets the neighbors for a philosopher; the provided values are left and right neighbors
    /// in that order.
    SetNeighbors(ActorId, ActorId),
    /// Command to start eating.
    StopEating,
    /// Command to stop eating.
    BecomeHungry,
    /// Request a fork from a neighbor.
    RequestFork(ActorId),
    /// A fork was received from a philosopher.
    ForkReceived { from: ActorId, fork: Fork },
}

/// The structure holding the data for the philosopher. This will be used to make an actor
/// from the structure itself.
struct Philosopher {
    /// The current state that the philosopher is in.
    state: PhilosopherState,
    /// The uuid of the philosopher to the right.
    right_neighbor: Option<ActorId>,
    /// The uuid of the philosopher to the left.
    left_neighbor: Option<ActorId>,
    /// The left fork holder. This will be a `Some` if the philosopher holds the fork with the
    /// first element in the tuple being the fork and the second element being a boolean
    /// indicating if the fork is clean (`true`) or not (`false`).
    left_fork: Option<(Fork, bool)>,
    /// The right fork holder. This is exactly like the left fork holder.
    right_fork: Option<(Fork, bool)>,
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
    pub fn new(left_fork: Option<Fork>, right_fork: Option<Fork>) -> Philosopher {
        Philosopher {
            state: PhilosopherState::Thinking,
            left_neighbor: None,
            right_neighbor: None,
            // Note that all forks start dirty as per problem solution.
            left_fork: left_fork.map(|f| (f, false)),
            right_fork: right_fork.map(|f| (f, false)),
            last_state_change: Instant::now(),
            time_thinking: Duration::from_micros(0),
            time_hungry: Duration::from_micros(0),
            time_eating: Duration::from_micros(0),
        }
    }

    /// Helper to find a fork option based on the requester.
    fn find_fork_for_aid(
        &mut self,
        context: &Context,
        requester: &ActorId,
    ) -> &mut Option<(Fork, bool)> {
        // Determine which fork was requested.
        if requester == self.left_neighbor.as_ref().unwrap() {
            &mut self.left_fork
        } else if requester == self.right_neighbor.as_ref().unwrap() {
            &mut self.right_fork
        } else {
            // This shouldn't happen if the problem is initialized corretly.
            panic!("[{}] Illegal requester: {}", context.aid, requester);
        }
    }

    /// Helper to clean a dirty fork and then send a fork to a requesting party. If the fork is
    /// sent then this function will return a `Processed` status but if the fork is not sent
    /// because it is already clean then it will return `Skipped`.
    fn clean_and_send_fork(&mut self, context: &Context, requester: &ActorId) -> Status {
        // Determine which fork was requested.
        let requested_fork = self.find_fork_for_aid(context, &requester);
        match requested_fork {
            fork @ Some((_, false)) => {
                let (fork, _) = fork.take().unwrap();
                requester.send_new(Command::ForkReceived {
                    from: context.aid.clone(),
                    fork,
                });
                Status::Processed
            }
            Some((_, true)) => Status::Skipped,
            None => Status::Skipped,
        }
    }

    /// Handles a request for a fork. This request will be sent by the philosopher's neighbors
    /// and will result in either the fork being sent or the request being skipped to be
    /// processed later if the requested fork is clean.
    fn fork_requested(&mut self, context: &Context, requester: &ActorId) -> Status {
        match &self.state {
            PhilosopherState::Thinking => self.clean_and_send_fork(context, requester),
            PhilosopherState::Hungry => self.clean_and_send_fork(context, requester),
            PhilosopherState::Eating => {
                // If eating all forks must be dirty.
                self.state = PhilosopherState::Thinking;
                self.clean_and_send_fork(context, requester)
            }
        }
    }

    /// The philosopher received a fork.
    fn fork_received(&mut self, context: &Context, fork: &Fork, from: &ActorId) -> Status {
        match &self.state {
            PhilosopherState::Hungry => {
                // Forks are always cleaned before being sent.
                let received_fork = self.find_fork_for_aid(context, &from);
                *received_fork = Some((fork, true));
                // If the philosopher has both forks he starts eating and marks them dirty.
                if let (Some((_, left_clean)), Some((_, right_clean))) =
                    (self.left_fork.as_mut(), self.right_fork.as_mut())
                {
                    self.state = PhilosopherState::Eating;
                    *left_clean = false;
                    *right_clean = false;
                };
                Status::Processed
            }
            PhilosopherState::Thinking => {
                panic!("[{}] Got Fork while thinking! {:?}", context.aid, fork);
            }
            PhilosopherState::Eating => {
                panic!("[{}] Got Fork while eating! {:?}", context.aid, fork);
            }
        }
    }

    /// Received when a philosopher is supposed to become hungry .
    fn become_hungry(&mut self, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Thinking => {
                self.state = PhilosopherState::Hungry;
                // Now that we are hungry we need both forks if we dont already have them.
                // or just start eating if we have both.
                match (self.left_fork.as_ref(), self.right_fork.as_ref()) {
                    (None, None) => {
                        self.left_neighbor
                            .as_ref()
                            .unwrap()
                            .send_new(Command::RequestFork(context.aid.clone()));
                        self.right_neighbor
                            .as_ref()
                            .unwrap()
                            .send_new(Command::RequestFork(context.aid.clone()));
                    }
                    (Some(_), None) => {
                        self.right_neighbor
                            .as_ref()
                            .unwrap()
                            .send_new(Command::RequestFork(context.aid.clone()));
                    }
                    (None, Some(_)) => {
                        self.left_neighbor
                            .as_ref()
                            .unwrap()
                            .send_new(Command::RequestFork(context.aid.clone()));
                    }
                    _ => self.state = PhilosopherState::Eating,
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

    fn stop_eating(&mut self, context: &Context) -> Status {
        match &self.state {
            PhilosopherState::Thinking => {
                println!("[{}] Got StopEating while thinking!", context.aid);
                Status::Processed
            }
            PhilosopherState::Hungry => {
                println!("[{}] Got StopEating while eating!", context.aid);
                Status::Processed
            }
            PhilosopherState::Eating => {
                self.state = PhilosopherState::Thinking;
                Status::Processed
            }
        }
    }

    pub fn handle(&mut self, context: &Context, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<Command>() {
            match &*msg {
                Command::StopEating => self.stop_eating(context),
                Command::BecomeHungry => self.become_hungry(context),
                Command::RequestFork(ref requester) => self.fork_requested(context, requester),
                Command::ForkReceived { from, fork } => self.fork_received(context, fork, from),
                Command::SetNeighbors(left, right) => {
                    self.left_neighbor = Some(left.clone());
                    self.right_neighbor = Some(right.clone());
                    Status::Processed
                }
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

    // Spawn the fork resources clockwise from top of table.
    let fork1 = Fork {
        uuid: Uuid::new_v4(),
    };
    let fork2 = Fork {
        uuid: Uuid::new_v4(),
    };
    let fork3 = Fork {
        uuid: Uuid::new_v4(),
    };
    let fork4 = Fork {
        uuid: Uuid::new_v4(),
    };
    let fork5 = Fork {
        uuid: Uuid::new_v4(),
    };

    // Spawn the philosopher actors clockwise from top of table.
    let philosopher1 = system
        .spawn_named(
            "Confucius",
            Philosopher::new(None, Some(fork1)),
            Philosopher::handle,
        )
        .unwrap();
    let philosopher2 = system
        .spawn_named(
            "Laozi",
            Philosopher::new(None, Some(fork2)),
            Philosopher::handle,
        )
        .unwrap();
    let philosopher3 = system
        .spawn_named(
            "Descartes",
            Philosopher::new(None, Some(fork3)),
            Philosopher::handle,
        )
        .unwrap();
    let philosopher4 = system
        .spawn_named(
            "Ben Franklin",
            Philosopher::new(None, Some(fork4)),
            Philosopher::handle,
        )
        .unwrap();
    let philosopher5 = system
        .spawn_named(
            "Thomas Jefferson",
            Philosopher::new(None, Some(fork5)),
            Philosopher::handle,
        )
        .unwrap();

    // Set up the neighbors for the actors.
    philosopher1.send_new(Command::SetNeighbors(
        philosopher5.clone(),
        philosopher2.clone(),
    ));
    philosopher2.send_new(Command::SetNeighbors(
        philosopher1.clone(),
        philosopher3.clone(),
    ));
    philosopher3.send_new(Command::SetNeighbors(
        philosopher2.clone(),
        philosopher4.clone(),
    ));
    philosopher4.send_new(Command::SetNeighbors(
        philosopher3.clone(),
        philosopher5.clone(),
    ));
    philosopher5.send_new(Command::SetNeighbors(
        philosopher4.clone(),
        philosopher1.clone(),
    ));

    // Have to do this since we want to call from outside actor system.
    system.init_current();

    // Spawn the actor and send the message.
    // let aid = system.spawn(true, hello);
    // aid.send(Message::new(HelloMessages::Greet));

    // The actor will trigger shutdown, we just wait for it.
    // system.await_shutdown();
}
