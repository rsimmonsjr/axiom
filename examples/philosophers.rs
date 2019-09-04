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
//!   * Ability to send messages after a specified time frame.
//!   * Ability to create an actor from a closure (the actor collecting metrics and shutting down).
//!   * Ability to inject data into a state of an actor (merics map).
//!   * Ability to send the same message to several targets without copying (requesting metrics).
//!   * Ability to use an enum, `ForkCommand` and `Command` as a message.
//!   * Ability to use a struct, `MetricsReply` and `EndSimulation` as a message.
//!  
//! This example is extremely strict. If the FSM at any time gets out of synch with expectations
//! panics ensue. Some FSM implementations might be quite a bit more lose, preferring to ignore
//! badly timeds messages. This is largely up to the user.
//!
//! FIXME There should be some way to abort the whole program other than panicing one actor.

use axiom::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    /// Instructs a philosopher to send the given actor its metrics.
    SendMetrics(ActorId),
}

// This struct is a message that carries metrics from a philosopher upon request.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MetricsReply {
    aid: ActorId,
    metrics: Metrics,
}

/// A struct that holds metrics for a philosopher.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Metrics {
    /// The time that the Philosopher spent thinking.
    time_thinking: Duration,
    /// The time that the Philosopher spent hungry.
    time_hungry: Duration,
    /// The time that the Philosopher spent eating.
    time_eating: Duration,
}

/// The structure holding the state of the philosopher actor.
struct Philosopher {
    /// The size of the time slice to use. This is used for scheduled state changes.
    time_slice: Duration,
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
    /// The metrics for this actor.
    metrics: Metrics,
}

impl Philosopher {
    /// Creates a new dining philosopher that starts hungry by default. The passed fork aids
    /// are used to request forks for eating.
    pub fn new(
        time_slice: Duration,
        left_fork_aid: ActorId,
        right_fork_aid: ActorId,
    ) -> Philosopher {
        Philosopher {
            time_slice,
            state: PhilosopherState::Thinking,
            left_fork_aid,
            has_left_fork: false,
            right_fork_aid,
            has_right_fork: false,
            last_state_change: Instant::now(),
            metrics: Metrics {
                time_thinking: Duration::from_micros(0),
                time_hungry: Duration::from_micros(0),
                time_eating: Duration::from_micros(0),
            },
        }
    }

    /// Changes the philosopher to a state of eating.
    fn begin_eating(&mut self, context: &Context) {
        println!("[{}] begin_eating()", context.aid);
        self.metrics.time_hungry += Instant::elapsed(&self.last_state_change);
        self.last_state_change = Instant::now();
        self.state = PhilosopherState::Eating;
        // Now that we are eating we will tell the fork that we are using it,
        // thus marking the fork as dirty.
        self.left_fork_aid
            .send_new(ForkCommand::UsingFork(context.aid.clone()));
        self.right_fork_aid
            .send_new(ForkCommand::UsingFork(context.aid.clone()));

        // Schedule to stop eating after an eating time slice elapsed.
        let msg = Message::new(Command::StopEating);
        context.aid.send_after(msg, self.time_slice);
    }

    /// The philosopher received a fork. Once they have both forks they can start eating.
    /// Otherwise they have to wait for the other fork to begin eating.
    fn fork_received(&mut self, context: &Context, fork_aid: &ActorId) -> Status {
        match &self.state {
            PhilosopherState::Hungry => {
                if self.left_fork_aid == *fork_aid {
                    println!("[{}] Got Left Fork {}", context.aid, fork_aid);
                    self.has_left_fork = true;
                } else if self.right_fork_aid == *fork_aid {
                    println!("[{}] Got Right Fork {}", context.aid, fork_aid);
                    self.has_right_fork = true;
                } else {
                    panic!("[{}] Unknown Fork Received: {}", context.aid, *fork_aid);
                }

                // If we have both forks then we can start eating.
                if self.has_left_fork && self.has_right_fork {
                    self.begin_eating(context);
                }
            }
            PhilosopherState::Thinking => {
                println!("[{}] Got Fork while thinking! {}", context.aid, fork_aid);
            }
            PhilosopherState::Eating => {
                println!("[{}] Got Fork while eating! {}", context.aid, fork_aid);
            }
        };
        Status::Processed
    }

    /// The philosopher is being instructed to get hungry which will cause them to ask for the
    /// forks to eat. Note that the philosopher will have to have both forks to start eating.
    /// Once a philosopher starts eating, the forks will be sent a message indicating they are
    /// being used.
    fn become_hungry(&mut self, context: &Context) -> Status {
        println!("[{}] become_hungry()", context.aid);
        match &self.state {
            PhilosopherState::Thinking => {
                self.metrics.time_thinking += Instant::elapsed(&self.last_state_change);
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
            }
            PhilosopherState::Hungry => {
                println!("[{}] Got BecomeHungry while eating!", context.aid);
            }
            PhilosopherState::Eating => {
                println!("[{}] Got BecomeHungry while eating!", context.aid);
            }
        };
        Status::Processed
    }

    /// Changes the philosopher to the state of thinking. Note that this doesn't mean that the
    /// philosopher will put down his forks. He will only do that if requested to.
    fn begin_thinking(&mut self, context: &Context) {
        println!("[{}] begin_thinking()", context.aid);
        self.state = PhilosopherState::Thinking;
        self.metrics.time_eating += Instant::elapsed(&self.last_state_change);
        self.last_state_change = Instant::now();

        let msg = Message::new(Command::BecomeHungry);
        context.aid.send_after(msg, self.time_slice);
    }

    /// Processes a command to stop eating.
    fn stop_eating(&mut self, context: &Context) -> Status {
        println!("[{}] stop_eating()", context.aid);
        match &self.state {
            PhilosopherState::Eating => {
                self.begin_thinking(context);
                Status::Processed
            }
            PhilosopherState::Thinking => {
                println!("[{}] Got StopEating while thinking, ignoring!", context.aid);
                Status::Processed
            }
            PhilosopherState::Hungry => {
                println!("[{}] Got StopEating while hungry, ignoring!", context.aid);
                Status::Processed
            }
        }
    }

    /// Processes a command to a philosopher to give up a fork. Note that this can be received
    /// when the philosopher is in any state since the philosopher will not put down a fork
    /// unless he is asked to. A philosopher can be eating, stop eating and start thinking
    /// and then start eating again if no one asked for his forks.
    fn give_up_fork(&mut self, context: &Context, fork_aid: &ActorId) -> Status {
        if self.left_fork_aid == *fork_aid {
            self.has_left_fork = false;
        } else if self.right_fork_aid == *fork_aid {
            self.has_right_fork = false;
        } else {
            panic!(
                "[{}] Unknown fork asked for: {}:\n left ==>  {}\n right ==> {}",
                context.aid, *fork_aid, self.left_fork_aid, self.right_fork_aid
            );
        }
        println!(
            "[{}] give_up_fork() {:?}",
            context.aid,
            (self.has_left_fork, self.has_right_fork)
        );

        // Tell the fork that we put it down.
        fork_aid.send_new(ForkCommand::ForkPutDown(context.aid.clone()));

        match &self.state {
            PhilosopherState::Thinking => Status::Processed,
            _ => {
                self.begin_thinking(context);
                Status::Processed
            }
        }
    }

    /// A function that handles sending metrics to an actor that requests the metrics.
    fn send_metrics(&mut self, context: &Context, reply_to: &ActorId) -> Status {
        println!("[{}] send_metrics()", context.aid);
        // We copy the metrics becase we want to send immutable data. This call
        // cant move the metrics out of self so it must copy them.
        reply_to.send_new(MetricsReply {
            aid: context.aid.clone(),
            metrics: self.metrics,
        });
        Status::Processed
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
                Command::SendMetrics(reply_to) => self.send_metrics(context, reply_to),
            }
        } else if let Some(msg) = message.content_as::<SystemMsg>() {
            match &*msg {
                // Note that we generally want to make handling this message last as we know that
                // this message will be recieved only once so we want to put the most used
                // message types first.
                SystemMsg::Start => {
                    context.aid.send_new(Command::BecomeHungry);
                    Status::Processed
                }
                _ => Status::Processed,
            }
        } else {
            Status::Processed
        }
    }
}

// This will serve as a signal to shutdown the simulation.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EndSimulation {}

/// Main method of the dining philosophers problem. This sets up the solution and starts the
/// actors.
pub fn main() {
    // FIXME Let the user pass in the number of philosophers at the table, time slice
    // and runtime as command line parameters.
    let count = 3 as usize;
    let time_slice = Duration::from_millis(100);
    let run_time = Duration::from_millis(1000);
    let mut forks: Vec<ActorId> = Vec::with_capacity(count);
    let mut results: HashMap<ActorId, Option<Metrics>> = HashMap::with_capacity(count);

    // Initialize the actor system.
    let config = ActorSystemConfig::default();
    let system = ActorSystem::create(config);

    // Spawn the fork actors clockwise from top of table.
    for i in 0..count {
        let name = format!("Fork{}", i);
        let fork = system
            .spawn_named(&name, Fork::new(), Fork::handle)
            .unwrap();
        forks.push(fork);
    }

    // FIXME Make this list support many more philosophers.
    let names = vec![
        "Confucius",
        "Laozi",
        "Descartes",
        "Ben Franklin",
        "Thomas Jefferson",
    ];

    // Spawn the philosopher actors clockwise from top of table.i
    for left in 0..count {
        let right = if left == 0 { count - 1 } else { left - 1 };
        let state = Philosopher::new(time_slice, forks[left].clone(), forks[right].clone());

        let philosopher = system
            .spawn_named(names[left], state, Philosopher::handle)
            .unwrap();
        results.insert(philosopher, None);
    }

    // This actor is created with a closure and when it gets the timed message it will
    // request metrics of all of the actors and then print the metrics when all collected
    // and shut down the actor system.
    let _shutdown = system.spawn(
        results,
        move |state: &mut HashMap<ActorId, Option<Metrics>>,
              context: &Context,
              message: &Message| {
            if let Some(msg) = message.content_as::<MetricsReply>() {
                println!("[{}] Got MetricsReply ", context.aid);
                state.insert(msg.aid.clone(), Some(msg.metrics));

                // Check to see if we have all of the metrics collected and if so then
                // output the results of the simulation and end the program by shutting
                // down the actor system.
                if !state.iter().any(|(_, metrics)| metrics.is_none()) {
                    println!("Final Metrics:");
                    for (aid, metrics) in state.iter() {
                        println!("{}: {:?}", aid, metrics);
                    }
                    context.system.trigger_shutdown();
                }
            } else if let Some(_) = message.content_as::<EndSimulation>() {
                println!("[{}] Got EndSimulation", context.aid);
                // We create a message that will be sent to all actors in our list. Note that
                // we can send the message with an extremely lightweight clone.
                let request = Message::new(Command::SendMetrics(context.aid.clone()));
                for (aid, _) in state.iter() {
                    aid.send(request.clone());
                }
            } else if let Some(msg) = message.content_as::<SystemMsg>() {
                // FIXME SERIOUSLY consider making SystemMsg variants into structs to simplify
                // code.
                if let SystemMsg::Start = &*msg {
                    println!("[{}] Got Start ()", context.aid);
                    let msg = Message::new(SystemMsg::Stop);
                    context.aid.send_after(msg, run_time);
                }
            }
            Status::Processed
        },
    );

    system.await_shutdown();
}
