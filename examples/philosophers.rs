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
use log::LevelFilter;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
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

    #[inline]
    fn print_state(&mut self, context: &Context, info: &str) {
        debug!(
            "[{}] {} ==> owned_by: {}, clean: {}",
            context.aid.name().unwrap(),
            info,
            self.owned_by
                .as_ref()
                .map_or("_".to_string(), |a| a.name().unwrap()),
            self.clean,
        );
    }

    /// Request that a fork be sent to a philosopher.
    fn fork_requested(&mut self, context: &Context, requester: &ActorId) -> Status {
        self.print_state(
            context,
            &format!("Fork Requested by {}", requester.name().unwrap()),
        );
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
        self.print_state(
            context,
            &format!("Fork put down by {}", sender.name().unwrap()),
        );
        match &self.owned_by {
            Some(owner) => {
                if owner == sender {
                    self.owned_by = None;
                    self.clean = true;
                    // Resetting the skip allows fork requests to be processed.
                    Status::ResetSkip
                } else {
                    error!(
                        "[{}] fork_put_down() from non-owner: {} real owner is: {}",
                        context.aid, sender, owner
                    );
                    Status::Processed
                }
            }
            None => {
                error!(
                    "[{}] fork_put_down() from non-owner: {} real owner is: None:",
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
        self.print_state(
            context,
            &format!("Using Fork from {}", sender.name().unwrap()),
        );
        match self.owned_by {
            Some(ref owner) => {
                if *owner == *sender {
                    self.clean = false;
                    // Resetting the skip allows fork requests to be processed now that the fork
                    // has been marked as being dirty.
                    Status::ResetSkip
                } else {
                    error!("[{}] Got UsingFork from non-owner: {}", context.aid, sender);
                    Status::Processed
                }
            }
            _ => {
                error!("[{}] Got UsingFork from non-owner: {}", context.aid, sender);
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
#[derive(Debug)]
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
    /// Command to start eating. The u16 is the current state change number when sent this will
    /// be used to track whether this message is old or if it should be handled.
    StopEating(u16),
    /// Command to stop eating. The u16 is the current state change number when sent this will
    /// be used to track whether this message is old or if it should be handled.
    BecomeHungry(u16),
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
    /// The number of state changes that have occurred.
    state_change_count: u16,
    /// The number of times a Philosopher failed to eat because he didnt have both forks.
    failed_to_eat: u16,
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
    /// Whether or not the philosopher has requested the left fork.
    left_fork_requested: bool,
    /// The `ActorId` of the philosopher's right fork.
    right_fork_aid: ActorId,
    /// Whether the philosopher has the right fork.
    has_right_fork: bool,
    /// Whether or not the philosopher has requested the right fork.
    right_fork_requested: bool,
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
            left_fork_requested: false,
            right_fork_aid,
            has_right_fork: false,
            right_fork_requested: false,
            last_state_change: Instant::now(),
            metrics: Metrics {
                state_change_count: 0,
                failed_to_eat: 0,
                time_thinking: Duration::from_micros(0),
                time_hungry: Duration::from_micros(0),
                time_eating: Duration::from_micros(0),
            },
        }
    }

    #[inline]
    fn print_state(&mut self, context: &Context, info: &str) {
        debug!(
            "[{}] {} ==> state: {:?}, left: {:?}, right: {:?}",
            context.aid.name().unwrap(),
            info,
            self.state,
            (self.has_left_fork, self.left_fork_requested),
            (self.has_right_fork, self.right_fork_requested),
        );
    }

    /// Changes the philosopher to a state of eating.
    fn begin_eating(&mut self, context: &Context) {
        self.metrics.time_hungry += Instant::elapsed(&self.last_state_change);
        self.last_state_change = Instant::now();
        self.state = PhilosopherState::Eating;
        self.metrics.state_change_count += 1;
        // Now that we are eating we will tell the fork that we are using it,
        // thus marking the fork as dirty.
        self.left_fork_aid
            .send_new(ForkCommand::UsingFork(context.aid.clone()));
        self.right_fork_aid
            .send_new(ForkCommand::UsingFork(context.aid.clone()));

        // Schedule to stop eating after an eating time slice elapsed.
        let msg = Message::new(Command::StopEating(self.metrics.state_change_count));
        context.aid.send_after(msg, self.time_slice);
    }

    /// The philosopher received a fork. Once they have both forks they can start eating.
    /// Otherwise they have to wait for the other fork to begin eating.
    fn fork_received(&mut self, context: &Context, fork_aid: &ActorId) -> Status {
        if self.left_fork_aid == *fork_aid {
            self.print_state(context, "Got Left Fork");
            self.has_left_fork = true;
            self.left_fork_requested = false;
        } else if self.right_fork_aid == *fork_aid {
            self.print_state(context, "Got Right Fork");
            self.has_right_fork = true;
            self.right_fork_requested = false;
        } else {
            panic!("[{}] Unknown Fork Received: {}", context.aid, *fork_aid);
        }

        // If we have both forks then we can start eating.
        if self.has_left_fork && self.has_right_fork {
            self.begin_eating(context);
        }
        Status::Processed
    }

    /// Helper to request forks that the philosopher doesnt have.
    fn request_missing_forks(&mut self, context: &Context) {
        if !self.has_left_fork && !self.left_fork_requested {
            self.left_fork_requested = true;
            self.left_fork_aid
                .send_new(ForkCommand::RequestFork(context.aid.clone()));
        }
        if !self.has_right_fork && !self.right_fork_requested {
            self.right_fork_requested = true;
            self.right_fork_aid
                .send_new(ForkCommand::RequestFork(context.aid.clone()));
        }
    }

    /// The philosopher is being instructed to get hungry which will cause them to ask for the
    /// forks to eat. Note that the philosopher will have to have both forks to start eating.
    /// Once a philosopher starts eating, the forks will be sent a message indicating they are
    /// being used. Note that since this is sent as a scheduled message it may arrive later
    /// after the philosopher has changed state. For this reason we track the state change count
    /// and compare it with the number in the message.
    fn become_hungry(&mut self, context: &Context, state_num: u16) -> Status {
        if self.metrics.state_change_count == state_num {
            if self.has_left_fork && self.has_right_fork {
                self.begin_eating(context);
            } else {
                match &self.state {
                    PhilosopherState::Thinking => {
                        self.metrics.time_thinking += Instant::elapsed(&self.last_state_change);
                        self.last_state_change = Instant::now();
                        self.state = PhilosopherState::Hungry;
                        self.metrics.state_change_count += 1;
                        self.request_missing_forks(context);
                    }
                    PhilosopherState::Hungry => {
                        error!("[{}] Got BecomeHungry while eating!", context.aid);
                    }
                    PhilosopherState::Eating => {
                        error!("[{}] Got BecomeHungry while eating!", context.aid);
                    }
                };
            }
        }
        Status::Processed
    }

    /// Changes the philosopher to the state of thinking. Note that this doesn't mean that the
    /// philosopher will put down his forks. He will only do that if requested to.
    fn begin_thinking(&mut self, context: &Context) {
        self.state = PhilosopherState::Thinking;
        self.metrics.state_change_count += 1;
        self.metrics.time_eating += Instant::elapsed(&self.last_state_change);
        self.last_state_change = Instant::now();

        let msg = Message::new(Command::BecomeHungry(self.metrics.state_change_count));
        context.aid.send_after(msg, self.time_slice);
    }

    /// Processes a command to stop eating. Note that this can be received in any state because
    /// it is a delayed message send and thus it was enqueued when the philosopher was in the
    /// eating state but the philosopher might be in another state when received. That is why
    /// we track the state change count and compare it with the number in the message.
    fn stop_eating(&mut self, context: &Context, state_num: u16) -> Status {
        if self.metrics.state_change_count == state_num {
            if let PhilosopherState::Eating = &self.state {
                self.begin_thinking(context);
            }
        }
        Status::Processed
    }

    /// Processes a command to a philosopher to give up a fork. Note that this can be received
    /// when the philosopher is in any state since the philosopher will not put down a fork
    /// unless he is asked to. A philosopher can be eating, stop eating and start thinking
    /// and then start eating again if no one asked for his forks. The fork actor is the only
    /// actor sending this message and it will only do so if the fork is dirty.
    fn give_up_fork(&mut self, context: &Context, fork_aid: &ActorId) -> Status {
        if self.left_fork_aid == *fork_aid {
            self.print_state(context, "Gave Up Left Fork");
            self.has_left_fork = false;
            fork_aid.send_new(ForkCommand::ForkPutDown(context.aid.clone()));
        } else if self.right_fork_aid == *fork_aid {
            self.print_state(context, "Gave Up Right Fork");
            self.has_right_fork = false;
            fork_aid.send_new(ForkCommand::ForkPutDown(context.aid.clone()));
        } else {
            error!(
                "[{}] Unknown fork asked for: {}:\n left ==>  {}\n right ==> {}",
                context.aid, *fork_aid, self.left_fork_aid, self.right_fork_aid
            );
        }

        match &self.state {
            PhilosopherState::Hungry => {
                self.metrics.failed_to_eat += 1;
                self.begin_thinking(context);
            }
            PhilosopherState::Eating => {
                self.begin_thinking(context);
            }
            _ => (),
        }
        Status::Processed
    }

    /// A function that handles sending metrics to an actor that requests the metrics.
    fn send_metrics(&mut self, context: &Context, reply_to: &ActorId) -> Status {
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
                Command::StopEating(state_num) => self.stop_eating(context, *state_num),
                Command::BecomeHungry(state_num) => self.become_hungry(context, *state_num),
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
                    context.aid.send_new(Command::BecomeHungry(0));
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
    let args: Vec<String> = env::args().collect();
    let level = if args.contains(&"-v".to_string()) {
        LevelFilter::Debug
    } else {
        LevelFilter::Error
    };

    env_logger::builder()
        .filter_level(level)
        .is_test(true)
        .try_init()
        .unwrap();

    // FIXME Let the user pass in the number of philosophers at the table, time slice
    // and runtime as command line parameters.
    let count = 5 as usize;
    let time_slice = Duration::from_millis(100);
    let run_time = Duration::from_millis(2000);
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
    let _shutdown = system
        .spawn_named(
            "Manager",
            results,
            move |state: &mut HashMap<ActorId, Option<Metrics>>,
                  context: &Context,
                  message: &Message| {
                if let Some(msg) = message.content_as::<MetricsReply>() {
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
                        let msg = Message::new(EndSimulation {});
                        context.aid.send_after(msg, run_time);
                    }
                }
                Status::Processed
            },
        )
        .unwrap();

    system.await_shutdown();
}
