//! Implementation of a highly-scalable and ergonomic actor model for Rust
//!
//! [![Latest version](https://img.shields.io/crates/v/axiom.svg)](https://crates.io/crates/axiom)
//! [![Build Status](https://api.travis-ci.org/rsimmonsjr/axiom.svg?branch=master)](https://travis-ci.org/rsimmonsjr/axiom)
//! [![Average time to resolve an issue](https://isitmaintained.com/badge/resolution/rsimmonsjr/axiom.svg)](https://isitmaintained.com/project/rsimmonsjr/axiom)
//! [![License](https://img.shields.io/crates/l/axiom.svg)](https://github.com/rsimmonsjr/axiom#license)
//!
//! # Axiom
//!
//! Axiom brings a highly-scalable actor model to the Rust language based on the many lessons
//! learned over years of Actor model implementations in Akka and Erlang. Axiom is, however,
//! not a direct re-implementation of either of the two aforementioned actor models but
//! rather a new implementation deriving inspiration from the good parts of those projects.
//!
//! ### What's New
//! * 2019-09-27 0.1.0
//!   * A lot of breaking changes have been introduced in an effort to keep them all in one release
//!   so that the API can stabilize. Please see examples and other sources for help in integrating
//!   all of the changes listed below.
//!   * BREAKING CHANGE: `ActorId` has been renamed to `Aid` to facilitate communication and lower
//!   confusion between the `uuid` field in the `Aid` and the `Aid` itself.
//!   * BREAKING CHANGE: `Status::Processed` has been renamed to `Status::Done`.
//!   * BREAKING CHANGE: `Status::Skipped` has been renamed to `Status::Skip`.
//!   * BREAKING CHANGE: `Status::ResetSkip` has been renamed to `Status::Reset`.
//!   * BREAKING CHANGE: `ActorError` has been moved to top level and renamed to `AxiomError`.
//!   * BREAKING CHANGE: `find_by_name` and `find_by_uuid` have been removed from `Aid` as the
//!   mechanism for looking up actors doesn't make sense the way it was before.
//!   * BREAKING CHANGE: `MessageContent` was unintentionally public and is now private.
//!   * BREAKING CHANGE: Changed `Processor` to take a `&Context` rather than `Aid`.
//!   * BREAKING CHANGE: The `send`, `send_new` and `send_after` methods now return a result type
//!   that the user must manage.
//!   * BREAKING CHANGE: All actor processors now should return `AxiomResult` which will allow them
//!   to use the `?` syntax for all functions that return `AxiomError` and return their own errors.
//!   * BREAKING CHANGE: Actors are now spawned with the builder pattern. This allows the
//!   configuration of an actor and leaves the door open for future flexibility. See documentation
//!   for more details.
//!   * Created a `Context` type that holds references to the `Aid` and `ActorSystem`.
//!   * `Processor` functions can get a reference to the `Aid` of the actor from `Context`.
//!   * `Processor` functions can get a reference to the `ActorSystem` from `Context`.
//!   * The methods `find_aid_by_uuid` and `find_aid_by_name` are added to the `ActorSystem`.
//!   * Calling `system.init_current()` is unneeded unless deserializing `Aid`s outside a
//!   `Processor`.
//!   * Metrics methods like `received()` in `Aid` return `Result` instead of using `panic!`.
//!   * Changed internal maps to use crate `dashmap` which expands dependencies but increases
//!   performance.
//!   * New methods `send_new` and `send_new_after` are available to shorten boilerplate.
//!   * Added a named system actor, which is registered under the name `System`, that is started
//!   as the 1st actor in an `ActorSystem`.
//!   * Added a method `system_actor_aid` to easily look up the `System` actor.
//!   * Added additional configuration options to `ActorSystemConfig`.
//!   * System will warn if an actor takes longer than the configured `warn_threshold` to process a
//!   message.
//!   * Instead of processing one message per receive, the system will now process pending messages
//!   up until the configured `time_slice`, allowing optimized processing for quick messages.
//!   * The default `message_channel_size` for actors is now configurable for the actor system as
//!   a whole.
//!   * Instead of waiting forever on a send, the system will wait for the configured
//!   `send_timeout` before returning a timeout error to the caller.
//!
//! [Release Notes for All Versions](https://github.com/rsimmonsjr/axiom/blob/master/RELEASE_NOTES.md)
//!
//! # Getting Started
//!
//! *An actor model is an architectural asynchronous programming paradigm characterized by the use
//! of actors for all processing activities.*
//!
//! Actors have the following characteristics:
//! 1. An actor can be interacted with only by means of messages.
//! 2. An actor processes only one message at a time.
//! 3. An actor will process a message only once.
//! 4. An actor can send a message to any other actor without knowledge of that actor's internals.
//! 5. Actors send only immutable data as messages, though they may have mutable internal state.
//! 6. Actors are location agnostic; they can be sent a message from anywhere in the cluster.
//!
//! Note that within the language of Rust, rule five cannot be enforced by Rust but is a best
//! practice which is important for developers creating actors based on Axiom. In Erlang and
//! Elixir rule five cannot be violated because of the structure of the language but this also
//! leads to performance limitations. It's better to allow internal mutable state and encourage
//! the good practice of not sending mutable messages.
//!
//! What is important to understand is that these rules combined together makes each actor operate
//! like a micro-service in the memory space of the program using them. Since actor messages are
//! immutable, actors can trade information safely and easily without copying large data
//! structures.
//!
//! Although programming in the actor model is quite an involved process you can get started with
//! Axiom in only a few lines of code.
//!
//! ```rust
//! use axiom::*;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
//!
//! let aid = system
//!     .spawn()
//!     .with(
//!         0 as usize,
//!         |_state: &mut usize, _context: &Context, _message: &Message| Ok(Status::Done),
//!     )
//!     .unwrap();
//!
//! aid.send(Message::new(11)).unwrap();
//!
//! // It is worth noting that you probably wouldn't just unwrap in real code but deal with
//! // the result as a panic in Axiom will take down a dispatcher thread and potentially
//! // hang the system.
//!
//! // This will wrap the value `17` in a Message for you!
//! aid.send_new(17).unwrap();
//!
//! // We can also create and send separately using just `send`, not `send_new`.
//! let message = Message::new(19);
//! aid.send(message).unwrap();
//!
//! // Another neat capability is to send a message after some time has elapsed.
//! aid.send_after(Message::new(7), Duration::from_millis(10)).unwrap();
//! aid.send_new_after(7, Duration::from_millis(10)).unwrap();
//! ```
//! This code creates an actor system, fetches a builder for an actor via the `spawn()` method,
//! spawns an actor and finally sends the actor a message. Creating an Axiom actor is literally
//! that easy but there is a lot more functionality available as well.
//!
//! If you want to create an actor with a struct that is simple as well. Let's create one that
//! handles a couple of different message types:
//!
//! ```rust
//! use axiom::*;
//! use std::sync::Arc;
//!
//! let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
//!
//! struct Data {
//!     value: i32,
//! }
//!
//! impl Data {
//!     fn handle_bool(mut self, message: &bool) -> AxiomResult<Self> {
//!         if *message {
//!             self.value += 1;
//!         } else {
//!             self.value -= 1;
//!         }
//!         Ok((self, Status::Done))
//!     }
//!
//!     fn handle_i32(mut self, message: &i32) -> AxiomResult<Self> {
//!         self.value += *message;
//!         Ok((self, Status::Done))
//!     }
//!
//!     fn handle(&mut self, _context: Context, message: Message) -> AxiomResult<Self> {
//!         if let Some(msg) = message.content_as::<bool>() {
//!             self.handle_bool(msg)
//!         } else if let Some(msg) = message.content_as::<i32>() {
//!             self.handle_i32(msg)
//!         } else {
//!             panic!("Failed to dispatch properly");
//!         }
//!     }
//! }
//!
//! let data = Data { value: 0 };
//! let aid = system.spawn().name("Fred").with(data, Data::handle).unwrap();
//!
//! aid.send_new(11).unwrap();
//! aid.send_new(true).unwrap();
//! aid.send_new(false).unwrap();
//! ```
//!
//! This code creates a named actor out of an arbitrary struct. Since the only requirement to make
//! an actor is to have a function that is compliant with the [`axiom::actors::Processor`] trait,
//! anything can be an actor. If this struct had been declared somewhere outside of your control
//! you could use it in an actor as state by declaring your own handler function and making the
//! calls to the 3rd party structure.
//!
//! *It's important to keep in mind that the starting state is moved into the actor and you
//! will not have external access to it afterwards.* This is by design and although you could
//! conceivably use a [`Arc`] or [`Mutex`] enclosing a structure as state, that would definitely
//! be a bad idea as it would break the rules we laid out for actors.
//!
//! There is a lot more to learn and explore and your best resource is the test code for Axiom.
//! The developers have a belief that test code should be well architected and well commented to
//! act as a set of examples for users of Axiom.
//!
//! # Detailed Examples
//! * [Hello World](https://github.com/rsimmonsjr/axiom/blob/master/examples/hello_world.rs): The
//! obligatory introduction to any computer system.
//! * [Dining Philosophers](https://github.com/rsimmonsjr/axiom/blob/master/examples/philosophers.rs):
//! An example of using Axiom to solve a classic Finite State Machine problem in computer science.
//! * [Monte Carlo](https://github.com/rsimmonsjr/axiom/blob/master/examples/montecarlo.rs): An
//! example of how to use Axiom for parallel computation.
//!
//! ## Design Principals of Axiom
//!
//! Based on previous experience with other actor models I wanted to design Axiom around some
//! core principles:
//! 1. **At its core an actor is just an function that processes messages.** The simplest actor is
//! a function that takes a message and simply ignores it. The benefit to the functional approach
//! over the Akka model is that it allows the user to create actors easily and simply. This is
//! the notion of _micro module programming_; the notion of building a complex system from the
//! smallest components. Software based on the actor model can get complicated; keeping it simple
//! at the core is fundamental to solid architecture.
//! 2. **Actors can be a Finite State Machine (FSM).** Actors receive and process messages
//! nominally in the order received. However, there are certain circumstances where an actor has
//! to change to another state and process other messages, skipping certain messages to be
//! processed later.
//! 3. **When skipping messages, the messages must not move.** Akka allows the skipping of messages
//! by _stashing_ the message in another data structure and then restoring this stash later. This
//! process has many inherent flaws. Instead Axiom allows an actor to skip messages in its
//! channel but leave them where they are, increasing performance and avoiding many problems.
//! 4. **Actors use a bounded capacity channel.** In Axiom the message capacity for the actor's
//! channel is bounded, resulting in greater simplicity and an emphasis on good actor design.
//! 5. **Axiom should be kept as small as possible.** Axiom is the core of the actor model and
//! should not be expanded to include everything possible for actors. That should be the
//! job of libraries that extend Axiom. Axiom itself should be an example of _micro module
//! programming_.
//! 6. **The tests are the best place for examples.** The tests of Axiom will be extensive and
//! well maintained and should be a resource for those wanting to use Axiom. They should not
//! be a dumping ground for copy-paste or throwaway code. The best tests will look like
//! architected code.  
//! 7. **A huge emphasis is put on crate user ergonomics.** Axiom should be easy to use.

use std::sync::RwLock;

use serde::{Deserialize, Serialize};

pub use crate::actors::Aid;
pub use crate::actors::Context;
pub use crate::actors::Status;
pub use crate::message::Message;
pub use crate::system::ActorSystem;
pub use crate::system::ActorSystemConfig;
pub use crate::system::SystemMsg;
pub use crate::system::WireMessage;

pub mod actors;
pub mod cluster;
mod executor;
pub mod message;
pub mod system;

/// Errors returned by various parts of Axiom.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AxiomError {
    /// Error sent when attempting to send to an actor that has already been stopped. A stopped
    /// actor cannot accept any more messages and is shut down. The holder of an [`Aid`] to
    /// a stopped actor should throw the [`Aid`] away as the actor can never be started again.
    ActorAlreadyStopped,

    /// An error returned when an actor is already using a local name at the time the user tries
    /// to register that name for a new actor. The error contains the name that was attempted
    /// to be registered.
    NameAlreadyUsed(String),

    /// Error returned when an Aid is not local and a user is trying to do operations that
    /// only work on local Aid instances.
    AidNotLocal,

    /// Used when unable to send to an actor's message channel within the scheduled timeout
    /// configured in the actor system. This could result from the actor's channel being too
    /// small to accomodate the message flow, the lack of thread count to process messages fast
    /// enough to keep up with the flow or something wrong with the actor itself that it is
    /// taking too long to clear the messages.
    SendTimedOut(Aid),

    /// Used when unable to schedule the actor for work in the work channel. This could be a
    /// result of having a work channel that is too small to accommodate the number of actors
    /// being concurrently scheduled, not enough threads to process actors in the channel fast
    /// enough or simply an actor that misbehaves, causing dispatcher threads to take a lot of
    /// time or not finish at all.
    UnableToSchedule,

    /// Returned by actors when there is an error in the actor. The optional enclosed string
    /// will be sent to monitoring actors and can tell the monitoring actors the nature of the
    /// error that occurred.
    ActorError(Option<String>),

    /// Returned when calling `ActorSystem::await_shutdown_with_timeout` and the timeout expires
    /// or some other error occurs while awaiting the shutdown.
    ShutdownError(String),
}

impl std::fmt::Display for AxiomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for AxiomError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// A type for a result from an actor's message processor.
/// A Result::Err is treated as a fatal error, and the Actor will be stopped.
pub type AxiomResult<State> = Result<(State, Status), AxiomError>;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use log::LevelFilter;
    use serde::{Deserialize, Serialize};

    use super::*;

    pub fn init_test_log() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }

    /// A function that just returns `Ok(Status::Done)` which can be used as a handler for
    /// a simple dummy actor.
    pub async fn simple_handler(_: (), _: Context, _: Message) -> AxiomResult<()> {
        Ok(((), Status::Done))
    }

    /// A utility that waits for a certain number of messages to arrive in a certain time and
    /// returns an `Ok<()>` when they do or an `Err<String>` when not.
    pub fn await_received(aid: &Aid, count: u8, timeout_ms: u64) -> Result<(), String> {
        use std::time::Instant;
        let start = Instant::now();
        let duration = Duration::from_millis(timeout_ms);
        while aid.received().unwrap() < count as usize {
            if Instant::elapsed(&start) > duration {
                return Err(format!(
                    "Timed out! count: {} timeout_ms: {}",
                    count, timeout_ms
                ));
            }
        }
        Ok(())
    }

    /// This test shows how the simplest actor can be built and used. This actor uses a closure
    /// that simply returns that the message is processed without doing anything with it.
    #[test]
    fn test_simplest_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let aid = system
            .spawn()
            .with((), |_: (), _: Context, _: Message| {
                async { Ok(((), Status::Done)) }
            })
            .unwrap();

        // Send a message to the actor.
        aid.send_new(11).unwrap();

        // The actor will get two messages including the Start message.
        await_received(&aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// This test shows how the simplest struct-based actor can be built and used. This actor
    /// merely returns that the message was processed.
    #[test]
    fn test_simplest_struct_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        // We declare a basic struct that has a handle method that does basically nothing.
        // Subsequently we will create that struct as a starting state when we spawn the actor
        // and then send the actor a message.
        struct Data {}

        impl Data {
            async fn handle(self, _: Context, _: Message) -> AxiomResult<Self> {
                Ok((self, Status::Done))
            }
        }

        let aid = system.spawn().with(Data {}, Data::handle).unwrap();

        // Send a message to the actor.
        aid.send_new(11).unwrap();

        await_received(&aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// This test shows how a closure based actor can be created to process different kinds of
    /// messages and mutate the actor's state based upon the messages passed. Note that the
    /// state of the actor is not available outside the actor itself.
    #[test]
    fn test_dispatching_with_closure() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        let starting_state: usize = 0 as usize;
        let closure = |mut state: usize, context: Context, message: Message| {
            async move {
                // Expected messages in the expected order.
                let expected: Vec<i32> = vec![11, 13, 17];
                // Attempt to downcast to expected message.
                if let Some(_msg) = message.content_as::<SystemMsg>() {
                    state += 1;
                    Ok((state, Status::Done))
                } else if let Some(msg) = message.content_as::<i32>() {
                    assert_eq!(expected[state - 1], *msg);
                    assert_eq!(state, context.aid.received().unwrap());
                    state += 1;
                    Ok((state, Status::Done))
                } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                    // Note that we put this last because it only is ever received once, we
                    // want the most frequently received messages first.
                    Ok((state, Status::Done))
                } else {
                    panic!("Failed to dispatch properly");
                }
            }
        };

        let aid = system.spawn().with(starting_state, closure).unwrap();

        // First message will always be the SystemMsg::Start.
        assert_eq!(1, aid.sent().unwrap());

        // Send some messages to the actor in the order required in the test. In a real actor
        // its unlikely any order restriction would be needed. However this test makes sure that
        // the messages are processed correctly.
        aid.send_new(11 as i32).unwrap();
        assert_eq!(2, aid.sent().unwrap());
        aid.send_new(13 as i32).unwrap();
        assert_eq!(3, aid.sent().unwrap());
        aid.send_new(17 as i32).unwrap();
        assert_eq!(4, aid.sent().unwrap());

        await_received(&aid, 4, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// This test shows how a struct-based actor can be used and process different kinds of
    /// messages and mutate the actor's state based upon the messages passed. Note that the
    /// state of the actor is not available outside the actor itself.
    #[test]
    fn test_dispatching_with_struct() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        // We create a basic struct with a handler and use that handler to dispatch to other
        // inherent methods in the struct. Note that we don't have to implement any traits here
        // and there is nothing forcing the handler to be an inherent method.
        struct Data {
            value: i32,
        }

        impl Data {
            fn handle_bool(mut self, message: bool) -> AxiomResult<Self> {
                if message {
                    self.value += 1;
                } else {
                    self.value -= 1;
                }
                Ok((self, Status::Done)) // This assertion will fail but we still have to return.
            }

            fn handle_i32(mut self, message: i32) -> AxiomResult<Self> {
                self.value += message;
                Ok((self, Status::Done)) // This assertion will fail but we still have to return.
            }

            async fn handle(self, _context: Context, message: Message) -> AxiomResult<Self> {
                if let Some(msg) = message.content_as::<bool>() {
                    self.handle_bool(*msg)
                } else if let Some(msg) = message.content_as::<i32>() {
                    self.handle_i32(*msg)
                } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                    // Note that we put this last because it only is ever received once, we
                    // want the most frequently received messages first.
                    Ok((self, Status::Done))
                } else {
                    panic!("Failed to dispatch properly");
                }
            }
        }

        let data = Data { value: 0 };

        let aid = system.spawn().with(data, Data::handle).unwrap();

        // Send some messages to the actor.
        aid.send_new(11).unwrap();
        aid.send_new(true).unwrap();
        aid.send_new(true).unwrap();
        aid.send_new(false).unwrap();

        await_received(&aid, 4, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// Tests and demonstrates the process to create a closure that captures the environment 
    /// outside the closure in a manner sufficient to be used in a future. 
    #[test]
    fn test_closure_with_move() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        let target_aid = system.spawn().with((), simple_handler).unwrap();

        let aid_moved = target_aid.clone(); // clone for the closure
        let aid = system
            .spawn()
            .with((), move |_: (), _: Context, _: Message| {
                // Each future needs its own copy of the target aid.
                let tgt = aid_moved.clone();
                async move { 
                    tgt.send_new(11)?;
                    Ok(((), Status::Done)) 
                }
            })
            .unwrap();

        aid.send_new(11).unwrap();
        await_received(&target_aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// Tests an example where one actor starts another actor, the actors exchange a simple
    /// ping-pong message and then the first actor triggers a shutdown when the pong message is
    /// received. Note that these actors just use simple functions to accomplish the task though
    /// they could have used functions on structures, closures, and even had a multiple methods
    /// to handle the messages.
    #[test]
    fn test_ping_pong() {
        /// A simple enum used as test messages.
        #[derive(Serialize, Deserialize)]
        pub enum PingPong {
            Ping(Aid),
            Pong,
        }

        async fn ping(_: (), context: Context, message: Message) -> AxiomResult<()> {
            if let Some(msg) = message.content_as::<PingPong>() {
                match &*msg {
                    PingPong::Pong => {
                        context.system.trigger_shutdown();
                        Ok(((), Status::Done))
                    }
                    _ => Err(AxiomError::ActorError(Some(
                        "Unexpected message".to_string(),
                    ))),
                }
            } else if let Some(msg) = message.content_as::<SystemMsg>() {
                // Start messages happen only once so we keep them last.
                match &*msg {
                    SystemMsg::Start => {
                        // Now we will spawn a new actor to handle our pong and send to it.
                        let pong_aid = context.system.spawn().with((), pong)?;
                        pong_aid.send_new(PingPong::Ping(context.aid.clone()))?;
                        Ok(((), Status::Done))
                    }
                    _ => Ok(((), Status::Done)),
                }
            } else {
                Ok(((), Status::Done))
            }
        }

        async fn pong(_: (), _: Context, message: Message) -> AxiomResult<()> {
            if let Some(msg) = message.content_as::<PingPong>() {
                match &*msg {
                    PingPong::Ping(from) => {
                        from.send_new(PingPong::Pong)?;
                        Ok(((), Status::Done))
                    }
                    _ => Err(AxiomError::ActorError(Some(
                        "Unexpected message".to_string(),
                    ))),
                }
            } else {
                Ok(((), Status::Done))
            }
        }

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        system.spawn().with((), ping).unwrap();
        system.await_shutdown();

        assert_eq!(2 + 2, 4);
    }
}
