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
//! learned over years of Actor model implementations in Akka and Erlang. Axiom is, however, not a
//! direct re-implementation of either of the two aforementioned actor models but rather a new
//! implementation deriving inspiration from the good parts of those projects.
//!
//! * 2019-12-06 0.2.0
//!   * Massive internal refactor in order to support async Actors. There are only a few breaking
//!   changes, so porting to this version will be relatively simple.
//!   * BREAKING CHANGE: The signature for Processors has changed from references for `Context` and
//!   `Message` to values. For closures-as-actors, wrap the body in an `async` block. `move |...|
//!   {...}` becomes `|...| async move { ... }`. For regular function syntax, simply add `async` in
//!   front of `fn`.
//!   * NOTE: the positioning of `move` may need to be different, depending on semantics. Values
//!   cannot be moved out of the closure and into the async block.
//!   * BREAKING CHANGE: Due to the nature of futures, the actor's processor cannot be given a
//!   mutable reference to the state of the actor. The state needs to live at least as long as the
//!   future and our research could find no way to do this easily. So now when the actor returns a
//!   status it will return the new state as well. See the examples for more info. The signature for
//!   the processor is now:
//!   ```ignore
//!   impl<F, S, R> Processor<S, R> for F where
//!       S: Send + Sync,
//!       R: Future<Output = AxiomResult<S>> + Send + 'static,
//!       F: (FnMut(S, Context, Message) -> R) + Send + Sync + 'static  {}
//!   ```
//!   * BREAKING: Actors are now panic-tolerant! This means `assert`s and `panic`s will be caught
//!   and converted, treated the same as errors. Errors should already be considered fatal, as
//!   Actors should handle any errors in their own scope.
//!   * BREAKING: Error types have been broken up to be more context-specific.
//!   * BREAKING: A `start_on_launch` flag has been added to the `ActorSystemConfig` struct. This
//!   allows for an ActorSystem to be created without immediately starting it. See `ActorSystem::start`
//!   for how to start an unstarted `ActorSystem`.
//!   * Helper methods have been added to `Status` to help with the return points in Actors. Each
//!   variant has a corresponding function that takes the Actor's state. `Ok(Status::Done)` is
//!   instead `Ok(Status::done(state))`.
//!   * The user should take be aware that, at runtime, Actors will follow the semantics of Rust
//!   Futures. This means that an Actor awaiting a future will not process any messages nor will
//!   continue executing until that future is ready to be polled again. While async/await will
//!   provide ergonomic usage of async APIs, this can be a concern and can affect timing.
//!   * A prelude has been introduced. Attempts will be made at keeping the prelude relatively the
//!   same even across major versions, and we recommend using it whenever possible.
//!   * More `log` points have been added across the codebase.
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
//! use axiom::prelude::*;
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
//!
//! let aid = system
//!     .spawn()
//!     .with(
//!         0 as usize,
//!         |state: usize, _context: Context, _message: Message| async move {
//!             Ok(Status::done(state))
//!         }
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
//! spawns an actor and finally sends the actor a message. Once the actor is done processing a
//! message it returns the new state of the actor and the status after handling this message. In
//! this case we didnt change the state so we just return it. Creating an Axiom actor is literally
//! that easy but there is a lot more functionality available as well.
//!
//! Keep in mind that if you are capturing variables from the environment you will have to wrap
//! the `async move {}` block in another block and then move your variables into the first block.
//! Please see the test cases for more examples of this.
//!
//! If you want to create an actor with a struct that is simple as well. Let's create one that
//! handles a couple of different message types:
//!
//! ```rust
//! use axiom::prelude::*;
//! use std::sync::Arc;
//!
//! let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
//!
//! struct Data {
//!     value: i32,
//! }
//!
//! impl Data {
//!     fn handle_bool(mut self, message: bool) -> ActorResult<Self> {
//!         if message {
//!             self.value += 1;
//!         } else {
//!             self.value -= 1;
//!         }
//!         Ok(Status::done(self))
//!     }
//!
//!     fn handle_i32(mut self, message: i32) -> ActorResult<Self> {
//!         self.value += message;
//!         Ok(Status::done(self))
//!     }
//!
//!     async fn handle(mut self, _context: Context, message: Message) -> ActorResult<Self> {
//!         if let Some(msg) = message.content_as::<bool>() {
//!             self.handle_bool(*msg)
//!         } else if let Some(msg) = message.content_as::<i32>() {
//!             self.handle_i32(*msg)
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
//! anything can be an actor. If this struct had been declared somewhere outside of your control you
//! could use it in an actor as state by declaring your own handler function and making the calls to
//! the 3rd party structure.
//!
//! *It's important to keep in mind that the starting state is moved into the actor and you will not
//! have external access to it afterwards.* This is by design and although you could conceivably use
//! a [`Arc`] or [`Mutex`] enclosing a structure as state, that would definitely be a bad idea as it
//! would break the rules we laid out for actors.
//!
//! There is a lot more to learn and explore and your best resource is the test code for Axiom. The
//! developers have a belief that test code should be well architected and well commented to act as
//! a set of examples for users of Axiom.
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
//! 1. **At its core an actor is just an function that processes messages.** The simplest actor is a
//! function that takes a message and simply ignores it. The benefit to the functional approach over
//! the Akka model is that it allows the user to create actors easily and simply. This is the notion
//! of _micro module programming_; the notion of building a complex system from the smallest
//! components. Software based on the actor model can get complicated; keeping it simple at the core
//! is fundamental to solid architecture.
//! 2. **Actors can be a Finite State Machine (FSM).** Actors receive and process messages nominally
//! in the order received. However, there are certain circumstances where an actor has to change to
//! another state and process other messages, skipping certain messages to be processed later.
//! 3. **When skipping messages, the messages must not move.** Akka allows the skipping of messages
//! by _stashing_ the message in another data structure and then restoring this stash later. This
//! process has many inherent flaws. Instead Axiom allows an actor to skip messages in its channel
//! but leave them where they are, increasing performance and avoiding many problems.
//! 4. **Actors use a bounded capacity channel.** In Axiom the message capacity for the actor's
//! channel is bounded, resulting in greater simplicity and an emphasis on good actor design.
//! 5. **Axiom should be kept as small as possible.** Axiom is the core of the actor model and
//! should not be expanded to include everything possible for actors. That should be the job of
//! libraries that extend Axiom. Axiom itself should be an example of _micro module programming_.
//! 6. **The tests are the best place for examples.** The tests of Axiom will be extensive and well
//! maintained and should be a resource for those wanting to use Axiom. They should not be a dumping
//! ground for copy-paste or throwaway code. The best tests will look like architected code.
//! 7. **A huge emphasis is put on crate user ergonomics.** Axiom should be easy to use.

use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter};

// Re-export futures so the user doesn't need to import it.
pub use futures;
use prelude::*;

pub mod actors;
pub mod cluster;
mod executor;
pub mod message;
pub mod system;

pub mod prelude;

/// A helper alias to ensure returned errors conform as needed.
pub type StdError = Box<dyn Error + Send + Sync + 'static>;

/// A type for a result from an actor's message processor.
/// A Result::Err is treated as a fatal error, and the Actor will be stopped.
pub type ActorResult<State> = Result<(State, Status), StdError>;

#[derive(Debug)]
pub struct Panic {
    panic_payload: String,
}

impl Display for Panic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.panic_payload)
    }
}

impl Error for Panic {}

impl From<Box<dyn Any + Send + 'static>> for Panic {
    fn from(val: Box<dyn Any + Send + 'static>) -> Self {
        let panic_payload = match val.downcast::<&'static str>() {
            Ok(s) => String::from(*s),
            Err(val) => match val.downcast::<String>() {
                Ok(s) => *s,
                Err(_) => String::from("Panic payload unserializable"),
            },
        };
        Self { panic_payload }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use log::LevelFilter;
    use secc::{SeccReceiver, SeccSender};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Clone)]
    pub struct AssertCollect {
        tx: SeccSender<(bool, String)>,
        rx: SeccReceiver<(bool, String)>,
    }

    impl AssertCollect {
        pub fn new() -> Self {
            let (tx, rx) = secc::create(256, Duration::from_millis(10));
            Self { tx, rx }
        }

        pub fn assert(&self, cond: bool, msg: impl Into<String>) {
            let m = msg.into();
            self.tx.send((cond, m.clone())).unwrap();

            if !cond {
                panic!("{}", m)
            }
        }

        pub fn panic(&self, msg: impl Into<String>) -> ! {
            let m = msg.into();
            self.tx.send((false, m.clone())).unwrap();
            panic!("{}", m)
        }

        pub fn collect(&self) {
            while let Ok((cond, s)) = self.rx.receive() {
                assert!(cond, "{}", s);
            }
        }
    }

    pub fn init_test_log() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Warn)
            .is_test(true)
            .try_init();
    }

    pub fn sleep(millis: u64) {
        thread::sleep(Duration::from_millis(millis))
    }

    /// A function that just returns `Ok(Status::Done)` which can be used as a handler for
    /// a simple dummy actor.
    pub async fn simple_handler(_: (), _: Context, _: Message) -> ActorResult<()> {
        Ok(Status::done(()))
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
                    "Timed out after {}ms! Messages received: {}; Messages expected: {}",
                    timeout_ms,
                    aid.received().unwrap(),
                    count
                ));
            }
        }
        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_assert_receive() {
        let tracker = AssertCollect::new();
        let t2 = tracker.clone();

        let join = thread::spawn(move || t2.panic("This is a panic"));
        let _ = join.join();

        tracker.collect();
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
            .with((), |_: (), _: Context, _: Message| async {
                Ok(Status::done(()))
            })
            .unwrap();

        // Send a message to the actor.
        aid.send_new(11).unwrap();

        // The actor will get two messages including the Start message.
        await_received(&aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown(None);
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
            async fn handle(self, _: Context, _: Message) -> ActorResult<Self> {
                Ok(Status::done(self))
            }
        }

        let aid = system.spawn().with(Data {}, Data::handle).unwrap();

        // Send a message to the actor.
        aid.send_new(11).unwrap();

        await_received(&aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown(None);
    }

    /// This test shows how a closure based actor can be created to process different kinds of
    /// messages and mutate the actor's state based upon the messages passed. Note that the
    /// state of the actor is not available outside the actor itself.
    #[test]
    fn test_dispatching_with_closure() {
        init_test_log();

        let tracker = AssertCollect::new();
        let t = tracker.clone();
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        let starting_state: usize = 0 as usize;
        let closure = move |mut state: usize, context: Context, message: Message| {
            let t = t.clone();
            async move {
                // Expected messages in the expected order.
                let expected: Vec<i32> = vec![11, 13, 17];
                // Attempt to downcast to expected message.
                if let Some(_msg) = message.content_as::<SystemMsg>() {
                    state += 1;
                    Ok(Status::done(state))
                } else if let Some(msg) = message.content_as::<i32>() {
                    t.assert(expected[state - 1] == *msg, "Unexpected message content");
                    t.assert(state == context.aid.received().unwrap(), "Unexpected state");
                    state += 1;
                    Ok(Status::done(state))
                } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                    // Note that we put this last because it only is ever received once, we
                    // want the most frequently received messages first.
                    Ok(Status::done(state))
                } else {
                    t.panic("Failed to dispatch properly")
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
        system.trigger_and_await_shutdown(None);
        tracker.collect();
    }

    /// This test shows how a struct-based actor can be used and process different kinds of
    /// messages and mutate the actor's state based upon the messages passed. Note that the
    /// state of the actor is not available outside the actor itself.
    #[test]
    fn test_dispatching_with_struct() {
        init_test_log();

        let tracker = AssertCollect::new();
        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));

        // We create a basic struct with a handler and use that handler to dispatch to other
        // inherent methods in the struct. Note that we don't have to implement any traits here
        // and there is nothing forcing the handler to be an inherent method.
        struct Data {
            value: i32,
            tracker: AssertCollect,
        }

        impl Data {
            fn handle_bool(mut self, message: bool) -> ActorResult<Self> {
                if message {
                    self.value += 1;
                } else {
                    self.value -= 1;
                }
                Ok(Status::done(self)) // This assertion will fail but we still have to return.
            }

            fn handle_i32(mut self, message: i32) -> ActorResult<Self> {
                self.value += message;
                Ok(Status::done(self)) // This assertion will fail but we still have to return.
            }

            async fn handle(self, _context: Context, message: Message) -> ActorResult<Self> {
                if let Some(msg) = message.content_as::<bool>() {
                    self.handle_bool(*msg)
                } else if let Some(msg) = message.content_as::<i32>() {
                    self.handle_i32(*msg)
                } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                    // Note that we put this last because it only is ever received once, we
                    // want the most frequently received messages first.
                    Ok(Status::done(self))
                } else {
                    self.tracker.panic("Failed to dispatch properly")
                }
            }
        }

        let data = Data {
            value: 0,
            tracker: tracker.clone(),
        };

        let aid = system.spawn().with(data, Data::handle).unwrap();

        // Send some messages to the actor.
        aid.send_new(11).unwrap();
        aid.send_new(true).unwrap();
        aid.send_new(true).unwrap();
        aid.send_new(false).unwrap();

        await_received(&aid, 4, 1000).unwrap();
        system.trigger_and_await_shutdown(None);
        tracker.collect();
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
                    Ok(Status::done(()))
                }
            })
            .unwrap();

        aid.send_new(11).unwrap();
        await_received(&target_aid, 2, 1000).unwrap();
        system.trigger_and_await_shutdown(None);
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

        async fn ping(_: (), context: Context, message: Message) -> ActorResult<()> {
            if let Some(msg) = message.content_as::<PingPong>() {
                match &*msg {
                    PingPong::Pong => {
                        context.system.trigger_shutdown();
                        Ok(Status::done(()))
                    }
                    _ => Err("Unexpected message".to_string().into()),
                }
            } else if let Some(msg) = message.content_as::<SystemMsg>() {
                // Start messages happen only once so we keep them last.
                match &*msg {
                    SystemMsg::Start => {
                        // Now we will spawn a new actor to handle our pong and send to it.
                        let pong_aid = context.system.spawn().with((), pong)?;
                        pong_aid.send_new(PingPong::Ping(context.aid.clone()))?;
                        Ok(Status::done(()))
                    }
                    _ => Ok(Status::done(())),
                }
            } else {
                Ok(Status::done(()))
            }
        }

        async fn pong(_: (), _: Context, message: Message) -> ActorResult<()> {
            if let Some(msg) = message.content_as::<PingPong>() {
                match &*msg {
                    PingPong::Ping(from) => {
                        from.send_new(PingPong::Pong)?;
                        Ok(Status::done(()))
                    }
                    _ => Err("Unexpected message".into()),
                }
            } else {
                Ok(Status::done(()))
            }
        }

        let system = ActorSystem::create(ActorSystemConfig::default().thread_pool_size(2));
        system.spawn().with((), ping).unwrap();
        system.await_shutdown(None);
    }
}
