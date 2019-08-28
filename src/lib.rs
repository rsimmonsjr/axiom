//! Implementation of a highly-scalable and ergonomic actor model for Rust
//! # Axiom
//!
//! Axiom brings a highly-scalable actor model to the Rust language based on the many lessons
//! learned over years of Actor model implementations in Akka and Erlang. Axiom is, however,
//! not a direct re-implementation of either of the two aforementioned actor models but
//! rather a new implementation deriving inspiration from the good parts of those models.
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
//! the good practice of not sending mutable state as messages.
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
//!
//! let system = ActorSystem::create(ActorSystemConfig::default());
//!
//! let aid = system.spawn(
//!     0 as usize,
//!     |_state: &mut usize, _context: &Context, message: &Message| Status::Processed,
//!  );
//!
//! aid.send(Message::new(11));
//! aid.send_new(17); // This will wrap the value in a Message for you!
//!
//! // We can also create and send separately using just `send`, not `send_new`.
//! let m = Message::new(19);
//! aid.send(m);
//! ```
//!
//! This code creates an actor system, spawns an actor and finally sends the actor a message.
//! That is really all there is to it but of course it doesn't end there. If you want to create
//! an actor with a struct that is simple as well. Let's create one that handles a couple of
//! different message types:
//!
//! ```rust
//! use axiom::*;
//! use std::sync::Arc;
//!
//! let system = ActorSystem::create(ActorSystemConfig::default());
//!
//! struct Data {
//!     value: i32,
//! }
//!
//! impl Data {
//!     fn handle_bool(&mut self, message: &bool) -> Status {
//!         if *message {
//!             self.value += 1;
//!         } else {
//!             self.value -= 1;
//!         }
//!         Status::Processed
//!     }
//!
//!     fn handle_i32(&mut self, message: &i32) -> Status {
//!         self.value += *message;
//!         Status::Processed
//!     }
//!
//!     fn handle(&mut self, _context: &Context, message: &Message) -> Status {
//!         if let Some(msg) = message.content_as::<bool>() {
//!             self.handle_bool(&*msg)
//!         } else if let Some(msg) = message.content_as::<i32>() {
//!             self.handle_i32(&*msg)
//!         } else {
//!             panic!("Failed to dispatch properly");
//!             Status::Stop
//!         }
//!     }
//! }
//!
//! let data = Data { value: 0 };
//! let aid = system.spawn( data, Data::handle);
//!
//! aid.send_new(11);
//! aid.send_new(true);
//! aid.send_new(false);
//! ```
//!
//! This code creates an actor out of an arbitrary struct. Since the only requirement to make
//! an actor is to have a function that is compliant with the [`axiom::actors::Processor`] trait,
//! anything can be an actor. If this struct had been declared somewhere outside of your control
//! you could use it in an actor as state by declaring your own handler function and making the
//! calls to the 3rd party structure.
//!
//! *It's important to keep in mind that the starting state is moved into the actor and you
//! will not have external access to it afterwards.* This is by design and although you could
//! conceivably use a [`std::sync::Arc`] to a structure as state, that would definitely be a bad
//! idea as it would break the rules we laid out for actors.
//!
//! There is a lot more to learn and explore and your best resource is the test code for Axiom.
//! The developers have a belief that test code should be well architected and well commented to
//! act as a set of examples for users of Axiom.
//!

pub mod actors;
pub mod message;
pub mod system;

pub use crate::actors::ActorError;
pub use crate::actors::ActorId;
pub use crate::actors::Context;
pub use crate::actors::Status;
pub use crate::message::Message;
pub use crate::system::ActorSystem;
pub use crate::system::ActorSystemConfig;
pub use crate::system::SystemMsg;

#[cfg(test)]
mod tests {
    use super::*;
    use log::LevelFilter;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    pub fn init_test_log() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }

    /// A function that just returns [`Status::Processed`] which can be used as a handler for
    /// a simple actor.
    pub fn simple_handler(_state: &mut usize, _: &Context, _: &Message) -> Status {
        Status::Processed
    }

    /// A utility that waits for a certain number of messages to arrive in a certain time and
    /// returns an `Ok<()>` when they do or an `Err<String>` when not.
    pub fn await_received(aid: &ActorId, count: u8, timeout_ms: u64) -> Result<(), String> {
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
    /// that simply returns that the message is processed.
    #[test]
    fn test_simplest_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());

        // We spawn the actor using a closure. Note that because of a bug in the Rust compiler
        // as of 2019-07-12 regarding type inference we have to specify all of the types manually
        // but when that bug goes away this will be even simpler.
        let aid = system.spawn(
            0 as usize,
            |_state: &mut usize, _context: &Context, _message: &Message| Status::Processed,
        );

        // Send a message to the actor.
        aid.send_new(11);

        // Wait for the message to get there because test is asynchronous.
        await_received(&aid, 1, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// This test shows how the simplest struct-based actor can be built and used. This actor
    /// merely returns that the message was processed.
    #[test]
    fn test_simplest_struct_actor() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());

        // We declare a basic struct that has a handle method that does basically nothing.
        // Subsequently we will create that struct when we spawn the actor and then send the
        // actor a message.
        struct Data {}

        impl Data {
            fn handle(&mut self, _context: &Context, _message: &Message) -> Status {
                Status::Processed
            }
        }

        let aid = system.spawn(Data {}, Data::handle);

        // Send a message to the actor.
        aid.send_new(11);

        // Wait for the message to get there because test is asynchronous.
        await_received(&aid, 1, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// This test shows how a closure based actor can be used and process different kinds of
    /// messages and mutate the actor's state based upon the messages passed. Note that the
    /// state of the actor is not available outside the actor itself.
    #[test]
    fn test_dispatching_with_closure() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());

        let starting_state: usize = 0 as usize;
        let closure = |state: &mut usize, context: &Context, message: &Message| {
            // Expected messages in the expected order.
            let expected: Vec<i32> = vec![11, 13, 17];
            // Attempt to downcast to expected message.
            if let Some(_msg) = message.content_as::<SystemMsg>() {
                *state += 1;
                Status::Processed
            } else if let Some(msg) = message.content_as::<i32>() {
                assert_eq!(expected[*state - 1], *msg);
                assert_eq!(*state, context.aid.received().unwrap());
                *state += 1;
                Status::Processed
            } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                // Note that we put this last because it only is ever received once, we
                // want the most frequently received messages first.
                Status::Processed
            } else {
                panic!("Failed to dispatch properly");
            }
        };

        let aid = system.spawn(starting_state, closure);

        // First message will always be the SystemMsg::Start.
        assert_eq!(1, aid.sent().unwrap());

        // Send some messages to the actor in the order required in the test. In a real actor
        // its unlikely any order restriction would be needed. However this test makes sure that
        // the messages are processed correctly.
        aid.send_new(11 as i32);
        assert_eq!(2, aid.sent().unwrap());
        aid.send_new(13 as i32);
        assert_eq!(3, aid.sent().unwrap());
        aid.send_new(17 as i32);
        assert_eq!(4, aid.sent().unwrap());

        // Wait for all of the messages to get there because test is asynchronous.
        await_received(&aid, 4, 1000).unwrap();
        system.trigger_and_await_shutdown();
    }

    /// This test shows how a struct-based actor can be used and process different kinds of
    /// messages and mutate the actor's state based upon the messages passed. Note that the
    /// state of the actor is not available outside the actor itself.
    #[test]
    fn test_dispatching_with_struct() {
        init_test_log();

        let system = ActorSystem::create(ActorSystemConfig::default());

        // We create a basic struct with a handler and use that handler to dispatch to other
        // inherent methods in the struct. Note that we don't have to implement any traits here
        // and there is nothing forcing the handler to be an inherent method.
        struct Data {
            value: i32,
        }

        impl Data {
            fn handle_bool(&mut self, message: &bool) -> Status {
                if *message {
                    self.value += 1;
                } else {
                    self.value -= 1;
                }
                Status::Processed // This assertion will fail but we still have to return.
            }

            fn handle_i32(&mut self, message: &i32) -> Status {
                self.value += *message;
                Status::Processed // This assertion will fail but we still have to return.
            }

            fn handle(&mut self, _context: &Context, message: &Message) -> Status {
                if let Some(msg) = message.content_as::<bool>() {
                    self.handle_bool(&*msg)
                } else if let Some(msg) = message.content_as::<i32>() {
                    self.handle_i32(&*msg)
                } else if let Some(_msg) = message.content_as::<SystemMsg>() {
                    // Note that we put this last because it only is ever received once, we
                    // want the most frequently received messages first.
                    Status::Processed
                } else {
                    panic!("Failed to dispatch properly");
                }
            }
        }

        let data = Data { value: 0 };

        let aid = system.spawn(data, Data::handle);

        // Send some messages to the actor.
        aid.send_new(11);
        aid.send_new(true);
        aid.send_new(true);
        aid.send_new(false);

        // Wait for all of the messages to get there because this test is asynchronous.
        await_received(&aid, 4, 1000).unwrap();
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
            Ping(ActorId),
            Pong,
        }

        fn ping(_state: &mut usize, context: &Context, message: &Message) -> Status {
            if let Some(msg) = message.content_as::<PingPong>() {
                match &*msg {
                    PingPong::Pong => {
                        context.system.trigger_shutdown();
                        Status::Processed
                    }
                    _ => panic!("Unexpected message"),
                }
            } else if let Some(msg) = message.content_as::<SystemMsg>() {
                // Start messages happen only once so we keep them last.
                match &*msg {
                    SystemMsg::Start => {
                        // Now we will spawn a new actor to handle our pong and send to it.
                        let pong_aid = context.system.spawn(0, pong);
                        pong_aid.send_new(PingPong::Ping(context.aid.clone()));
                        Status::Processed
                    }
                    _ => Status::Processed,
                }
            } else {
                Status::Processed
            }
        }

        fn pong(_state: &mut usize, _context: &Context, message: &Message) -> Status {
            if let Some(msg) = message.content_as::<PingPong>() {
                match &*msg {
                    PingPong::Ping(from) => {
                        from.send_new(PingPong::Pong);
                        Status::Processed
                    }
                    _ => panic!("Unexpected message"),
                }
            } else {
                Status::Processed
            }
        }

        let system = ActorSystem::create(ActorSystemConfig::default());
        system.spawn(0, ping);
        system.await_shutdown();

        assert_eq!(2 + 2, 4);
    }
}
