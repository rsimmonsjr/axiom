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
//! system.init_current(); // Needed to call from outside of actor system threads.
//!
//! let aid = system.spawn(
//!     0 as usize,
//!     |_state: &mut usize, _aid: ActorId, message: &Message| Status::Processed,
//!  );
//!
//! aid.send(Message::new(11));
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
//! system.init_current(); // Needed to call from outside of actor system threads.
//!
//! struct Data {
//!     value: i32,
//! }
//!
//! impl Data {
//!     fn handle_bool(&mut self, _aid: ActorId, message: &bool) -> Status {
//!         if *message {
//!             self.value += 1;
//!         } else {
//!             self.value -= 1;
//!         }
//!         Status::Processed // This assertion will fail but we still have to return.
//!     }
//!
//!     fn handle_i32(&mut self, _aid: ActorId, message: &i32) -> Status {
//!         self.value += *message;
//!         Status::Processed // This assertion will fail but we still have to return.
//!     }
//!
//!     fn handle(&mut self, aid: ActorId, message: &Message) -> Status {
//!         if let Some(msg) = message.content_as::<bool>() {
//!             self.handle_bool(aid, &*msg)
//!         } else if let Some(msg) = message.content_as::<i32>() {
//!             self.handle_i32(aid, &*msg)
//!         } else {
//!             assert!(false, "Failed to dispatch properly");
//!             Status::Stop // This assertion will fail but we still have to return.
//!         }
//!     }
//! }
//!
//! let data = Data { value: 0 };
//! let aid = system.spawn( data, Data::handle);
//!
//! aid.send(Message::new(11));
//! aid.send(Message::new(true));
//! aid.send(Message::new(true));
//! aid.send(Message::new(false));
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
pub mod secc;

pub use crate::actors::ActorError;
pub use crate::actors::ActorId;
pub use crate::actors::ActorSystem;
pub use crate::actors::ActorSystemConfig;
pub use crate::actors::Status;
pub use crate::actors::SystemMsg;
pub use crate::message::Message;

#[cfg(test)]
mod tests {
    use super::*;
    use log::LevelFilter;
    use serde::{Deserialize, Serialize};

    pub fn init_test_log() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }

    #[derive(Serialize, Deserialize)]
    enum PingPong {
        Ping(ActorId),
        Pong,
    }

    fn ping(_state: &mut usize, aid: ActorId, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<PingPong>() {
            match &*msg {
                PingPong::Pong => {
                    ActorSystem::current().trigger_shutdown();
                    Status::Processed
                }
                _ => panic!("Unexpected message"),
            }
        } else if let Some(msg) = message.content_as::<SystemMsg>() {
            // start messages happen only once so we keep them last.
            match &*msg {
                SystemMsg::Start => {
                    let pong_aid = ActorSystem::current().spawn(0, pong);
                    pong_aid.send(Message::new(PingPong::Ping(aid.clone())));
                    Status::Processed
                }
                _ => Status::Processed,
            }
        } else {
            Status::Processed
        }
    }

    fn pong(_state: &mut usize, _aid: ActorId, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<PingPong>() {
            match &*msg {
                PingPong::Ping(from) => {
                    from.send(Message::new(PingPong::Pong));
                    Status::Processed
                }
                _ => panic!("Unexpected message"),
            }
        } else {
            Status::Processed
        }
    }

    #[test]
    fn test_ping_pong() {
        let system = ActorSystem::create(ActorSystemConfig::default());
        system.init_current();
        system.spawn(0, ping);
        system.await_shutdown();

        assert_eq!(2 + 2, 4);
    }
}
