//! This is an example of implementing the classic "Hello World" in the Axiom actor system.
//!
//! Demonstrates
//! * Creating an actor system.
//! * Spawning an actor with a static function handler.
//! * Setting up the current thread to talk to the actor system.
//! * Sending a message to an actor.
//! * Determining the content of a message and acting on the message.
//! * Triggering an actor system shutdown within the actor.
//! * Awaiting the actor system to shut down.

use axiom::*;
use serde::{Deserialize, Serialize};

/// The messages we will be sending to our actor. All messages must be serializeable and
/// deserializeable with serde.
#[derive(Serialize, Deserialize)]
enum HelloMessages {
    Greet,
}

/// This is the handler that will be used by the actor.
async fn hello(_: (), context: Context, message: Message) -> AxiomResult<()> {
    if let Some(_msg) = message.content_as::<HelloMessages>() {
        println!("Hello World from Actor: {:?}", context.aid);
        context.system.trigger_shutdown();
    }
    Ok(((), Status::Done))
}

pub fn main() {
    // First we initialize the actor system using the default config.
    let config = ActorSystemConfig::default();
    let system = ActorSystem::create(config);

    // Spawn the actor and send the message.
    let aid = system.spawn().with((), hello).unwrap();
    aid.send(Message::new(HelloMessages::Greet)).unwrap();

    // The actor will trigger shutdown, we just wait for it.
    system.await_shutdown(None);
}
