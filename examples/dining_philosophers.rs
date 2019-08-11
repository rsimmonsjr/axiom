//! An implementation of the classic finite state machine (FSM) [Dining Philosophers]
//! (https://en.wikipedia.org/wiki/Dining_philosophers_problem) problem using Axiom.
use axiom::*;

pub struct Data {}

pub fn main() {
    // First we initialize the actor system using the default config.
    let config = ActorSystemConfig::create();
    let system = ActorSystem::create(config);

    // Have to do this since we want to call from outside actor system.
    system.init_current();

    // Spawn the actor and send the message.
    // let aid = system.spawn(true, hello);
    // aid.send(Message::new(HelloMessages::Greet));

    // The actor will trigger shutdown, we just wait for it.
    // system.await_shutdown();
}
