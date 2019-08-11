//! This is an example of a parallel processing implementation of a Monte-Carlo simulation
//! to predict the outcome of soccer matches based on the [Dixon-Coles
//! Model](http://web.math.ku.dk/~rolf/teaching/thesis/DixonColes.pdf).

use axiom::*;

pub struct Data {}

pub fn main() {
    // TODO Pending implementation.

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
