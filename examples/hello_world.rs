use axiom::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
enum HelloMessages {
    Greet,
}

fn hello(_state: &mut bool, aid: ActorId, message: &Message) -> Status {
    if let Some(_msg) = message.content_as::<HelloMessages>() {
        println!("Hello World from Actor: {:?}", aid);
        ActorSystem::current().trigger_shutdown();
    }
    Status::Processed
}

pub fn main() {
    let config = ActorSystemConfig::create();
    let system = ActorSystem::create(config);

    // Have to do this since we want to call from outside actor system.
    system.init_current();

    // Spawn the actor and send the message.
    let aid = system.spawn(true, hello);
    aid.send(Message::new(HelloMessages::Greet));

    // The actor will trigger shutdown, we just wait for it.
    system.await_shutdown();
    println!("Hello World");
}
