use axiom::*;

pub fn main() {
    let config = ActorSystemConfig::create();
    let system = ActorSystem::create(config);
    println!("Hello World");
}
