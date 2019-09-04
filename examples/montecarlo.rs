//! This is an example of a parallel processing implementation of a Monte-Carlo simulation
//! The simulation is of a basic gambling game adapted from this page:
//! https://towardsdatascience.com/the-house-always-wins-monte-carlo-simulation-eb82787da2a3

use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};

use std::collections::{HashMap, HashSet};

use axiom::*;

#[derive(Debug, Clone)]
struct Game {
    funds: u32,
    wager: u32,
    total_plays: u32,
}

impl Game {
    fn play(&mut self, ctx: &Context, msg: &Message) -> Status {
        if let Some(results_aid) = msg.content_as::<ActorId>() {
            let mut current_play = 1;
            while current_play <= self.total_plays {
                match roll_dice() {
                    true => {
                        self.funds += self.wager;
                        current_play += 1;
                    }
                    false => {
                        self.funds -= self.wager;
                        current_play += 1;
                    }
                }
                results_aid.send_new(GameMsg::new(ctx.aid.clone(), self.funds));
            }
            println!("Stopping `Game`...");
            return Status::Stop;
        }
        Status::Processed
    }
}

impl Default for Game {
    fn default() -> Self {
        Self {
            funds: 10_000,
            wager: 100,
            total_plays: 10,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GameMsg {
    aid: ActorId,
    funds: u32,
}

impl GameMsg {
    fn new(aid: ActorId, funds: u32) -> Self {
        Self {
            aid: aid,
            funds: funds,
        }
    }
}

#[derive(Debug)]
struct GameResults {
    results: HashMap<ActorId, Vec<u32>>,
    monitoring: HashSet<ActorId>,
}

impl GameResults {
    fn new() -> Self {
        Self {
            results: HashMap::new(),
            monitoring: HashSet::new(),
        }
    }
}

impl GameResults {
    fn gather(&mut self, ctx: &Context, msg: &Message) -> Status {
        if let Some(game_msg) = msg.content_as::<GameMsg>() {
            // Keep track of all the ActorIds that have sent us messages
            ActorSystem::current().monitor(&ctx.aid, &game_msg.aid);
            self.monitoring.insert(game_msg.aid.clone());

            let entry = self.results.entry(game_msg.aid.clone()).or_insert(Vec::new());
            entry.push(game_msg.funds);
        } else if let Some(sys_msg) = msg.content_as::<SystemMsg>() {
            match &*sys_msg {
                SystemMsg::Stopped(aid) => {
                    self.monitoring.remove(&aid);
                    // Send the Stop command only if all the actors talking to us
                    // have also stopped
                    if self.monitoring.is_empty() {
                        println!("{:?}", self.results);
                        println!("Stopping `GameResults`...");
                        return Status::Stop;
                    }
                },
                _ => {},
            }
        }

        Status::Processed
    }
}

fn roll_dice() -> bool {
    let mut rng = thread_rng();
    match rng.gen_range(0, 101) {
        x if x > 51 => true,
        _ => false,
    }
}

const NUM_GAMES: usize = 1;

fn main() {
    // First we initialize the actor system using the default config.
    let config = ActorSystemConfig::default();
    let system = ActorSystem::create(config);

    // Have to do this since we want to call from outside actor system.
    system.init_current();

    // Spawn the Game actors.
    let game_ids = std::iter::repeat_with(|| system.spawn(Game::default(), Game::play))
        .take(NUM_GAMES)
        .collect::<Vec<ActorId>>();

    // Spawn the results aggregator.
    let results_aid = system.spawn(GameResults::new(), GameResults::gather);

    // Start the game by sending the ID for the GameResults actor to each Game instance
    for id in game_ids {
        id.send_new(results_aid.clone());
    }

    // The actor will trigger shutdown, we just wait for it.
    system.await_shutdown();
}
