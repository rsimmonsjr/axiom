//! This is an example of a parallel processing implementation of a Monte-Carlo simulation
//! The simulation is of a basic gambling game adapted from this page:
//! https://towardsdatascience.com/the-house-always-wins-monte-carlo-simulation-eb82787da2a3

use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};

use axiom::*;

#[derive(Debug, Clone)]
struct Game {
    funds: i64,
    wager: u32,
    total_plays: u32,
}

impl Game {
    fn roll_dice() -> bool {
        let mut rng = thread_rng();
        match rng.gen_range(0, 101) {
            x if x > 51 => true,
            _ => false,
        }
    }

    fn play(&mut self, ctx: &Context, msg: &Message) -> Status {
        if let Some(results_aid) = msg.content_as::<ActorId>() {
            let mut current_play = 1;
            while current_play <= self.total_plays {
                current_play += 1;
                match Game::roll_dice() {
                    true => self.funds += self.wager as i64,
                    false => self.funds -= self.wager as i64,
                }
                results_aid.send_new(GameMsg::new(ctx.aid.clone(), self.funds));
            }
            println!("Stopping `Game`");
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
    funds: i64,
}

impl GameMsg {
    fn new(aid: ActorId, funds: i64) -> Self {
        Self {
            aid: aid,
            funds: funds,
        }
    }
}

#[derive(Debug)]
struct GameResults {
    results: HashMap<ActorId, Vec<i64>>,
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
            if self.monitoring.insert(game_msg.aid.clone()) {
                ActorSystem::current().monitor(&ctx.aid, &game_msg.aid);
            }
            let entry = self
                .results
                .entry(game_msg.aid.clone())
                .or_insert(Vec::new());
            entry.push(game_msg.funds);
        }

        if let Some(sys_msg) = msg.content_as::<SystemMsg>() {
            match &*sys_msg {
                SystemMsg::Stopped(aid) => {
                    // Send the Stop command only if all the actors talking to us
                    // have also stopped
                    if self.monitoring.contains(&aid) {
                        if self
                            .monitoring
                            .iter()
                            .all(|aid| !ActorSystem::current().is_actor_alive(&aid))
                        {
                            println!("Stopping `GameResults`");
                            println!("{:#?}", self.results);
                            ActorSystem::current().trigger_shutdown();
                        }
                    }
                }
                _ => {}
            }
        }
        Status::Processed
    }
}

const NUM_GAMES: usize = 5;

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

    // Start the game by sending the ID for the GameResults actor to each Game instance.
    for id in game_ids {
        id.send_new(results_aid.clone());
    }

    // Wait for the actor system to shut down.
    system.await_shutdown();
}
