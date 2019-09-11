//! This is an example of a parallel processing implementation of a Monte-Carlo simulation
//! The simulation is of a basic gambling game adapted from this page:
//! https://towardsdatascience.com/the-house-always-wins-monte-carlo-simulation-eb82787da2a3

use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;

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
        // The game starts when this actor receives a go message from the GameResults actor
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
    games_finished: u32,
    total_games: u32,
    results: HashMap<ActorId, Vec<i64>>,
}

impl GameResults {
    fn new(total_games: u32) -> Self {
        Self {
            games_finished: 0,
            total_games: total_games,
            results: HashMap::new(),
        }
    }
}

impl GameResults {
    fn gather(&mut self, ctx: &Context, msg: &Message) -> Status {
        // Receive messages from the Game actors and aggregate their results
        if let Some(game_msg) = msg.content_as::<GameMsg>() {
            let entry = self
                .results
                .entry(game_msg.aid.clone())
                .or_insert(Vec::new());
            entry.push(game_msg.funds);
        }

        if let Some(sys_msg) = msg.content_as::<SystemMsg>() {
            match &*sys_msg {
                // This is the first code that will run in the actor. It spawns the Game actors,
                // registers them to its monitoring list, then sends them a start signal
                SystemMsg::Start => {
                    for _ in 0..self.total_games {
                        let aid = ctx.system.spawn(Game::default(), Game::play);
                        ctx.system.monitor(&ctx.aid, &aid);
                        aid.send_new(ctx.aid.clone());
                    }
                },
                // This code runs each time a monitored actor stops. Once all Game actors are finished,
                // the actor system will be shut down.
                SystemMsg::Stopped(_) => {
                    self.games_finished += 1;
                    if self.games_finished == self.total_games {
                        println!("Stopping `GameResults`");
                        println!("{:#?}", self.results);
                        ctx.system.trigger_shutdown();
                    }
                }
                _ => {}
            }
        }
        Status::Processed
    }
}

const NUM_GAMES: u32 = 5;

fn main() {
    // Initialize the actor system
    let config = ActorSystemConfig {
        work_channel_size: 16,
        threads_size: 4,
        thread_wait_time: 10,
    };
    let system = ActorSystem::create(config);

    // Spawn the results aggregator, which will in turn spawn the Game actors.
    system.spawn(GameResults::new(NUM_GAMES), GameResults::gather);

    // Wait for the actor system to shut down.
    system.await_shutdown();
}