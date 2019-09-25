//! This is an example of a parallel processing implementation of a Monte-Carlo simulation
//! The simulation is of a basic gambling game adapted from this page:
//! https://towardsdatascience.com/the-house-always-wins-monte-carlo-simulation-eb82787da2a3

use axiom::*;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Copy, Clone)]
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

    fn play(&mut self, ctx: &Context, msg: &Message) -> AxiomResult {
        // The game starts when this actor receives a go message from the GameResults actor
        if let Some(results_aid) = msg.content_as::<ActorId>() {
            let mut current_play = 1;
            let mut results_vec = Vec::new();
            while current_play <= self.total_plays {
                current_play += 1;
                match Game::roll_dice() {
                    true => self.funds += self.wager as i64,
                    false => self.funds -= self.wager as i64,
                }
                results_vec.push(self.funds);
            }
            results_aid
                .send_new(GameMsg::new(ctx.aid.clone(), results_vec))
                .unwrap();
            return Ok(Status::Stop);
        }
        Ok(Status::Done)
    }
}

impl Default for Game {
    fn default() -> Self {
        Self {
            funds: 10_000,
            wager: 100,
            total_plays: 100,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct GameMsg {
    aid: ActorId,
    results_vec: Vec<i64>,
}

impl GameMsg {
    fn new(aid: ActorId, vec: Vec<i64>) -> Self {
        Self {
            aid: aid,
            results_vec: vec,
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
    fn gather(&mut self, ctx: &Context, msg: &Message) -> AxiomResult {
        // Receive messages from the Game actors and aggregate their results
        if let Some(game_msg) = msg.content_as::<GameMsg>() {
            self.results
                .insert(game_msg.aid.clone(), game_msg.results_vec.clone());
        }

        if let Some(sys_msg) = msg.content_as::<SystemMsg>() {
            match &*sys_msg {
                // This is the first code that will run in the actor. It spawns the Game actors,
                // registers them to its monitoring list, then sends them a start signal
                SystemMsg::Start => {
                    let game_conditions = Game::default();
                    println!("Starting funds: ${}", game_conditions.funds);
                    println!("Wager per round: ${}", game_conditions.wager);
                    println!("Rounds per game: {}", game_conditions.total_plays);
                    println!("Running simulations...");
                    for i in 0..self.total_games {
                        let name = format!("Game{}", i);
                        let aid = ctx
                            .system
                            .spawn()
                            .name(&name)
                            .with(game_conditions, Game::play)
                            .unwrap();
                        ctx.system.monitor(&ctx.aid, &aid);
                        aid.send_new(ctx.aid.clone()).unwrap();
                    }
                }
                // This code runs each time a monitored actor stops. Once all Game actors are finished,
                // the actor system will be shut down.
                SystemMsg::Stopped(_) => {
                    self.games_finished += 1;
                    if self.games_finished == self.total_games {
                        let average_funds = self
                            .results
                            .values()
                            .map(|v| v.last().unwrap())
                            .sum::<i64>()
                            / self.total_games as i64;
                        println!("Simulations ran: {}", self.results.len());
                        println!("Final average funds: ${}", average_funds);
                        ctx.system.trigger_shutdown();
                    }
                }
                _ => {}
            }
        }
        Ok(Status::Done)
    }
}

const NUM_GAMES: u32 = 100;

fn main() {
    // Initialize the actor system
    // FIXME: We spawn an unreasonable number of worker threads here because that magically prevents
    // a deadlock from happening somehow. Fixing the source of the deadlock would be preferable.
    let mut config = ActorSystemConfig::default();
    config.work_channel_size = 110;
    config.threads_size = 4;
    config.message_channel_size = 210;

    let system = ActorSystem::create(config);

    // Spawn the results aggregator, which will in turn spawn the Game actors.
    system
        .spawn()
        .name("Manager")
        .with(GameResults::new(NUM_GAMES), GameResults::gather)
        .unwrap();

    // Wait for the actor system to shut down.
    system.await_shutdown();
}
