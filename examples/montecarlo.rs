//! This is an example of a parallel processing implementation of a Monte-Carlo simulation
//! The simulation is of a basic gambling game adapted from this page:
//! https://towardsdatascience.com/the-house-always-wins-monte-carlo-simulation-eb82787da2a3
//!
//! This example demonstrates:
//! * Using many instances of the same actor to run a simulation multiple times in parallel.
//! * Spawning a manager actor that itself spawns other actors.
//! * Using monitors to allow a manager actor to know when each of its child actors have completed
//!   their work.

use std::collections::HashMap;

use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};

use axiom::*;

/// Represents the state of a simplified gambling game as described on the website linked above.
#[derive(Debug, Copy, Clone)]
struct Game {
    /// The player's current funds. Funds are allowed to be negative since the player
    /// can potentially lose more money than they started with.
    funds: i64,
    /// How much money the player wagers per turn.
    wager: u32,
    /// The total number of game rounds that will be played.
    total_rounds: u32,
}

impl Game {
    /// This function performs a dice roll according to the rules of the simple gambling game.
    /// On average, the player will win their roll 49 out of 100 times, resulting in a house edge
    /// of 2%
    fn roll_dice() -> bool {
        let mut rng = thread_rng();
        match rng.gen_range(0, 101) {
            x if x > 51 => true,
            _ => false,
        }
    }

    /// This is the Processor function for the game actors.
    fn play(&mut self, ctx: &Context, msg: &Message) -> AxiomResult {
        // A game instance starts when the `GameManager` actor sends a message containing its `Aid`.
        // This allows this actor to send its results back to the manager once the game is complete.
        if let Some(results_aid) = msg.content_as::<Aid>() {
            // Set up some extra starting state for the game.
            let mut current_round = 1;
            let mut results_vec = Vec::new();

            // Play the game and record the amount of funds that the player has after each roll of the dice
            // in the results_vec.
            while current_round <= self.total_rounds {
                current_round += 1;
                match Game::roll_dice() {
                    true => self.funds += self.wager as i64,
                    false => self.funds -= self.wager as i64,
                }
                results_vec.push(self.funds);
            }

            // Now that the game is finished, the results of the game need to be reported
            // to the `GameManager`.
            results_aid
                .send_new(GameMsg::new(ctx.aid.clone(), results_vec))
                .unwrap();
            // Because the `GameManager` is monitoring this actor, sending the `Stop` status
            // will inform the manager that this game is now completed.
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
            total_rounds: 100,
        }
    }
}

/// The message type that `Game` instances send to the `GameManager` when they are finished
/// with their work.
#[derive(Debug, Serialize, Deserialize)]
struct GameMsg {
    /// The ID of the actor that sent this message. This is used by the `GameManager` to
    /// associate game results with the actor that created them.
    aid: Aid,
    /// This vec contains a history of the player's current funds after each dice roll during
    /// the game.
    results_vec: Vec<i64>,
}

impl GameMsg {
    fn new(aid: Aid, vec: Vec<i64>) -> Self {
        Self {
            aid: aid,
            results_vec: vec,
        }
    }
}

/// This actor's job is to spawn a number of `Game` actors and then aggregate the
/// results of each game that is played.
#[derive(Debug)]
struct GameManager {
    /// The number of games that have been played so far.
    games_finished: u32,
    /// The total number of games that are to be played.
    total_games: u32,
    /// The results of each finished game, keyed by `Game` actor ID
    results: HashMap<Aid, Vec<i64>>,
}

impl GameManager {
    fn new(total_games: u32) -> Self {
        Self {
            games_finished: 0,
            total_games: total_games,
            results: HashMap::new(),
        }
    }
}

impl GameManager {
    // This is the Processor function for the manager actor.
    fn gather_results(&mut self, ctx: &Context, msg: &Message) -> AxiomResult {
        // Receive messages from the Game actors and aggregate their results in a `HashMap`.
        if let Some(game_msg) = msg.content_as::<GameMsg>() {
            self.results
                .insert(game_msg.aid.clone(), game_msg.results_vec.clone());
        }

        if let Some(sys_msg) = msg.content_as::<SystemMsg>() {
            match &*sys_msg {
                // This is the first code that will run in the actor. It spawns the Game actors,
                // registers them to its monitoring list, then sends them a message indicating
                // that they should start their games.

                // The message contains the `Aid` of this actor, which the `Game` actors will use
                // to report their results back to this actor when they are finished.
                SystemMsg::Start => {
                    let game_conditions = Game::default();
                    println!("Starting funds: ${}", game_conditions.funds);
                    println!("Wager per round: ${}", game_conditions.wager);
                    println!("Rounds per game: {}", game_conditions.total_rounds);
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
                // This code runs each time a monitored `Game` actor stops. Once all the actors are
                // finished, the average final results of each game will be printed and then the
                // actor system will be shut down.
                SystemMsg::Stopped(_) => {
                    self.games_finished += 1;
                    if self.games_finished == self.total_games {
                        // Each vec of results contains the entire history of a game for every time that
                        // the dice was rolled. Instead of printing out all of that data, we will simply
                        // print the average of the funds that the player had at the end of each game.
                        let average_funds = self
                            .results
                            .values()
                            .map(|v| v.last().unwrap())
                            .sum::<i64>()
                            / self.total_games as i64;
                        println!("Simulations ran: {}", self.results.len());
                        println!("Final average funds: ${}", average_funds);

                        // We're all done here, time to shut things down.
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
    // Initialize the actor system.
    let config = ActorSystemConfig::default()
        .work_channel_size(110)
        .message_channel_size(210);

    let system = ActorSystem::create(config);

    // Spawn the results aggregator, which will in turn spawn the Game actors.
    system
        .spawn()
        .name("Manager")
        .with(GameManager::new(NUM_GAMES), GameManager::gather_results)
        .unwrap();

    // Wait for the actor system to shut down.
    system.await_shutdown();
}
