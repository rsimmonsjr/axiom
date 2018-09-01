use actors::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub struct ActorSystem {
    /// Holds a table of actors in the system keyed by their actor id (aid).
    actors_by_aid: HashMap<ActorId, Arc<Mutex<Actor>>>,
    /// Holds an [`AtomicUsize`] that is incremented when creating each new actor to assign
    /// a unique id within this actor system to the actor.
    aid_sequence: AtomicUsize,
}

impl ActorSystem {
    pub fn new() -> ActorSystem {
        ActorSystem {
            // TODO do I need to give size hints for the table?
            actors_by_aid: HashMap::new(),
            aid_sequence: AtomicUsize::new(1),
        }
    }

    pub fn spawn<T: Actor + 'static, F: Fn(ActorContext) -> T>(
        &mut self,
        f: F,
    ) -> Arc<Mutex<Actor>> {
        // FIXME This should probably be hidden so a actor table can track the actor instead.
        // FIXME Implement a spawn() function in the ActorSystem to call this API
        // FIXME The actor needs to implement an ID assigned by the ActorSystem.
        // FIXME the spawn() function should move the dispatcher or have some means of making sure the user has no more access.
        // FIXME A new kind of channel is needed to support skipping messages and should be integrated into the actor.
        let aid = ActorId::new(0, self.aid_sequence.fetch_add(1, Ordering::Relaxed));
        let context = ActorContext::new(aid.clone());
        let actor = Arc::new(Mutex::new(f(context)));
        self.actors_by_aid.insert(aid, actor.clone());
        actor
    }
}
