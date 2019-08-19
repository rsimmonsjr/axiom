//! An attempt at making something like ROS, except in Rust.
//!
//! TODO: Replace all send's with try_send()
//!
//!
//! TODO: .......I just realized I have absolutely no idea how to do timing for messages.
//! It must be possible, 'cause ROS does it, but I think the roscore does
//! some timing mediation there.  How?
//! For this, if all things are running in the same process they WILL have
//! the same clock, but in the future that may not happen!
//! I guess?
//! Maybe in ROS the ros core sends a constant timing signal and all others
//! listen to that?  That sounds inefficient.

use axiom::*;
use std::thread;
use std::time;
// use std::sync::Arc;

// I'm mainly using `im` here for fun, 'cause I want to see how well it works out.
// Might be useful for sharing messages across threads easily, maybe?  idk, message data
// are all Arc'ed anyway.
use im;

use serde::{/*de::DeserializeOwned,*/ Deserialize, Serialize};

//pub mod logger;

// To do: LOGGING, Monitoring, message replay, timing
// node names, node listing/registration/etc, supervision trees,
//

/// For now, topics are merely strings.  They may become more sophisticated
/// someday.
type Topic = String;

/// Hmmm.  I don't particularly like this database-y method of
/// storing topic<->publisher<->subscriber
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PubsubRelation {
    topic: Topic,
    actor: ActorId,
}

/// A control message of some kind sent by a publisher or subscriber to the `Herder`.
///
/// TODO: Supervisors?  Probably eventually.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum SubMsg {
    /// Notification that the actor wishes to receive messages sent to a topic.
    Subscribe(Topic, ActorId),
    /// What it says on the tin.
    Unsubscribe(Topic, ActorId),
    /// A request for all the nodes subscribing to a given topic
    Subscribers(Topic, ActorId),
}

/// A control message sent to a publisher by the `Herder`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum PubMsg {
    Subscribers(im::HashSet<ActorId>),
}

/// A message sent between nodes/actors.  All messages have a time
/// attached.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct Msg /*<T> where T: Clone + Debug + PartialEq + Eq + std::hash::Hash + Serialize + Deserialize<'static> + 'static*/
{/*data: T,*/}

/// State for, the supervisor node that coordinates
/// communication between disparate actors.
///
/// Really we end up with a pubsub system: Subscribers contact the `Herder`
/// saying what topics they are interested in.  Publishers contact it
/// saying what topics they provide.  The publishers then get informed of
/// new subscribers adding or leaving the system.
///
/// It must be able to:
///  * List topics
///  * List nodes subscribing to a topic (or topics a node is subscribed to)
///  * List nodes publishing to a topic (or topics a node is publishing)
/// ...wait, we DON'T actually need to know what topics nodes are publishing to,
/// do we?  It might be useful for debugging but it's not actually necessary,
/// right?
///
/// I dunno.  For now we stick to it I guess.
pub struct Herder {
    _publishers: im::HashMap<Topic, im::HashSet<ActorId>>,
    subscribers: im::HashMap<Topic, im::HashSet<ActorId>>,
}

impl Herder {
    pub fn new() -> Self {
        Self {
            _publishers: im::HashMap::new(),
            subscribers: im::HashMap::new(),
        }
    }

    /// Marks the given actor as subscribed to the given topic.
    /// Idempotent.
    /// TODO: Is subscribing twice an error?
    fn subscribe(&mut self, topic: Topic, aid: ActorId) {
        // ...I COULD use `HashSet::insert()` here to mutably update the hashset.
        // I am so confused.
        self.subscribers =
            self.subscribers
                .update_with(topic, im::hashset![aid], |old_val, new_val| {
                    old_val.union(new_val)
                });
    }

    /// Removes the given actor from the list of subscribers.
    /// Idempotent.
    /// TODO: Is unsubscribing twice an error?
    /// TODO: This feels graceless.
    fn unsubscribe(&mut self, topic: Topic, aid: ActorId) {
        self.subscribers =
            self.subscribers
                .update_with(topic, im::hashset![], |old_val, new_val| {
                    old_val.difference(new_val.update(aid))
                });
    }

    pub fn subscribers(&self, topic: &Topic) -> im::HashSet<ActorId> {
        self.subscribers
            .get(topic)
            .cloned()
            .unwrap_or_else(im::HashSet::new)
    }

    /// Handle an incoming message of any type.
    fn handle(&mut self, _my_aid: ActorId, message: &Message) -> Status {
        if let Some(m) = message.content_as::<SubMsg>() {
            match *m {
                SubMsg::Subscribe(ref topic, ref aid) => {
                    self.subscribe(topic.to_string(), aid.clone())
                }
                SubMsg::Unsubscribe(ref topic, ref aid) => {
                    self.unsubscribe(topic.to_string(), aid.clone())
                }
                SubMsg::Subscribers(ref topic, ref aid) => {
                    let subs = self.subscribers(topic);
                    println!("Subscribers sent: {:?}", &subs);
                    aid.send(Message::new(PubMsg::Subscribers(subs)))
                }
            }
        } else if let Some(m) = message.content_as::<SystemMsg>() {
            match *m {
                SystemMsg::Stop => {
                    // ...I think that we should instruct all actors to shut down?  Not sure.
                    ()
                }
                _ => (),
            }
        }
        Status::Processed
    }
}

pub struct Publisher {
    _topic: Topic,
    _herder: ActorId,
    known_subscribers: im::HashSet<ActorId>,
}

impl Publisher {
    fn new(topic: &Topic, herder: ActorId) -> Self {
        Self {
            _topic: topic.clone(),
            _herder: herder,
            known_subscribers: im::HashSet::new(),
        }
    }

    pub fn spawn_new(system: &ActorSystem, topic: &Topic, herder: &ActorId) -> ActorId {
        // Just ask for all subscribers to our topic first thing.
        let my_aid = system.spawn(Self::new(topic, herder.clone()), Self::handle);
        let msg = SubMsg::Subscribers(topic.clone(), my_aid.clone());
        herder.send(Message::new(msg));
        my_aid
    }

    pub fn publish<T>(&self, _msg: T) {
        for sub in self.known_subscribers.iter() {
            let m = Msg {
                // TODO: Figure out the Deserialize derive here
                //data: msg,
            };
            sub.send(Message::new(m))
        }
    }

    /// Handle an incoming message of any type.
    fn handle(&mut self, _aid: ActorId, message: &Message) -> Status {
        if let Some(m) = message.content_as::<PubMsg>() {
            match &*m {
                PubMsg::Subscribers(subs) => self.known_subscribers = subs.clone(),
            }
        }

        Status::Processed
    }
}

/// A node that listens for particular topic(s)
pub struct Subscriber {
    topics: im::HashSet<String>,
    herder: ActorId,
}

impl Subscriber {
    fn new(topics: im::HashSet<Topic>, herder: ActorId) -> Self {
        Self { topics, herder }
    }

    pub fn spawn_new(
        system: &ActorSystem,
        topics: &im::HashSet<Topic>,
        herder: &ActorId,
    ) -> ActorId {
        let logger = Self::new(topics.clone(), herder.clone());
        let my_aid = system.spawn(logger, Self::handle);
        // Subscribe to the topics we care about
        for t in topics.iter() {
            herder.send(Message::new(SubMsg::Subscribe(
                t.to_owned(),
                my_aid.clone(),
            )));
        }
        my_aid
    }

    /// TODO: Figure out message types???  Hmm, did not think of that...!
    fn handle(&mut self, my_aid: ActorId, message: &Message) -> Status {
        if let Some(_m) = message.content_as::<Msg>() {
            // TODO: Do stuff.
            unimplemented!()
        } else if let Some(m) = message.content_as::<SystemMsg>() {
            // Unsubscribe properly from topics when we are asked to shut down
            match *m {
                SystemMsg::Stop => {
                    for t in self.topics.iter() {
                        let msg = Message::new(SubMsg::Unsubscribe(t.to_owned(), my_aid.clone()));
                        self.herder.send(msg);
                    }
                }
                _ => (),
            }
        }

        Status::Processed
    }
}

fn main() {
    let topic = "test".to_string();
    let system = ActorSystem::create(ActorSystemConfig::default());
    system.init_current();

    let h = system.spawn(Herder::new(), Herder::handle);

    let p1 = Publisher::spawn_new(&system, &topic, &h);
    let s1 = Subscriber::spawn_new(&system, &im::hashset![topic.clone()], &h);
    let s2 = Subscriber::spawn_new(&system, &im::hashset![topic.clone()], &h);

    let testnode = system.spawn(
        0 as usize,
        move |_state: &mut usize, _aid: ActorId, message: &Message| {
            println!("Test node: {:?}", std::mem::size_of::<Message>());
            if let Some(m) = message.content_as::<PubMsg>() {
                match *m {
                    PubMsg::Subscribers(ref subs) => {
                        println!("Subscribers gotten: {:?}", subs);
                        // assert_eq!(subs, im::hashset![s1, s2])
                    }
                }
            }
            Status::Processed
        },
    );
    // Ask the herder for the list of subscribers
    let target = h.received() + 1;
    let msg = Message::new(SubMsg::Subscribers(topic, testnode));
    h.send(msg);

    while target > h.received() {}
    thread::sleep(time::Duration::from_secs(1));
    system.trigger_and_await_shutdown();
}
