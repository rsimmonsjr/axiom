[![Latest version](https://img.shields.io/crates/v/axiom.svg)](https://crates.io/crates/axiom)
[![Build Status](https://api.travis-ci.org/rsimmonsjr/axiom.svg?branch=master)](https://travis-ci.org/rsimmonsjr/axiom)
[![Average time to resolve an issue](https://isitmaintained.com/badge/resolution/rsimmonsjr/axiom.svg)](https://isitmaintained.com/project/rsimmonsjr/axiom)
[![License](https://img.shields.io/crates/l/axiom.svg)](https://github.com/rsimmonsjr/axiom#license)

# Axiom 

Axiom brings a highly-scalable actor model to the Rust language based on the many lessons learned 
over years of Actor model implementations in Akka and Erlang. Axiom is, however, not a direct 
re-implementation of either of the two aforementioned actor models but rather a new 
implementation deriving inspiration from the good parts of those models.

### What's New
* 2019-08-11: 0.0.6 
  * Significant changes in API. Migration should be simple but see the examples for differences.
  * Implemented major serialization functionality for messages and ActorIds with serde.
  * Put in ground work for implementation of Remote actors.
  * Improved user ergonomics. 

[Release Notes for All Versions](https://github.com/rsimmonsjr/axiom/blob/master/RELEASE_NOTES.md)

# Getting Started

*An actor model is an architectural asynchronous programming paradigm characterized by the use
of actors for all processing activities.*

Actors have the following characteristics:
1. An actor can be interacted with only by means of messages.
2. An actor processes only one message at a time.
3. An actor will process a message only once.
4. An actor can send a message to any other actor without knowledge of that actor's internals.
5. Actors send only immutable data as messages, though they may have mutable internal state.
6. Actors are location agnostic; they can be sent a message from anywhere in the cluster.

Note that within the language of Rust, rule five can't be enforced by Rust but is a best practice
which is important for developers creating actors based on Axiom. In Erlang and Elixir rule
five cannot be violated because of the structure of the language but this also leads to
performance limitations. It's better to allow internal mutable state and encourage the good
practice of not sending mutable state as messages.

What is important to understand is that these rules combined together makes each actor operate
like a micro-service in the memory space of the program using them. Since actor messages are
immutable, actors can trade information safely and easily without copying large data
structures.

Although programming in the actor model is quite an involved process you can get started with
Axiom in only a few lines of code.

```rust
use axiom::*;
use std::sync::Arc;

let system = ActorSystem::create(ActorSystemConfig::default());
system.init_current(); // Needed to call from outside of actor system threads.

let aid = system.spawn(
    0 as usize,
    |_state: &mut usize, _aid: ActorId, message: &Message| Status::Processed,
 );

aid.send(Message::new(11));
```

This code creates an actor system, spawns an actor and finally sends the actor a message.
This example uses a closure but any static function with the right signature will work 
as well. 

If you want to create an actor with a struct that is simple as well. Let's create one that 
handles a couple of different message types:

```rust
use axiom::*;
use std::sync::Arc;

let system = ActorSystem::create(ActorSystemConfig::default());
system.init_current(); // Needed to call from outside of actor system threads.

struct Data {
    value: i32,
}

impl Data {
    fn handle_bool(&mut self, _aid: ActorId, message: &bool) -> Status {
        if *message {
            self.value += 1;
        } else {
            self.value -= 1;
        }
        Status::Processed // This assertion will fail but we still have to return.
    }

    fn handle_i32(&mut self, _aid: ActorId, message: &i32) -> Status {
        self.value += *message;
        Status::Processed // This assertion will fail but we still have to return.
    }

    fn handle(&mut self, aid: ActorId, message: &Message) -> Status {
        if let Some(msg) = message.content_as::<bool>() {
            self.handle_bool(aid, &*msg)
        } else if let Some(msg) = message.content_as::<i32>() {
            self.handle_i32(aid, &*msg)
        } else {
            assert!(false, "Failed to dispatch properly");
            Status::Stop // This assertion will fail but we still have to return.
        }
    }
}

let data = Data { value: 0 };
let aid = system.spawn( data, Data::handle);

aid.send(Message::new(11));
aid.send(Message::new(true));
aid.send(Message::new(true));
aid.send(Message::new(false));
```

This code creates an actor out of an arbitrary struct. Since the only requirement to make
an actor is to have a function that is compliant with the [`axiom::actors::Processor`] trait,
anything can be an actor. If this struct had been declared somewhere outside of your control
you could use it in an actor as state by declaring your own handler function and making the
calls to the 3rd party structure.

*It's important to keep in mind that the starting state is moved into the actor and you
will not have external access to it afterwards.* This is by design and although you could
conceivably use a `std::sync::Arc` to a structure as state, that would definitely be a bad
idea as it would break the rules we laid out for actors.

There is a lot more to learn and explore and your best resource is the test code for Axiom.
The developers have a belief that test code should be well architected and well commented to
act as a set of examples for users of Axiom.


## Design Principals of Axiom

Based on previous experience with other actor models I wanted to design Axiom around some
core principles: 
1. **At its core an actor is just an function that processes messages.** The simplest actor is a 
   function that takes a message and simply ignores it. The benefit to the functional approach 
   over the Akka model is that it allows the user to create actors easily and simply. This is 
   the notion of _micro module programming_; the notion of building a complex system from the 
   smallest components. Software based on the actor model can get complicated; keeping it simple
   at the core is fundamental to solid architecture.
2. **Actors can be a Finite State Machine (FSM).** Actors receive and process messages nominally
   in the order received. However, there are certain circumstances where an actor has to change
   to another state and process other messages, skipping certain messages to be processed later. 
3. **When skipping messages, the messages must not move.** Akka allows the skipping of messages
   by _stashing_ the message in another data structure and then restoring this stash later. This
   process has many inherent flaws. Instead Axiom allows an actor to skip messages in its
   channel but leave them where they are, increasing performance and avoiding many problems.
4. **Actors use a bounded capacity channel.** In Axiom the message capacity for the actor's 
   channel is bounded, resulting in greater simplicity and an emphasis on good actor design.
5. **Axiom should be kept as small as possible.** Axiom is the core of the actor model and 
   should not be expanded to include everything possible for actors. That should be the 
   job of libraries that extend Axiom. Axiom itself should be an example of _micro module
   programming_.
6. **The tests are the best place for examples.** The tests of Axiom will be extensive and
   well maintained and should be a resource for those wanting to use Axiom. They should not
   be a dumping ground for copy-paste or throwaway code. The best tests will look like 
   architected code.  
7. **A huge emphasis is put on crate user ergonomics.** Axiom should be easy to use.

