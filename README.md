# RAMP 

 **R**ust **A**ctor **M**odel **P**roject 
 
This library has the goal of bringing a highly scalable actor model to the Rust language based 
on the many lessons learned over years of Actor model implementations in Akka and Erlang. This 
library is, however, not a direct re-implementation of either of the two aforementioned actor 
models but rather a new implementation deriving inspiration from the good parts of those models. 

### What is an Actor Model 

An actor model is an architectural asynchrounous programming paradigm characterized by the use 
of actors for all processing activities. Actors have the following characteristics:
1. An actor can be interracted with only by means of messages.
2. An actor processes only one message at a time.
3. An actor will process a message only once.
4. An actor can send a message to any other actor without knowledge of that actor's internals.
5. Actors send only immutable data as messages, though they may have mutable internal state.
6. Actors are location agnostic. An actor can be sent a message from anywhere in the cluster.

Note that for the purposes of Rust, rule five cannot be enforced by this library but is important
for users of the library developing actors based on this library. In Erlang and Elixir this 
rule cannot be violated because of the structure of the language but that leads to limitations
as well because Erlang and Elixir actors cannot hold internal mutable state.

What is important to understand is that these rules combined together makes each actor operate 
like a microservice in the memory space of the program using them. Since actor messages are 
immutable, actors can trade information safely and easily without copying large data structures.

### Design Principals of RAMP

Based on previous experience with other actor models I wanted to design this library around some
core principles: 
1. **At its core an actor is just an function that processes messages.** The simplest actor is a 
   function that takes a message and simply ignores it. The benefit to the functional approach 
   over the Akka model is that it allows the user to create actors easily and simply. I like to
   refer to this _quantum programming_; the notion of building a complex system from the smallest 
   components. Software based on the actor model can get complicated; keeping it simple at the 
   core is fundamental to solid architecture.
2. **An actor can supervise other actors.** In the Erlang model there is a strong separation 
   between the actor, known as a process, and a supervisor which manages other Erlang processes. 
   This separation makes implementing certain kinds of applications cumbersome. This library, by 
   contrast, borrows from the Akka approach allowing any actor to supervise other actors and act
   as routers to those child actors. 
3. **Actors can be a Finite State Machine (FSM).** Actors recieve and process messages nominally
   in the order received. However, there are certain circumstances where an actor has to change
   to another state and process other messages, skipping certain messages to be processed later. 
   For example, consider an e-commerce application where a user can get their balance or make a 
   purchase. When making a purchase the `UserActor` may need information from the `InventoryActor` 
   actor to finish the transaction. While the `UserActor` waits for information from the 
   `InventoryActor` the `UserActor` will skip any purchase messages waiting for the information
   from the `InventoryActor` to arrive in its channel. Upon completion of the purchase, the 
   skip is reset and processing goes on normally.
4. **When skipping messages, the messages must not move.** Akka allows the skipping of messages
   by "stashing" the message in another data structure and then restoring this stash later. This
   process has many inherent flaws. Instead this library allows an actor to skip messages in its
   channel but leave them where they are, increasing performance and avoiding many problems.
5. **Actors use a bounded channel capacity.** In this library the channel capacity for the actor's
   channel is bounded but can be set by the user creating the actor. Avoiding allowing resizing
   the channel makes the channel simpler and forces the user to optimize how many messages are 
   sitting in the channel at any one time. Optimizing message flow is one of the core jobs of
   software architects implementing actor-based software. 
6. **This library should be kept as small as possible.** This library is the core of the actor
   model and should not be expanded to include everything possible for actors. That should be the 
   job of libraries that extend this library. The library itself shoult be an example of _quantum
   programming_.
7. **The tests are the best place for examples.** The tests of this library will be extensive and
   well maintained and should be a resource for those wanting to use the library. They should not
   be a dumping ground for copy-paste and throwaway code. The tests should be engineered as 
   software, not just thrown together. The best tests will look like architected code.  



