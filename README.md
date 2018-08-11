# RAMP 

The **R**ust **A**ctor **M**odel **P**roject has the goal of bringing a highly scalable actor model to the Rust 
language based on many lessons learned over years of Actor model work on Akka and Erlang. This project is, however,
not a direct re-implementation of either of the two aforementioned actor models but rather a new model deriving
inspiration from the good parts of those project. 

### What is an Actor Model 

An actor model is a programming paradigm characterized by actors which all have the following principles.
1. An actor communicates with the outside **only** via immutable messages. 
2. An actor processes only a single message at a time. 

What is important to understand is that this makes each actor operate like a microservice in the VM. Since 
actor messages are immutable, they can achieve hivgh velocity and safety. 

### Design of RAMP

Based on previous experience with other actor models I wanted to design RAMP around some common core principles. 
1. A user sending a message to an actor need not know if the actor processes the message. It is the actor's job
   to determine if the message is processed, ignored or the actor fails completely. 
2. At its core an actor is just an function that processes messages. The simplest actor is a function that takes
   a message and simply ignores it. You can call this a DevNull actor. The benefit to this over the Akka model
   is that it allows the user to create actors at a very simple low level and could be considered something like 
   quantum programming. Actor model based software can get complicated and keeping it simple at the core is 
   important.
3. An actor can supervise other actors. In the Erlang model there is a strong separation between the actor, known
   as a process, and a supervisor. This makes some mechanisms cumbersome to implement and requires cross calling
   between supervisors and actors. In RAMP any actor can supervise other actors and will get messages based on 
   the lifecycel of those actors. 
4. Actors are sent messages as Arc<Any> values through a channel. Any message on that channel will be conceptually
   be processed in the order received except in the case where the actor does not process the message. In certain
   circumstances an actor will enter a state in which it needs to suspend processing of some messages and start 
   processing other messages. This means the channel to the actor needs to support look-ahead to get the messages
   being processed rather than taking messages in pure FIFO order. 
   