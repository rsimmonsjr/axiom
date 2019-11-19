# Release Notes

* 2019-09-27 0.1.0
  * A lot of breaking changes have been introduced in an effort to keep them all in one release
  so that the API can stabilize. Please see examples and other sources for help in integrating
  all of the changes listed below.
  * BREAKING CHANGE: `ActorId` has been renamed to `Aid` to facilitate communication and lower
  confusion between the `uuid` field in the `Aid` and the `Aid` itself.
  * BREAKING CHANGE: `Status::Processed` has been renamed to `Status::Done`.
  * BREAKING CHANGE: `Status::Skipped` has been renamed to `Status::Skip`.
  * BREAKING CHANGE: `Status::ResetSkip` has been renamed to `Status::Reset`.
  * BREAKING CHANGE: `ActorError` has been moved to top level and renamed to `AxiomError`.
  * BREAKING CHANGE: `find_by_name` and `find_by_uuid` have been removed from `Aid` as the
  mechanism for looking up actors doesn't make sense the way it was before.
  * BREAKING CHANGE: `MessageContent` was unintentionally public and is now private.
  * BREAKING CHANGE: Changed `Processor` to take a `&Context` rather than `Aid`.
  * BREAKING CHANGE: The `send`, `send_new` and `send_after` methods now return a result type
  that the user must manage.
  * BREAKING CHANGE: All actor processors now should return `AxiomResult` which will allow them
  to use the `?` syntax for all functions that return `AxiomError` and return their own errors.
  * BREAKING CHANGE: Actors are now spawned with the builder pattern. This allows the
  configuration of an actor and leaves the door open for future flexibility. See documentation
  for more details.
  * Created a `Context` type that holds references to the `Aid` and `ActorSystem`.
  * `Processor` functions can get a reference to the `Aid` of the actor from `Context`.
  * `Processor` functions can get a reference to the `ActorSystem` from `Context`.
  * The methods `find_aid_by_uuid` and `find_aid_by_name` are added to the `ActorSystem`.
  * Calling `system.init_current()` is unneeded unless deserializing `Aid`s outside a
  `Processor`.
  * Metrics methods like `received()` in `Aid` return `Result` instead of using `panic!`.
  * Changed internal maps to use crate `dashmap` which expands dependencies but increases
  performance.
  * New methods `send_new` and `send_new_after` are available to shorten boilerplate.
  * Added a named system actor, which is registered under the name `System`, that is started
  as the 1st actor in an `ActorSystem`.
  * Added a method `system_actor_aid` to easily look up the `System` actor.
  * Added additional configuration options to `ActorSystemConfig`.
  * System will warn if an actor takes longer than the configured `warn_threshold` to process a
  message.
  * Instead of processing one message per receive, the system will now process pending messages
  up until the configured `time_slice`, allowing optimized processing for quick messages.
  * The default `message_channel_size` for actors is now configurable for the actor system as
  a whole.
  * Instead of waiting forever on a send, the system will wait for the configured
  `send_timeout` before returning a timeout error to the caller.
* 2019-08-11: 0.0.7 
  * Simplified some of the casts and cleaned up code. 
  * Fixed issues related to major bug in fixed in `secc-0.0.9`
* 2019-08-11: 0.0.6 
  * Significant changes in API. Migration should be simple but see the examples for differences.
  * Implemented major serialization functionality for messages and ActorIds with serde.
  * Put in ground work for implementation of Remote actors.
  * Improved user ergonomics. 
* 2019-07-23: 0.0.5 
  * Various documentation and miscellaneous fixes.
  * Issue 11: Actors shouldn't panic if they cant schedule the actor.
  * Issue 16: Put in a means to allow an actor to monitor another actor.
  * Issue 25: Implement a means to lookup an Actor by its UUID.
  * Issue 27: Add the ability to register a local name for an actor.
  * Issue 29: Implement monitors for actors.
* 2019-07-18: 0.0.4-a
  * Maintenance release with some devops work, no features. 
* 2019-07-19: 0.0.3
  * I have added a large amount of copy edited documentation and fixed many spelling mistakes 
  * Fixed a couple of actual small bugs in the receiving of messages. 
* 2019-07-18: 0.0.2
  * Maintenance release with some devops work, no features. 
* 2019-07-18: 0.0.1
  * Initial release with basic actors, channel processing of messages and so on.
