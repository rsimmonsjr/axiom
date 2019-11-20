use crate::prelude::*;
use crate::system::SystemActorMessage;
use log::{debug, error};

/// The system actor is a unique actor on the system registered with the name "System".
/// This actor provides core functionality that other actors will utilize.
pub(crate) struct SystemActor;

impl SystemActor {
    /// The processor function for the system actor.
    pub(crate) async fn processor(self, context: Context, message: Message) -> ActorResult<Self> {
        // Handle the SystemActorMessage.
        if let Some(msg) = message.content_as::<SystemActorMessage>() {
            // Someone requested that this system actor find an actor by name.
            if let SystemActorMessage::FindByName { reply_to, name } = &*msg {
                debug!("Attempting to locate Actor by name: {}", name);
                let found = context.system.find_aid_by_name(&name);
                let reply = Message::new(SystemActorMessage::FindByNameResult {
                    system_uuid: context.system.uuid(),
                    name: name.clone(),
                    aid: found,
                });
                // Note that you can't just unwrap or you could panic the dispatcher thread if
                // there is a problem sending the reply. In this case, the error is logged but the
                // actor moves on.
                reply_to.send(reply).unwrap_or_else(|error| {
                    error!(
                        "Could not send reply to FindByName to actor {}. Error: {:?}",
                        reply_to, error
                    )
                });
            }
            Ok((self, Status::Done))
        // Do nothing special if we get a SystemMsg.
        } else if let Some(_) = message.content_as::<SystemMsg>() {
            Ok((self, Status::Done))
        // Log an error if we get an unexpected message kind, but continue processing as normal.
        } else {
            error!("Unhandled message received.");
            Ok((self, Status::Done))
        }
    }
}
