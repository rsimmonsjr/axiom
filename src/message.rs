use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::{Any, TypeId};
use std::sync::Arc;

pub trait ActorMessage: Send + Sync + Any {
    /// Get a JSON representation of `self`. Note that the default implementation uses
    /// `serde_json` to do the conversion.
    fn to_json(&self) -> String;
}

impl dyn ActorMessage {
    fn downcast_ref<T: Any>(&self) -> Option<&T> {
        if TypeId::of::<T>() == self.type_id() {
            unsafe { Some(&*(self as *const Self as *const T)) }
        } else {
            None
        }
    }
}

impl<T: 'static> ActorMessage for T
where
    T: Serialize + DeserializeOwned + Sync + Send + ?Sized,
{
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

/// The message content in a message.
pub enum MessageContent {
    /// The message is a local message.
    Local(Arc<dyn ActorMessage + 'static>),
    /// The message is from remote and has the given type id and string.
    Remote(String),
}

pub struct Message {
    content: MessageContent,
}

impl Message {
    pub fn new<T>(value: T) -> Message
    where
        T: 'static + ActorMessage,
    {
        Message {
            content: MessageContent::Local(Arc::new(value)),
        }
    }

    pub fn from_arc<T>(value: Arc<T>) -> Message
    where
        T: 'static + ActorMessage,
    {
        Message {
            content: MessageContent::Local(value.clone()),
        }
    }

    /// Get the content as an arc to the given type. If this fails a `None` will be returned.
    pub fn content_as<T>(&self) -> Option<&T>
    where
        T: 'static + ActorMessage,
    {
        // We first have to figure out if the content is local or remote because they have
        // vastly different implications.
        match &self.content {
            MessageContent::Local(content) => {
                // If the content is local then we need to downcast the ref to the arc of
                // the required type. This should fail fast if the type ids don't match.
                content.downcast_ref::<T>()
            }
            MessageContent::Remote(_content) => {
                println!("====> Using Remote");
                None
                // Remote content means it was serialized from elsewhere and we need to try to
                // deserialize it if possible.
                // TODO Change the interior type to Local without requiring `&mut self` to allow
                // the need to deserialize it only once!
                /*
                if self.type_id != TypeId::of::<T>() {
                    // If the Type ids don't match, we want to fail fast!
                    // FIXME This ASSUMES that the type id is stable if code is compiled more than
                    // once on the same machine or if compiled only once and used on multiple
                    // machines. This needs to be verified!
                    None
                } else {
                    match serde_json::from_str::<T>(&content) {
                        Ok(concrete) => {
                            let result = Arc::new(concrete);
                            Some(result)
                        }
                        Err(_) => None,
                    }
                }
                */
            }
        }
    }

    pub fn to_json(&self) -> String {
        match &self.content {
            MessageContent::Local(m) => m.to_json(),
            MessageContent::Remote(m) => m.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Foo {
        content: Arc<i32>,
    }

    #[test]
    fn test_downcast() {
        let a = Arc::new(10 as i32);
        let msg_a = Message::from_arc(a);
        assert_eq!(10 as i32, *msg_a.content_as::<i32>().unwrap());

        let msg_b = Message::new(11 as u32);
        assert_eq!(11 as u32, *msg_b.content_as::<u32>().unwrap());
    }
}
