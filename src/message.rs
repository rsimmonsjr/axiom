use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::{Arc, RwLock};

pub trait ActorMessage: Send + Sync + Any {
    /// Get a JSON representation of `self`. Note that the default implementation uses
    /// `serde_json` to do the conversion.
    fn to_json(&self) -> String;
}

// TODO Take off pub from methods if possible and remove downcast_ref method if unused?
impl dyn ActorMessage {
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        if TypeId::of::<T>() == self.type_id() {
            unsafe { Some(&*(self as *const Self as *const T)) }
        } else {
            None
        }
    }

    fn downcast<T: ActorMessage>(self: Arc<Self>) -> Option<Arc<T>> {
        if TypeId::of::<T>() == (*self).type_id() {
            unsafe {
                let clone = self.clone();
                let ptr = Arc::into_raw(clone) as *const T;
                let converted: Arc<T> = Arc::from_raw(ptr);
                Some(converted.clone())
            }
        } else {
            None
        }
    }
}

impl<T: 'static> ActorMessage for T
where
    T: Serialize + DeserializeOwned + Sync + Send + Any + ?Sized,
{
    fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

/// The message content in a message.
pub enum MessageContent {
    /// The message is a local message.
    Local(Arc<dyn ActorMessage + 'static>),
    /// The message is from remote and has the given hash of a [`std::any::TypeId`] and the
    /// serialized content.
    /// FIXME Don't use JSON here in production.
    Remote(u64, String),
}

impl Serialize for MessageContent {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        panic!("Not implemented yet")
        /*match *self {
            Local(ref actor_message) => serializer.serialize_some(value),
            None => serializer.serialize_none(),
        }*/
    }
}

impl<'de> Deserialize<'de> for MessageContent {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        panic!("Not Implemented!!!")
    }
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    content: RwLock<MessageContent>,
}

impl Message {
    /// Creates a new message from a value, transferring ownership to the message.
    ///
    /// # Examples
    /// ```rust
    /// use axiom::message::Message;
    ///
    /// let msg = Message::new(11);
    /// ```
    pub fn new<T>(value: T) -> Message
    where
        T: 'static + ActorMessage,
    {
        Message {
            content: RwLock::new(MessageContent::Local(Arc::new(value))),
        }
    }

    /// Creates a new message from an [`Arc`], transferring ownership of the Arc to the message.
    ///
    /// # Examples
    /// ```rust
    /// use axiom::message::Message;
    /// use std::sync::Arc;
    ///
    /// let arc = Arc::new(11);
    /// let msg = Message::new(arc);
    /// ```
    pub fn from_arc<T>(value: Arc<T>) -> Message
    where
        T: 'static + ActorMessage,
    {
        Message {
            content: RwLock::new(MessageContent::Local(value.clone())),
        }
    }

    /// A helper that will return the hash of the type id for `T`.
    fn hash_type_id<T: 'static>() -> u64 {
        let mut hasher = DefaultHasher::new();
        TypeId::of::<T>().hash(&mut hasher);
        hasher.finish()
    }

    /// Get the content as an [`Arc<T>`]. If this fails a `None` will be returned.  Note that
    /// the user need not worry whether the message came from a local or remote source as the
    /// heavy lifting for that is done internally. The first successful attempt to downcast a
    /// remote message will result in the value being converted to a local message.
    ///
    /// # Examples
    /// ```rust
    /// use axiom::message::Message;
    /// use std::sync::Arc;
    ///
    /// let value = 11 as i32;
    /// let msg = Message::new(value);
    /// assert_eq!(value, *msg.content_as::<i32>().unwrap());
    /// assert_eq!(None, msg.content_as::<u32>());
    /// ```
    pub fn content_as<T>(&self) -> Option<Arc<T>>
    where
        T: 'static + ActorMessage + DeserializeOwned + ?Sized,
    {
        // We first have to figure out if the content is local or remote because they have
        // vastly different implications.
        let read_guard = self.content.read().unwrap();
        match &*read_guard {
            // If the content is local then we need to downcast to the arc of the required type.
            // This should fail fast if the type ids don't match.
            MessageContent::Local(content) => content.clone().downcast::<T>(),
            MessageContent::Remote(type_id_hash, _) => {
                // Remote content means it was serialized from elsewhere and we need to try to
                // deserialize it if possible but first we will cross check the type the user
                // wants so that we can fail fast if they don't match.
                if *type_id_hash != Message::hash_type_id::<T>() {
                    None
                } else {
                    // Since we have to change the content to be local, we have to drop the
                    // read lock and try to re-acquire a write.
                    drop(read_guard);
                    let mut write_guard = self.content.write().unwrap();
                    match &*write_guard {
                        // Someone beat us to the write and now we just downcast normally.
                        MessageContent::Local(content) => content.clone().downcast::<T>(),
                        // We got the write lock and the content is still remote.
                        MessageContent::Remote(_, content) => {
                            match serde_json::from_str::<T>(&content) {
                                Ok(concrete) => {
                                    // We deserialize the content and replace it in the message
                                    // with a new local variant.
                                    let new_value: Arc<T> = Arc::new(concrete);
                                    *write_guard = MessageContent::Local(new_value.clone());
                                    drop(write_guard);
                                    Some(new_value.clone())
                                }
                                Err(err) => {
                                    // The only reason this should happen is if the type id hash
                                    // check is somehow broken so we will want to fix it.
                                    panic!("Deserialization shouldn't have failed: {:?}", err)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn to_json(&self) -> String {
        let read_guard = self.content.read().unwrap();
        match &*read_guard {
            MessageContent::Local(m) => m.to_json(),
            MessageContent::Remote(_type_id, m) => m.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Testing helper to create an actor message from a value.
    fn new_actor_msg<T>(value: T) -> Arc<dyn ActorMessage>
    where
        T: 'static + ActorMessage,
    {
        Arc::new(value)
    }

    #[test]
    fn test_actor_message_downcast_ref() {
        let value = 11 as i32;
        let msg = new_actor_msg(value);
        assert_eq!(value, *msg.downcast_ref::<i32>().unwrap());
        assert_eq!(None, msg.downcast_ref::<u32>());
    }

    #[test]
    fn test_actor_message_downcast() {
        let value = 11 as i32;
        let msg = new_actor_msg(value);
        assert_eq!(value, *msg.clone().downcast::<i32>().unwrap());
        assert_eq!(None, msg.downcast::<u32>());
    }

    #[test]
    fn test_message_new() {
        let value = 11 as i32;
        let msg = Message::new(value);
        let read_guard = msg.content.read().unwrap();
        match &*read_guard {
            MessageContent::Remote(_, _) => assert!(false, "Expected a Local variant."),
            MessageContent::Local(content) => {
                assert_eq!(value, *content.clone().downcast::<i32>().unwrap());
            }
        }
    }

    #[test]
    fn test_message_from_arc() {
        let value = 11 as i32;
        let arc = Arc::new(value);
        let msg = Message::from_arc(arc.clone());
        let read_guard = msg.content.read().unwrap();
        match &*read_guard {
            MessageContent::Remote(_, _) => assert!(false, "Expected a Local variant."),
            MessageContent::Local(content) => {
                assert_eq!(value, *content.clone().downcast::<i32>().unwrap());
                assert!(Arc::ptr_eq(
                    &arc,
                    &content.clone().downcast::<i32>().unwrap()
                ));
            }
        }
    }

    #[test]
    fn test_message_downcast() {
        let value = 11 as i32;
        let msg = Message::new(value);
        assert_eq!(value, *msg.content_as::<i32>().unwrap());
        assert_eq!(None, msg.content_as::<u32>());
    }

    #[test]
    fn test_remote_to_local() {
        // TODO Create and test a new_remote for Message only if we need that for serialization.
        let value = 11 as i32;
        let serialized = serde_json::to_string(&value).unwrap();
        let hash = Message::hash_type_id::<i32>();
        let content = MessageContent::Remote(hash, serialized.clone());
        let msg = Message {
            content: RwLock::new(content),
        };

        {
            // A failure to downcast should leave the message as it is.
            assert_eq!(None, msg.content_as::<u32>());
            let read_guard = msg.content.read().unwrap();
            match &*read_guard {
                MessageContent::Local(_) => assert!(false, "Expected a Remote variant."),
                MessageContent::Remote(tid_hash, content) => {
                    assert_eq!(hash, *tid_hash);
                    assert_eq!(serialized, *content);
                }
            }
        }

        {
            // We will try to downcast the message to the proper type which should work and
            // convert the message to a local variant.
            assert_eq!(value, *msg.content_as::<i32>().unwrap());

            // Now we test to make sure that it indeed got converted.
            let read_guard = msg.content.read().unwrap();
            match &*read_guard {
                MessageContent::Remote(_, _) => assert!(false, "Expected a Local variant."),
                MessageContent::Local(content) => {
                    assert_eq!(value, *content.clone().downcast::<i32>().unwrap());
                }
            }
        }
    }
}
