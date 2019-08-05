use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::{Arc, RwLock};

pub trait ActorMessage: Send + Sync + Any {
    /// Get a JSON representation of `self`.
    fn to_json(&self) -> String;
}

impl dyn ActorMessage {
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
    Remote(String),
}

impl Serialize for MessageContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MessageContent::Local(v) => MessageContent::Remote(v.to_json()).serialize(serializer),
            MessageContent::Remote(content) => {
                // Replace hardcoded name with `core::intrinsics::type_name` when stable.
                serializer.serialize_str(content)
            }
        }
    }
}

impl<'de> Deserialize<'de> for MessageContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(MessageContent::Remote(String::deserialize(deserializer)?))
    }
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    /// The hash of the [`TypeId`] for the type used to construct the message.
    type_id_hash: u64,
    /// The content of the message in a RwLock. The lock is needed because if the message
    /// came from remote, it will need to be converted to a local message variant.
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
            type_id_hash: Message::hash_type_id::<T>(),
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
            type_id_hash: Message::hash_type_id::<T>(),
            content: RwLock::new(MessageContent::Local(value.clone())),
        }
    }

    /// A helper that will return the hash of the type id for `T`.
    #[inline]
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
        // To make this fail fast we will first check against the hash of the type_id that the
        // user wants to convert the message content to.
        if self.type_id_hash != Message::hash_type_id::<T>() {
            None
        } else {
            // We first have to figure out if the content is Local or Remote because they have
            // vastly different implications.
            let read_guard = self.content.read().unwrap();
            match &*read_guard {
                // If the content is Local then we just downcast the arc type.
                // type. This should fail fast if the type ids don't match.
                MessageContent::Local(content) => content.clone().downcast::<T>(),
                // If the content is Remote then we will turn it into a Local.
                MessageContent::Remote(_) => {
                    // To convert the message we have to drop the read lock and re-acquire a
                    // write lock on the content.
                    drop(read_guard);
                    let mut write_guard = self.content.write().unwrap();
                    // Because of a potential race we will try again.
                    match &*write_guard {
                        // Another thread beat us to the write so we just downcast normally.
                        MessageContent::Local(content) => content.clone().downcast::<T>(),
                        // This thread got the write lock and the content is still remote.
                        MessageContent::Remote(content) => {
                            // We deserialize the content and replace it in the message.
                            match serde_json::from_str::<T>(&content) {
                                Ok(concrete) => {
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
            MessageContent::Remote(m) => m.clone(),
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
            MessageContent::Remote(_) => assert!(false, "Expected a Local variant."),
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
            MessageContent::Remote(_) => assert!(false, "Expected a Local variant."),
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
    fn test_message_serialization() {
        let value = 11 as i32;
        let msg = Message::new(value);
        let serialized = serde_json::to_string_pretty(&msg).expect("Couldn't serialize.");
        let deserialized: Message =
            serde_json::from_str(&serialized).expect("Couldn't deserialize.");
        let read_guard = deserialized.content.read().unwrap();
        match &*read_guard {
            MessageContent::Local(_) => panic!("Expected a Remote variant."),
            MessageContent::Remote(_) => {
                drop(read_guard);
                match deserialized.content_as::<i32>() {
                    None => panic!("Could not cast content."),
                    Some(v) => assert_eq!(value, *v),
                }
            }
        }
    }

    #[test]
    fn test_remote_to_local() {
        let value = 11 as i32;
        let serialized = serde_json::to_string(&value).unwrap();
        let hash = Message::hash_type_id::<i32>();
        let content = MessageContent::Remote(serialized.clone());
        let msg = Message {
            type_id_hash: hash,
            content: RwLock::new(content),
        };

        {
            // A failure to downcast should leave the message as it is.
            assert_eq!(None, msg.content_as::<u32>());
            let read_guard = msg.content.read().unwrap();
            assert_eq!(hash, msg.type_id_hash);
            match &*read_guard {
                MessageContent::Local(_) => assert!(false, "Expected a Remote variant."),
                MessageContent::Remote(content) => {
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
            assert_eq!(hash, msg.type_id_hash);
            match &*read_guard {
                MessageContent::Remote(_) => assert!(false, "Expected a Local variant."),
                MessageContent::Local(content) => {
                    assert_eq!(value, *content.clone().downcast::<i32>().unwrap());
                }
            }
        }
    }
}
