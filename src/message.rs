//! Defines the types associated with messages sent to actors.

use crate::AidError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::{Any, TypeId};
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::{Arc, RwLock};

/// This defines any value safe to send across threads as an ActorMessage.
pub trait ActorMessage: Send + Sync + Any {
    /// Gets a bincode serialized version of the message and returns it in a result or an error
    /// indicating what went wrong.
    fn to_bincode(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        Err(Box::new(AidError::CantConvertToBincode))
    }

    fn from_bincode(_data: &Vec<u8>) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        Err(Box::new(AidError::CantConvertFromBincode))
    }
}

impl dyn ActorMessage {
    fn downcast<T: ActorMessage>(self: Arc<Self>) -> Option<Arc<T>> {
        if TypeId::of::<T>() == (*self).type_id() {
            unsafe {
                let ptr = Arc::into_raw(self) as *const T;
                Some(Arc::from_raw(ptr))
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
    fn to_bincode(&self) -> Result<Vec<u8>, Box<dyn Error>> {
        let data = bincode::serialize(self)?;
        Ok(data)
    }

    fn from_bincode(data: &Vec<u8>) -> Result<Self, Box<dyn Error>> {
        let decoded: Self = bincode::deserialize(data)?;
        Ok(decoded)
    }
}

/// The message content in a message.
enum MessageContent {
    /// The message is a local message.
    Local(Arc<dyn ActorMessage + 'static>),
    /// The message is from remote and has the given hash of a [`std::any::TypeId`] and the
    /// serialized content.
    Remote(Vec<u8>),
}

impl Serialize for MessageContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MessageContent::Local(v) => {
                let data = v
                    .to_bincode()
                    .map_err(|e| serde::ser::Error::custom(format!("{}", e)))?;
                MessageContent::Remote(data).serialize(serializer)
            }
            MessageContent::Remote(content) => serializer.serialize_bytes(content),
        }
    }
}

impl<'de> Deserialize<'de> for MessageContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(MessageContent::Remote(Vec::<u8>::deserialize(
            deserializer,
        )?))
    }
}

/// Holds the data used in a message.
#[derive(Serialize, Deserialize)]
struct MessageData {
    /// The hash of the [`TypeId`] for the type used to construct the message.
    type_id_hash: u64,
    /// The content of the message in a RwLock. The lock is needed because if the message
    /// came from remote, it will need to be converted to a local message variant.
    content: RwLock<MessageContent>,
}

/// A type for a message sent to an actor channel.
///
/// Note that this type uses an internal [`Arc`] so there is no reason to surround it with
/// another [`Arc`] to make it thread safe.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    data: Arc<MessageData>,
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
            data: Arc::new(MessageData {
                type_id_hash: Message::hash_type_id::<T>(),
                content: RwLock::new(MessageContent::Local(Arc::new(value))),
            }),
        }
    }

    /// Creates a new message from an [`Arc`], transferring ownership of the Arc to the message.
    /// Note that this is more efficient if a user wants to send a message that is already an
    /// [`Arc`] so they dont create an arc inside an [`Arc`].
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
            data: Arc::new(MessageData {
                type_id_hash: Message::hash_type_id::<T>(),
                content: RwLock::new(MessageContent::Local(value)),
            }),
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
        T: 'static + ActorMessage,
    {
        // To make this fail fast we will first check against the hash of the type_id that the
        // user wants to convert the message content to.
        if self.data.type_id_hash != Message::hash_type_id::<T>() {
            None
        } else {
            // We first have to figure out if the content is Local or Remote because they have
            // vastly different implications.
            let read_guard = self.data.content.read().unwrap();
            match &*read_guard {
                // If the content is Local then we just downcast the arc type.
                // type. This should fail fast if the type ids don't match.
                MessageContent::Local(content) => content.clone().downcast::<T>(),
                // If the content is Remote then we will turn it into a Local.
                MessageContent::Remote(_) => {
                    // To convert the message we have to drop the read lock and re-acquire a
                    // write lock on the content.
                    drop(read_guard);
                    let mut write_guard = self.data.content.write().unwrap();
                    // Because of a potential race we will try again.
                    match &*write_guard {
                        // Another thread beat us to the write so we just downcast normally.
                        MessageContent::Local(content) => content.clone().downcast::<T>(),
                        // This thread got the write lock and the content is still remote.
                        MessageContent::Remote(content) => {
                            // We deserialize the content and replace it in the message with a
                            // new local variant.
                            match T::from_bincode(&content) {
                                Ok(concrete) => {
                                    let new_value: Arc<T> = Arc::new(concrete);
                                    *write_guard = MessageContent::Local(new_value.clone());
                                    drop(write_guard);
                                    Some(new_value)
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

    /// Tests the basic downcast functionality for an `ActorMessage` type which is owned by
    /// the `Message`.
    #[test]
    fn test_actor_message_downcast() {
        let value = 11 as i32;
        let msg = new_actor_msg(value);
        assert_eq!(value, *msg.clone().downcast::<i32>().unwrap());
        assert_eq!(None, msg.downcast::<u32>());
    }

    /// Tests that messages can be created with the `new` method and that they use `Local`
    /// content for the message.
    #[test]
    fn test_message_new() {
        let value = 11 as i32;
        let msg = Message::new(value);
        let read_guard = msg.data.content.read().unwrap();
        match &*read_guard {
            MessageContent::Remote(_) => panic!("Expected a Local variant."),
            MessageContent::Local(content) => {
                assert_eq!(value, *content.clone().downcast::<i32>().unwrap());
            }
        }
    }

    /// Tests that messages can be easily created from an `Arc` in an efficient manner without
    /// nested `Arc`s.
    #[test]
    fn test_message_from_arc() {
        let value = 11 as i32;
        let arc = Arc::new(value);
        let msg = Message::from_arc(arc.clone());
        let read_guard = msg.data.content.read().unwrap();
        match &*read_guard {
            MessageContent::Remote(_) => panic!("Expected a Local variant."),
            MessageContent::Local(content) => {
                let downcasted = content.clone().downcast::<i32>().unwrap();
                assert_eq!(value, *downcasted);
                assert!(Arc::ptr_eq(&arc, &downcasted));
            }
        }
    }

    /// Tests the basic downcast functionality for a `Message` type.
    #[test]
    fn test_message_downcast() {
        let value = 11 as i32;
        let msg = Message::new(value);
        assert_eq!(value, *msg.content_as::<i32>().unwrap());
        assert_eq!(None, msg.content_as::<u32>());
    }

    /// Tests that messages can be serialized and deserialized properly.
    #[test]
    fn test_message_serialization() {
        let value = 11 as i32;
        let msg = Message::new(value);
        let serialized = bincode::serialize(&msg).expect("Couldn't serialize.");
        let deserialized: Message =
            bincode::deserialize(&serialized).expect("Couldn't deserialize.");
        let read_guard = deserialized.data.content.read().unwrap();
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

    /// Tests that `Message`s with `MessageContent::Remote` values are converted to
    /// `MessageContent::Local` the first time that they are successfully downcasted.
    #[test]
    fn test_remote_to_local() {
        let value = 11 as i32;
        let local = Message::new(value);
        let serialized = bincode::serialize(&local).expect("Couldn't serialize.");
        let msg: Message = bincode::deserialize(&serialized).expect("Couldn't deserialize.");
        let hash = Message::hash_type_id::<i32>();
        {
            // A failure to downcast should leave the message as it is.
            assert_eq!(None, msg.content_as::<u32>());
            let read_guard = msg.data.content.read().unwrap();
            assert_eq!(hash, msg.data.type_id_hash);
            match &*read_guard {
                MessageContent::Local(_) => panic!("Expected a Remote variant."),
                MessageContent::Remote(content) => {
                    assert_eq!(bincode::serialize(&value).unwrap(), *content);
                }
            }
        }

        {
            // We will try to downcast the message to the proper type which should work and
            // convert the message to a local variant.
            assert_eq!(value, *msg.content_as::<i32>().unwrap());

            // Now we test to make sure that it indeed got converted.
            let read_guard = msg.data.content.read().unwrap();
            assert_eq!(hash, msg.data.type_id_hash);
            match &*read_guard {
                MessageContent::Remote(_) => panic!("Expected a Local variant."),
                MessageContent::Local(content) => {
                    assert_eq!(value, *content.clone().downcast::<i32>().unwrap());
                }
            }
        }
    }
}
