extern crate uuid;

pub mod actor_system;
pub mod actors;
pub mod mailbox;
pub mod pooled_queue;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
