extern crate uuid;
#[macro_use]
extern crate lazy_static;

pub mod actors;
pub mod secc;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
