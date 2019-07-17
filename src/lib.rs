pub mod actors;
pub mod secc;

#[cfg(test)]
mod tests {
    use log::LevelFilter;

    pub fn init_test_log() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .is_test(true)
            .try_init();
    }

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
