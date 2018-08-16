use base::*;
use std::sync::Arc;
use std::sync::Mutex;

mod base {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    pub trait Processor {
        fn process(&mut self, value: i32);

        fn get_parent(&self) -> &Parent;
        fn get_parent_mut(&mut self) -> &mut Parent;

        fn send(&mut self, message: i32) {
            self.get_parent_mut().queue.push(message);
            self.process(message);
        }

        fn id(&self) -> i64 {
            self.get_parent().id
        }
    }

    #[derive(Debug)]
    pub struct Parent {
        id: i64,
        queue: Vec<i32>,
    }

    impl Parent {
        pub fn id(&self) -> i64 {
            self.id
        }

        fn new() -> Parent {
            Parent {
                id: 123456,
                queue: Vec::new(),
            }
        }
    }

    pub struct Manager {
        processors_by_id: HashMap<i64, Arc<Mutex<Processor>>>,
    }

    impl Manager {
        pub fn new() -> Manager {
            Manager {
                processors_by_id: HashMap::new(),
            }
        }

        pub fn processor_count(&self) -> usize {
            self.processors_by_id.len()
        }

        pub fn new_processor(&mut self, processor: Arc<Mutex<Processor>>) {
            self.processors_by_id
                .insert(processor.lock().unwrap().id(), processor.clone());
        }
    }
}

#[derive(Debug)]
pub struct ProcessorImpl {
    parent: Parent,
    state: i32,
}

impl Processor for ProcessorImpl {
    fn process(&mut self, value: i32) {
        self.state += value;
        // FIXME: How can I reference the ID in here?
        println!("state: {}", self.state);
    }

    fn get_parent(&self) -> &Parent {
        &self.parent
    }
    fn get_parent_mut(&mut self) -> &mut Parent {
        &mut self.parent
    }
}

fn main() {
    let container = Arc::new(Mutex::new(ProcessorImpl {
        parent: base::Parent::new(),
        state: 0,
    }));
    let mut manager = base::Manager::new();
    manager.new_processor(container.clone());
    let clone = container.clone();
    let mut c = clone.lock().unwrap();
    println!("processors: {}", manager.processor_count());
    c.send(10);
    c.send(3);
}
