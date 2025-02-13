use crate::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessState {
    pub counter: u64,
}

impl State for ProcessState {
    fn new() -> Self {
        Self { counter: 0 }
    }
}
