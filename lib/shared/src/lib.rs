use cronchware::*;
use kinode_process_lib::Address;
use process_macros::SerdeJsonInto;
use serde::{Deserialize, Serialize};

pub fn receiver_address_a() -> Address {
    ("our", "async-receiver-a", "async-app", "uncentered.os").into()
}

pub fn receiver_address_b() -> Address {
    ("our", "async-receiver-b", "async-app", "uncentered.os").into()
}

pub fn receiver_address_c() -> Address {
    ("our", "async-receiver-c", "async-app", "uncentered.os").into()
}

pub fn requester_address() -> Address {
    ("our", "async-requester", "async-app", "uncentered.os").into()
}

declare_types! {
    // We're redundant here just so we can demo and modify if we want
    Async {
        StepA String => i32
        StepB i32 => u64
        StepC u64 => String
        Gather () => Result<String, String>
    },
    AsyncNoExist {
        Gather () => Result<String, String>
    }
}

#[derive(Debug, Serialize, Deserialize, SerdeJsonInto, Clone)]
pub struct SomeStruct {
    pub counter: u64,
}

#[derive(Debug, Serialize, Deserialize, SerdeJsonInto, Clone)]
pub struct SomeOtherStruct {
    pub message: String,
}
