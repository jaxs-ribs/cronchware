use crate::*;

pub fn message_a() {
    // TODO: New async
    // send_async!(
    //     receiver_address_a(),
    //     AsyncRequest::StepA("Mashed Potatoes".to_string()),
    //     (resp: i32, st: ProcessState) {
    //         on_step_a(resp, st);
    //     },
    // );
}

// pub fn on_step_a(response: i32, state: &mut ProcessState) {
//     kiprintln!("Sender: Received response: {}", response);
//     kiprintln!("Sender: State: {}", state.counter);
//     state.counter += 1;
//     send_async!(
//         receiver_address_a(),
//         AsyncRequest::StepB(response),
//         (resp: u64, st: ProcessState) {
//             let _ = on_step_b(resp, st);
//         },
//     );
// }

// pub fn on_step_b(response: u64, state: &mut ProcessState) -> anyhow::Result<()> {
//     kiprintln!("Sender: Received response: {}", response);
//     kiprintln!("Sender: State: {}", state.counter);
//     state.counter += 1;
//     send_async!(
//         receiver_address_a(),
//         AsyncRequest::StepC(response),
//         (resp: String, st: ProcessState) {
//             on_step_c(resp, st);
//         },
//     );
//     Ok(())
// }

// pub fn on_step_c(response: String, state: &mut ProcessState) {
//     kiprintln!("Sender: Received response: {}", response);
//     kiprintln!("Sender: State: {}", state.counter);
//     state.counter += 1;
// }

// pub fn fanout_message() {
//     let addresses: Vec<Address> = vec![
//         ("our", "async-receiver-zzz", "async-app", "uncentered.os").into(), // Doesn't exist
//         receiver_address_a(),
//         receiver_address_b(),
//         receiver_address_c(),
//         ("our", "async-receiver-zzz123", "async-app", "uncentered.os").into(), // Doesn't exist
//     ];

//     let requests: Vec<AsyncRequest> = vec![
//         AsyncRequest::Gather(()),
//         AsyncRequest::Gather(()),
//         AsyncRequest::Gather(()),
//         AsyncRequest::Gather(()),
//         AsyncRequest::Gather(()),
//     ];

//     fan_out!(
//         addresses,
//         requests,
//         (all_results, st: ProcessState) {
//             kiprintln!("fan_out done => subresponses: {:#?}", all_results);
//             st.counter += 1;
//         },
//         5
//     );
// }

pub fn repeated_timer(state: &mut ProcessState) {
    kiprintln!("Repeated timer called! Our counter is {}", state.counter);
    timer!(3000, (st: ProcessState) {
        st.counter += 1;
        repeated_timer(st);
    });
}
