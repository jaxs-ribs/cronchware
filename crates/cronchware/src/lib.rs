use kinode_process_lib::get_state;
use kinode_process_lib::http::server::send_response;
use kinode_process_lib::http::server::WsMessageType;
use kinode_process_lib::http::StatusCode;
use kinode_process_lib::logging::info;
use kinode_process_lib::logging::init_logging;
use kinode_process_lib::logging::warn;
use kinode_process_lib::logging::Level;
use std::any::Any;
use std::collections::HashMap;
use std::cell::RefCell;
use kinode_process_lib::http::server::HttpServerRequest;

use kinode_process_lib::{
    await_message, homepage, http, kiprintln, set_state, LazyLoadBlob, Message, SendError,
};

pub mod prelude {
    pub use crate::HIDDEN_STATE;
    // Add other commonly used items here
}

thread_local! {
    pub static HIDDEN_STATE: RefCell<Option<HiddenState>> = RefCell::new(None);
}
// Enum defining the state persistance behaviour
#[derive(Clone)]
pub enum SaveOptions {
    // Never Persist State
    Never,
    // Persist State Every Message
    EveryMessage,
    // Persist State Every N Messages
    EveryNMessage(u64),
    // Persist State Every N Seconds
    EveryNSeconds(u64),
}
/// The application state containing the callback map plus the user state. This is the actual state.
pub struct HiddenState {
    pub pending_callbacks: HashMap<String, PendingCallback>,
    pub accumulators: HashMap<String, Box<dyn Any + Send>>,
    save_config: SaveOptions,
    message_count: u64,
    timer_active: bool,
}

pub struct PendingCallback {
    pub on_success: Box<dyn FnOnce(&[u8], &mut dyn Any) -> anyhow::Result<()> + Send>,
    pub on_timeout: Option<Box<dyn FnOnce(&mut dyn Any) -> anyhow::Result<()> + Send>>,
}

pub struct FanOutAggregator<T> {
    total: usize,
    results: Vec<Option<Result<T, anyhow::Error>>>,
    completed: usize,

    /// Called once, when completed == total
    on_done:
        Box<dyn FnOnce(Vec<Result<T, anyhow::Error>>, &mut dyn Any) -> anyhow::Result<()> + Send>,
}

impl<T> FanOutAggregator<T> {
    pub fn new(
        total: usize,
        on_done: Box<
            dyn FnOnce(Vec<Result<T, anyhow::Error>>, &mut dyn Any) -> anyhow::Result<()> + Send,
        >,
    ) -> Self {
        let mut results = Vec::with_capacity(total);
        for _ in 0..total {
            results.push(None);
        }

        Self {
            total,
            results,
            completed: 0,
            on_done,
        }
    }

    pub fn set_result(&mut self, i: usize, result: Result<T, anyhow::Error>) {
        if i < self.results.len() && self.results[i].is_none() {
            self.results[i] = Some(result);
            self.completed += 1;
        }
    }

    pub fn is_done(&self) -> bool {
        self.completed >= self.total
    }

    /// Finalize => calls on_done(...) with the final vector
    pub fn finalize(self, user_state: &mut dyn Any) -> anyhow::Result<()> {
        let FanOutAggregator {
            results, on_done, ..
        } = self;
        let final_vec = results
            .into_iter()
            .map(|item| item.unwrap_or_else(|| Err(anyhow::anyhow!("missing subresponse"))))
            .collect();

        (on_done)(final_vec, user_state)
    }
}

impl HiddenState {
    pub fn new(save_config: SaveOptions) -> Self {
        Self {
            pending_callbacks: HashMap::new(),
            accumulators: HashMap::new(),
            save_config,
            message_count: 0,
            timer_active: false,
        }
    }

    fn should_save_state(&mut self) -> bool {
        match self.save_config {
            SaveOptions::Never => false,
            SaveOptions::EveryMessage => true,
            SaveOptions::EveryNMessage(n) => {
                self.message_count += 1;
                if self.message_count >= n {
                    self.message_count = 0;
                    true
                } else {
                    false
                }
            }
            SaveOptions::EveryNSeconds(_) => false, // Handled by timer instead
        }
    }
}

#[macro_export]
macro_rules! timer {
    ($duration:expr, ($st:ident : $user_state_ty:ty) $callback_block:block) => {{
        use uuid::Uuid;

        let correlation_id = Uuid::new_v4().to_string();

        $crate::HIDDEN_STATE.with(|cell| {
            let mut hs = cell.borrow_mut();
            if let Some(ref mut hidden_state) = *hs {
                hidden_state.pending_callbacks.insert(
                    correlation_id.clone(),
                    $crate::PendingCallback {
                        on_success: Box::new(move |_resp_bytes: &[u8], any_state: &mut dyn std::any::Any| {
                            let $st = any_state
                                .downcast_mut::<$user_state_ty>()
                                .ok_or_else(|| anyhow::anyhow!("Downcast failed!"))?;
                            $callback_block
                            Ok(())
                        }),
                        on_timeout: None,
                    },
                );
            }
        });

        let total_timeout_seconds = ($duration / 1000) + 1;
        let _ = kinode_process_lib::Request::to(("our", "timer", "distro", "sys"))
            .body(kinode_process_lib::timer::TimerAction::SetTimer($duration))
            .expects_response(total_timeout_seconds)
            .context(correlation_id.as_bytes())
            .send();
    }};
}

/// Trait that must be implemented by application state types
pub trait State {
    /// Creates a new instance of the state.
    fn new() -> Self;
}

/// Initialize state from persisted storage or create new if none exists
fn initialize_state<S>() -> S
where
    S: State + for<'de> serde::Deserialize<'de>,
{
    match get_state() {
        Some(bytes) => match rmp_serde::from_slice::<S>(&bytes) {
            Ok(state) => state,
            Err(e) => {
                panic!("error deserializing existing state: {e}. We're panicking because we don't want to nuke state by setting it to a new instance.");
            }
        },
        None => {
            info!("no existing state, creating new one");
            S::new()
        }
    }
}

fn setup_server(
    ui_config: Option<&kinode_process_lib::http::server::HttpBindingConfig>,
    endpoints: &[Binding],
) -> http::server::HttpServer {
    let mut server = http::server::HttpServer::new(5);

    if let Some(ui) = ui_config {
        if let Err(e) = server.serve_ui("ui", vec!["/"], ui.clone()) {
            panic!("failed to serve UI: {e}. Make sure that a ui folder is in /pkg");
        }
    }

    // Verify no duplicate paths
    let mut seen_paths = std::collections::HashSet::new();
    for endpoint in endpoints.iter() {
        let path = match endpoint {
            Binding::Http { path, .. } => path,
            Binding::Ws { path, .. } => path,
        };
        if !seen_paths.insert(path) {
            panic!("duplicate path found: {}", path);
        }
    }

    for endpoint in endpoints {
        match endpoint {
            Binding::Http { path, config } => {
                server
                    .bind_http_path(path.to_string(), config.clone())
                    .expect("failed to serve API path");
            }
            Binding::Ws { path, config } => {
                server
                    .bind_ws_path(path.to_string(), config.clone())
                    .expect("failed to bind WS path");
            }
        }
    }

    server
}

fn handle_message<S, T1, T2, T3>(
    message: Message,
    user_state: &mut S,
    server: &mut http::server::HttpServer,
    handle_api_call: &impl Fn(&mut S, &str, T1),
    handle_local_request: &impl Fn(&Message, &mut S, &mut http::server::HttpServer, T2),
    handle_remote_request: &impl Fn(&Message, &mut S, &mut http::server::HttpServer, T3),
    handle_ws: &impl Fn(&mut S, &mut http::server::HttpServer, u32, WsMessageType, LazyLoadBlob),
) where
    S: State + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    T1: serde::Serialize + serde::de::DeserializeOwned,
    T2: serde::Serialize + serde::de::DeserializeOwned,
    T3: serde::Serialize + serde::de::DeserializeOwned,
{
    // Get the correlation id
    let correlation_id = message
        .context()
        .map(|c| String::from_utf8_lossy(c).to_string());

    if let Some(cid) = correlation_id {
        // --------------------------------------------------------------------------------------------
        // Regular callback case
        // --------------------------------------------------------------------------------------------
        let pending = HIDDEN_STATE.with(|cell| {
            let mut hs = cell.borrow_mut();
            hs.as_mut()
                .and_then(|state| state.pending_callbacks.remove(&cid))
        });
        if let Some(pending) = pending {
            // EXECUTE the callback
            if let Err(e) = (pending.on_success)(message.body(), user_state) {
                kiprintln!("Error in callback: {e}");
            }
            // Get a fresh borrow of user_state for serializing:
            if let Ok(s_bytes) = rmp_serde::to_vec(user_state) {
                let _ = set_state(&s_bytes);
            }
            return;
        }

        // --------------------------------------------------------------------------------------------
        // Fan out case
        // --------------------------------------------------------------------------------------------
        if let Some((agg_id, idx)) = parse_aggregator_cid(&cid) {
            let result = match serde_json::from_slice::<serde_json::Value>(message.body()) {
                Ok(val) => Ok(val),
                Err(e) => Err(anyhow::anyhow!(e)),
            };

            aggregator_mark_result(&agg_id, idx, result, user_state);
            return;
        }
    }

    if message.is_local() {
        if message.source().process == "http-server:distro:sys" {
            http_request(&message, user_state, server, handle_api_call, handle_ws);
        } else {
            local_request(&message, user_state, server, handle_local_request);
        }
    } else {
        remote_request(&message, user_state, server, handle_remote_request);
    }

    // Try to save state based on configuration
    maybe_save_state(user_state);
}

pub fn app<S, T1, T2, T3>(
    app_name: &str,
    app_icon: Option<&str>,
    app_widget: Option<&str>,
    ui_config: Option<kinode_process_lib::http::server::HttpBindingConfig>,
    endpoints: Vec<Binding>,
    save_config: SaveOptions,
    handle_api_call: impl Fn(&mut S, &str, T1),
    handle_local_request: impl Fn(&Message, &mut S, &mut http::server::HttpServer, T2),
    handle_remote_request: impl Fn(&Message, &mut S, &mut http::server::HttpServer, T3),
    handle_ws: impl Fn(&mut S, &mut http::server::HttpServer, u32, WsMessageType, LazyLoadBlob),
    init_fn: fn(&mut S),
) -> impl Fn()
where
    S: State + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    T1: serde::Serialize + serde::de::DeserializeOwned,
    T2: serde::Serialize + serde::de::DeserializeOwned,
    T3: serde::Serialize + serde::de::DeserializeOwned,
{
    HIDDEN_STATE.with(|cell| {
        let mut hs = cell.borrow_mut();
        *hs = Some(HiddenState::new(save_config.clone()));
    });

    if app_icon.is_some() && app_widget.is_some() {
        homepage::add_to_homepage(app_name, app_icon, Some("/"), app_widget);
    }

    // Set up timer if needed
    if let SaveOptions::EveryNSeconds(seconds) = save_config {
        setup_periodic_save_timer::<S>(seconds);
    }

    move || {
        init_logging(Level::DEBUG, Level::INFO, None, Some((0, 0, 1, 1)), None).unwrap();
        info!("starting app");

        let mut server = setup_server(ui_config.as_ref(), &endpoints);
        let mut user_state = initialize_state::<S>();

        // Execute the user specified init function
        // Note: It should always be called _after_ the hidden state is initialized, so that
        // users can call macros that depend on having the callback map in the hidden state.
        {
            init_fn(&mut user_state);
        }

        loop {
            match await_message() {
                Err(send_error) => {
                    handle_send_error::<S>(&send_error, &mut user_state);
                }
                Ok(message) => {
                    handle_message(
                        message,
                        &mut user_state,
                        &mut server,
                        &handle_api_call,
                        &handle_local_request,
                        &handle_remote_request,
                        &handle_ws,
                    );
                }
            }
        }
    }
}

fn http_request<S, T1>(
    message: &Message,
    state: &mut S,
    server: &mut http::server::HttpServer,
    handle_api_call: impl Fn(&mut S, &str, T1),
    handle_ws: impl Fn(&mut S, &mut http::server::HttpServer, u32, WsMessageType, LazyLoadBlob),
) where
    T1: serde::Serialize + serde::de::DeserializeOwned,
{
    let http_request = serde_json::from_slice::<http::server::HttpServerRequest>(message.body())
        .expect("failed to parse HTTP request");

    match http_request {
        HttpServerRequest::Http(http_request) => {

            let Ok(path) = http_request.path() else {
                warn!("Failed to get path for Http, exiting, this should never happen");
                send_response(StatusCode::BAD_REQUEST, None, vec![]);
                return;
            };
            let Some(blob) = message.blob() else {
                warn!("Failed to get blob for Http, exiting");
                send_response(StatusCode::BAD_REQUEST, None, vec![]);
                return;
            };
            let Ok(deserialized_struct) = serde_json::from_slice::<T1>(blob.bytes()) else {
                let body_str = String::from_utf8_lossy(blob.bytes());
                warn!("Raw request body was: {:#?}", body_str);
                warn!("Failed to deserialize into type parameter T1 (type: {}), check that the request body matches this type's structure", std::any::type_name::<T1>());
                send_response(StatusCode::BAD_REQUEST, None, vec![]);
                return;
            };

            handle_api_call(state, &path, deserialized_struct);
        }
        HttpServerRequest::WebSocketPush {
            channel_id,
            message_type,
        } => {
            let Some(blob) = message.blob() else {
                warn!("Failed to get blob for WebSocketPush, exiting");
                return;
            };
            handle_ws(state, server, channel_id, message_type, blob)
        }
        HttpServerRequest::WebSocketOpen { path, channel_id } => {
            server.handle_websocket_open(&path, channel_id);
        }
        HttpServerRequest::WebSocketClose(channel_id) => {
            server.handle_websocket_close(channel_id);
        }
    }
}

fn local_request<S, T>(
    message: &Message,
    state: &mut S,
    server: &mut http::server::HttpServer,
    handle_local_request: &impl Fn(&Message, &mut S, &mut http::server::HttpServer, T),
) where
    S: std::fmt::Debug,
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let Ok(request) = serde_json::from_slice::<T>(message.body()) else {
        if message.body() == b"debug" {
            kiprintln!("state:\n{:#?}", state);
        } else {
            warn!("Failed to deserialize local request into struct, exiting");
            let body_str = String::from_utf8_lossy(message.body());
            warn!("Raw request body was: {:#?}", body_str);
        }
        return;
    };
    handle_local_request(message, state, server, request);
}

fn remote_request<S, T>(
    message: &Message,
    state: &mut S,
    server: &mut http::server::HttpServer,
    handle_remote_request: &impl Fn(&Message, &mut S, &mut http::server::HttpServer, T),
) where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let Ok(request) = serde_json::from_slice::<T>(message.body()) else {
        warn!("Failed to deserialize remote request into struct, exiting");
        let body_str = String::from_utf8_lossy(message.body());
        warn!("Raw request body was: {:#?}", body_str);
        return;
    };
    handle_remote_request(message, state, server, request);
}

/// Pretty prints a SendError in a more readable format
fn pretty_print_send_error(error: &SendError) {
    let kind = &error.kind;
    let target = &error.target;

    // Try to decode body as UTF-8 string, fall back to showing as bytes
    let body = String::from_utf8(error.message.body().to_vec())
        .map(|s| format!("\"{}\"", s))
        .unwrap_or_else(|_| format!("{:?}", error.message.body()));

    // Try to decode context as UTF-8 string
    let context = error
        .context
        .as_ref()
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned());

    kiprintln!(
        "SendError {{
    kind: {:?},
    target: {},
    body: {},
    context: {}
}}",
        kind,
        target,
        body,
        context
            .map(|s| format!("\"{}\"", s))
            .unwrap_or("None".to_string())
    );
}

fn handle_send_error<S: Any + serde::Serialize>(send_error: &SendError, user_state: &mut S) {
    // Print the error
    pretty_print_send_error(send_error);

    // Get the correlation id
    let Some(correlation_id) = send_error
        .context
        .as_ref()
        .map(|bytes| String::from_utf8_lossy(bytes).to_string())
    else {
        return;
    };

    // --------------------------------------------------------------------------------------------
    // Handle the single callback case
    // --------------------------------------------------------------------------------------------
    let single_callback = HIDDEN_STATE.with(|cell| {
        let mut hs = cell.borrow_mut();
        hs.as_mut()
            .and_then(|st| st.pending_callbacks.remove(&correlation_id))
    });
    if let Some(single_callback) = single_callback {
        if let Some(cb) = single_callback.on_timeout {
            if let Err(e) = cb(user_state) {
                kiprintln!("Error in on_timeout callback: {e}");
            }
            // Persist the state
            if let Ok(s_bytes) = rmp_serde::to_vec(&user_state) {
                let _ = set_state(&s_bytes);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Handle the fan out case
    // --------------------------------------------------------------------------------------------
    if let Some((agg_id, idx)) = parse_aggregator_cid(&correlation_id) {
        aggregator_mark_result(&agg_id, idx, Err(anyhow::anyhow!("timeout")), user_state);

        if let Ok(s_bytes) = rmp_serde::to_vec(&user_state) {
            let _ = set_state(&s_bytes);
        }
    }
}

// Parse a correlation id of the form "agg_id:idx"
fn parse_aggregator_cid(corr: &str) -> Option<(String, usize)> {
    let mut parts = corr.split(':');
    let agg_id = parts.next()?.to_owned();
    let idx_str = parts.next()?;
    let idx = idx_str.parse::<usize>().ok()?;
    Some((agg_id, idx))
}

#[doc(hidden)]
#[macro_export]
macro_rules! __check_not_all_empty {
    (_, _, _, _, _) => {
        compile_error!("At least one handler must be defined. Cannot use '_' for all handlers.");
    };
    ($($any:tt)*) => {};
}

/// # How to Use the `erect!` Macro
///
/// The `erect!` macro is a powerful tool for defining and exporting a new component in your Kinode application.
/// It allows you to bundle together the component's metadata, UI configuration, endpoints, message handlers,
/// and initialization logic in one concise block. Below is a breakdown of each parameter:
///
/// - **name**:  
///   A string literal that represents the name of the component. This name may be used for display purposes in a UI
///   or in logging.
///
/// - **icon**:  
///   An optional parameter defining the component's icon. Pass `None` if you do not wish to specify one, or use
///   `Some(icon_identifier)` where `icon_identifier` describes your icon.
///
/// - **widget**:  
///   An optional widget attachment for your component. This could be a UI fragment or identifier. Use `None` if not needed.
///
/// - **ui**:  
///   An optional configuration of type `HttpBindingConfig`. This is used for setting up the UI binding for your component.
///   For example, using `Some(HttpBindingConfig::default())` applies the default UI configuration.
///
/// - **endpoints**:  
///   A list (array) of endpoints that your component will expose. Each endpoint is specified as a variant of the
///   `Binding` enum:
///     - `Binding::Http { path, config }`: Defines an HTTP API endpoint.  
///       * `path`: A string representing the URL path.  
///       * `config`: The associated HTTP binding configuration.
///     - `Binding::Ws { path, config }`: Defines a WebSocket endpoint.  
///       * `path`: A string representing the URL path.  
///       * `config`: The WebSocket-specific binding configuration.
///
/// - **save_config**:  
///   The save configuration for the component. This determines how often the state should be saved.
///   Available options are:
///   - `SaveOptions::Never`: State is never automatically saved
///   - `SaveOptions::EveryNSeconds(n)`: State is saved every n seconds
///   - `SaveOptions::OnChange`: State is saved after every modification
///
/// - **handlers**:  
///   A block for providing callbacks to handle incoming messages. These handlers correspond to various kinds
///   of requests:
///     - **api**: The handler for HTTP API calls. If your component does not require an API handler, you can
///       simply pass `_`.
///     - **local**: The handler function for processing local (internal) requests. This must be provided if your
///       component deals with internal messages (e.g., `kino_local_handler`).
///     - **remote**: The handler for remote requests. If not applicable, specify `_`.
///     - **ws**: The WebSocket message handler. Use `_` if no WebSocket handling is needed.
///
/// - **init**:  
///   The initialization function that sets up the component's state when the component starts. This function should
///   match the expected signature (taking a mutable reference to your state) and perform any necessary setup tasks.
///
/// - **wit_world**:  
///   The name of the WIT world file to generate. This should match the name of the WIT file in the `target/wit` directory.
///
/// - **additional_derives**:  
///   Additional derives to add to the generated code. Mostly not needed.
///
/// **Example Usage:**
///
/// The following example creates a component named **"Async Requester"** with one HTTP endpoint and one WebSocket endpoint:
///
/// ```rust:cronchware/src/lib.rs
/// erect!(
///     name: "Async Requester",
///     icon: None,
///     widget: None,
///     ui: Some(HttpBindingConfig::default()),
///     endpoints: [
///         Binding::Http {
///             path: "/api",
///             config: HttpBindingConfig::default(),
///         },
///         Binding::Ws {
///             path: "/updates",
///             config: WsBindingConfig::default(),
///         },
///     ],
///     save_config: SaveOptions::EveryNSeconds(10),
///     handlers: {
///         http: _, // The handler for HTTP API calls
///         local: kino_local_handler, // The handler for local kinode messages
///         remote: _, // The handler for remote kinode messages
///         ws: _, // The websocket handler
///     },
///     init: init_fn, // The init function that will get run first
///     wit_world: "async-app-template-dot-os-v0", // Your wit file name
///     additional_derives: [Debug, Clone] // Mostly not needed
/// );
/// ```
///
/// **Important:**  
/// - Make sure that the signatures of your handler functions (e.g., `kino_local_handler`) and the initialization
///   function (`init_fn`) match the expected types in the Kinode application framework.  
/// - Specifying `_` for any handler indicates that the corresponding functionality is not implemented or needed
///   for this component.
#[macro_export]
macro_rules! erect {
    (
        name: $name:expr,
        icon: $icon:expr,
        widget: $widget:expr,
        ui: $ui:expr,
        endpoints: [ $($endpoints:expr),* $(,)? ],
        save_config: $save_config:expr,
        handlers: {
            http: $http:tt,
            local: $local:tt,
            remote: $remote:tt,
            ws: $ws:tt,
        },
        init: $init:tt,
        wit_world: $wit_world:expr
        $(, additional_derives: [ $($additional_derives:path),* $(,)? ] )?
        $(,)?
    ) => {
        wit_bindgen::generate!({
            path: "target/wit",
            world: $wit_world,
            generate_unused_types: true,
            additional_derives: [
                serde::Deserialize,
                serde::Serialize,
                process_macros::SerdeJsonInto,
                $($($additional_derives,)*)?
            ],
        });

        $crate::__check_not_all_empty!($http, $local, $remote, $ws, $init);

        struct Component;
        impl Guest for Component {
            fn init(_our: String) {
                use cronchware::prelude::*;

                // Map `_` to the appropriate fallback function
                let handle_http_api_call = $crate::__maybe!($http => $crate::no_http_api_call);
                let handle_local_request = $crate::__maybe!($local => $crate::no_local_request);
                let handle_remote_request = $crate::__maybe!($remote => $crate::no_remote_request);
                let handle_ws = $crate::__maybe!($ws => $crate::no_ws_handler);
                let init_fn = $crate::__maybe!($init => $crate::no_init_fn);

                // Build the vector of endpoints from user input
                let endpoints_vec = vec![$($endpoints),*];

                let closure = $crate::app(
                    $name,
                    $icon,
                    $widget,
                    $ui,
                    endpoints_vec,
                    $save_config,
                    handle_http_api_call,
                    handle_local_request,
                    handle_remote_request,
                    handle_ws,
                    init_fn,
                );
                closure();
            }
        }
        export!(Component);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __declare_types_internal {
    (
        $(
            $outer:ident {
                $(
                    $variant:ident $req_ty:ty => $res_ty:ty
                )*
            }
        ),*
        $(,)?
    ) => {
        $crate::paste::paste! {
            #[derive(Debug, Serialize, Deserialize, SerdeJsonInto, Clone)]
            pub enum Req {
                $(
                    $outer([<$outer Request>]),
                )*
            }

            $(
                #[derive(Debug, Serialize, Deserialize, SerdeJsonInto, Clone)]
                pub enum [<$outer Request>] {
                    $(
                        $variant($req_ty),
                    )*
                }

                #[derive(Debug, Serialize, Deserialize, SerdeJsonInto, Clone)]
                pub enum [<$outer Response>] {
                    $(
                        $variant($res_ty),
                    )*
                }
            )*
        }
    };
}

#[macro_export]
macro_rules! declare_types {
    ($($tt:tt)*) => {
        $crate::__declare_types_internal! { $($tt)* }
    };
}

// Re-export paste for use in our macros
pub use paste;

pub fn aggregator_mark_result(
    aggregator_id: &str,
    i: usize,
    result: anyhow::Result<serde_json::Value>,
    user_state_any: &mut dyn Any,
) {
    // Indentationmaxxing
    HIDDEN_STATE.with(|cell| {
        if let Some(ref mut hidden) = *cell.borrow_mut() {
            if let Some(acc_any) = hidden.accumulators.get_mut(aggregator_id) {
                if let Some(agg) = acc_any.downcast_mut::<FanOutAggregator<serde_json::Value>>() {
                    agg.set_result(i, result);
                    if agg.is_done() {
                        let aggregator_box = hidden.accumulators.remove(aggregator_id).unwrap();
                        if let Ok(agg2) =
                            aggregator_box.downcast::<FanOutAggregator<serde_json::Value>>()
                        {
                            let aggregator = *agg2;
                            if let Err(e) = aggregator.finalize(user_state_any) {
                                kiprintln!("Error finalizing aggregator: {e}");
                            }
                        }
                    }
                }
            }
        }
    });
}

#[macro_export]
macro_rules! fan_out {
    (
        $addresses:expr,
        $requests:expr,
        ($all_results:ident, $st:ident : $st_ty:ty) $done_block:block,
        $timeout:expr
    ) => {{
        use ::anyhow::{anyhow, Result as AnyResult};
        use ::uuid::Uuid;
        use ::serde_json;

        // A unique aggregator ID for this fan-out sequence
        let aggregator_id = Uuid::new_v4().to_string();
        let total = $addresses.len();

        // Build the aggregator that will hold partial results and run your final callback
        let aggregator = $crate::FanOutAggregator::new(
            total,
            Box::new(move |results: Vec<AnyResult<serde_json::Value>>, user_state: &mut dyn std::any::Any| -> AnyResult<()> {
                let $st = user_state
                    .downcast_mut::<$st_ty>()
                    .ok_or_else(|| anyhow!("Downcast failed!"))?;

                let $all_results = results;
                $done_block
                Ok(())
            }),
        );

        // Insert this aggregator into HIDDEN_STATE's accumulators
        $crate::HIDDEN_STATE.with(|cell| {
            if let Some(ref mut hidden_state) = *cell.borrow_mut() {
                hidden_state
                    .accumulators
                    .insert(aggregator_id.clone(), Box::new(aggregator));
            }
        });

        // For each address+request, serialize/send, and if there's an immediate error, mark aggregator
        for (i, address) in $addresses.iter().enumerate() {
            let correlation_id = format!("{}:{}", aggregator_id, i);

            // Serialize request
            let body = match serde_json::to_vec(&$requests[i]) {
                Ok(b) => b,
                Err(e) => {
                    // Mark aggregator's i'th result as an error
                    $crate::HIDDEN_STATE.with(|cell| {
                        if let Some(ref mut hidden_st) = *cell.borrow_mut() {
                            $crate::aggregator_mark_result(
                                &aggregator_id,
                                i,
                                Err(anyhow!("Error serializing request: {}", e)),
                                hidden_st as &mut dyn std::any::Any,
                            );
                        }
                    });
                    continue;
                }
            };

            // Send the request - using fully qualified path
            let send_result = kinode_process_lib::Request::to(address)
                .body(body)
                .expects_response($timeout)
                .context(correlation_id.as_bytes())
                .send();

            // If the immediate send fails, also mark aggregator
            if let Err(e) = send_result {
                $crate::HIDDEN_STATE.with(|cell| {
                    if let Some(ref mut hidden_st) = *cell.borrow_mut() {
                        $crate::aggregator_mark_result(
                            &aggregator_id,
                            i,
                            Err(anyhow!("Send error: {:?}", e)),
                            hidden_st as &mut dyn std::any::Any,
                        );
                    }
                });
            }
        }
    }};
}

#[macro_export]
macro_rules! __maybe {
    // If the user wrote `_`, then expand to the default expression
    ( _ => $default:expr ) => {
        $default
    };
    // Otherwise use whatever they passed in
    ( $actual:expr => $default:expr ) => {
        $actual
    };
}

// For demonstration, we'll define them all in one place.
// Make sure the signatures match the real function signatures you require!
pub fn no_init_fn<S>(_state: &mut S) {
    // does nothing
}

pub fn no_ws_handler<S>(
    _state: &mut S,
    _server: &mut http::server::HttpServer,
    _channel_id: u32,
    _msg_type: http::server::WsMessageType,
    _blob: LazyLoadBlob,
) {
    // does nothing
}

pub fn no_http_api_call<S>(_state: &mut S, _path: &str, _req: ()) {
    // does nothing
}

pub fn no_local_request<S>(
    _msg: &Message,
    _state: &mut S,
    _server: &mut http::server::HttpServer,
    _req: (),
) {
    // does nothing
}

pub fn no_remote_request<S>(
    _msg: &Message,
    _state: &mut S,
    _server: &mut http::server::HttpServer,
    _req: (),
) {
    // does nothing
}

#[derive(Clone, Debug)]
pub enum Binding {
    Http {
        path: &'static str,
        config: kinode_process_lib::http::server::HttpBindingConfig,
    },
    Ws {
        path: &'static str,
        config: kinode_process_lib::http::server::WsBindingConfig,
    },
}
fn setup_periodic_save_timer<S>(seconds: u64) 
where
    S: State + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    HIDDEN_STATE.with(|cell| {
        let mut hs = cell.borrow_mut();
        if let Some(ref mut hidden_state) = *hs {
            if !hidden_state.timer_active {
                hidden_state.timer_active = true;
                timer!(
                    seconds * 1000,
                    (state: S) {
                        // Save state
                        if let Ok(s_bytes) = rmp_serde::to_vec(state) {
                            let _ = set_state(&s_bytes);
                        }
                        
                        // Set up next timer only if still in EveryNSeconds mode
                        HIDDEN_STATE.with(|cell| {
                            let mut hs = cell.borrow_mut();
                            if let Some(ref mut hidden_state) = *hs {
                                if let SaveOptions::EveryNSeconds(secs) = hidden_state.save_config {
                                    hidden_state.timer_active = false;
                                    // I am not sure what the
                                    kiprintln!("Setting up next timer for {} seconds", secs);
                                    setup_periodic_save_timer::<S>(secs);
                                }
                            }
                        });
                    }
                );
            }
        }
    });
}

fn maybe_save_state<S>(state: &S) 
where
    S: serde::Serialize,
{
    HIDDEN_STATE.with(|cell| {
        let mut hs = cell.borrow_mut();
        if let Some(ref mut hidden_state) = *hs {
            if hidden_state.should_save_state() {
                if let Ok(s_bytes) = rmp_serde::to_vec(state) {
                    kiprintln!("State persisted");
                    let _ = set_state(&s_bytes);
                }
            }
        }
    });
}