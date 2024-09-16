use dioxus::prelude::*;
use std::rc::Rc;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fmt;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::{EventSource, MessageEvent};

/// Represents the possible connection statuses for SSE streams.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
enum ConnectionStatus {
    Connected,
    Error,
    Unknown,
}

impl fmt::Display for ConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionStatus::Connected => write!(f, "Connected"),
            ConnectionStatus::Error => write!(f, "Error"),
            ConnectionStatus::Unknown => write!(f, "Unknown"),
        }
    }
}

impl Default for ConnectionStatus {
    fn default() -> Self {
        ConnectionStatus::Unknown
    }
}

/// Represents the possible states of a valve.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
enum ValveState {
    Open,
    Close,
    Error,
    Unknown,
}

impl Default for ValveState {
    fn default() -> Self {
        ValveState::Unknown
    }
}

/// Maps a string value to a `ValveState` enum variant.
fn map_to_valve_state(value: &str) -> ValveState {
    match value {
        "open" => ValveState::Open,
        "closed" => ValveState::Close,
        _ => ValveState::Unknown,
    }
}

/// Determines the color associated with a given status or state.
/// This is used for styling elements based on their status.
fn color_map<T: std::fmt::Debug>(status: &T) -> &'static str {
    match format!("{:?}", status).as_str() {
        "Connected" => "green",
        "Error" => "red",
        "Unknown" => "gray",
        "Open" => "green",
        "Close" => "red",
        _ => "gray",
    }
}

/// Main function to start the application.
/// This function initializes the application and mounts it to the DOM.
fn main() {
    // Initialize the application and mount it to the DOM.
    launch(app)
}

/// The main entry point for the application.
fn app() -> Element {
    // List of actions available for valves.
    let actions = vec![
        ("open".to_string(), "Open".to_string()),
        ("closed".to_string(), "Close".to_string()),
    ];

    // Base URL for API requests.
    let base_url = Rc::new("http://padstation-prod.local:8000".to_string());

    // State variables for various pressure and temperature readings.
    let supply_pt = use_signal(|| "".to_string());
    let fill_pt = use_signal(|| "".to_string());
    let tank_pt = use_signal(|| "".to_string());
    let test_stand_load = use_signal(|| "".to_string());
    let tank_tc = use_signal(|| "".to_string());

    // State variables for valve statuses.
    let fill_state = use_signal(|| ValveState::Unknown);
    let supply_state = use_signal(|| ValveState::Unknown);
    let pilot_state = use_signal(|| ValveState::Unknown);

    // State variables for selected options in dropdowns.
    let mut selected_fill_option = use_signal(|| "".to_string());
    let mut selected_supply_option = use_signal(|| "".to_string());
    let mut selected_pilot_option = use_signal(|| "".to_string());

    // State variables for SSE connection statuses.
    let fill_sse_status = use_signal(|| "".to_string());
    let supply_sse_status = use_signal(|| "".to_string());
    let tank_sse_status = use_signal(|| "".to_string());
    // let tank_tc_sse_status = use_signal(|| "".to_string());

    // State variable to track if logging is active.
    let is_logging = use_signal(|| false);

    // Set up SSE connections for each data stream.
    {
        // Clone state variables to move into the closure.
        let mut fill_pt = fill_pt.clone();
        let mut fill_sse_status = fill_sse_status.clone();
        let base_url = base_url.clone();

        use_effect(move || {
            let event_source =
                EventSource::new(&format!("{}/pressure/fill/datastream", base_url)).unwrap();

            let onmessage_callback = Closure::wrap(Box::new(move |event: MessageEvent| {
                if let Ok(data) = event.data().dyn_into::<js_sys::JsString>() {
                    fill_pt.set(data.as_string().unwrap_or_default());
                }
            }) as Box<dyn FnMut(_)>);

            let mut fill_sse_status_clone = fill_sse_status.clone();

            let onerror_callback = Closure::wrap(Box::new(move |_event: web_sys::Event| {
                fill_sse_status_clone.set(ConnectionStatus::Error.to_string());
            }) as Box<dyn FnMut(_)>);

            event_source.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
            event_source.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));

            // Prevent the callbacks from being garbage collected.
            onmessage_callback.forget();
            onerror_callback.forget();

            // Update the connection status to Connected.
            fill_sse_status.set(ConnectionStatus::Connected.to_string());

            // Return a cleanup function to close the EventSource when the component unmounts.
            Some(move || {
                event_source.close();
            });
        });
    }

    // Repeat similar `use_effect` blocks for supply_pt, tank_pt, tank_tc, and test_stand_load
    // Ensure each SSE connection is properly set up with cloned `base_url` and respective signals.
    // ...

    // Function to actuate valves based on the selected option.
    let actuate_valve = {
        let base_url = base_url.clone();
        move |valve_type: &str,
              selected_option: &Signal<String>,
              valve_state: &Signal<ValveState>| {
            let selected_option_value = selected_option().clone();
            if selected_option_value.is_empty() {
                return;
            }
            let url = match valve_type {
                "fill" => format!("{}/valve/engine?state={}", base_url, selected_option_value),
                "supply" => format!("{}/valve/relief?state={}", base_url, selected_option_value),
                "pilot" => format!(
                    "{}/pilot_valve/pilot_valve?state={}&timeout=5",
                    base_url, selected_option_value
                ),
                _ => return,
            };
            let mut valve_state = valve_state.clone(); // Clone outside async move
            spawn_local(async move {
                let client = Client::new();
                let resp = client.get(&url).send().await;
                match resp {
                    Ok(response) => {
                        if response.status().is_success() {
                            valve_state.set(map_to_valve_state(&selected_option_value));
                        } else {
                            valve_state.set(ValveState::Error);
                        }
                    }
                    Err(_) => {
                        valve_state.set(ValveState::Error);
                    }
                }
            });
        }
    };
    // Functions to handle logging and other actions.
    // Start Logging
    let start_logging = {
        let mut is_logging = is_logging.clone();
        let base_url = base_url.clone();
        move |_| {
            // Update the logging state to true.
            is_logging.set(true);
            let mut is_logging = is_logging.clone();
            let base_url = base_url.clone();
            spawn_local(async move {
                let client = Client::new();
                let url = format!("{}/log_data/start", base_url);
                let resp = client.get(&url).send().await;
                if resp.is_err() || !resp.unwrap().status().is_success() {
                    // If the request fails, set logging state to false.
                    is_logging.set(false);
                }
            });
        }
    };

    // Stop Logging
    let stop_logging = {
        let mut is_logging = is_logging.clone();
        let base_url = base_url.clone();
        move |_| {
            // Update the logging state to false.
            is_logging.set(false);
            let base_url = base_url.clone();
            spawn_local(async move {
                let client = Client::new();
                let url = format!("{}/log_data/stop", base_url);
                let _ = client.get(&url).send().await;
            });
        }
    };

    // Ignite Function
    let ignite = {
        let base_url = base_url.clone();
        move |_| {
            let base_url = base_url.clone();
            spawn_local(async move {
                let client = Client::new();
                let url = format!("{}/ignition?delay=1", base_url);
                let _ = client.get(&url).send().await;
            });
        }
    };

    // Fire QD Function
    let fire_qd = {
        let base_url = base_url.clone();
        move |_| {
            let base_url = base_url.clone();
            spawn_local(async move {
                let client = Client::new();
                let url = format!("{}/relays/qd", base_url);
                let _ = client.get(&url).send().await;
            });
        }
    };

    // Fire Vent Function
    let fire_vent = {
        let base_url = base_url.clone();
        move |_| {
            let base_url = base_url.clone();
            spawn_local(async move {
                let client = Client::new();
                let url = format!("{}/relays/vent", base_url);
                let _ = client.get(&url).send().await;
            });
        }
    };

    // Render the component's UI.
    rsx! {
            div {
                class: "w-full min-h-screen flex flex-col container mx-auto p-4 justify-evenly max-w-screen-lg",
                // Heading
                h1 {
                    class: "font-bold text-center text-4xl lg:text-5xl",
                    "MHPR Nitrous Fill Box Control"
                },

                // Status Badges for SSE Connections
                div {
                    class: "flex justify-evenly gap-4",
                    // Fill Pressure SSE Status Badge
                    { status_badge(format!("Fill Pressure SSE: {:?}", fill_sse_status()), color_map(&fill_sse_status())) }
                    // Supply Pressure SSE Status Badge
                    { status_badge(format!("Supply Pressure SSE: {:?}", supply_sse_status()), color_map(&supply_sse_status())) }
                    // Tank Pressure SSE Status Badge
                    { status_badge(format!("Tank Pressure SSE: {:?}", tank_sse_status()), color_map(&tank_sse_status())) }
                },

                // Pilot Valve Controls
    div {
        class: "grid grid-cols-3 gap-4",
        div {
            class: "flex flex-col gap-8",
            // Valve Label and Status
            div {
                class: "flex gap-2 lg:gap-4",
                p {
                    class: "text-lg lg:text-xl",
                    "Pilot Valve"
                },
                // Valve Status Badge
                { valve_badge(format!("{:?}", pilot_state()), color_map(&pilot_state())) }
            },
            // Control Buttons and Dropdown
            div {
                class: "flex gap-4",
                select {
                    class: "form-select",
                    value: "{selected_pilot_option()}",
                    onchange: move |e| selected_pilot_option.set(e.value().clone()),
                    // Embed the mapped `option` elements directly without using `res:`
                    {
                        actions.iter().map(|(value, name)| rsx!(
                            option { value: "{value}", "{name}" }
                        ))
                    }
                },
                button {
                    class: "btn",
                    onclick: {
                        let actuate_valve = actuate_valve.clone();
                        move |_| actuate_valve("pilot", &selected_pilot_option, &pilot_state)
                    },
                    "Execute"
                },
            },
        }, // End of inner div (flex flex-col gap-8)
    }, // End of outer div (grid grid-cols-3 gap-4)

    // Repeat similar blocks for other valves

                // Bottom Row - Fill Valve, Fill Pressure, Tank Pressure
                div {
                    class: "grid grid-cols-3 gap-4",
                    // Fill Valve Controls
                    div {
                        class: "flex flex-col gap-8",
                        div {
                            class: "flex gap-2 lg:gap-4",
                            p {
                                class: "text-lg lg:text-xl",
                                "Fill Valve"
                            },
                            { valve_badge(format!("{:?}", fill_state()), color_map(&fill_state())) }
                        },
                        div {
                            class: "flex gap-4",
                            select {
                                class: "form-select",
                                value: "{selected_fill_option()}",
                                onchange: move |e| selected_fill_option.set(e.value().clone()),
                                {
                                    actions.iter().map(|(value, name)| rsx!(
                                        option { value: "{value}", "{name}" }
                                    ))
                                }
                            },
                            button {
                            class: "btn",
                            onclick: {
                                move |_| actuate_valve("fill", &selected_fill_option, &fill_state)
                            },
                            "Execute"
                        },
                        },
                    },
                    // Fill Pressure Display
                    div {
                        class: "flex flex-col gap-4",
                        p {
                            class: "text-lg lg:text-xl text-end",
                            "Fill Pressure"
                        },
                        p {
                            class: "font-bold text-2xl lg:text-4xl text-end",
                            "{fill_pt()} Bar" // Updated here
                        },
                    },
                    // Tank Pressure Display
                    div {
                        class: "flex flex-col gap-4",
                        p {
                            class: "text-lg lg:text-xl text-end",
                            "Tank Pressure"
                        },
                        p {
                            class: "font-bold text-2xl lg:text-4xl text-end",
                            "{tank_pt()} Bar" // Updated here
                        },
                    },
                },

                // Top Row - Dump Valve, Supply Pressure, Test Stand Force
                div {
                    class: "grid grid-cols-3 gap-4",
                    // Dump Valve Controls
                    div {
                        class: "flex flex-col gap-8",
                        div {
                            class: "flex gap-2 lg:gap-4",
                            p {
                                class: "text-lg lg:text-xl",
                                "Dump Valve"
                            },
                            { valve_badge(format!("{:?}", supply_state()), color_map(&supply_state())) }
                        },
                        div {
                            class: "flex gap-4",
                            select {
                                class: "form-select",
                                value: "{selected_supply_option()}",
                                onchange: move |e| selected_supply_option.set(e.value().clone()),
                                {
                                    actions.iter().map(|(value, name)| rsx!(
                                        option { value: "{value}", "{name}" }
                                    ))
                                }
                            },
                            button {
                                class: "btn",
                                
                                "Execute"
                            },
                        },
                    },
                    // Supply Pressure Display
                    div {
                        class: "flex flex-col gap-4",
                        p {
                            class: "text-lg lg:text-xl text-end",
                            "Supply Pressure"
                        },
                        p {
                            class: "font-bold text-2xl lg:text-4xl text-end",
                            "{supply_pt()} Bar" // Updated here
                        },
                    },
                    // Test Stand Force Display
                    div {
                        class: "flex flex-col gap-4",
                        p {
                            class: "text-lg lg:text-xl text-end",
                            "Test Stand Force"
                        },
                        p {
                            class: "font-bold text-2xl lg:text-4xl text-end",
                            "{test_stand_load()} N" // Updated here
                        },
                    },
                },

                // Logging Controls and Tank Temperature
                div {
                    class: "grid grid-cols-3 gap-4",
                    // Logging Controls
                    div {
                        class: "flex flex-col gap-8",
                        div {
                            class: "flex gap-2 lg:gap-4",
                            p {
                                class: "text-lg lg:text-xl",
                                "Logging"
                            },
                            { valve_badge(
                                if is_logging() { "Active" } else { "Inactive" }.to_string(), // Updated here
                                if is_logging() { "green" } else { "red" }, // Updated here
                            ) }
                        },
                        div {
                            class: "flex gap-4",
                            button {
                                class: "btn",
                                onclick: start_logging,
                                "Start"
                            },
                            button {
                                class: "btn",
                                onclick: stop_logging,
                                "Stop"
                            },
                        },
                    },
                    // Tank Temperature Display
                    div {
                        class: "flex flex-col gap-4",
                        p {
                            class: "text-lg lg:text-xl text-end",
                            "Tank Temp"
                        },
                        p {
                            class: "font-bold text-2xl lg:text-4xl text-end",
                            "{tank_tc()} °C" // Updated here
                        },
                    },
                },

                // Action Buttons (Ignite, Fire QD, Fire Vent)
                div {
                    class: "flex flex-col gap-8",
                    div {
                        class: "flex gap-4",
                        button {
                            class: "bg-red-500 text-white text-xl py-4 px-8 rounded",
                            onclick: ignite,
                            "Ignite"
                        },
                        button {
                            class: "btn",
                            onclick: fire_qd,
                            "Fire QD"
                        },
                        button {
                            class: "btn",
                            onclick: fire_vent,
                            "Fire Vent"
                        },
                    },
                },
            }
        }
}

/// Helper function to create status badges.
fn status_badge(text: String, color: &'static str) -> Element {
    rsx! {
        span {
            class: "px-2.5 py-0.5 rounded",
            style: format!("background-color: {}; color: white;", color),
            { text }
        }
    }
}

/// Helper function to create valve badges showing the valve state.
/// Includes an indicator and the state text.
fn valve_badge(status: String, color: &'static str) -> Element {
    rsx! {
        span {
            class: "px-2.5 py-0.5 rounded flex items-center",
            style: format!("background-color: {}; color: white;", color),
            // Indicator dot
            span {
                class: "mr-1.5",
                style: "display: inline-block; width: 10px; height: 10px; border-radius: 50%; background-color: white;",
            },
            // State text
            { status }
        }
    }
}
