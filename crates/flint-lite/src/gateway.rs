//! Steel gateway client: Hello, Identify, heartbeats and dispatches, reconnecting on failure.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::model::{Channel, Message};

/// What the network worker needs from a gateway dispatch.
#[derive(Debug)]
pub enum GwEvent {
	Ready {
		channels: Vec<Channel>,
	},
	Message(Message),
	ChannelCreate(Channel),
	RefreshRelationships,
	Signal {
		from: String,
		call_id: String,
		kind: String,
		payload: Value,
	},
	Notice(String),
}

/// Keep a gateway session open for `token` until the task is aborted.
pub async fn run(server: String, token: String, out: UnboundedSender<GwEvent>) {
	loop {
		let note = match connect_once(&server, &token, &out).await {
			Ok(()) => "Connection closed. Reconnecting.".to_string(),
			Err(e) => format!("Connection lost ({e}). Reconnecting."),
		};
		let _ = out.send(GwEvent::Notice(note));
		tokio::time::sleep(Duration::from_secs(2)).await;
	}
}

/// `http://host` becomes `ws://host/gateway`, and `https://host` becomes `wss://host/gateway`.
pub fn ws_url(server: &str) -> Result<String, String> {
	let base = server.trim_end_matches('/');
	let rest = base
		.strip_prefix("https://")
		.map(|r| format!("wss://{r}"))
		.or_else(|| base.strip_prefix("http://").map(|r| format!("ws://{r}")))
		.ok_or_else(|| "server URL must start with http:// or https://".to_string())?;
	Ok(format!("{rest}/gateway"))
}

async fn connect_once(
	server: &str,
	token: &str,
	out: &UnboundedSender<GwEvent>,
) -> Result<(), String> {
	let url = ws_url(server)?;
	let (mut ws, _) = tokio_tungstenite::connect_async(url)
		.await
		.map_err(|e| e.to_string())?;
	let mut heartbeat: Option<tokio::time::Interval> = None;
	loop {
		tokio::select! {
			_ = tick(&mut heartbeat) => {
				ws.send(WsMessage::Text(json!({ "op": 1, "d": null }).to_string().into()))
					.await
					.map_err(|e| e.to_string())?;
			}
			msg = ws.next() => {
				let msg = match msg {
					Some(Ok(msg)) => msg,
					Some(Err(e)) => return Err(e.to_string()),
					None => return Ok(()),
				};
				let text = match msg {
					WsMessage::Text(text) => text,
					WsMessage::Close(_) => return Ok(()),
					_ => continue,
				};
				let frame: Value = serde_json::from_str(text.as_str()).map_err(|e| e.to_string())?;
				match frame["op"].as_u64() {
					Some(10) => {
						let ms = frame["d"]["heartbeat_interval"].as_u64().unwrap_or(41_250);
						heartbeat = Some(tokio::time::interval(Duration::from_millis(ms)));
						let identify = json!({ "op": 2, "d": { "token": token } });
						ws.send(WsMessage::Text(identify.to_string().into()))
							.await
							.map_err(|e| e.to_string())?;
					}
					Some(0) => dispatch(&frame, out),
					Some(7) | Some(9) => return Err("the server asked us to reconnect".into()),
					_ => {}
				}
			}
		}
	}
}

/// Wait for the next heartbeat tick, or forever when no heartbeat is running yet.
async fn tick(heartbeat: &mut Option<tokio::time::Interval>) {
	match heartbeat {
		Some(interval) => {
			interval.tick().await;
		}
		None => std::future::pending::<()>().await,
	}
}

fn dispatch(frame: &Value, out: &UnboundedSender<GwEvent>) {
	let d = &frame["d"];
	let event = match frame["t"].as_str().unwrap_or("") {
		"READY" => GwEvent::Ready {
			channels: serde_json::from_value(d["private_channels"].clone()).unwrap_or_default(),
		},
		"MESSAGE_CREATE" => match serde_json::from_value(d.clone()) {
			Ok(message) => GwEvent::Message(message),
			Err(_) => return,
		},
		"CHANNEL_CREATE" => match serde_json::from_value(d.clone()) {
			Ok(channel) => GwEvent::ChannelCreate(channel),
			Err(_) => return,
		},
		"RELATIONSHIP_ADD" | "RELATIONSHIP_REMOVE" | "RELATIONSHIP_UPDATE" => {
			GwEvent::RefreshRelationships
		}
		"CALL_SIGNAL" => GwEvent::Signal {
			from: d["from"]["id"].as_str().unwrap_or_default().to_string(),
			call_id: d["call_id"].as_str().unwrap_or_default().to_string(),
			kind: d["kind"].as_str().unwrap_or_default().to_string(),
			payload: d["payload"].clone(),
		},
		_ => return,
	};
	let _ = out.send(event);
}

#[cfg(test)]
mod tests {
	use super::ws_url;

	#[test]
	fn maps_http_schemes_to_websocket() {
		assert_eq!(
			ws_url("http://127.0.0.1:8080").unwrap(),
			"ws://127.0.0.1:8080/gateway"
		);
		assert_eq!(
			ws_url("https://chat.example/").unwrap(),
			"wss://chat.example/gateway"
		);
		assert!(ws_url("chat.example").is_err());
	}
}
