//! Network worker: runs on its own thread with a tokio runtime. It owns the REST client, the
//! gateway connection and the peer-to-peer share state, and talks to the UI through `Cmd` and `Ev`.

use std::{
	path::{Path, PathBuf},
	sync::Arc,
	sync::mpsc::Sender as StdSender,
};

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
	sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
	task::JoinHandle,
};
use webrtc::ice_transport::ice_server::RTCIceServer;

use crate::{
	capture::Source,
	gateway::{self, GwEvent},
	model::{Channel, Cmd, Credentials, Ev, Message, Relationship, TokenResponse, User},
	p2p::{Emit, OutSignal, P2p},
	voice::AudioSource,
};

pub type Ui = (UnboundedSender<Cmd>, std::sync::mpsc::Receiver<Ev>);

/// Start the worker thread. Repaints the UI through `ctx` whenever it sends an event.
pub fn spawn(ctx: egui::Context, source: Source, audio: AudioSource, home: PathBuf) -> Ui {
	let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
	let (ev_tx, ev_rx): (StdSender<Ev>, _) = std::sync::mpsc::channel();
	let emit: Emit = Arc::new(move |ev| {
		let _ = ev_tx.send(ev);
		ctx.request_repaint();
	});
	std::thread::Builder::new()
		.name("flint-lite-net".into())
		.spawn(move || {
			let rt = tokio::runtime::Builder::new_multi_thread()
				.enable_all()
				.build()
				.expect("tokio runtime");
			rt.block_on(run(cmd_rx, emit, source, audio, home));
		})
		.expect("spawn network thread");
	(cmd_tx, ev_rx)
}

/// Saved sign-in, so a restart keeps the session.
#[derive(Serialize, Deserialize)]
pub struct SavedSession {
	pub server: String,
	pub token: String,
}

pub fn load_session(home: &Path) -> Option<SavedSession> {
	let text = std::fs::read_to_string(home.join("session.json")).ok()?;
	serde_json::from_str(&text).ok()
}

fn save_session(home: &Path, session: &SavedSession) {
	let _ = std::fs::create_dir_all(home);
	if let Ok(text) = serde_json::to_string_pretty(session) {
		let _ = std::fs::write(home.join("session.json"), text);
	}
}

fn clear_session(home: &Path) {
	let _ = std::fs::remove_file(home.join("session.json"));
}

/// The server the user last signed in to. Kept apart from the session so it survives logout.
pub fn load_last_server(home: &Path) -> Option<String> {
	let text = std::fs::read_to_string(home.join("server.txt")).ok()?;
	let server = text.trim();
	(!server.is_empty()).then(|| server.to_string())
}

fn save_last_server(home: &Path, server: &str) {
	let _ = std::fs::create_dir_all(home);
	let _ = std::fs::write(home.join("server.txt"), server);
}

/// One REST call against `server`. Returns the JSON body, or Null for 204.
async fn api(
	http: &reqwest::Client,
	server: &str,
	token: Option<&str>,
	method: Method,
	path: &str,
	body: Option<Value>,
) -> Result<Value, String> {
	let mut req = http.request(method, format!("{server}/api/v10{path}"));
	if let Some(token) = token {
		req = req.header("authorization", format!("Bearer {token}"));
	}
	if let Some(body) = body {
		req = req.json(&body);
	}
	let resp = req
		.send()
		.await
		.map_err(|e| format!("cannot reach the server: {e}"))?;
	let status = resp.status();
	let text = resp.text().await.map_err(|e| e.to_string())?;
	if !status.is_success() {
		let message = serde_json::from_str::<Value>(&text)
			.ok()
			.and_then(|v| v["message"].as_str().map(str::to_string))
			.unwrap_or_else(|| format!("request failed ({status})"));
		return Err(message);
	}
	if text.trim().is_empty() {
		return Ok(Value::Null);
	}
	serde_json::from_str(&text).map_err(|e| format!("bad response: {e}"))
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, String> {
	serde_json::from_value(value).map_err(|e| format!("unexpected response: {e}"))
}

struct Net {
	http: reqwest::Client,
	home: PathBuf,
	server: String,
	token: String,
	me: Option<User>,
	gateway: Option<JoinHandle<()>>,
	gw_tx: UnboundedSender<GwEvent>,
	p2p: P2p,
	emit: Emit,
}

async fn run(
	mut cmds: UnboundedReceiver<Cmd>,
	emit: Emit,
	source: Source,
	audio: AudioSource,
	home: PathBuf,
) {
	let (gw_tx, mut gw_rx) = mpsc::unbounded_channel::<GwEvent>();
	let (sig_tx, mut sig_rx) = mpsc::unbounded_channel::<OutSignal>();
	let p2p = P2p::new(source, audio, sig_tx, emit.clone());
	let mut net = Net {
		http: reqwest::Client::new(),
		home,
		server: String::new(),
		token: String::new(),
		me: None,
		gateway: None,
		gw_tx,
		p2p,
		emit,
	};
	loop {
		tokio::select! {
			cmd = cmds.recv() => match cmd {
				Some(cmd) => net.command(cmd).await,
				None => break,
			},
			Some(ev) = gw_rx.recv() => net.gateway_event(ev).await,
			Some(sig) = sig_rx.recv() => net.post_signal(sig).await,
		}
	}
}

impl Net {
	fn error(&self, message: impl Into<String>) {
		(self.emit)(Ev::Error(message.into()));
	}

	async fn command(&mut self, cmd: Cmd) {
		match cmd {
			Cmd::Login {
				server,
				username,
				password,
				register,
			} => {
				let server = server.trim_end_matches('/').to_string();
				let path = if register {
					"/auth/register"
				} else {
					"/auth/login"
				};
				let body = json!(Credentials {
					username: &username,
					password: &password
				});
				match api(&self.http, &server, None, Method::POST, path, Some(body))
					.await
					.and_then(parse::<TokenResponse>)
				{
					Ok(auth) => self.session_started(server, auth.token, auth.user).await,
					Err(e) => self.error(e),
				}
			}
			Cmd::Resume { server, token } => {
				match api(
					&self.http,
					&server,
					Some(&token),
					Method::GET,
					"/users/@me",
					None,
				)
				.await
				.and_then(parse::<User>)
				{
					Ok(me) => self.session_started(server, token, me).await,
					Err(e) => {
						clear_session(&self.home);
						self.error(format!("Your saved sign-in ended: {e}"));
						(self.emit)(Ev::LoggedOut);
					}
				}
			}
			Cmd::Logout => {
				self.p2p.stop_share().await;
				if let Some(handle) = self.gateway.take() {
					handle.abort();
				}
				self.me = None;
				self.token.clear();
				clear_session(&self.home);
				(self.emit)(Ev::LoggedOut);
			}
			Cmd::OpenChannel(channel_id) => {
				let path = format!("/channels/{channel_id}/messages?limit=50");
				match self.get(&path).await.and_then(parse::<Vec<Message>>) {
					Ok(mut messages) => {
						messages.sort_by_key(|m| m.id.parse::<u64>().unwrap_or(0));
						(self.emit)(Ev::History {
							channel_id,
							messages,
						});
					}
					Err(e) => self.error(e),
				}
			}
			Cmd::Send {
				channel_id,
				content,
			} => {
				let path = format!("/channels/{channel_id}/messages");
				if let Err(e) = self
					.call(Method::POST, &path, Some(json!({ "content": content })))
					.await
				{
					self.error(e);
				}
			}
			Cmd::AddFriend(username) => {
				match self
					.call(
						Method::POST,
						"/users/@me/relationships",
						Some(json!({ "username": username })),
					)
					.await
				{
					Ok(_) => self.refresh_relationships().await,
					Err(e) => self.error(e),
				}
			}
			Cmd::AcceptFriend(user_id) => {
				let path = format!("/users/@me/relationships/{user_id}");
				match self.call(Method::PUT, &path, Some(json!({}))).await {
					Ok(_) => self.refresh_relationships().await,
					Err(e) => self.error(e),
				}
			}
			Cmd::RemoveRelationship(user_id) => {
				let path = format!("/users/@me/relationships/{user_id}");
				match self.call(Method::DELETE, &path, None).await {
					Ok(_) => self.refresh_relationships().await,
					Err(e) => self.error(e),
				}
			}
			Cmd::CreateDm(ids) => {
				let body = if ids.len() == 1 {
					json!({ "recipient_id": ids[0] })
				} else {
					json!({ "recipients": ids })
				};
				match self
					.call(Method::POST, "/users/@me/channels", Some(body))
					.await
					.and_then(parse::<Channel>)
				{
					Ok(channel) => (self.emit)(Ev::ChannelUpsert(channel)),
					Err(e) => self.error(e),
				}
			}
			Cmd::ShareScreen(peers) => {
				if let Err(e) = self.p2p.start_share(peers).await {
					self.error(format!("Could not start screen share: {e}"));
				}
			}
			Cmd::StopShare => self.p2p.stop_share().await,
			Cmd::StartCall(peers) => {
				if let Err(e) = self.p2p.start_call(peers).await {
					self.error(format!("Could not start the call: {e}"));
				}
			}
			Cmd::LeaveCall => self.p2p.leave_call().await,
			Cmd::SetMuted(muted) => self.p2p.set_muted(muted),
			Cmd::AcceptCall(from) => {
				if let Err(e) = self.p2p.accept_call(&from).await {
					self.error(format!("Could not join the call: {e}"));
				}
			}
			Cmd::DeclineCall(from) => self.p2p.decline_call(&from).await,
		}
	}

	async fn session_started(&mut self, server: String, token: String, me: User) {
		save_session(
			&self.home,
			&SavedSession {
				server: server.clone(),
				token: token.clone(),
			},
		);
		save_last_server(&self.home, &server);
		if let Some(handle) = self.gateway.take() {
			handle.abort();
		}
		self.server = server.clone();
		self.token = token.clone();
		self.me = Some(me.clone());
		self.gateway = Some(tokio::spawn(gateway::run(
			server.clone(),
			token.clone(),
			self.gw_tx.clone(),
		)));
		(self.emit)(Ev::LoggedIn {
			server: server.clone(),
			me,
		});

		match self.get("/calls/ice-servers").await {
			Ok(config) => self.p2p.set_ice(ice_from_config(&config)).await,
			Err(e) => self.error(format!("Using default STUN servers: {e}")),
		}
		self.refresh_relationships().await;
	}

	async fn gateway_event(&mut self, ev: GwEvent) {
		match ev {
			GwEvent::Ready { channels } => (self.emit)(Ev::Channels(channels)),
			GwEvent::Message(message) => (self.emit)(Ev::Message(message)),
			GwEvent::ChannelCreate(channel) => (self.emit)(Ev::ChannelUpsert(channel)),
			GwEvent::RefreshRelationships => self.refresh_relationships().await,
			GwEvent::Signal {
				from,
				call_id,
				kind,
				payload,
			} => {
				if let Err(e) = self.p2p.on_signal(from, call_id, kind, payload).await {
					self.error(format!("Call signal failed: {e}"));
				}
			}
			GwEvent::Notice(text) => (self.emit)(Ev::Notice(text)),
		}
	}

	async fn post_signal(&mut self, sig: OutSignal) {
		let body = json!({ "to": sig.to, "call_id": sig.call_id, "kind": sig.kind, "payload": sig.payload });
		if let Err(e) = self.call(Method::POST, "/calls/signal", Some(body)).await {
			self.error(format!("Could not send call signal: {e}"));
		}
	}

	async fn refresh_relationships(&self) {
		match self
			.get("/users/@me/relationships")
			.await
			.and_then(parse::<Vec<Relationship>>)
		{
			Ok(list) => (self.emit)(Ev::Relationships(list)),
			Err(e) => self.error(e),
		}
	}

	async fn get(&self, path: &str) -> Result<Value, String> {
		self.call(Method::GET, path, None).await
	}

	async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value, String> {
		let token = (!self.token.is_empty()).then_some(self.token.as_str());
		api(&self.http, &self.server, token, method, path, body).await
	}
}

/// Convert Steel's `{"iceServers": [...]}` into WebRTC ICE servers.
pub fn ice_from_config(config: &Value) -> Vec<RTCIceServer> {
	config["iceServers"]
		.as_array()
		.map(|servers| {
			servers
				.iter()
				.map(|s| RTCIceServer {
					urls: s["urls"]
						.as_array()
						.map(|urls| {
							urls.iter()
								.filter_map(|u| u.as_str().map(str::to_string))
								.collect()
						})
						.unwrap_or_default(),
					username: s["username"].as_str().unwrap_or_default().to_string(),
					credential: s["credential"].as_str().unwrap_or_default().to_string(),
					..Default::default()
				})
				.collect()
		})
		.unwrap_or_default()
}

/// End to end against a running Steel: two clients sign up, become friends, open a DM, chat,
/// and share a screen peer to peer. Ignored by default. Run with
/// `FLINT_LITE_TEST_SERVER=http://127.0.0.1:8080 cargo test -p flint-lite -- --ignored`.
#[cfg(test)]
mod e2e {
	use super::*;
	use crate::model::{channel_type, relationship_type};
	use std::time::{Duration, SystemTime, UNIX_EPOCH};

	const WAIT: Duration = Duration::from_secs(60);

	fn wait<T>(
		rx: &std::sync::mpsc::Receiver<Ev>,
		what: &str,
		pick: impl Fn(&Ev) -> Option<T>,
	) -> T {
		let deadline = std::time::Instant::now() + WAIT;
		loop {
			let left = deadline.saturating_duration_since(std::time::Instant::now());
			assert!(!left.is_zero(), "timed out waiting for {what}");
			match rx.recv_timeout(left.min(Duration::from_millis(200))) {
				Ok(ev) => {
					if let Ev::Error(e) = &ev {
						eprintln!("client error while waiting for {what}: {e}");
					}
					if let Some(value) = pick(&ev) {
						return value;
					}
				}
				Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
				Err(_) => panic!("client stopped while waiting for {what}"),
			}
		}
	}

	fn scratch(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("flint-lite-e2e-{}-{name}", std::process::id()))
	}

	#[test]
	#[ignore = "needs a running Steel server; set FLINT_LITE_TEST_SERVER"]
	fn two_clients_chat_and_share_screen() {
		let server = std::env::var("FLINT_LITE_TEST_SERVER").expect("FLINT_LITE_TEST_SERVER");
		let stamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis()
			% 100_000_000;
		let (name_a, name_b) = (format!("fla{stamp}"), format!("flb{stamp}"));
		let ctx = egui::Context::default();
		let (a_cmd, a_ev) = spawn(
			ctx.clone(),
			Source::TestPattern,
			AudioSource::Silence,
			scratch("a"),
		);
		let (b_cmd, b_ev) = spawn(ctx, Source::TestPattern, AudioSource::Silence, scratch("b"));

		let login = |cmd: &UnboundedSender<Cmd>, name: &str| {
			cmd.send(Cmd::Login {
				server: server.clone(),
				username: name.to_string(),
				password: "password123".into(),
				register: true,
			})
			.unwrap();
		};
		login(&a_cmd, &name_a);
		login(&b_cmd, &name_b);
		let id_a = wait(&a_ev, "a signed in", |e| match e {
			Ev::LoggedIn { me, .. } => Some(me.id.clone()),
			_ => None,
		});
		let id_b = wait(&b_ev, "b signed in", |e| match e {
			Ev::LoggedIn { me, .. } => Some(me.id.clone()),
			_ => None,
		});

		a_cmd.send(Cmd::AddFriend(name_b.clone())).unwrap();
		wait(&b_ev, "friend request", |e| match e {
			Ev::Relationships(list)
				if list.iter().any(|r| {
					r.kind == relationship_type::INCOMING_REQUEST && r.user.id == id_a
				}) =>
			{
				Some(())
			}
			_ => None,
		});
		b_cmd.send(Cmd::AcceptFriend(id_a.clone())).unwrap();
		wait(&a_ev, "friendship", |e| match e {
			Ev::Relationships(list)
				if list
					.iter()
					.any(|r| r.kind == relationship_type::FRIEND && r.user.id == id_b) =>
			{
				Some(())
			}
			_ => None,
		});

		a_cmd.send(Cmd::CreateDm(vec![id_b.clone()])).unwrap();
		let channel = wait(&a_ev, "DM channel", |e| match e {
			Ev::ChannelUpsert(c) if c.kind == channel_type::DM => Some(c.clone()),
			_ => None,
		});
		wait(&b_ev, "DM channel on b", |e| match e {
			Ev::ChannelUpsert(c) if c.id == channel.id => Some(()),
			_ => None,
		});

		a_cmd
			.send(Cmd::Send {
				channel_id: channel.id.clone(),
				content: "hello from a".into(),
			})
			.unwrap();
		wait(&b_ev, "chat message", |e| match e {
			Ev::Message(m) if m.content == "hello from a" && m.author.id == id_a => Some(()),
			_ => None,
		});

		a_cmd.send(Cmd::ShareScreen(vec![id_b.clone()])).unwrap();
		let (from, width, height) = wait(&b_ev, "screen frame", |e| match e {
			Ev::Frame {
				from,
				width,
				height,
				..
			} => Some((from.clone(), *width, *height)),
			_ => None,
		});
		assert_eq!(from, id_a);
		assert_eq!((width, height), (960, 540));
		a_cmd.send(Cmd::StopShare).unwrap();
	}

	#[test]
	#[ignore = "needs a running Steel server; set FLINT_LITE_TEST_SERVER"]
	fn three_clients_group_chat_and_mesh_share() {
		let server = std::env::var("FLINT_LITE_TEST_SERVER").expect("FLINT_LITE_TEST_SERVER");
		let stamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_millis()
			% 100_000_000;
		let ctx = egui::Context::default();
		let names = [
			format!("fga{stamp}"),
			format!("fgb{stamp}"),
			format!("fgc{stamp}"),
		];
		let clients: Vec<Ui> = ["a", "b", "c"]
			.iter()
			.map(|tag| {
				spawn(
					ctx.clone(),
					Source::TestPattern,
					AudioSource::Silence,
					scratch(&format!("g{tag}")),
				)
			})
			.collect();
		let mut ids = Vec::new();
		for (i, (cmd, ev)) in clients.iter().enumerate() {
			cmd.send(Cmd::Login {
				server: server.clone(),
				username: names[i].clone(),
				password: "password123".into(),
				register: true,
			})
			.unwrap();
			ids.push(wait(ev, "signed in", |e| match e {
				Ev::LoggedIn { me, .. } => Some(me.id.clone()),
				_ => None,
			}));
		}
		// a befriends b and c.
		for other in [1usize, 2] {
			clients[0]
				.0
				.send(Cmd::AddFriend(names[other].clone()))
				.unwrap();
			let (oc, oe) = &clients[other];
			let a_id = ids[0].clone();
			wait(oe, "friend request", |e| match e {
				Ev::Relationships(list)
					if list.iter().any(|r| {
						r.kind == relationship_type::INCOMING_REQUEST && r.user.id == a_id
					}) =>
				{
					Some(())
				}
				_ => None,
			});
			oc.send(Cmd::AcceptFriend(a_id)).unwrap();
			let other_id = ids[other].clone();
			wait(&clients[0].1, "friendship", |e| match e {
				Ev::Relationships(list)
					if list
						.iter()
						.any(|r| r.kind == relationship_type::FRIEND && r.user.id == other_id) =>
				{
					Some(())
				}
				_ => None,
			});
		}

		clients[0]
			.0
			.send(Cmd::CreateDm(vec![ids[1].clone(), ids[2].clone()]))
			.unwrap();
		let group = wait(&clients[0].1, "group DM", |e| match e {
			Ev::ChannelUpsert(c) if c.kind == channel_type::GROUP_DM => Some(c.clone()),
			_ => None,
		});
		for i in [1usize, 2] {
			wait(&clients[i].1, "group DM on member", |e| match e {
				Ev::ChannelUpsert(c) if c.id == group.id => Some(()),
				_ => None,
			});
		}
		clients[0]
			.0
			.send(Cmd::Send {
				channel_id: group.id.clone(),
				content: "group hi".into(),
			})
			.unwrap();
		for i in [1usize, 2] {
			wait(&clients[i].1, "group message", |e| match e {
				Ev::Message(m) if m.content == "group hi" => Some(()),
				_ => None,
			});
		}

		// a shares to both b and c, a mesh of two connections.
		clients[0]
			.0
			.send(Cmd::ShareScreen(vec![ids[1].clone(), ids[2].clone()]))
			.unwrap();
		for i in [1usize, 2] {
			let from = wait(&clients[i].1, "screen frame in group", |e| match e {
				Ev::Frame { from, .. } => Some(from.clone()),
				_ => None,
			});
			assert_eq!(from, ids[0]);
		}
		clients[0].0.send(Cmd::StopShare).unwrap();
	}
}

#[cfg(test)]
mod last_server_tests {
	use super::*;

	#[test]
	fn last_server_survives_logout() {
		let home = std::env::temp_dir().join(format!("flint-lite-last-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&home);
		save_session(
			&home,
			&SavedSession {
				server: "https://steel.example".into(),
				token: "t".into(),
			},
		);
		save_last_server(&home, "https://steel.example");
		clear_session(&home);

		assert!(load_session(&home).is_none());
		assert_eq!(
			load_last_server(&home).as_deref(),
			Some("https://steel.example")
		);
		let _ = std::fs::remove_dir_all(&home);
	}
}
