//! Peer-to-peer screen share over WebRTC data channels.
//!
//! Steel carries the offer, answer and ICE candidates (`CALL_SIGNAL`). Frames go straight
//! between the two clients. A sharer makes one connection per viewer, and each viewer answers.
//! Every frame is a JPEG split into chunks. Each chunk is a data channel message:
//! `[u32 frame sequence BE][u8 last chunk flag][payload]`.

use std::{
	collections::HashMap,
	sync::{
		Arc, Mutex as StdMutex,
		atomic::{AtomicBool, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use serde_json::{Value, json};
use tokio::{
	sync::{Mutex, mpsc::UnboundedSender, watch},
	task::JoinHandle,
};
use webrtc::{
	api::{API, APIBuilder},
	data_channel::{RTCDataChannel, data_channel_message::DataChannelMessage},
	ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
	peer_connection::{
		RTCPeerConnection, configuration::RTCConfiguration,
		peer_connection_state::RTCPeerConnectionState,
		sdp::session_description::RTCSessionDescription,
	},
};

use crate::{
	capture::{self, Source},
	model::Ev,
};

pub const SCREEN_CHANNEL: &str = "screen";
const CHUNK: usize = 16 * 1024;
/// Frames are dropped for a viewer whose send buffer is this full, so a slow link lags instead of queueing.
const MAX_BUFFERED: usize = 4 * 1024 * 1024;
const FRAME_INTERVAL: Duration = Duration::from_millis(125);

/// A signal for the network worker to post through `POST /calls/signal`.
#[derive(Debug, Clone)]
pub struct OutSignal {
	pub to: String,
	pub call_id: String,
	pub kind: &'static str,
	pub payload: Value,
}

pub type Emit = Arc<dyn Fn(Ev) + Send + Sync>;

#[derive(Clone)]
struct Frame {
	seq: u64,
	jpeg: Arc<Vec<u8>>,
}

struct Peer {
	call_id: String,
	pc: Arc<RTCPeerConnection>,
	alive: Arc<AtomicBool>,
	sharing: bool,
}

struct Inner {
	api: API,
	ice: Mutex<Vec<RTCIceServer>>,
	peers: Mutex<HashMap<String, Peer>>,
	out: UnboundedSender<OutSignal>,
	emit: Emit,
	frames: watch::Sender<Option<Frame>>,
	source: Source,
	capture: StdMutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct P2p {
	inner: Arc<Inner>,
}

fn s(e: impl std::fmt::Display) -> String {
	e.to_string()
}

impl P2p {
	pub fn new(source: Source, out: UnboundedSender<OutSignal>, emit: Emit) -> Self {
		// WebRTC's DTLS needs a process-wide rustls provider. Installing twice is harmless.
		let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
		let (frames, _) = watch::channel(None);
		Self {
			inner: Arc::new(Inner {
				api: APIBuilder::new().build(),
				ice: Mutex::new(Vec::new()),
				peers: Mutex::new(HashMap::new()),
				out,
				emit,
				frames,
				source,
				capture: StdMutex::new(None),
			}),
		}
	}

	pub async fn set_ice(&self, servers: Vec<RTCIceServer>) {
		*self.inner.ice.lock().await = servers;
	}

	/// Start sharing the screen with each of `peers`. Viewers that are already connected are skipped.
	pub async fn start_share(&self, peers: Vec<String>) -> Result<(), String> {
		self.ensure_capture();
		for peer in peers {
			if self.inner.peers.lock().await.contains_key(&peer) {
				continue;
			}
			let call_id = new_call_id();
			let pc = self.new_pc().await?;
			let alive = Arc::new(AtomicBool::new(true));
			let dc = pc
				.create_data_channel(SCREEN_CHANNEL, None)
				.await
				.map_err(s)?;
			{
				let rx = self.inner.frames.subscribe();
				let dc_open = dc.clone();
				let alive_open = alive.clone();
				dc.on_open(Box::new(move || {
					Box::pin(async move {
						tokio::spawn(send_frames(dc_open, rx, alive_open));
					})
				}));
			}
			pc.on_peer_connection_state_change(self.state_handler(peer.clone(), call_id.clone()));

			let gathered = pc.gathering_complete_promise().await;
			let offer = pc.create_offer(None).await.map_err(s)?;
			pc.set_local_description(offer).await.map_err(s)?;
			let mut gathered = gathered;
			let _ = gathered.recv().await;
			let local = pc.local_description().await.ok_or("no local description")?;

			self.inner.peers.lock().await.insert(
				peer.clone(),
				Peer {
					call_id: call_id.clone(),
					pc,
					alive,
					sharing: true,
				},
			);
			let _ = self.inner.out.send(OutSignal {
				to: peer,
				call_id,
				kind: "offer",
				payload: json!({ "sdp": local.sdp }),
			});
		}
		self.emit_share().await;
		Ok(())
	}

	/// Stop every outgoing share and tell each viewer to hang up.
	pub async fn stop_share(&self) {
		let sharing: Vec<(String, String)> = self
			.inner
			.peers
			.lock()
			.await
			.iter()
			.filter(|(_, p)| p.sharing)
			.map(|(id, p)| (id.clone(), p.call_id.clone()))
			.collect();
		for (peer, call_id) in sharing {
			self.drop_peer(&peer).await;
			let _ = self.inner.out.send(OutSignal {
				to: peer,
				call_id,
				kind: "hangup",
				payload: Value::Null,
			});
		}
		self.emit_share().await;
	}

	/// Handle one `CALL_SIGNAL` from `from`.
	pub async fn on_signal(
		&self,
		from: String,
		call_id: String,
		kind: String,
		payload: Value,
	) -> Result<(), String> {
		match kind.as_str() {
			"offer" => {
				let sdp = payload["sdp"]
					.as_str()
					.ok_or("offer without sdp")?
					.to_string();
				self.on_offer(from, call_id, sdp).await
			}
			"answer" => {
				let sdp = payload["sdp"]
					.as_str()
					.ok_or("answer without sdp")?
					.to_string();
				let pc = self.pc_for(&from, &call_id).await;
				match pc {
					Some(pc) => pc
						.set_remote_description(RTCSessionDescription::answer(sdp).map_err(s)?)
						.await
						.map_err(s),
					None => Ok(()),
				}
			}
			"ice" => {
				let Some(pc) = self.pc_for(&from, &call_id).await else {
					return Ok(());
				};
				let candidate = RTCIceCandidateInit {
					candidate: payload["candidate"]
						.as_str()
						.unwrap_or_default()
						.to_string(),
					sdp_mid: payload["sdpMid"].as_str().map(str::to_string),
					sdp_mline_index: payload["sdpMLineIndex"].as_u64().map(|v| v as u16),
					username_fragment: None,
				};
				pc.add_ice_candidate(candidate).await.map_err(s)
			}
			"hangup" | "decline" => {
				self.remove_if(&from, &call_id).await;
				Ok(())
			}
			_ => Ok(()),
		}
	}

	async fn on_offer(&self, from: String, call_id: String, sdp: String) -> Result<(), String> {
		self.drop_peer(&from).await;
		let pc = self.new_pc().await?;
		let alive = Arc::new(AtomicBool::new(true));
		self.attach_receiver(from.clone(), &pc);
		pc.on_peer_connection_state_change(self.state_handler(from.clone(), call_id.clone()));

		pc.set_remote_description(RTCSessionDescription::offer(sdp).map_err(s)?)
			.await
			.map_err(s)?;
		let answer = pc.create_answer(None).await.map_err(s)?;
		let gathered = pc.gathering_complete_promise().await;
		pc.set_local_description(answer).await.map_err(s)?;
		let mut gathered = gathered;
		let _ = gathered.recv().await;
		let local = pc.local_description().await.ok_or("no local description")?;

		self.inner.peers.lock().await.insert(
			from.clone(),
			Peer {
				call_id: call_id.clone(),
				pc,
				alive,
				sharing: false,
			},
		);
		let _ = self.inner.out.send(OutSignal {
			to: from,
			call_id,
			kind: "answer",
			payload: json!({ "sdp": local.sdp }),
		});
		self.emit_share().await;
		Ok(())
	}

	/// Route received chunks into frames and emit each complete JPEG as decoded pixels.
	fn attach_receiver(&self, from: String, pc: &RTCPeerConnection) {
		let emit = self.inner.emit.clone();
		pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
			let emit = emit.clone();
			let from = from.clone();
			Box::pin(async move {
				let assembly = Arc::new(StdMutex::new(Assembly::default()));
				dc.on_message(Box::new(move |msg: DataChannelMessage| {
					let emit = emit.clone();
					let from = from.clone();
					let assembly = assembly.clone();
					Box::pin(async move {
						let complete = assembly
							.lock()
							.unwrap_or_else(|e| e.into_inner())
							.push(&msg.data);
						if let Some(jpeg) = complete {
							tokio::task::spawn_blocking(move || {
								match capture::decode_jpeg(&jpeg) {
									Ok((width, height, rgba)) => emit(Ev::Frame {
										from,
										width,
										height,
										rgba,
									}),
									Err(e) => emit(Ev::Error(e)),
								}
							});
						}
					})
				}));
			})
		}));
	}

	async fn new_pc(&self) -> Result<Arc<RTCPeerConnection>, String> {
		let config = RTCConfiguration {
			ice_servers: self.inner.ice.lock().await.clone(),
			..Default::default()
		};
		let pc = self
			.inner
			.api
			.new_peer_connection(config)
			.await
			.map_err(s)?;
		Ok(Arc::new(pc))
	}

	fn state_handler(
		&self,
		peer: String,
		call_id: String,
	) -> Box<
		dyn FnMut(
				RTCPeerConnectionState,
			) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
			+ Send
			+ Sync,
	> {
		let this = self.clone();
		Box::new(move |state| {
			let this = this.clone();
			let peer = peer.clone();
			let call_id = call_id.clone();
			Box::pin(async move {
				if matches!(
					state,
					RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
				) {
					this.remove_if(&peer, &call_id).await;
				}
			})
		})
	}

	async fn pc_for(&self, peer: &str, call_id: &str) -> Option<Arc<RTCPeerConnection>> {
		self.inner
			.peers
			.lock()
			.await
			.get(peer)
			.filter(|p| p.call_id == call_id)
			.map(|p| p.pc.clone())
	}

	async fn remove_if(&self, peer: &str, call_id: &str) {
		let matches = self
			.inner
			.peers
			.lock()
			.await
			.get(peer)
			.is_some_and(|p| p.call_id == call_id);
		if matches {
			self.drop_peer(peer).await;
			self.emit_share().await;
		}
	}

	/// Remove a peer from the table and close its connection. The lock is not held while closing.
	async fn drop_peer(&self, peer: &str) {
		let removed = self.inner.peers.lock().await.remove(peer);
		if let Some(p) = removed {
			p.alive.store(false, Ordering::SeqCst);
			let _ = p.pc.close().await;
		}
		let any_sharing = self.inner.peers.lock().await.values().any(|p| p.sharing);
		if !any_sharing {
			if let Some(handle) = self
				.inner
				.capture
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.take()
			{
				handle.abort();
			}
		}
	}

	fn ensure_capture(&self) {
		let mut slot = self.inner.capture.lock().unwrap_or_else(|e| e.into_inner());
		if slot.as_ref().is_some_and(|h| !h.is_finished()) {
			return;
		}
		let inner = self.inner.clone();
		*slot = Some(tokio::spawn(async move {
			let source = inner.source;
			let mut counter = 0u64;
			let mut tick = tokio::time::interval(FRAME_INTERVAL);
			loop {
				tick.tick().await;
				counter += 1;
				let c = counter;
				match tokio::task::spawn_blocking(move || capture::grab_jpeg(source, c)).await {
					Ok(Ok(jpeg)) => {
						inner.frames.send_replace(Some(Frame {
							seq: c,
							jpeg: Arc::new(jpeg),
						}));
					}
					Ok(Err(e)) => {
						(inner.emit)(Ev::Error(e));
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
					Err(_) => {}
				}
			}
		}));
	}

	async fn emit_share(&self) {
		let peers = self.inner.peers.lock().await;
		let mut outgoing: Vec<String> = peers
			.iter()
			.filter(|(_, p)| p.sharing)
			.map(|(id, _)| id.clone())
			.collect();
		let mut incoming: Vec<String> = peers
			.iter()
			.filter(|(_, p)| !p.sharing)
			.map(|(id, _)| id.clone())
			.collect();
		outgoing.sort();
		incoming.sort();
		(self.inner.emit)(Ev::Share { outgoing, incoming });
	}
}

/// Send each new frame to one viewer until the peer is dropped.
async fn send_frames(
	dc: Arc<RTCDataChannel>,
	mut rx: watch::Receiver<Option<Frame>>,
	alive: Arc<AtomicBool>,
) {
	let mut last = 0u64;
	while alive.load(Ordering::SeqCst) {
		match tokio::time::timeout(Duration::from_millis(500), rx.changed()).await {
			Err(_) => continue,
			Ok(Err(_)) => break,
			Ok(Ok(())) => {}
		}
		let frame = rx.borrow_and_update().clone();
		let Some(frame) = frame else { continue };
		if frame.seq == last {
			continue;
		}
		last = frame.seq;
		if dc.buffered_amount().await > MAX_BUFFERED {
			continue;
		}
		let seq = frame.seq as u32;
		let total = frame.jpeg.len();
		let mut offset = 0;
		while offset < total {
			let end = (offset + CHUNK).min(total);
			let mut msg = Vec::with_capacity(5 + end - offset);
			msg.extend_from_slice(&seq.to_be_bytes());
			msg.push((end == total) as u8);
			msg.extend_from_slice(&frame.jpeg[offset..end]);
			if dc.send(&Bytes::from(msg)).await.is_err() {
				return;
			}
			offset = end;
		}
	}
}

/// Reassembles one viewer's chunks into JPEG frames.
#[derive(Default)]
struct Assembly {
	seq: Option<u32>,
	buf: Vec<u8>,
}

impl Assembly {
	fn push(&mut self, data: &[u8]) -> Option<Vec<u8>> {
		if data.len() < 5 {
			return None;
		}
		let seq = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
		if self.seq != Some(seq) {
			self.seq = Some(seq);
			self.buf.clear();
		}
		self.buf.extend_from_slice(&data[5..]);
		if data[4] == 1 {
			self.seq = None;
			Some(std::mem::take(&mut self.buf))
		} else {
			None
		}
	}
}

fn new_call_id() -> String {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|d| d.as_nanos())
		.unwrap_or(0);
	format!("c{nanos:x}")
}

#[cfg(test)]
mod tests {
	use super::Assembly;

	#[test]
	fn reassembles_chunks_and_resets_on_new_sequence() {
		let mut asm = Assembly::default();
		let frame = |seq: u32, last: bool, body: &[u8]| {
			let mut v = seq.to_be_bytes().to_vec();
			v.push(last as u8);
			v.extend_from_slice(body);
			v
		};
		assert_eq!(asm.push(&frame(1, false, b"ab")), None);
		// A new sequence drops the unfinished one.
		assert_eq!(asm.push(&frame(2, false, b"cd")), None);
		assert_eq!(asm.push(&frame(2, true, b"ef")), Some(b"cdef".to_vec()));
	}
}

#[cfg(test)]
mod loopback {
	//! Two clients on one machine: A shares a synthetic screen, B receives it through the same
	//! signaling path the gateway uses. Covers capture, JPEG, chunking, and WebRTC on loopback.
	use super::*;
	use std::sync::mpsc;

	#[tokio::test(flavor = "multi_thread")]
	async fn screen_share_reaches_viewer() {
		let (a_out_tx, mut a_out) = tokio::sync::mpsc::unbounded_channel();
		let (b_out_tx, mut b_out) = tokio::sync::mpsc::unbounded_channel();
		let (a_ev_tx, _a_ev) = mpsc::channel::<Ev>();
		let (b_ev_tx, b_ev) = mpsc::channel::<Ev>();
		let emit_a: Emit = Arc::new(move |e| {
			let _ = a_ev_tx.send(e);
		});
		let emit_b: Emit = Arc::new(move |e| {
			let _ = b_ev_tx.send(e);
		});
		let a = P2p::new(Source::TestPattern, a_out_tx, emit_a);
		let b = P2p::new(Source::TestPattern, b_out_tx, emit_b);

		let (to_b, to_a) = (b.clone(), a.clone());
		tokio::spawn(async move {
			while let Some(s) = a_out.recv().await {
				let _ = to_b
					.on_signal("a".into(), s.call_id, s.kind.into(), s.payload)
					.await;
			}
		});
		tokio::spawn(async move {
			while let Some(s) = b_out.recv().await {
				let _ = to_a
					.on_signal("b".into(), s.call_id, s.kind.into(), s.payload)
					.await;
			}
		});

		a.start_share(vec!["b".into()]).await.expect("start share");
		let frame = tokio::task::spawn_blocking(move || {
			let deadline = std::time::Instant::now() + Duration::from_secs(60);
			while std::time::Instant::now() < deadline {
				if let Ok(Ev::Frame {
					from,
					width,
					height,
					rgba,
				}) = b_ev.recv_timeout(Duration::from_millis(200))
				{
					return Some((from, width, height, rgba.len()));
				}
			}
			None
		})
		.await
		.unwrap();
		a.stop_share().await;
		let (from, width, height, len) = frame.expect("viewer received a frame");
		assert_eq!(from, "a");
		assert_eq!((width, height), (960, 540));
		assert_eq!(len, 960 * 540 * 4);
	}
}
