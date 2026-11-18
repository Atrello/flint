//! Peer-to-peer screen share and voice over WebRTC.
//!
//! Steel carries the offer, answer and ICE candidates (`CALL_SIGNAL`). Media goes straight
//! between clients. Each pair of clients has one connection, which carries an Opus audio track
//! and a data channel. The data channel carries screen frames: each JPEG is split into chunks,
//! and every chunk is a message `[u32 frame sequence BE][u8 last chunk flag][payload]`.
//!
//! A call sends the microphone to every connected peer. A share sends frames and leaves the
//! microphone alone. Ending a share or a call closes the connections it used.

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
	api::{
		API, APIBuilder,
		media_engine::{MIME_TYPE_OPUS, MediaEngine},
	},
	data_channel::{RTCDataChannel, data_channel_message::DataChannelMessage},
	ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
	media::Sample,
	peer_connection::{
		RTCPeerConnection, configuration::RTCConfiguration,
		peer_connection_state::RTCPeerConnectionState,
		sdp::session_description::RTCSessionDescription,
	},
	rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
	track::{
		track_local::{TrackLocal, track_local_static_sample::TrackLocalStaticSample},
		track_remote::TrackRemote,
	},
};

use crate::{
	capture::{self, Source},
	model::Ev,
	voice::{self, AudioSource, Mixer},
};

pub const SCREEN_CHANNEL: &str = "screen";
const CHUNK: usize = 16 * 1024;
/// Frames are dropped for a viewer whose send buffer is this full, so a slow link lags instead of queueing.
const MAX_BUFFERED: usize = 4 * 1024 * 1024;
const FRAME_INTERVAL: Duration = Duration::from_millis(125);
/// Report progress to the UI every this many decoded voice frames (one second).
const VOICE_REPORT_EVERY: u64 = 50;

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
	audio: Arc<TrackLocalStaticSample>,
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
	in_call: AtomicBool,
	mixer: Mixer,
}

#[derive(Clone)]
pub struct P2p {
	inner: Arc<Inner>,
}

/// WebRTC API with the default codecs, including Opus for voice and the H.264/VP8 defaults.
fn build_api() -> API {
	let mut media = MediaEngine::default();
	media
		.register_default_codecs()
		.expect("register default WebRTC codecs");
	APIBuilder::new().with_media_engine(media).build()
}

fn s(e: impl std::fmt::Display) -> String {
	e.to_string()
}

impl P2p {
	/// `audio` picks the microphone. `AudioSource::Device` also starts the speaker, which is what
	/// the app uses. Tests use `Silence`, which needs no audio hardware.
	pub fn new(
		source: Source,
		audio: AudioSource,
		out: UnboundedSender<OutSignal>,
		emit: Emit,
	) -> Self {
		// WebRTC's DTLS needs a process-wide rustls provider. Installing twice is harmless.
		let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
		let (frames, _) = watch::channel(None);
		let (mic_tx, mic_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<f32>>();
		let mixer = Mixer::default();
		let inner = Arc::new(Inner {
			api: build_api(),
			ice: Mutex::new(Vec::new()),
			peers: Mutex::new(HashMap::new()),
			out,
			emit: emit.clone(),
			frames,
			source,
			capture: StdMutex::new(None),
			in_call: AtomicBool::new(false),
			mixer: mixer.clone(),
		});
		let on_error = {
			let emit = emit.clone();
			move |e: String| emit(Ev::Notice(e))
		};
		voice::start_microphone(audio, mic_tx, on_error.clone());
		if audio == AudioSource::Device {
			voice::start_speaker(mixer, on_error);
		}
		tokio::spawn(voice_sender(inner.clone(), mic_rx));
		Self { inner }
	}

	pub async fn set_ice(&self, servers: Vec<RTCIceServer>) {
		*self.inner.ice.lock().await = servers;
	}

	/// Start sharing the screen with each of `peers`. Connections that already exist are reused.
	pub async fn start_share(&self, peers: Vec<String>) -> Result<(), String> {
		self.ensure_capture();
		self.connect(peers, false).await
	}

	/// Join a voice call with `peers`. The microphone goes to every connected peer.
	pub async fn start_call(&self, peers: Vec<String>) -> Result<(), String> {
		self.inner.in_call.store(true, Ordering::SeqCst);
		self.connect(peers, true).await?;
		self.emit_state().await;
		Ok(())
	}

	/// Stop sharing the screen. This closes the connections to viewers, as before.
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
			self.hang_up(&peer, call_id).await;
		}
		self.emit_state().await;
	}

	/// Leave the voice call. This closes every connection, which also ends any share.
	pub async fn leave_call(&self) {
		self.inner.in_call.store(false, Ordering::SeqCst);
		let all: Vec<(String, String)> = self
			.inner
			.peers
			.lock()
			.await
			.iter()
			.map(|(id, p)| (id.clone(), p.call_id.clone()))
			.collect();
		for (peer, call_id) in all {
			self.hang_up(&peer, call_id).await;
		}
		self.emit_state().await;
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
				// An offer that carries voice means the other side started a call; join it.
				if payload["voice"].as_bool().unwrap_or(false) {
					self.inner.in_call.store(true, Ordering::SeqCst);
				}
				self.on_offer(from, call_id, sdp).await
			}
			"answer" => {
				let sdp = payload["sdp"]
					.as_str()
					.ok_or("answer without sdp")?
					.to_string();
				match self.pc_for(&from, &call_id).await {
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

	/// Create connections to `peers` that do not exist yet, and send offers on them.
	async fn connect(&self, peers: Vec<String>, voice: bool) -> Result<(), String> {
		for peer in peers {
			if self.inner.peers.lock().await.contains_key(&peer) {
				continue;
			}
			let call_id = new_call_id();
			let (pc, audio) = self.new_pc(&peer).await?;
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
					sharing: !voice,
					audio,
				},
			);
			let _ = self.inner.out.send(OutSignal {
				to: peer,
				call_id,
				kind: "offer",
				payload: json!({ "sdp": local.sdp, "voice": voice }),
			});
		}
		self.emit_state().await;
		Ok(())
	}

	async fn on_offer(&self, from: String, call_id: String, sdp: String) -> Result<(), String> {
		self.drop_peer(&from).await;
		let (pc, audio) = self.new_pc(&from).await?;
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
				audio,
			},
		);
		let _ = self.inner.out.send(OutSignal {
			to: from,
			call_id,
			kind: "answer",
			payload: json!({ "sdp": local.sdp }),
		});
		self.emit_state().await;
		Ok(())
	}

	/// A new connection with an Opus audio track, plus playback of whatever the peer sends.
	async fn new_pc(
		&self,
		peer: &str,
	) -> Result<(Arc<RTCPeerConnection>, Arc<TrackLocalStaticSample>), String> {
		let config = RTCConfiguration {
			ice_servers: self.inner.ice.lock().await.clone(),
			..Default::default()
		};
		let pc = Arc::new(
			self.inner
				.api
				.new_peer_connection(config)
				.await
				.map_err(s)?,
		);
		let audio = Arc::new(TrackLocalStaticSample::new(
			RTCRtpCodecCapability {
				mime_type: MIME_TYPE_OPUS.to_owned(),
				clock_rate: voice::RATE,
				channels: 2,
				sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
				..Default::default()
			},
			"audio".to_owned(),
			"flint-lite".to_owned(),
		));
		pc.add_track(audio.clone() as Arc<dyn TrackLocal + Send + Sync>)
			.await
			.map_err(s)?;
		self.attach_playback(peer.to_string(), &pc);
		Ok((pc, audio))
	}

	/// Decode the peer's audio track into the speaker mixer.
	fn attach_playback(&self, peer: String, pc: &RTCPeerConnection) {
		let inner = self.inner.clone();
		pc.on_track(Box::new(
			move |track: Arc<TrackRemote>, _receiver, _transceiver| {
				let inner = inner.clone();
				let peer = peer.clone();
				Box::pin(async move {
					let Ok(mut decoder) = voice::new_decoder() else {
						return;
					};
					let mut frames = 0u64;
					while let Ok((packet, _)) = track.read_rtp().await {
						let Ok(samples) = voice::decode(&mut decoder, &packet.payload) else {
							continue;
						};
						inner.mixer.push(&peer, &samples);
						frames += 1;
						if frames % VOICE_REPORT_EVERY == 0 {
							(inner.emit)(Ev::Voice {
								from: peer.clone(),
								frames,
							});
						}
					}
					inner.mixer.remove(&peer);
				})
			},
		));
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
			self.emit_state().await;
		}
	}

	/// Tell the peer we are hanging up, then drop the connection.
	async fn hang_up(&self, peer: &str, call_id: String) {
		self.drop_peer(peer).await;
		let _ = self.inner.out.send(OutSignal {
			to: peer.to_string(),
			call_id,
			kind: "hangup",
			payload: Value::Null,
		});
	}

	/// Remove a peer from the table and close its connection. The lock is not held while closing.
	async fn drop_peer(&self, peer: &str) {
		let removed = self.inner.peers.lock().await.remove(peer);
		if let Some(p) = removed {
			p.alive.store(false, Ordering::SeqCst);
			let _ = p.pc.close().await;
		}
		self.inner.mixer.remove(peer);
		let (any_sharing, any_left) = {
			let peers = self.inner.peers.lock().await;
			(peers.values().any(|p| p.sharing), !peers.is_empty())
		};
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
		if !any_left {
			self.inner.in_call.store(false, Ordering::SeqCst);
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

	/// Tell the UI which peers we share with and whether we are in a call.
	async fn emit_state(&self) {
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
		let mut connected: Vec<String> = peers.keys().cloned().collect();
		outgoing.sort();
		incoming.sort();
		connected.sort();
		(self.inner.emit)(Ev::Share { outgoing, incoming });
		(self.inner.emit)(Ev::Call {
			in_call: self.inner.in_call.load(Ordering::SeqCst),
			peers: connected,
		});
	}
}

/// Encode each microphone frame once and write it to every connected peer while in a call.
async fn voice_sender(inner: Arc<Inner>, mut mic: tokio::sync::mpsc::UnboundedReceiver<Vec<f32>>) {
	let Ok(mut encoder) = voice::new_encoder() else {
		return;
	};
	while let Some(frame) = mic.recv().await {
		if !inner.in_call.load(Ordering::SeqCst) {
			continue;
		}
		let Ok(packet) = voice::encode(&mut encoder, &frame) else {
			continue;
		};
		let tracks: Vec<Arc<TrackLocalStaticSample>> = inner
			.peers
			.lock()
			.await
			.values()
			.map(|p| p.audio.clone())
			.collect();
		for track in tracks {
			let sample = Sample {
				data: Bytes::from(packet.clone()),
				duration: voice::FRAME_DURATION,
				..Default::default()
			};
			let _ = track.write_sample(&sample).await;
		}
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
	//! Two clients on one machine. A shares a synthetic screen and then starts a voice call, and
	//! B receives both over the same signaling path the gateway uses.
	use super::*;
	use std::sync::mpsc;

	#[tokio::test(flavor = "multi_thread")]
	async fn screen_share_and_voice_reach_the_other_client() {
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
		let a = P2p::new(Source::TestPattern, AudioSource::Silence, a_out_tx, emit_a);
		let b = P2p::new(Source::TestPattern, AudioSource::Silence, b_out_tx, emit_b);

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
		// Join the call on the same connection, so the listener gets audio and frames together.
		a.start_call(vec!["b".into()]).await.expect("start call");
		let (frame, voice) = tokio::task::spawn_blocking(move || {
			let deadline = std::time::Instant::now() + Duration::from_secs(60);
			let mut frame = None;
			let mut voice = None;
			while std::time::Instant::now() < deadline && (frame.is_none() || voice.is_none()) {
				match b_ev.recv_timeout(Duration::from_millis(200)) {
					Ok(Ev::Frame {
						from,
						width,
						height,
						rgba,
					}) if frame.is_none() => {
						frame = Some((from, width, height, rgba.len()));
					}
					Ok(Ev::Voice { from, frames }) if voice.is_none() && frames >= 25 => {
						voice = Some((from, frames));
					}
					_ => {}
				}
			}
			(frame, voice)
		})
		.await
		.unwrap();

		// Check that frames and audio both arrived, then tear down.
		a.stop_share().await;
		a.leave_call().await;

		let (from, width, height, len) = frame.expect("viewer received a frame");
		assert_eq!(from, "a");
		assert_eq!((width, height), (960, 540));
		assert_eq!(len, 960 * 540 * 4);
		let (from, frames) = voice.expect("listener heard audio frames");
		assert_eq!(from, "a");
		assert!(frames >= 25);
	}
}
