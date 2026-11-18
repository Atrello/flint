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
		atomic::{AtomicBool, AtomicU32, Ordering},
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
		media_engine::{MIME_TYPE_H264, MIME_TYPE_OPUS, MediaEngine},
	},
	data_channel::{RTCDataChannel, data_channel_message::DataChannelMessage},
	ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
	media::Sample,
	peer_connection::{
		RTCPeerConnection, configuration::RTCConfiguration,
		peer_connection_state::RTCPeerConnectionState,
		sdp::session_description::RTCSessionDescription,
	},
	rtcp,
	rtp::{codecs::h264::H264Packet, packetizer::Depacketizer},
	rtp_transceiver::{
		rtp_codec::{RTCRtpCodecCapability, RTPCodecType},
		rtp_sender::RTCRtpSender,
	},
	track::{
		track_local::{TrackLocal, track_local_static_sample::TrackLocalStaticSample},
		track_remote::TrackRemote,
	},
};

use crate::{
	capture::{self, Source},
	model::Ev,
	video::{VideoDecoder, VideoEncoder},
	voice::{self, AudioSource, Mixer},
};

pub const SCREEN_CHANNEL: &str = "screen";
const CHUNK: usize = 16 * 1024;
/// Frames are dropped for a viewer whose send buffer is this full, so a slow link lags instead of queueing.
const MAX_BUFFERED: usize = 4 * 1024 * 1024;
const FRAME_INTERVAL: Duration = Duration::from_millis(125);
/// Camera video: 720p at 30 frames per second, H.264 at 1.5 Mbit/s to start.
const CAMERA_FPS: f32 = 30.0;
const CAMERA_BITRATE: u32 = 1_500_000;
const CAMERA_MIN_BITRATE: u32 = 300_000;
const CAMERA_MAX_BITRATE: u32 = 2_500_000;
/// A camera track with no packets for this long means the peer turned the camera off.
const CAMERA_QUIET: Duration = Duration::from_secs(2);
/// Report progress to the UI every this many decoded voice frames (one second).
const VOICE_REPORT_EVERY: u64 = 50;
/// How long a ring lasts before it is declined or cancelled.
pub const RING_TIMEOUT: Duration = Duration::from_secs(30);
const RINGTONE: &str = "ringtone";
const RINGBACK: &str = "ringback";

/// An incoming ring: the offer, and whether it renegotiates a connection that already exists.
struct PendingRing {
	call_id: String,
	sdp: String,
	renegotiate: bool,
}

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

/// One camera frame before encoding.
#[derive(Clone)]
struct RawFrame {
	seq: u64,
	rgba: Arc<Vec<u8>>,
}

struct Peer {
	call_id: String,
	pc: Arc<RTCPeerConnection>,
	alive: Arc<AtomicBool>,
	sharing: bool,
	ringing: bool,
	/// True for a connection made for a voice call, so its audio is part of the call.
	joined: bool,
	/// The ring is on a connection that already carries a share, so timing out keeps it.
	renegotiating: bool,
	audio: Arc<TrackLocalStaticSample>,
	video: Arc<TrackLocalStaticSample>,
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
	camera_frames: watch::Sender<Option<RawFrame>>,
	camera_on: AtomicBool,
	/// The next camera frame must be a keyframe (a viewer just joined or the camera just started).
	camera_key: AtomicBool,
	/// Target camera bitrate, moved by the receivers' loss reports.
	camera_bitrate: AtomicU32,
	camera_capture: StdMutex<Option<JoinHandle<()>>>,
	in_call: AtomicBool,
	muted: AtomicBool,
	/// Incoming voice offers waiting for the user: peer -> (call id, offer SDP).
	pending: Mutex<HashMap<String, PendingRing>>,
	ringback_on: AtomicBool,
	ringback_running: AtomicBool,
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
		let (camera_frames, _) = watch::channel(None);
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
			camera_frames,
			camera_on: AtomicBool::new(false),
			camera_key: AtomicBool::new(false),
			camera_bitrate: AtomicU32::new(CAMERA_BITRATE),
			camera_capture: StdMutex::new(None),
			in_call: AtomicBool::new(false),
			muted: AtomicBool::new(false),
			pending: Mutex::new(HashMap::new()),
			ringback_on: AtomicBool::new(false),
			ringback_running: AtomicBool::new(false),
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
		tokio::spawn(video_sender(inner.clone()));
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

	/// Mute or unmute the microphone. Muting keeps the call and the connections open.
	pub fn set_muted(&self, muted: bool) {
		self.inner.muted.store(muted, Ordering::SeqCst);
	}

	/// Turn the camera on or off. Frames go only to peers who have joined the call.
	pub fn set_camera(&self, on: bool) {
		self.inner.camera_on.store(on, Ordering::SeqCst);
		if on {
			self.inner.camera_key.store(true, Ordering::SeqCst);
			self.ensure_camera_capture();
		} else {
			self.inner.camera_frames.send_replace(None);
		}
	}

	/// Leave the voice call. This closes every connection, which also ends any share and the camera.
	pub async fn leave_call(&self) {
		self.set_camera(false);
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
				// A voice offer rings the user. Nothing connects until they accept.
				if payload["voice"].as_bool().unwrap_or(false) {
					let renegotiate = payload["renegotiate"].as_bool().unwrap_or(false);
					return self.receive_ring(from, call_id, sdp, renegotiate).await;
				}
				self.on_offer(from, call_id, sdp, false).await
			}
			"answer" => {
				let sdp = payload["sdp"]
					.as_str()
					.ok_or("answer without sdp")?
					.to_string();
				match self.pc_for(&from, &call_id).await {
					Some(pc) => {
						pc.set_remote_description(RTCSessionDescription::answer(sdp).map_err(s)?)
							.await
							.map_err(s)?;
						if let Some(p) = self.inner.peers.lock().await.get_mut(&from) {
							if p.call_id == call_id {
								// Only a ring makes this connection part of the call; a share answer does not.
								if p.ringing {
									p.joined = true;
								}
								p.ringing = false;
								p.renegotiating = false;
							}
						}
						self.emit_state().await;
						Ok(())
					}
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
			"decline" if payload["renegotiate"].as_bool().unwrap_or(false) => {
				// Declined on a live connection: keep the connection and drop the ring.
				if let Some(p) = self.inner.peers.lock().await.get_mut(&from) {
					if p.call_id == call_id {
						p.ringing = false;
						p.renegotiating = false;
					}
				}
				self.emit_state().await;
				Ok(())
			}
			"hangup" | "decline" => {
				let cancelled = {
					let mut pending = self.inner.pending.lock().await;
					let matches = pending.get(&from).is_some_and(|r| r.call_id == call_id);
					if matches {
						pending.remove(&from);
					}
					matches
				};
				if cancelled {
					self.emit_state().await;
				}
				self.remove_if(&from, &call_id).await;
				Ok(())
			}
			_ => Ok(()),
		}
	}

	/// Create connections to `peers` that do not exist yet, and send offers on them.
	async fn connect(&self, peers: Vec<String>, voice: bool) -> Result<(), String> {
		for peer in peers {
			// A connection that is already in the call stays. A share-only connection is
			// replaced, so the other side is rung for the call instead of joining silently.
			let existing = self
				.inner
				.peers
				.lock()
				.await
				.get(&peer)
				.map(|p| (p.joined, p.call_id.clone()));
			if let Some((joined, live_call)) = existing {
				if joined || !voice {
					continue;
				}
				// A share is live on this connection: ring over it so the share keeps running.
				self.renegotiate_ring(&peer, live_call).await?;
				continue;
			}
			let call_id = new_call_id();
			let (pc, audio, video) = self.new_pc(&peer).await?;
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
					ringing: voice,
					joined: voice,
					renegotiating: false,
					audio,
					video,
				},
			);
			if voice {
				self.expire_ring_later(peer.clone(), call_id.clone());
			}
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

	async fn on_offer(
		&self,
		from: String,
		call_id: String,
		sdp: String,
		voice: bool,
	) -> Result<(), String> {
		self.drop_peer(&from).await;
		let (pc, audio, video) = self.new_pc(&from).await?;
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
				ringing: false,
				joined: voice,
				renegotiating: false,
				audio,
				video,
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

	/// A voice offer rings the user. It waits in `pending` until they accept or decline.
	async fn receive_ring(
		&self,
		from: String,
		call_id: String,
		sdp: String,
		renegotiate: bool,
	) -> Result<(), String> {
		self.inner.pending.lock().await.insert(
			from.clone(),
			PendingRing {
				call_id: call_id.clone(),
				sdp,
				renegotiate,
			},
		);
		self.start_ringing(from, call_id);
		self.emit_state().await;
		Ok(())
	}

	/// Accept a ring. A new connection is answered from scratch. A ring on a live connection is
	/// answered on that same connection, so a share keeps running.
	pub async fn accept_call(&self, from: &str) -> Result<(), String> {
		let pending = self.inner.pending.lock().await.remove(from);
		let Some(ring) = pending else {
			return Err("that call has already ended".into());
		};
		self.inner.in_call.store(true, Ordering::SeqCst);
		if ring.renegotiate {
			let Some(pc) = self.pc_for(from, &ring.call_id).await else {
				return Err("the connection has already closed".into());
			};
			pc.set_remote_description(RTCSessionDescription::offer(ring.sdp).map_err(s)?)
				.await
				.map_err(s)?;
			let answer = pc.create_answer(None).await.map_err(s)?;
			let gathered = pc.gathering_complete_promise().await;
			pc.set_local_description(answer).await.map_err(s)?;
			let mut gathered = gathered;
			let _ = gathered.recv().await;
			let local = pc.local_description().await.ok_or("no local description")?;
			if let Some(p) = self.inner.peers.lock().await.get_mut(from) {
				if p.call_id == ring.call_id {
					p.joined = true;
					p.ringing = false;
					p.renegotiating = false;
				}
			}
			let _ = self.inner.out.send(OutSignal {
				to: from.to_string(),
				call_id: ring.call_id,
				kind: "answer",
				payload: json!({ "sdp": local.sdp }),
			});
		} else {
			self.on_offer(from.to_string(), ring.call_id, ring.sdp, true)
				.await?;
		}
		self.emit_state().await;
		Ok(())
	}

	/// Decline a ring, or let it expire: tell the caller and forget the offer.
	pub async fn decline_call(&self, from: &str) {
		let pending = self.inner.pending.lock().await.remove(from);
		if let Some(ring) = pending {
			let _ = self.inner.out.send(OutSignal {
				to: from.to_string(),
				call_id: ring.call_id,
				kind: "decline",
				payload: json!({ "renegotiate": ring.renegotiate }),
			});
		}
		self.emit_state().await;
	}

	/// Ring over a connection that already carries a share. The offer renegotiates it.
	async fn renegotiate_ring(&self, peer: &str, call_id: String) -> Result<(), String> {
		let Some(pc) = self.pc_for(peer, &call_id).await else {
			return Ok(());
		};
		let gathered = pc.gathering_complete_promise().await;
		let offer = pc.create_offer(None).await.map_err(s)?;
		pc.set_local_description(offer).await.map_err(s)?;
		let mut gathered = gathered;
		let _ = gathered.recv().await;
		let local = pc.local_description().await.ok_or("no local description")?;
		if let Some(p) = self.inner.peers.lock().await.get_mut(peer) {
			if p.call_id == call_id {
				p.ringing = true;
				p.renegotiating = true;
			}
		}
		self.expire_ring_later(peer.to_string(), call_id.clone());
		let _ = self.inner.out.send(OutSignal {
			to: peer.to_string(),
			call_id,
			kind: "offer",
			payload: json!({ "sdp": local.sdp, "voice": true, "renegotiate": true }),
		});
		self.emit_state().await;
		Ok(())
	}

	/// Play a ringtone while a ring waits. It stops when the ring is answered or expires.
	fn start_ringing(&self, from: String, call_id: String) {
		let this = self.clone();
		tokio::spawn(async move {
			let started = std::time::Instant::now();
			let mut tick = tokio::time::interval(voice::FRAME_DURATION);
			let mut frame = 0u64;
			loop {
				tick.tick().await;
				let waiting = this
					.inner
					.pending
					.lock()
					.await
					.get(&from)
					.is_some_and(|r| r.call_id == call_id);
				if !waiting {
					break;
				}
				if started.elapsed() >= RING_TIMEOUT {
					this.decline_call(&from).await;
					break;
				}
				this.inner
					.mixer
					.push(RINGTONE, &voice::cadence_frame(frame, 440.0, 50, 150));
				frame += 1;
			}
			this.inner.mixer.remove(RINGTONE);
		});
	}

	/// Start or stop the caller's ringback tone. It plays while any outgoing ring waits.
	fn set_ringback(&self, on: bool) {
		self.inner.ringback_on.store(on, Ordering::SeqCst);
		if on && !self.inner.ringback_running.swap(true, Ordering::SeqCst) {
			let inner = self.inner.clone();
			tokio::spawn(async move {
				let mut tick = tokio::time::interval(voice::FRAME_DURATION);
				let mut frame = 0u64;
				while inner.ringback_on.load(Ordering::SeqCst) {
					tick.tick().await;
					// Two seconds of tone, then four of silence.
					inner
						.mixer
						.push(RINGBACK, &voice::cadence_frame(frame, 440.0, 100, 300));
					frame += 1;
				}
				inner.mixer.remove(RINGBACK);
				inner.ringback_running.store(false, Ordering::SeqCst);
			});
		}
	}

	/// A ring nobody answers ends after `RING_TIMEOUT`, on the caller's side too. A ring on a
	/// live connection only ends the ring; the connection and its share stay.
	fn expire_ring_later(&self, peer: String, call_id: String) {
		let this = self.clone();
		tokio::spawn(async move {
			tokio::time::sleep(RING_TIMEOUT).await;
			let action = {
				let mut peers = this.inner.peers.lock().await;
				match peers.get_mut(&peer) {
					Some(p) if p.ringing && p.call_id == call_id => {
						if p.renegotiating {
							p.ringing = false;
							p.renegotiating = false;
							Some(false)
						} else {
							Some(true)
						}
					}
					_ => None,
				}
			};
			match action {
				Some(true) => {
					this.hang_up(&peer, call_id).await;
					this.emit_state().await;
				}
				Some(false) => this.emit_state().await,
				None => {}
			}
		});
	}

	/// A new connection with an Opus audio track, plus playback of whatever the peer sends.
	async fn new_pc(
		&self,
		peer: &str,
	) -> Result<
		(
			Arc<RTCPeerConnection>,
			Arc<TrackLocalStaticSample>,
			Arc<TrackLocalStaticSample>,
		),
		String,
	> {
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
		let video = Arc::new(TrackLocalStaticSample::new(
			RTCRtpCodecCapability {
				mime_type: MIME_TYPE_H264.to_owned(),
				clock_rate: 90000,
				sdp_fmtp_line:
					"level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"
						.to_owned(),
				..Default::default()
			},
			"camera".to_owned(),
			"flint-lite".to_owned(),
		));
		let video_rtp = pc
			.add_track(video.clone() as Arc<dyn TrackLocal + Send + Sync>)
			.await
			.map_err(s)?;
		tokio::spawn(watch_camera_feedback(video_rtp, self.inner.clone()));
		self.attach_playback(peer.to_string(), &pc);
		Ok((pc, audio, video))
	}

	/// Decode the peer's audio track into the speaker mixer.
	fn attach_playback(&self, peer: String, pc: &RTCPeerConnection) {
		let inner = self.inner.clone();
		pc.on_track(Box::new(
			move |track: Arc<TrackRemote>, _receiver, _transceiver| {
				let inner = inner.clone();
				let peer = peer.clone();
				// webrtc-rs runs the handler under a lock until it returns, so the reading
				// loops run on their own task to let the next track (video) be handled.
				tokio::spawn(async move {
					if track.kind() == RTPCodecType::Video {
						receive_video(track, peer, inner.emit.clone()).await;
						return;
					}
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
				});
				Box::pin(async {})
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
										camera: false,
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
				if matches!(state, RTCPeerConnectionState::Failed) {
					(this.inner.emit)(Ev::Error(format!(
						"Call with {peer} could not connect. Check your network or TURN settings."
					)));
				}
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

	fn ensure_camera_capture(&self) {
		let mut slot = self
			.inner
			.camera_capture
			.lock()
			.unwrap_or_else(|e| e.into_inner());
		if slot.as_ref().is_some_and(|h| !h.is_finished()) {
			return;
		}
		let inner = self.inner.clone();
		*slot = Some(tokio::spawn(async move {
			let source = inner.source;
			let mut counter = 0u64;
			let mut tick = tokio::time::interval(Duration::from_secs_f32(1.0 / CAMERA_FPS));
			tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
			while inner.camera_on.load(Ordering::SeqCst) {
				tick.tick().await;
				counter += 1;
				let c = counter;
				match tokio::task::spawn_blocking(move || capture::grab_camera_frame(source, c))
					.await
				{
					Ok(Ok(Some(image))) => {
						inner.camera_frames.send_replace(Some(RawFrame {
							seq: c,
							rgba: Arc::new(image.into_raw()),
						}));
					}
					Ok(Ok(None)) => {}
					Ok(Err(e)) => {
						(inner.emit)(Ev::Notice(e));
						inner.camera_on.store(false, Ordering::SeqCst);
						(inner.emit)(Ev::Camera(false));
					}
					Err(_) => {}
				}
			}
		}));
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
		let mut ringing_in: Vec<String> = self.inner.pending.lock().await.keys().cloned().collect();
		ringing_in.sort();
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
		let mut connected: Vec<String> = peers
			.iter()
			.filter(|(_, p)| !p.ringing)
			.map(|(id, _)| id.clone())
			.collect();
		let mut ringing_out: Vec<String> = peers
			.iter()
			.filter(|(_, p)| p.ringing)
			.map(|(id, _)| id.clone())
			.collect();
		ringing_out.sort();
		self.set_ringback(!ringing_out.is_empty());
		outgoing.sort();
		incoming.sort();
		connected.sort();
		(self.inner.emit)(Ev::Share { outgoing, incoming });
		(self.inner.emit)(Ev::Call {
			in_call: self.inner.in_call.load(Ordering::SeqCst),
			peers: connected,
			ringing_out,
			ringing_in,
		});
	}
}

/// Encode each microphone frame once and write it to every connected peer while in a call.
async fn voice_sender(inner: Arc<Inner>, mut mic: tokio::sync::mpsc::UnboundedReceiver<Vec<f32>>) {
	let Ok(mut encoder) = voice::new_encoder() else {
		return;
	};
	while let Some(frame) = mic.recv().await {
		if !inner.in_call.load(Ordering::SeqCst) || inner.muted.load(Ordering::SeqCst) {
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
			// Only connections in the call with an accepted peer carry the microphone.
			.filter(|p| p.joined && !p.ringing)
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
/// Read receiver reports for one camera sender and move the camera bitrate: down when peers
/// lose packets, back up slowly when the link is clean.
async fn watch_camera_feedback(sender: Arc<RTCRtpSender>, inner: Arc<Inner>) {
	while let Ok((packets, _)) = sender.read_rtcp().await {
		for packet in packets {
			let Some(report) = packet
				.as_any()
				.downcast_ref::<rtcp::receiver_report::ReceiverReport>()
			else {
				continue;
			};
			let worst = report
				.reports
				.iter()
				.map(|r| r.fraction_lost)
				.max()
				.unwrap_or(0);
			let current = inner.camera_bitrate.load(Ordering::SeqCst);
			// fraction_lost counts 1/256ths: about 5% and 2% loss.
			let next = if worst > 13 {
				current / 10 * 7
			} else if worst < 5 {
				current / 10 * 11
			} else {
				current
			};
			inner.camera_bitrate.store(
				next.clamp(CAMERA_MIN_BITRATE, CAMERA_MAX_BITRATE),
				Ordering::SeqCst,
			);
		}
	}
}

/// Encode the camera once and write each access unit to every peer who has joined the call.
async fn video_sender(inner: Arc<Inner>) {
	let mut rx = inner.camera_frames.subscribe();
	let mut encoder: Option<VideoEncoder> = None;
	let mut last = 0u64;
	while rx.changed().await.is_ok() {
		let frame = rx.borrow_and_update().clone();
		let Some(frame) = frame else {
			// The camera is off: release the encoder until it starts again.
			encoder = None;
			continue;
		};
		if frame.seq == last || !inner.camera_on.load(Ordering::SeqCst) {
			continue;
		}
		last = frame.seq;
		let key = inner.camera_key.swap(false, Ordering::SeqCst);
		let target = inner.camera_bitrate.load(Ordering::SeqCst);
		let mut enc = match encoder.take() {
			Some(mut enc) => {
				let _ = enc.set_bitrate(target);
				enc
			}
			None => match VideoEncoder::new(
				capture::CAMERA_WIDTH,
				capture::CAMERA_HEIGHT,
				CAMERA_FPS,
				target,
			) {
				Ok(enc) => enc,
				Err(e) => {
					(inner.emit)(Ev::Notice(e));
					inner.camera_on.store(false, Ordering::SeqCst);
					(inner.emit)(Ev::Camera(false));
					continue;
				}
			},
		};
		let (enc, result) = tokio::task::spawn_blocking(move || {
			if key {
				enc.force_keyframe();
			}
			let result = enc.encode(&frame.rgba);
			(enc, result)
		})
		.await
		.unwrap_or_else(|_| unreachable!("encode task panicked"));
		encoder = Some(enc);
		let Ok(access_unit) = result else {
			continue;
		};
		let tracks: Vec<Arc<TrackLocalStaticSample>> = inner
			.peers
			.lock()
			.await
			.values()
			.filter(|p| p.joined && !p.ringing)
			.map(|p| p.video.clone())
			.collect();
		let data = Bytes::from(access_unit);
		for track in tracks {
			let sample = Sample {
				data: data.clone(),
				duration: Duration::from_secs_f32(1.0 / CAMERA_FPS),
				..Default::default()
			};
			let _ = track.write_sample(&sample).await;
		}
	}
}

/// Decode one peer's camera track into frames for the UI. A track with no packets for
/// `CAMERA_QUIET` means the peer turned the camera off.
async fn receive_video(track: Arc<TrackRemote>, peer: String, emit: Emit) {
	let Ok(mut decoder) = VideoDecoder::new() else {
		return;
	};
	let mut depacketizer = H264Packet::default();
	let mut access_unit: Vec<u8> = Vec::new();
	loop {
		let packet = match tokio::time::timeout(CAMERA_QUIET, track.read_rtp()).await {
			Err(_) => {
				emit(Ev::CameraOff(peer.clone()));
				continue;
			}
			Ok(Err(_)) => break,
			Ok(Ok((packet, _))) => packet,
		};
		if let Ok(nals) = depacketizer.depacketize(&packet.payload) {
			access_unit.extend_from_slice(&nals);
		}
		if !packet.header.marker || access_unit.is_empty() {
			continue;
		}
		let unit = std::mem::take(&mut access_unit);
		let (dec, result) = match tokio::task::spawn_blocking(move || {
			let result = decoder.decode(&unit);
			(decoder, result)
		})
		.await
		{
			Ok(pair) => pair,
			Err(_) => break,
		};
		decoder = dec;
		if let Ok(Some((width, height, rgba))) = result {
			emit(Ev::Frame {
				from: peer.clone(),
				camera: true,
				width,
				height,
				rgba,
			});
		}
	}
	emit(Ev::CameraOff(peer));
}

/// Send each new screen frame on `dc` to one viewer until the connection closes.
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

		// Ring first, accept, then share on the joined connection.
		a.start_call(vec!["b".into()]).await.expect("start call");
		let b_ev = tokio::task::spawn_blocking(move || {
			let deadline = std::time::Instant::now() + Duration::from_secs(20);
			while std::time::Instant::now() < deadline {
				if let Ok(Ev::Call { ringing_in, .. }) =
					b_ev.recv_timeout(Duration::from_millis(200))
				{
					if ringing_in.contains(&"a".to_string()) {
						break;
					}
				}
			}
			b_ev
		})
		.await
		.unwrap();
		b.accept_call("a").await.expect("accept");
		a.start_share(vec!["b".into()]).await.expect("start share");

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
						..
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

#[cfg(test)]
mod ring_tests {
	//! Calls ring the other side. Nothing connects until it accepts, and a decline ends the ring.
	use super::*;
	use std::sync::mpsc;

	fn pair() -> (P2p, P2p, mpsc::Receiver<Ev>, mpsc::Receiver<Ev>) {
		let (a_out_tx, mut a_out) = tokio::sync::mpsc::unbounded_channel();
		let (b_out_tx, mut b_out) = tokio::sync::mpsc::unbounded_channel();
		let (a_ev_tx, a_ev) = mpsc::channel::<Ev>();
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
		(a, b, a_ev, b_ev)
	}

	/// Wait for an event matching `pick`, up to 20 seconds.
	fn wait<T>(rx: &mpsc::Receiver<Ev>, pick: impl Fn(&Ev) -> Option<T>) -> Option<T> {
		let deadline = std::time::Instant::now() + Duration::from_secs(20);
		while std::time::Instant::now() < deadline {
			if let Ok(ev) = rx.recv_timeout(Duration::from_millis(100)) {
				if let Some(v) = pick(&ev) {
					return Some(v);
				}
			}
		}
		None
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn call_rings_until_accepted() {
		let (a, b, a_ev, b_ev) = pair();
		a.start_call(vec!["b".into()]).await.expect("start call");

		let ringing = tokio::task::spawn_blocking(move || {
			let got = wait(&b_ev, |e| match e {
				Ev::Call { ringing_in, .. } if ringing_in.contains(&"a".to_string()) => Some(true),
				_ => None,
			});
			(got, b_ev)
		})
		.await
		.unwrap();
		assert_eq!(ringing.0, Some(true), "callee is rung");
		let b_ev = ringing.1;

		b.accept_call("a").await.expect("accept");
		let connected = tokio::task::spawn_blocking(move || {
			wait(&a_ev, |e| match e {
				Ev::Call {
					peers, ringing_out, ..
				} if peers.contains(&"b".to_string()) && ringing_out.is_empty() => Some(()),
				_ => None,
			})
		})
		.await
		.unwrap();
		assert!(connected.is_some(), "caller connects after accept");
		let _ = b_ev;
		a.leave_call().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn camera_frames_reach_a_joined_peer() {
		let (a, b, a_ev, b_ev) = pair();
		a.start_call(vec!["b".into()]).await.expect("start call");
		let b_ev = tokio::task::spawn_blocking(move || {
			let rang = wait(&b_ev, |e| match e {
				Ev::Call { ringing_in, .. } if ringing_in.contains(&"a".to_string()) => Some(()),
				_ => None,
			});
			assert!(rang.is_some(), "callee is rung");
			b_ev
		})
		.await
		.unwrap();
		b.accept_call("a").await.expect("accept");
		a.set_camera(true);

		let got = tokio::task::spawn_blocking(move || {
			wait(&b_ev, |e| match e {
				Ev::Frame {
					from,
					camera: true,
					width,
					height,
					..
				} => Some((from.clone(), *width, *height)),
				_ => None,
			})
		})
		.await
		.unwrap();
		assert_eq!(got, Some(("a".to_string(), 1280, 720)));
		a.set_camera(false);
		let _ = a_ev;
		a.leave_call().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn decline_ends_the_ring_for_the_caller() {
		let (a, b, a_ev, b_ev) = pair();
		a.start_call(vec!["b".into()]).await.expect("start call");
		let rung = tokio::task::spawn_blocking(move || {
			let got = wait(&b_ev, |e| match e {
				Ev::Call { ringing_in, .. } if !ringing_in.is_empty() => Some(()),
				_ => None,
			});
			(got, b_ev)
		})
		.await
		.unwrap();
		assert!(rung.0.is_some(), "callee is rung");
		b.decline_call("a").await;
		let done = tokio::task::spawn_blocking(move || {
			wait(&a_ev, |e| match e {
				Ev::Call {
					ringing_out, peers, ..
				} if ringing_out.is_empty() && peers.is_empty() => Some(()),
				_ => None,
			})
		})
		.await
		.unwrap();
		assert!(done.is_some(), "caller sees the decline");
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn call_over_a_live_share_keeps_the_share() {
		let (a, b, a_ev, b_ev) = pair();
		drop(a_ev);
		a.start_share(vec!["b".into()]).await.expect("start share");
		let handle = tokio::runtime::Handle::current();
		let (a2, b2) = (a.clone(), b.clone());
		let outcome = tokio::task::spawn_blocking(move || {
			let shared = wait(&b_ev, |e| matches!(e, Ev::Frame { .. }).then_some(())).is_some();
			handle
				.block_on(a2.start_call(vec!["b".into()]))
				.expect("start call");
			let rung = wait(&b_ev, |e| match e {
				Ev::Call { ringing_in, .. } if ringing_in.contains(&"a".to_string()) => Some(()),
				_ => None,
			})
			.is_some();
			handle.block_on(b2.accept_call("a")).expect("accept");
			// Frames keep arriving on the same connection after the call joins.
			let mut frames_after = 0;
			let deadline = std::time::Instant::now() + Duration::from_secs(30);
			while std::time::Instant::now() < deadline && frames_after < 5 {
				if let Ok(Ev::Frame { .. }) = b_ev.recv_timeout(Duration::from_millis(200)) {
					frames_after += 1;
				}
			}
			(shared, rung, frames_after)
		})
		.await
		.unwrap();
		assert!(outcome.0, "viewer received frames before the call");
		assert!(outcome.1, "viewer is rung over the live share");
		assert!(outcome.2 >= 5, "share keeps running after accept");
		a.stop_share().await;
		a.leave_call().await;
	}
}
