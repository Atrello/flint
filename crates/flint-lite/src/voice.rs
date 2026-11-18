//! Voice: microphone capture, Opus encoding and decoding, and speaker playback.
//!
//! Audio is 48 kHz Opus in 20 ms frames. Each frame is encoded mono, and sent over a WebRTC
//! audio track. Remote audio is decoded and mixed into one speaker stream. `AudioSource::Silence`
//! and `AudioSink::Silent` keep the path working on machines without audio devices and in tests.

use std::{
	collections::{HashMap, VecDeque},
	sync::{Arc, Mutex},
	time::Duration,
};

use tokio::sync::mpsc::UnboundedSender;

pub const RATE: u32 = 48_000;
/// 20 ms of mono audio at 48 kHz.
pub const FRAME: usize = 960;
pub const FRAME_DURATION: Duration = Duration::from_millis(20);
/// One frame of a ringtone: a 440 Hz tone for one second of every three, at low volume.
pub fn ringtone_frame(index: u64) -> Vec<f32> {
	let period = 3 * 50; // frames
	let on = (index % period) < 50;
	(0..FRAME)
		.map(|k| {
			if !on {
				return 0.0;
			}
			let t = (index * FRAME as u64 + k as u64) as f32 / RATE as f32;
			0.15 * (2.0 * std::f32::consts::PI * 440.0 * t).sin()
		})
		.collect()
}

/// Remote audio queued per speaker, at most this much (about 300 ms), so latency stays bounded.
const MAX_QUEUED: usize = (RATE as usize * 3) / 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AudioSource {
	/// The default microphone.
	Device,
	/// Silent frames, for tests and machines without a microphone.
	Silence,
}

/// Speaker mixer shared by every remote connection.
#[derive(Clone, Default)]
pub struct Mixer {
	peers: Arc<Mutex<HashMap<String, VecDeque<f32>>>>,
}

impl Mixer {
	/// Queue decoded mono samples from `peer`, dropping the oldest if the queue grows too long.
	pub fn push(&self, peer: &str, samples: &[f32]) {
		let mut peers = self.peers.lock().unwrap_or_else(|e| e.into_inner());
		let queue = peers.entry(peer.to_string()).or_default();
		queue.extend(samples.iter().copied());
		while queue.len() > MAX_QUEUED {
			queue.pop_front();
		}
	}

	pub fn remove(&self, peer: &str) {
		self.peers
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.remove(peer);
	}

	/// Next output sample at `rate`, summed over every speaker and clamped.
	fn mix_sample(
		peers: &mut HashMap<String, VecDeque<f32>>,
		step: f64,
		acc: &mut f64,
		last: &mut HashMap<String, f32>,
	) -> f32 {
		let mut sum = 0.0f32;
		*acc += step;
		for (peer, queue) in peers.iter_mut() {
			let value = last.entry(peer.clone()).or_insert(0.0);
			// Consume one input sample for each whole step, so a device at another rate plays at the right speed.
			while *acc >= 1.0 {
				*acc -= 1.0;
				if let Some(v) = queue.pop_front() {
					*value = v;
				}
			}
			sum += *value;
		}
		sum.clamp(-1.0, 1.0)
	}
}

/// Start the microphone on its own thread. Mono frames of `FRAME` samples arrive on `out`.
pub fn start_microphone(
	source: AudioSource,
	out: UnboundedSender<Vec<f32>>,
	on_error: impl Fn(String) + Send + 'static,
) {
	match source {
		AudioSource::Silence => {
			tokio::spawn(async move {
				let mut tick = tokio::time::interval(FRAME_DURATION);
				loop {
					tick.tick().await;
					if out.send(vec![0.0; FRAME]).is_err() {
						break;
					}
				}
			});
		}
		AudioSource::Device => {
			std::thread::Builder::new()
				.name("flint-lite-mic".into())
				.spawn(move || {
					if let Err(e) = run_input(out) {
						on_error(format!("microphone: {e}"));
					}
				})
				.ok();
		}
	}
}

fn run_input(out: UnboundedSender<Vec<f32>>) -> Result<(), String> {
	use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
	let device = cpal::default_host()
		.default_input_device()
		.ok_or("no input device")?;
	let config = device.default_input_config().map_err(|e| e.to_string())?;
	let channels = config.channels() as usize;
	let device_rate = config.sample_rate() as f64;
	// Input samples per output sample, for linear resampling to 48 kHz.
	let ratio = device_rate / RATE as f64;
	let mut raw: Vec<f32> = Vec::new();
	let mut cursor = 0.0f64;
	let mut frame: Vec<f32> = Vec::with_capacity(FRAME);
	let mut handle = move |mono: &[f32]| {
		raw.extend_from_slice(mono);
		while cursor + 1.0 < raw.len() as f64 {
			let i = cursor.floor() as usize;
			let frac = (cursor - i as f64) as f32;
			frame.push(raw[i] * (1.0 - frac) + raw[i + 1] * frac);
			cursor += ratio;
			if frame.len() == FRAME {
				let _ = out.send(std::mem::take(&mut frame));
				frame = Vec::with_capacity(FRAME);
			}
		}
		let consumed = cursor.floor() as usize;
		if consumed > 0 {
			raw.drain(..consumed);
			cursor -= consumed as f64;
		}
	};
	let err = |e| eprintln!("microphone stream: {e}");
	let stream = match config.sample_format() {
		cpal::SampleFormat::F32 => device.build_input_stream(
			config.clone().into(),
			move |data: &[f32], _| {
				let mono: Vec<f32> = data
					.chunks(channels)
					.map(|c| c.iter().sum::<f32>() / channels as f32)
					.collect();
				handle(&mono);
			},
			err,
			None,
		),
		cpal::SampleFormat::I16 => device.build_input_stream(
			config.clone().into(),
			move |data: &[i16], _| {
				let mono: Vec<f32> = data
					.chunks(channels)
					.map(|c| c.iter().map(|s| *s as f32 / 32768.0).sum::<f32>() / channels as f32)
					.collect();
				handle(&mono);
			},
			err,
			None,
		),
		other => return Err(format!("unsupported sample format {other:?}")),
	}
	.map_err(|e| e.to_string())?;
	stream.play().map_err(|e| e.to_string())?;
	// Keep the stream alive for the life of the process.
	loop {
		std::thread::park();
	}
}

/// Start the speaker. It plays the mixer's output. Without an output device it stays silent.
pub fn start_speaker(mixer: Mixer, on_error: impl Fn(String) + Send + 'static) {
	std::thread::Builder::new()
		.name("flint-lite-speaker".into())
		.spawn(move || {
			if let Err(e) = run_output(mixer) {
				on_error(format!("speaker: {e}"));
			}
		})
		.ok();
}

fn run_output(mixer: Mixer) -> Result<(), String> {
	use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
	let device = cpal::default_host()
		.default_output_device()
		.ok_or("no output device")?;
	let config = device.default_output_config().map_err(|e| e.to_string())?;
	let channels = config.channels() as usize;
	let step = RATE as f64 / config.sample_rate() as f64;
	let mut acc = 0.0f64;
	let mut last: HashMap<String, f32> = HashMap::new();
	let err = |e| eprintln!("speaker stream: {e}");
	let stream = match config.sample_format() {
		cpal::SampleFormat::F32 => device.build_output_stream(
			config.clone().into(),
			move |out: &mut [f32], _| {
				let mut peers = mixer.peers.lock().unwrap_or_else(|e| e.into_inner());
				for frame in out.chunks_mut(channels) {
					let v = Mixer::mix_sample(&mut peers, step, &mut acc, &mut last);
					frame.iter_mut().for_each(|s| *s = v);
				}
			},
			err,
			None,
		),
		cpal::SampleFormat::I16 => device.build_output_stream(
			config.clone().into(),
			move |out: &mut [i16], _| {
				let mut peers = mixer.peers.lock().unwrap_or_else(|e| e.into_inner());
				for frame in out.chunks_mut(channels) {
					let v = Mixer::mix_sample(&mut peers, step, &mut acc, &mut last);
					frame.iter_mut().for_each(|s| *s = (v * 32767.0) as i16);
				}
			},
			err,
			None,
		),
		other => return Err(format!("unsupported sample format {other:?}")),
	}
	.map_err(|e| e.to_string())?;
	stream.play().map_err(|e| e.to_string())?;
	loop {
		std::thread::park();
	}
}

/// Encode one mono frame to an Opus packet (a stereo-capable encoder is not needed; the track
/// advertises Opus/48000/2 and receivers decode mono packets fine).
pub fn encode(encoder: &mut opus2::Encoder, frame: &[f32]) -> Result<Vec<u8>, String> {
	let pcm: Vec<i16> = frame
		.iter()
		.map(|s| (s.clamp(-1.0, 1.0) * 32767.0) as i16)
		.collect();
	let mut out = vec![0u8; 4000];
	let n = encoder.encode(&pcm, &mut out).map_err(|e| e.to_string())?;
	out.truncate(n);
	Ok(out)
}

/// Decode one Opus packet to mono samples.
pub fn decode(decoder: &mut opus2::Decoder, packet: &[u8]) -> Result<Vec<f32>, String> {
	let mut pcm = vec![0i16; FRAME * 6];
	let n = decoder
		.decode(packet, &mut pcm, false)
		.map_err(|e| e.to_string())?;
	// Decoded as stereo; keep the left channel.
	Ok(pcm[..n * 2]
		.chunks(2)
		.map(|c| c[0] as f32 / 32768.0)
		.collect())
}

pub fn new_encoder() -> Result<opus2::Encoder, String> {
	opus2::Encoder::new(RATE, opus2::Channels::Mono, opus2::Application::Voip)
		.map_err(|e| e.to_string())
}

pub fn new_decoder() -> Result<opus2::Decoder, String> {
	opus2::Decoder::new(RATE, opus2::Channels::Stereo).map_err(|e| e.to_string())
}
