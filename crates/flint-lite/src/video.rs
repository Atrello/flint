//! H.264 for camera and screen share: encode RGBA frames, decode back to RGBA.
//!
//! Frames travel as RTP through a WebRTC video track, so this module only deals with
//! Annex B byte streams and pixels. Width and height must be even for the encoder.

use openh264::{
	decoder::Decoder,
	encoder::{BitRate, Encoder, EncoderConfig, FrameRate, RateControlMode, UsageType},
	formats::YUVSource,
	formats::{RgbaSliceU8, YUVBuffer},
};

/// Encodes RGBA frames at a fixed target bitrate. Changing the bitrate rebuilds the encoder,
/// so the next frame is a keyframe; callers should change it rarely.
pub struct VideoEncoder {
	encoder: Encoder,
	width: usize,
	height: usize,
	bitrate_bps: u32,
	fps: f32,
}

impl VideoEncoder {
	pub fn new(width: u32, height: u32, fps: f32, bitrate_bps: u32) -> Result<Self, String> {
		if width == 0 || height == 0 || width % 2 != 0 || height % 2 != 0 {
			return Err(format!(
				"video size must be even and non-zero, got {width}x{height}"
			));
		}
		Ok(Self {
			encoder: build(fps, bitrate_bps)?,
			width: width as usize,
			height: height as usize,
			bitrate_bps,
			fps,
		})
	}

	/// Set a new target bitrate. The encoder is rebuilt only when the change is large enough
	/// to matter, so small wobbles in the estimate do not force keyframes.
	pub fn set_bitrate(&mut self, bitrate_bps: u32) -> Result<(), String> {
		let change = bitrate_bps.abs_diff(self.bitrate_bps) as f32 / self.bitrate_bps as f32;
		if change < 0.15 {
			return Ok(());
		}
		self.encoder = build(self.fps, bitrate_bps)?;
		self.bitrate_bps = bitrate_bps;
		Ok(())
	}

	/// Encode one RGBA frame of the size given to `new`. Returns an Annex B access unit.
	pub fn encode(&mut self, rgba: &[u8]) -> Result<Vec<u8>, String> {
		if rgba.len() != self.width * self.height * 4 {
			return Err("video frame has the wrong size".to_string());
		}
		let yuv = YUVBuffer::from_rgba8_source(RgbaSliceU8::new(rgba, (self.width, self.height)));
		let stream = self
			.encoder
			.encode(&yuv)
			.map_err(|e| format!("h264 encode: {e}"))?;
		Ok(stream.to_vec())
	}

	/// Make the next frame a keyframe, for a viewer who has just joined.
	pub fn force_keyframe(&mut self) {
		self.encoder.force_intra_frame();
	}
}

fn build(fps: f32, bitrate_bps: u32) -> Result<Encoder, String> {
	let config = EncoderConfig::new()
		.usage_type(UsageType::CameraVideoRealTime)
		.rate_control_mode(RateControlMode::Bitrate)
		.bitrate(BitRate::from_bps(bitrate_bps))
		.max_frame_rate(FrameRate::from_hz(fps))
		.skip_frames(true);
	Encoder::with_api_config(openh264::OpenH264API::from_source(), config)
		.map_err(|e| format!("h264 encoder: {e}"))
}

/// Decodes Annex B access units to RGBA.
pub struct VideoDecoder {
	decoder: Decoder,
}

impl VideoDecoder {
	pub fn new() -> Result<Self, String> {
		Decoder::new()
			.map(|decoder| Self { decoder })
			.map_err(|e| format!("h264 decoder: {e}"))
	}

	/// Decode one access unit. Returns `(width, height, rgba)` when a picture is produced.
	pub fn decode(&mut self, annexb: &[u8]) -> Result<Option<(u32, u32, Vec<u8>)>, String> {
		let Some(yuv) = self
			.decoder
			.decode(annexb)
			.map_err(|e| format!("h264 decode: {e}"))?
		else {
			return Ok(None);
		};
		let (w, h) = yuv.dimensions();
		let mut rgba = vec![0u8; w * h * 4];
		yuv.write_rgba8(&mut rgba);
		Ok(Some((w as u32, h as u32, rgba)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn gradient(w: usize, h: usize, shift: usize) -> Vec<u8> {
		let mut out = Vec::with_capacity(w * h * 4);
		for y in 0..h {
			for x in 0..w {
				out.extend_from_slice(&[((x + shift) % 256) as u8, (y % 256) as u8, 128, 255]);
			}
		}
		out
	}

	#[test]
	fn h264_round_trip_keeps_size_and_content() {
		let (w, h) = (320usize, 240usize);
		let mut enc = VideoEncoder::new(w as u32, h as u32, 30.0, 1_000_000).expect("encoder");
		let mut dec = VideoDecoder::new().expect("decoder");
		let mut picture = None;
		for i in 0..3 {
			let packet = enc.encode(&gradient(w, h, i * 4)).expect("encode");
			if let Some(p) = dec.decode(&packet).expect("decode") {
				picture = Some(p);
			}
		}
		let (dw, dh, rgba) = picture.expect("a decoded picture");
		assert_eq!((dw as usize, dh as usize), (w, h));
		assert_eq!(rgba.len(), w * h * 4);
		// The first decoded picture should look like the gradient, not a blank frame.
		let mean: u64 = rgba.iter().step_by(4).map(|v| *v as u64).sum::<u64>() / (w * h) as u64;
		assert!(mean > 10, "decoded frame is nearly black");
	}

	#[test]
	fn odd_sizes_are_rejected() {
		assert!(VideoEncoder::new(321, 240, 30.0, 500_000).is_err());
	}
}
