//! Screen frames for screen share: grab, downscale, and JPEG-encode.
//!
//! `Source::TestPattern` produces a moving pattern without a display, so the share path can be
//! exercised headless. `Source::Monitor` captures the primary monitor through `xcap`.

use image::{
	ExtendedColorType, RgbImage, RgbaImage, codecs::jpeg::JpegEncoder, imageops::FilterType,
};

/// Frames wider than this are scaled down before encoding.
const MAX_WIDTH: u32 = 1280;
const JPEG_QUALITY: u8 = 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
	Monitor,
	TestPattern,
}

/// Grab one frame from `source` and encode it as JPEG.
pub fn grab_jpeg(source: Source, counter: u64) -> Result<Vec<u8>, String> {
	let frame = match source {
		Source::Monitor => grab_monitor()?,
		Source::TestPattern => test_pattern(counter),
	};
	encode_jpeg(frame)
}

fn grab_monitor() -> Result<RgbaImage, String> {
	let monitors = xcap::Monitor::all().map_err(|e| format!("screen capture: {e}"))?;
	let monitor = monitors
		.iter()
		.find(|m| m.is_primary().unwrap_or(false))
		.or_else(|| monitors.first())
		.ok_or_else(|| "no monitor found to capture".to_string())?;
	monitor
		.capture_image()
		.map_err(|e| format!("screen capture: {e}"))
}

/// A 960x540 gradient with a bar that moves with the frame counter.
fn test_pattern(counter: u64) -> RgbaImage {
	let (w, h) = (960u32, 540u32);
	let bar = (counter as u32 * 12) % w;
	RgbaImage::from_fn(w, h, |x, y| {
		let in_bar = x >= bar && x < bar + 80;
		if in_bar {
			image::Rgba([255, 255, 255, 255])
		} else {
			image::Rgba([(x * 255 / w) as u8, (y * 255 / h) as u8, 96, 255])
		}
	})
}

pub fn encode_jpeg(frame: RgbaImage) -> Result<Vec<u8>, String> {
	let frame = if frame.width() > MAX_WIDTH {
		let h = (frame.height() as u64 * MAX_WIDTH as u64 / frame.width() as u64) as u32;
		image::imageops::resize(&frame, MAX_WIDTH, h.max(1), FilterType::Triangle)
	} else {
		frame
	};
	let rgb: RgbImage = image::DynamicImage::ImageRgba8(frame).to_rgb8();
	let mut out = Vec::new();
	JpegEncoder::new_with_quality(&mut out, JPEG_QUALITY)
		.encode(
			rgb.as_raw(),
			rgb.width(),
			rgb.height(),
			ExtendedColorType::Rgb8,
		)
		.map_err(|e| format!("jpeg encode: {e}"))?;
	Ok(out)
}

/// Decode a JPEG into RGBA pixels for the UI.
pub fn decode_jpeg(data: &[u8]) -> Result<(u32, u32, Vec<u8>), String> {
	let img = image::load_from_memory_with_format(data, image::ImageFormat::Jpeg)
		.map_err(|e| format!("jpeg decode: {e}"))?
		.to_rgba8();
	Ok((img.width(), img.height(), img.into_raw()))
}
