//! Screen frames for screen share: grab, downscale, and JPEG-encode.
//!
//! `Source::TestPattern` produces a moving pattern without a display, so the share path can be
//! exercised headless. `Source::Monitor` captures the primary monitor through `xcap`.

use std::{
	sync::{
		Arc, Mutex as StdMutex,
		atomic::{AtomicBool, Ordering},
	},
	time::{Duration, Instant},
};

use image::{
	ExtendedColorType, RgbImage, RgbaImage, codecs::jpeg::JpegEncoder, imageops::FilterType,
};
use nokhwa::{
	Camera,
	pixel_format::RgbFormat,
	utils::{CameraIndex, RequestedFormat, RequestedFormatType, Resolution},
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

/// Camera frames are letterboxed to this size so the H.264 encoder gets one fixed size.
pub const CAMERA_WIDTH: u32 = 1280;
pub const CAMERA_HEIGHT: u32 = 720;

/// Grab one camera frame as RGBA at `CAMERA_WIDTH` x `CAMERA_HEIGHT`. `Ok(None)` means the
/// camera has not produced its first frame yet.
pub fn grab_camera_frame(source: Source, counter: u64) -> Result<Option<RgbaImage>, String> {
	let frame = match source {
		Source::Monitor => match grab_device_camera()? {
			Some(frame) => frame,
			None => return Ok(None),
		},
		Source::TestPattern => camera_test_pattern(counter),
	};
	Ok(Some(fit_to_camera(frame)))
}

/// Scale `frame` to fit the camera size, keeping its aspect ratio, and pad the rest with black.
fn fit_to_camera(frame: RgbaImage) -> RgbaImage {
	if frame.width() == CAMERA_WIDTH && frame.height() == CAMERA_HEIGHT {
		return frame;
	}
	let scale = (CAMERA_WIDTH as f32 / frame.width() as f32)
		.min(CAMERA_HEIGHT as f32 / frame.height() as f32);
	let w = ((frame.width() as f32 * scale) as u32).clamp(2, CAMERA_WIDTH);
	let h = ((frame.height() as f32 * scale) as u32).clamp(2, CAMERA_HEIGHT);
	let scaled = image::imageops::resize(&frame, w, h, FilterType::Triangle);
	let mut canvas =
		RgbaImage::from_pixel(CAMERA_WIDTH, CAMERA_HEIGHT, image::Rgba([0, 0, 0, 255]));
	image::imageops::replace(
		&mut canvas,
		&scaled,
		((CAMERA_WIDTH - w) / 2) as i64,
		((CAMERA_HEIGHT - h) / 2) as i64,
	);
	canvas
}

/// The camera is owned by one thread. It runs while frames are wanted and closes the device
/// after a few seconds without a request, so the camera light goes off when nothing needs it.
struct CameraFeed {
	latest: Arc<StdMutex<Option<RgbaImage>>>,
	wanted: Arc<StdMutex<Instant>>,
	error: Arc<StdMutex<Option<String>>>,
	running: Arc<AtomicBool>,
}

static CAMERA: StdMutex<Option<CameraFeed>> = StdMutex::new(None);
const CAMERA_IDLE: Duration = Duration::from_secs(3);

fn grab_device_camera() -> Result<Option<RgbaImage>, String> {
	let mut slot = CAMERA.lock().unwrap_or_else(|e| e.into_inner());
	let feed = slot.get_or_insert_with(|| CameraFeed {
		latest: Arc::new(StdMutex::new(None)),
		wanted: Arc::new(StdMutex::new(Instant::now())),
		error: Arc::new(StdMutex::new(None)),
		running: Arc::new(AtomicBool::new(false)),
	});
	*feed.wanted.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
	if let Some(err) = feed.error.lock().unwrap_or_else(|e| e.into_inner()).take() {
		return Err(err);
	}
	if !feed.running.swap(true, Ordering::SeqCst) {
		let (latest, wanted, error, running) = (
			feed.latest.clone(),
			feed.wanted.clone(),
			feed.error.clone(),
			feed.running.clone(),
		);
		std::thread::spawn(move || {
			run_camera(&latest, &wanted, &error);
			*latest.lock().unwrap_or_else(|e| e.into_inner()) = None;
			running.store(false, Ordering::SeqCst);
		});
	}
	let frame = feed
		.latest
		.lock()
		.unwrap_or_else(|e| e.into_inner())
		.clone();
	Ok(frame)
}

fn run_camera(
	latest: &StdMutex<Option<RgbaImage>>,
	wanted: &StdMutex<Instant>,
	error: &StdMutex<Option<String>>,
) {
	let format = RequestedFormat::new::<RgbFormat>(RequestedFormatType::HighestResolution(
		Resolution::new(CAMERA_WIDTH, CAMERA_HEIGHT),
	));
	let mut camera = match Camera::new(CameraIndex::Index(0), format)
		.and_then(|mut camera| camera.open_stream().map(|_| camera))
	{
		Ok(camera) => camera,
		Err(e) => {
			*error.lock().unwrap_or_else(|e| e.into_inner()) = Some(format!("camera: {e}"));
			return;
		}
	};
	loop {
		if wanted.lock().unwrap_or_else(|e| e.into_inner()).elapsed() > CAMERA_IDLE {
			break;
		}
		let decoded = camera
			.frame()
			.and_then(|buffer| buffer.decode_image::<RgbFormat>());
		match decoded {
			Ok(rgb) => {
				*latest.lock().unwrap_or_else(|e| e.into_inner()) =
					Some(image::DynamicImage::ImageRgb8(rgb).to_rgba8());
			}
			Err(e) => {
				*error.lock().unwrap_or_else(|e| e.into_inner()) = Some(format!("camera: {e}"));
				break;
			}
		}
	}
	let _ = camera.stop_stream();
}

/// A moving disc on a gradient, to exercise the camera path without a device.
fn camera_test_pattern(counter: u64) -> RgbaImage {
	let (w, h) = (640u32, 360u32);
	let cx = (counter as f32 * 9.0) % w as f32;
	let cy = h as f32 / 2.0;
	RgbaImage::from_fn(w, h, |x, y| {
		let dx = x as f32 - cx;
		let dy = y as f32 - cy;
		if dx * dx + dy * dy < 40.0 * 40.0 {
			image::Rgba([250, 220, 60, 255])
		} else {
			image::Rgba([30, 40, (y * 120 / h) as u8 + 40, 255])
		}
	})
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

#[cfg(test)]
mod tests {
	use super::*;

	/// Needs a display: run with `xvfb-run -a cargo test -p flint-lite monitor -- --ignored`.
	#[test]
	#[ignore = "needs a display (xvfb-run)"]
	fn monitor_capture_produces_a_jpeg() {
		let jpeg = grab_jpeg(Source::Monitor, 1).expect("capture the primary monitor");
		let (w, h, rgba) = decode_jpeg(&jpeg).expect("decode");
		assert!(w > 0 && h > 0);
		assert_eq!(rgba.len(), (w * h * 4) as usize);
	}
}
