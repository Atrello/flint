//! Flint Lite: a slim client for chatting with friends on a Steel server.
//!
//! Chat, friends and groups go through Steel. Screen share goes peer to peer, and Steel only
//! relays the WebRTC signaling.

mod app;
mod capture;
mod gateway;
mod model;
mod net;
mod p2p;
mod voice;

use std::path::PathBuf;

use model::Cmd;
use voice::AudioSource;

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

struct Args {
	/// Set only when `--server` is passed; otherwise the last server signed in to is used.
	server: Option<String>,
	home: PathBuf,
	capture: capture::Source,
	audio: AudioSource,
}

fn parse_args() -> Args {
	let mut args = std::env::args().skip(1);
	let mut server = None;
	let mut home = dirs::data_local_dir()
		.unwrap_or_else(|| PathBuf::from("."))
		.join("flint-lite");
	let mut capture = capture::Source::Monitor;
	let mut audio = AudioSource::Device;
	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--server" => {
				server = Some(args.next().unwrap_or_else(|| usage("--server needs a URL")))
			}
			"--home" => {
				home = PathBuf::from(
					args.next()
						.unwrap_or_else(|| usage("--home needs a folder")),
				)
			}
			// Synthetic frames instead of the screen, for machines without a display.
			"--test-pattern" => {
				capture = capture::Source::TestPattern;
				audio = AudioSource::Silence;
			}
			"--help" | "-h" => usage(""),
			other => usage(&format!("unknown option {other}")),
		}
	}
	Args {
		server,
		home,
		capture,
		audio,
	}
}

fn usage(problem: &str) -> ! {
	if !problem.is_empty() {
		eprintln!("flint-lite: {problem}");
	}
	eprintln!("usage: flint-lite [--server URL] [--home DIR] [--test-pattern]");
	std::process::exit(if problem.is_empty() { 0 } else { 2 });
}

fn main() -> eframe::Result {
	let args = parse_args();
	let options = eframe::NativeOptions {
		viewport: egui::ViewportBuilder::default()
			.with_title("Flint Lite")
			.with_inner_size([1040.0, 680.0])
			.with_min_inner_size([760.0, 480.0]),
		..Default::default()
	};
	let home = args.home.clone();
	let server = args
		.server
		.clone()
		.or_else(|| net::load_last_server(&home))
		.unwrap_or_else(|| DEFAULT_SERVER.to_string());
	let source = args.capture;
	let audio = args.audio;
	eframe::run_native(
		"Flint Lite",
		options,
		Box::new(move |cc| {
			let (cmd, ev) = net::spawn(cc.egui_ctx.clone(), source, audio, home.clone());
			if let Some(saved) = net::load_session(&home) {
				let _ = cmd.send(Cmd::Resume {
					server: saved.server,
					token: saved.token,
				});
			}
			Ok(Box::new(app::App::new(cc, cmd, ev, server)))
		}),
	)
}
