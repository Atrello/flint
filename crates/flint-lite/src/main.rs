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

use std::path::PathBuf;

use model::Cmd;

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

struct Args {
	server: String,
	home: PathBuf,
	capture: capture::Source,
}

fn parse_args() -> Args {
	let mut args = std::env::args().skip(1);
	let mut server = DEFAULT_SERVER.to_string();
	let mut home = dirs::data_local_dir()
		.unwrap_or_else(|| PathBuf::from("."))
		.join("flint-lite");
	let mut capture = capture::Source::Monitor;
	while let Some(arg) = args.next() {
		match arg.as_str() {
			"--server" => server = args.next().unwrap_or_else(|| usage("--server needs a URL")),
			"--home" => {
				home = PathBuf::from(
					args.next()
						.unwrap_or_else(|| usage("--home needs a folder")),
				)
			}
			// Synthetic frames instead of the screen, for machines without a display.
			"--test-pattern" => capture = capture::Source::TestPattern,
			"--help" | "-h" => usage(""),
			other => usage(&format!("unknown option {other}")),
		}
	}
	Args {
		server,
		home,
		capture,
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
	let server = args.server.clone();
	let home = args.home.clone();
	let source = args.capture;
	eframe::run_native(
		"Flint Lite",
		options,
		Box::new(move |cc| {
			let (cmd, ev) = net::spawn(cc.egui_ctx.clone(), source, home.clone());
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
