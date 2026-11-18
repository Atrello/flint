//! The egui interface: sign-in, friends, direct and group messages, and screen share.

use std::{
	collections::{BTreeSet, HashMap},
	sync::mpsc::Receiver,
};

use egui::{
	Color32, ColorImage, CornerRadius, Margin, RichText, TextureHandle, TextureOptions, Ui,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::model::{
	Channel, Cmd, Ev, Message, Relationship, User, channel_type, relationship_type,
};

const ACCENT: Color32 = Color32::from_rgb(124, 92, 255);
const PANEL: Color32 = Color32::from_rgb(22, 23, 29);
const SURFACE: Color32 = Color32::from_rgb(32, 34, 42);
const MUTED: Color32 = Color32::from_rgb(150, 154, 170);
const DANGER: Color32 = Color32::from_rgb(237, 96, 96);
const GOOD: Color32 = Color32::from_rgb(90, 200, 140);

pub struct App {
	cmd: UnboundedSender<Cmd>,
	ev: Receiver<Ev>,
	server: String,
	username: String,
	password: String,
	register: bool,
	me: Option<User>,
	error: Option<String>,
	notice: Option<String>,
	channels: Vec<Channel>,
	relationships: Vec<Relationship>,
	messages: HashMap<String, Vec<Message>>,
	active: Option<String>,
	compose: String,
	add_friend: String,
	group_pick: BTreeSet<String>,
	outgoing: Vec<String>,
	incoming: Vec<String>,
	screens: HashMap<String, TextureHandle>,
	screen_size: HashMap<String, [usize; 2]>,
	watching: Option<String>,
	/// Latest camera frame per peer, shown as tiles during a call.
	cameras: HashMap<String, TextureHandle>,
	camera_on: bool,
	in_call: bool,
	call_peers: Vec<String>,
	speaking: HashMap<String, u64>,
	muted: bool,
	ringing_out: Vec<String>,
	ringing_in: Vec<String>,
}

impl App {
	pub fn new(
		cc: &eframe::CreationContext<'_>,
		cmd: UnboundedSender<Cmd>,
		ev: Receiver<Ev>,
		server: String,
	) -> Self {
		apply_theme(&cc.egui_ctx);
		Self {
			cmd,
			ev,
			server,
			username: String::new(),
			password: String::new(),
			register: false,
			me: None,
			error: None,
			notice: None,
			channels: Vec::new(),
			relationships: Vec::new(),
			messages: HashMap::new(),
			active: None,
			compose: String::new(),
			add_friend: String::new(),
			group_pick: BTreeSet::new(),
			outgoing: Vec::new(),
			incoming: Vec::new(),
			screens: HashMap::new(),
			screen_size: HashMap::new(),
			watching: None,
			cameras: HashMap::new(),
			camera_on: false,
			in_call: false,
			call_peers: Vec::new(),
			speaking: HashMap::new(),
			muted: false,
			ringing_out: Vec::new(),
			ringing_in: Vec::new(),
		}
	}

	fn send(&self, cmd: Cmd) {
		let _ = self.cmd.send(cmd);
	}

	fn handle_event(&mut self, ctx: &egui::Context, ev: Ev) {
		match ev {
			Ev::Error(e) => self.error = Some(e),
			Ev::Notice(n) => self.notice = Some(n),
			Ev::LoggedIn { server, me } => {
				self.server = server;
				self.me = Some(me);
				self.error = None;
				self.password.clear();
			}
			Ev::LoggedOut => {
				self.me = None;
				self.error = None;
				self.channels.clear();
				self.relationships.clear();
				self.messages.clear();
				self.active = None;
				self.compose.clear();
				self.group_pick.clear();
				self.outgoing.clear();
				self.incoming.clear();
				self.screens.clear();
				self.screen_size.clear();
				self.cameras.clear();
				self.camera_on = false;
				self.watching = None;
			}
			Ev::Channels(list) => self.channels = list,
			Ev::ChannelUpsert(channel) => {
				match self.channels.iter_mut().find(|c| c.id == channel.id) {
					Some(existing) => *existing = channel,
					None => self.channels.push(channel),
				}
			}
			Ev::Relationships(list) => self.relationships = list,
			Ev::History {
				channel_id,
				messages,
			} => {
				self.messages.insert(channel_id, messages);
			}
			Ev::Message(message) => {
				let list = self.messages.entry(message.channel_id.clone()).or_default();
				if !list.iter().any(|m| m.id == message.id) {
					list.push(message);
					list.sort_by_key(|m| m.id.parse::<u64>().unwrap_or(0));
				}
			}
			Ev::Call {
				in_call,
				peers,
				ringing_out,
				ringing_in,
			} => {
				self.in_call = in_call;
				self.call_peers = peers;
				self.ringing_out = ringing_out;
				self.ringing_in = ringing_in;
				if !in_call {
					self.speaking.clear();
					self.camera_on = false;
				}
				self.cameras
					.retain(|peer, _| self.call_peers.contains(peer));
			}
			Ev::Voice { from, frames } => {
				self.speaking.insert(from, frames);
			}
			Ev::Share { outgoing, incoming } => {
				self.screens.retain(|peer, _| incoming.contains(peer));
				self.screen_size.retain(|peer, _| incoming.contains(peer));
				if self
					.watching
					.as_ref()
					.is_some_and(|w| !incoming.contains(w))
				{
					self.watching = None;
				}
				self.outgoing = outgoing;
				self.incoming = incoming;
			}
			Ev::Camera(on) => self.camera_on = on,
			Ev::CameraOff(peer) => {
				self.cameras.remove(&peer);
			}
			Ev::Frame {
				from,
				camera: true,
				width,
				height,
				rgba,
			} => {
				let image =
					ColorImage::from_rgba_unmultiplied([width as usize, height as usize], &rgba);
				match self.cameras.get_mut(&from) {
					Some(handle) => handle.set(image, TextureOptions::LINEAR),
					None => {
						let handle = ctx.load_texture(
							format!("camera-{from}"),
							image,
							TextureOptions::LINEAR,
						);
						self.cameras.insert(from, handle);
					}
				}
			}
			Ev::Frame {
				from,
				width,
				height,
				rgba,
				..
			} => {
				let image =
					ColorImage::from_rgba_unmultiplied([width as usize, height as usize], &rgba);
				match self.screens.get_mut(&from) {
					Some(handle) => handle.set(image, TextureOptions::LINEAR),
					None => {
						let handle = ctx.load_texture(
							format!("screen-{from}"),
							image,
							TextureOptions::LINEAR,
						);
						self.screens.insert(from.clone(), handle);
					}
				}
				self.screen_size
					.insert(from.clone(), [width as usize, height as usize]);
				self.watching = Some(from);
			}
		}
	}

	fn name_of(&self, user_id: &str) -> String {
		if let Some(me) = &self.me {
			if me.id == user_id {
				return me.label().to_string();
			}
		}
		for rel in &self.relationships {
			if rel.user.id == user_id {
				return rel.user.label().to_string();
			}
		}
		for channel in &self.channels {
			if let Some(u) = channel.recipients.iter().find(|u| u.id == user_id) {
				return u.label().to_string();
			}
		}
		user_id.to_string()
	}

	fn channel_title(&self, channel: &Channel) -> String {
		let me = self.me.as_ref().map(|u| u.id.as_str()).unwrap_or("");
		channel.title(me)
	}

	fn sign_in_screen(&mut self, ui: &mut Ui) {
		ui.vertical_centered(|ui| {
			ui.add_space(ui.available_height() * 0.18);
			egui::Frame::new()
				.fill(SURFACE)
				.corner_radius(CornerRadius::same(14))
				.inner_margin(Margin::same(28))
				.show(ui, |ui| {
					ui.set_width(340.0);
					ui.label(RichText::new("Flint Lite").size(26.0).strong());
					ui.label(RichText::new("Chat and screen share with friends").color(MUTED));
					ui.add_space(16.0);
					ui.label("Server");
					ui.add(
						egui::TextEdit::singleline(&mut self.server).desired_width(f32::INFINITY),
					);
					ui.label("Username");
					ui.add(
						egui::TextEdit::singleline(&mut self.username).desired_width(f32::INFINITY),
					);
					ui.label("Password");
					let password = ui.add(
						egui::TextEdit::singleline(&mut self.password)
							.password(true)
							.desired_width(f32::INFINITY),
					);
					let enter =
						password.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
					ui.add_space(12.0);
					let label = if self.register {
						"Create account"
					} else {
						"Log in"
					};
					let submit = ui
						.add_sized(
							[ui.available_width(), 34.0],
							egui::Button::new(RichText::new(label).strong()).fill(ACCENT),
						)
						.clicked();
					if submit || enter {
						self.send(Cmd::Login {
							server: self.server.clone(),
							username: self.username.trim().to_string(),
							password: self.password.clone(),
							register: self.register,
						});
					}
					ui.add_space(6.0);
					let toggle = if self.register {
						"I already have an account"
					} else {
						"Create an account"
					};
					if ui.link(toggle).clicked() {
						self.register = !self.register;
					}
					if let Some(error) = &self.error {
						ui.add_space(8.0);
						ui.label(RichText::new(error).color(DANGER));
					}
				});
		});
	}

	fn sidebar(&mut self, ui: &mut Ui) {
		let me_label = self
			.me
			.as_ref()
			.map(|u| u.label().to_string())
			.unwrap_or_default();
		ui.horizontal(|ui| {
			ui.label(RichText::new(me_label).strong().size(16.0));
			ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
				if ui.small_button("Log out").clicked() {
					self.send(Cmd::Logout);
				}
			});
		});
		ui.label(RichText::new(&self.server).small().color(MUTED));
		ui.add_space(10.0);

		ui.horizontal(|ui| {
			let input = ui.add(
				egui::TextEdit::singleline(&mut self.add_friend)
					.hint_text("Add friend by username")
					.desired_width(ui.available_width() - 56.0),
			);
			let enter = input.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
			if (ui.button("Add").clicked() || enter) && !self.add_friend.trim().is_empty() {
				self.send(Cmd::AddFriend(self.add_friend.trim().to_string()));
				self.add_friend.clear();
			}
		});

		let incoming: Vec<Relationship> = self
			.relationships
			.iter()
			.filter(|r| r.kind == relationship_type::INCOMING_REQUEST)
			.cloned()
			.collect();
		let outgoing_count = self
			.relationships
			.iter()
			.filter(|r| r.kind == relationship_type::OUTGOING_REQUEST)
			.count();
		if !incoming.is_empty() || outgoing_count > 0 {
			ui.add_space(10.0);
			section_label(ui, "Requests");
			for rel in incoming {
				ui.horizontal(|ui| {
					ui.label(rel.user.label());
					if ui.small_button("Accept").clicked() {
						self.send(Cmd::AcceptFriend(rel.user.id.clone()));
					}
					if ui.small_button("Decline").clicked() {
						self.send(Cmd::RemoveRelationship(rel.user.id.clone()));
					}
				});
			}
			if outgoing_count > 0 {
				ui.label(
					RichText::new(format!("{outgoing_count} pending sent"))
						.small()
						.color(MUTED),
				);
			}
		}

		ui.add_space(10.0);
		section_label(ui, "Friends");
		let friends: Vec<Relationship> = self
			.relationships
			.iter()
			.filter(|r| r.kind == relationship_type::FRIEND)
			.cloned()
			.collect();
		if friends.is_empty() {
			ui.label(
				RichText::new("No friends yet. Add someone by username.")
					.small()
					.color(MUTED),
			);
		}
		for rel in &friends {
			ui.horizontal(|ui| {
				let mut picked = self.group_pick.contains(&rel.user.id);
				if ui
					.checkbox(&mut picked, "")
					.on_hover_text("Pick for a group")
					.changed()
				{
					if picked {
						self.group_pick.insert(rel.user.id.clone());
					} else {
						self.group_pick.remove(&rel.user.id);
					}
				}
				if ui
					.add(egui::Button::new(rel.user.label()).frame(false))
					.clicked()
				{
					self.send(Cmd::CreateDm(vec![rel.user.id.clone()]));
				}
				ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
					if ui.small_button("Remove").clicked() {
						self.send(Cmd::RemoveRelationship(rel.user.id.clone()));
					}
				});
			});
		}
		if self.group_pick.len() >= 2 {
			ui.add_space(4.0);
			let label = format!("Start group with {}", self.group_pick.len());
			if ui
				.add_sized(
					[ui.available_width(), 28.0],
					egui::Button::new(label).fill(ACCENT),
				)
				.clicked()
			{
				self.send(Cmd::CreateDm(self.group_pick.iter().cloned().collect()));
				self.group_pick.clear();
			}
		} else if !self.group_pick.is_empty() {
			ui.label(
				RichText::new("Pick one more friend to start a group")
					.small()
					.color(MUTED),
			);
		}

		ui.add_space(10.0);
		section_label(ui, "Conversations");
		let me_id = self.me.as_ref().map(|u| u.id.clone()).unwrap_or_default();
		let mut conversations: Vec<&Channel> = self
			.channels
			.iter()
			.filter(|c| c.kind == channel_type::DM || c.kind == channel_type::GROUP_DM)
			.collect();
		conversations.sort_by_key(|c| std::cmp::Reverse(c.id.parse::<u64>().unwrap_or(0)));
		for channel in conversations {
			let title = channel.title(&me_id);
			let selected = self.active.as_deref() == Some(channel.id.as_str());
			let prefix = if channel.kind == channel_type::GROUP_DM {
				"# "
			} else {
				""
			};
			if ui
				.selectable_label(selected, format!("{prefix}{title}"))
				.clicked()
			{
				self.active = Some(channel.id.clone());
				self.compose.clear();
				self.send(Cmd::OpenChannel(channel.id.clone()));
			}
		}
	}

	fn incoming_calls(&mut self, ui: &mut Ui) {
		let callers = self.ringing_in.clone();
		for from in callers {
			let name = self.name_of(&from);
			egui::Frame::new()
				.fill(ACCENT.gamma_multiply(0.35))
				.corner_radius(CornerRadius::same(10))
				.inner_margin(Margin::same(10))
				.show(ui, |ui| {
					ui.horizontal(|ui| {
						ui.label(RichText::new(format!("{name} is calling")).strong());
						if ui.add(egui::Button::new("Accept").fill(GOOD)).clicked() {
							self.send(Cmd::AcceptCall(from.clone()));
						}
						if ui.button("Decline").clicked() {
							self.send(Cmd::DeclineCall(from.clone()));
						}
					});
				});
			ui.add_space(6.0);
		}
	}

	fn conversation(&mut self, ui: &mut Ui) {
		if !self.ringing_in.is_empty() {
			self.incoming_calls(ui);
		}
		let Some(active_id) = self.active.clone() else {
			ui.centered_and_justified(|ui| {
				ui.label(RichText::new("Pick a friend or conversation to start").color(MUTED));
			});
			return;
		};
		let Some(channel) = self.channels.iter().find(|c| c.id == active_id).cloned() else {
			return;
		};
		let title = self.channel_title(&channel);
		let peers = channel.peer_ids(self.me.as_ref().map(|u| u.id.as_str()).unwrap_or(""));

		ui.horizontal(|ui| {
			ui.heading(title.clone());
			ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
				if self.in_call {
					let camera_label = if self.camera_on {
						"Camera off"
					} else {
						"Camera on"
					};
					if ui.button(camera_label).clicked() {
						self.camera_on = !self.camera_on;
						self.send(Cmd::SetCamera(self.camera_on));
					}
					let mute_label = if self.muted { "Unmute" } else { "Mute" };
					if ui.button(mute_label).clicked() {
						self.muted = !self.muted;
						self.send(Cmd::SetMuted(self.muted));
					}
					if ui
						.add(egui::Button::new("Leave call").fill(DANGER))
						.clicked()
					{
						self.send(Cmd::LeaveCall);
					}
					let heard = self.speaking.len();
					let text = if heard > 0 {
						format!("In call, hearing {heard}")
					} else {
						"In call".to_string()
					};
					ui.label(RichText::new(text).color(GOOD));
					if !self.ringing_out.is_empty() {
						ui.label(
							RichText::new(format!("Calling {}", self.ringing_out.len()))
								.color(MUTED),
						);
					}
					let not_in_call = peers.iter().any(|p| !self.call_peers.contains(p));
					if not_in_call
						&& ui
							.add(egui::Button::new("Ring others").fill(ACCENT))
							.clicked()
					{
						self.send(Cmd::StartCall(peers.clone()));
					}
					if self.muted {
						ui.label(RichText::new("Muted").color(MUTED));
					}
				} else if ui
					.add_enabled(
						!peers.is_empty(),
						egui::Button::new("Start call").fill(ACCENT),
					)
					.clicked()
				{
					self.send(Cmd::StartCall(peers.clone()));
				}
				let sharing = !self.outgoing.is_empty();
				if sharing {
					if ui
						.add(egui::Button::new("Stop sharing").fill(DANGER))
						.clicked()
					{
						self.send(Cmd::StopShare);
					}
					ui.label(RichText::new("Sharing your screen").color(GOOD));
				} else if ui
					.add_enabled(
						!peers.is_empty(),
						egui::Button::new("Share screen").fill(ACCENT),
					)
					.on_disabled_hover_text("Nobody else is in this conversation")
					.clicked()
				{
					self.send(Cmd::ShareScreen(peers.clone()));
				}
			});
		});
		ui.separator();

		if self.in_call && !self.cameras.is_empty() {
			let mut peers: Vec<String> = self.cameras.keys().cloned().collect();
			peers.sort();
			ui.horizontal_wrapped(|ui| {
				for peer in peers {
					let Some(texture) = self.cameras.get(&peer) else {
						continue;
					};
					let size = texture.resolution_vec2();
					let scale = (150.0 / size.y).min(1.0);
					let name = self.name_of(&peer);
					ui.vertical(|ui| {
						ui.add(egui::Image::new((texture.id(), size * scale)));
						ui.label(RichText::new(name).small().color(MUTED));
					});
				}
			});
			ui.add_space(8.0);
		}

		if let Some(watching) = self.watching.clone() {
			if let Some(texture) = self.screens.get(&watching) {
				let size = texture.resolution_vec2();
				let max_w = ui.available_width();
				let max_h = (ui.available_height() * 0.55).max(120.0);
				let scale = (max_w / size.x).min(max_h / size.y).min(1.0);
				egui::Frame::new()
					.fill(Color32::BLACK)
					.corner_radius(CornerRadius::same(10))
					.inner_margin(Margin::same(4))
					.show(ui, |ui| {
						ui.label(
							RichText::new(format!(
								"{} is sharing their screen",
								self.name_of(&watching)
							))
							.small()
							.color(MUTED),
						);
						ui.add(egui::Image::new((texture.id(), size * scale)));
					});
				ui.add_space(8.0);
			}
		}

		let composer_height = 52.0;
		let history_height = (ui.available_height() - composer_height).max(60.0);
		let messages = self.messages.get(&active_id).cloned().unwrap_or_default();
		egui::ScrollArea::vertical()
			.stick_to_bottom(true)
			.max_height(history_height)
			.auto_shrink([false, false])
			.show(ui, |ui| {
				for message in &messages {
					ui.horizontal_wrapped(|ui| {
						ui.label(RichText::new(message.author.label()).strong().color(ACCENT));
						ui.label(
							RichText::new(short_time(&message.timestamp))
								.small()
								.color(MUTED),
						);
					});
					ui.label(&message.content);
					ui.add_space(6.0);
				}
				if messages.is_empty() {
					ui.label(RichText::new("No messages yet. Say hello.").color(MUTED));
				}
			});

		ui.horizontal(|ui| {
			let input = ui.add(
				egui::TextEdit::singleline(&mut self.compose)
					.hint_text(format!("Message {title}"))
					.desired_width(ui.available_width() - 80.0),
			);
			let enter = input.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
			let send = ui
				.add_sized([70.0, 28.0], egui::Button::new("Send").fill(ACCENT))
				.clicked();
			if (send || enter) && !self.compose.trim().is_empty() {
				let content = self.compose.trim().to_string();
				self.compose.clear();
				self.send(Cmd::Send {
					channel_id: active_id.clone(),
					content,
				});
			}
			input.request_focus();
		});
	}
}

impl eframe::App for App {
	fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
		while let Ok(ev) = self.ev.try_recv() {
			self.handle_event(ctx, ev);
		}
	}

	fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
		if self.me.is_none() {
			egui::CentralPanel::default().show(ui, |ui| self.sign_in_screen(ui));
			return;
		}
		if let Some(error) = self.error.clone() {
			egui::Panel::top("error").show(ui, |ui| {
				ui.horizontal(|ui| {
					ui.label(RichText::new(error).color(DANGER));
					if ui.small_button("Dismiss").clicked() {
						self.error = None;
					}
				});
			});
		} else if let Some(notice) = self.notice.clone() {
			egui::Panel::top("notice").show(ui, |ui| {
				ui.label(RichText::new(notice).small().color(MUTED));
			});
		}
		egui::Panel::left("sidebar")
			.resizable(false)
			.exact_size(280.0)
			.frame(
				egui::Frame::new()
					.fill(PANEL)
					.inner_margin(Margin::same(14)),
			)
			.show(ui, |ui| {
				egui::ScrollArea::vertical().show(ui, |ui| self.sidebar(ui));
			});
		egui::CentralPanel::default()
			.frame(
				egui::Frame::new()
					.fill(SURFACE)
					.inner_margin(Margin::same(16)),
			)
			.show(ui, |ui| self.conversation(ui));
	}
}

fn section_label(ui: &mut Ui, text: &str) {
	ui.label(
		RichText::new(text.to_uppercase())
			.small()
			.strong()
			.color(MUTED),
	);
}

/// `2026-11-18T20:01:42Z` becomes `20:01`.
fn short_time(timestamp: &str) -> String {
	timestamp.get(11..16).unwrap_or(timestamp).to_string()
}

fn apply_theme(ctx: &egui::Context) {
	let mut visuals = egui::Visuals::dark();
	visuals.panel_fill = SURFACE;
	visuals.window_fill = SURFACE;
	visuals.selection.bg_fill = ACCENT;
	visuals.hyperlink_color = ACCENT;
	for widget in [
		&mut visuals.widgets.inactive,
		&mut visuals.widgets.hovered,
		&mut visuals.widgets.active,
	] {
		widget.corner_radius = CornerRadius::same(8);
	}
	ctx.set_visuals(visuals);
	ctx.global_style_mut(|style| {
		style.spacing.item_spacing = egui::vec2(8.0, 6.0);
	});
}
