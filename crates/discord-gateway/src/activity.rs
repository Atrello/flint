use client_core::auth::Failure;
use discord_protocol::activity_sessions::{self, Observation};
use discord_protocol::rpc::Activity;
use model::{OwnPresence, PresenceStatus};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

/// One replaceable game, custom status, and last attempted values.
/// Five seconds between attempts is stricter than the documented 5 updates/20 seconds.
pub(super) struct Pending {
	current: Option<Activity>,
	own_presence: OwnPresence,
	sent_presence: Option<OwnPresence>,
	idle_since: Option<u64>,
	afk: bool,
	afk_since: Option<u64>,
	sent_afk: Option<bool>,
	sent: Option<Option<Activity>>,
	next_send: Instant,
	pub observation: Observation,
}
impl Default for Pending {
	fn default() -> Self {
		Self {
			current: None,
			own_presence: OwnPresence::default(),
			sent_presence: None,
			idle_since: None,
			afk: false,
			afk_since: None,
			sent_afk: None,
			sent: None,
			next_send: Instant::now(),
			observation: Observation::Unconfirmed,
		}
	}
}
impl Pending {
	pub fn update_presence(&mut self, presence: &OwnPresence) -> Result<(), Failure> {
		if !presence.valid() {
			return Err(Failure::Protocol);
		}
		if self.own_presence.status != presence.status {
			self.idle_since = (presence.status == PresenceStatus::Idle).then(now_ms);
		}
		if self.own_presence != *presence {
			self.own_presence = presence.clone();
			self.observation = Observation::Unconfirmed;
		}
		Ok(())
	}
	/// System inactivity (unofficial/unverified, mirrors the official client): Online shows as
	/// idle while the saved status stays untouched.
	pub fn update_afk(&mut self, afk: bool) {
		if self.afk != afk {
			self.afk = afk;
			self.afk_since = afk.then(now_ms);
			self.observation = Observation::Unconfirmed;
		}
	}
	/// Reidentifying must retain Invisible; activity publication still waits for READY.
	pub fn identify_presence(&self) -> serde_json::Value {
		let status = self.own_presence.status;
		if self.afk {
			let status = if status == PresenceStatus::Online {
				PresenceStatus::Idle
			} else {
				status
			};
			return serde_json::json!({"since":self.afk_since,"activities":[],"status":status.wire(),"afk":true});
		}
		serde_json::json!({"since":self.idle_since,"activities":[],"status":status.wire(),"afk":false})
	}
	pub fn update(&mut self, activity: &Option<Activity>) -> Result<(), Failure> {
		if let Some(activity) = activity {
			activity.validate().map_err(|_| Failure::Protocol)?;
		}
		if self.current != *activity {
			self.current = activity.clone();
			self.observation = Observation::Unconfirmed;
		}
		Ok(())
	}
	pub fn reconnect(&mut self) {
		self.sent = None;
		self.sent_presence = None;
		self.sent_afk = None;
		self.observation = Observation::Unconfirmed;
	}
	pub fn observe(&mut self, bytes: &[u8], session: &str) {
		if self.sent.as_ref() != Some(&self.current)
			|| self.sent_presence.as_ref() != Some(&self.own_presence)
			|| self.sent_afk != Some(self.afk)
			|| self.own_presence.status == PresenceStatus::Invisible
		{
			self.observation = Observation::Unconfirmed;
			return;
		}
		self.observation = self
			.current
			.as_ref()
			.map_or(Observation::Unconfirmed, |current| {
				activity_sessions::observe(bytes, session, current)
					.unwrap_or(Observation::Unconfirmed)
			});
	}
	pub fn deadline(&self) -> Option<Instant> {
		(self.sent.as_ref() != Some(&self.current)
			|| self.sent_presence.as_ref() != Some(&self.own_presence)
			|| self.sent_afk != Some(self.afk))
		.then_some(self.next_send)
	}
	pub fn packet(&mut self, now: Instant) -> Option<Message> {
		if self.deadline().is_none_or(|deadline| now < deadline) {
			return None;
		}
		let mut payload = self.identify_presence();
		if self.own_presence.status != PresenceStatus::Invisible {
			let mut activities: Vec<_> = self
				.current
				.iter()
				.map(|activity| serde_json::json!(activity))
				.collect();
			if !self.own_presence.custom_status.is_empty() {
				activities.push(
					serde_json::json!({"name":"Custom Status","type":4,"state":self.own_presence.custom_status}),
				);
			}
			payload["activities"] = serde_json::json!(activities);
		}
		if self.sent.as_ref() != Some(&self.current)
			|| self.sent_presence.as_ref() != Some(&self.own_presence)
			|| self.sent_afk != Some(self.afk)
		{
			self.observation = Observation::Unconfirmed;
		}
		self.sent = Some(self.current.clone());
		self.sent_presence = Some(self.own_presence.clone());
		self.sent_afk = Some(self.afk);
		// Charge attempts too: a failed write may have reached Discord.
		self.next_send = now + Duration::from_secs(5);
		Some(Message::Text(
			serde_json::json!({"op": 3, "d": payload})
				.to_string()
				.into(),
		))
	}
}

fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
	use super::*;
	use discord_protocol::rpc::{ActivityFields, Assets, Timestamps};
	use model::Id;

	fn game(name: &str) -> Activity {
		ActivityFields {
			details: Some("Ranked match".into()),
			state: Some("Round 2".into()),
			timestamps: Some(Timestamps {
				start: Some(1_700_000_000),
				end: None,
			}),
			assets: Some(Assets {
				large_image: Some("123".into()),
				..Default::default()
			}),
			..Default::default()
		}
		.into_activity(Id(42), name.into())
		.unwrap()
	}

	#[test]
	fn own_presence_preserves_games_clears_and_restores_after_invisible() {
		let mut pending = Pending::default();
		let now = Instant::now();
		let packet = |frame: Message| -> serde_json::Value {
			serde_json::from_str(frame.to_text().unwrap()).unwrap()
		};
		let mut own = OwnPresence {
			status: PresenceStatus::Idle,
			custom_status: "Taking a break".into(),
			expires_at_ms: None,
		};
		pending.update_presence(&own).unwrap();
		pending.update(&Some(game("osu!"))).unwrap();
		let first = packet(pending.packet(now).unwrap());
		assert_eq!(first["d"]["status"], "idle");
		assert!(first["d"]["since"].as_u64().is_some_and(|since| since > 0));
		assert_eq!(first["d"]["activities"][0]["name"], "osu!");
		assert_eq!(
			first["d"]["activities"][1],
			serde_json::json!({"type":4,"name":"Custom Status","state":"Taking a break"})
		);
		pending.update(&Some(game("Minecraft"))).unwrap();
		let next = packet(pending.packet(now + Duration::from_secs(5)).unwrap());
		assert_eq!(first["d"]["since"], next["d"]["since"]);
		own.status = PresenceStatus::Invisible;
		pending.update_presence(&own).unwrap();
		assert_eq!(pending.identify_presence()["status"], "invisible");
		assert!(pending.packet(now + Duration::from_secs(9)).is_none());
		let hidden = packet(pending.packet(now + Duration::from_secs(10)).unwrap());
		assert_eq!(hidden["d"]["activities"], serde_json::json!([]));
		assert_eq!(hidden["d"]["since"], serde_json::Value::Null);
		pending.reconnect();
		assert_eq!(pending.identify_presence()["status"], "invisible");
		assert!(pending.packet(now + Duration::from_secs(14)).is_none());
		pending.packet(now + Duration::from_secs(15)).unwrap();
		own.status = PresenceStatus::DoNotDisturb;
		pending.update_presence(&own).unwrap();
		let restored = packet(pending.packet(now + Duration::from_secs(20)).unwrap());
		assert_eq!(restored["d"]["status"], "dnd");
		assert_eq!(restored["d"]["activities"][0]["name"], "Minecraft");
		assert_eq!(restored["d"]["activities"][1]["state"], "Taking a break");
		let invalid = OwnPresence {
			custom_status: "x".repeat(129),
			..own.clone()
		};
		assert_eq!(pending.update_presence(&invalid), Err(Failure::Protocol));
		assert!(pending.deadline().is_none());
		own.custom_status.clear();
		pending.update_presence(&own).unwrap();
		let cleared = packet(pending.packet(now + Duration::from_secs(25)).unwrap());
		assert_eq!(cleared["d"]["activities"].as_array().unwrap().len(), 1);
		assert_eq!(cleared["d"]["activities"][0]["name"], "Minecraft");
		own.custom_status = "Still here".into();
		pending.update_presence(&own).unwrap();
		pending.update(&None).unwrap();
		let custom_only = packet(pending.packet(now + Duration::from_secs(30)).unwrap());
		assert_eq!(
			custom_only["d"]["activities"],
			serde_json::json!([{"type":4,"name":"Custom Status","state":"Still here"}])
		);
		// System inactivity shows Online as idle without touching the chosen status.
		own.status = PresenceStatus::Online;
		pending.update_presence(&own).unwrap();
		pending.packet(now + Duration::from_secs(35)).unwrap();
		pending.update_afk(true);
		let afk = packet(pending.packet(now + Duration::from_secs(40)).unwrap());
		assert_eq!(afk["d"]["status"], "idle");
		assert_eq!(afk["d"]["afk"], true);
		assert!(afk["d"]["since"].as_u64().is_some_and(|since| since > 0));
		pending.update_afk(true);
		assert!(pending.deadline().is_none());
		pending.update(&Some(game("osu!"))).unwrap();
		let again = packet(pending.packet(now + Duration::from_secs(45)).unwrap());
		assert_eq!(afk["d"]["since"], again["d"]["since"]);
		pending.update_afk(false);
		let back = packet(pending.packet(now + Duration::from_secs(50)).unwrap());
		assert_eq!(back["d"]["status"], "online");
		assert_eq!(back["d"]["afk"], false);
		own.status = PresenceStatus::DoNotDisturb;
		pending.update_presence(&own).unwrap();
		pending.update_afk(true);
		let dnd = packet(pending.packet(now + Duration::from_secs(55)).unwrap());
		assert_eq!(dnd["d"]["status"], "dnd");
		assert_eq!(dnd["d"]["afk"], true);
	}

	#[test]
	fn server_observations_do_not_confirm_unsent_changed_or_cleared_games() {
		let mut pending = Pending::default();
		let listed = br#"[{"session_id":"all","activities":[{"application_id":"42","type":0}]}]"#;
		pending.update(&Some(game("osu!"))).unwrap();
		pending.observe(listed, "own");
		assert_eq!(pending.observation, Observation::Unconfirmed);
		pending.packet(Instant::now()).unwrap();
		pending.observe(listed, "own");
		assert_eq!(pending.observation, Observation::ServerListed);
		let mut changed = game("osu!");
		changed.details = Some("Next map".into());
		pending.update(&Some(changed)).unwrap();
		pending.observe(listed, "own");
		assert_eq!(pending.observation, Observation::Unconfirmed);
		pending.update(&None).unwrap();
		pending.observe(listed, "own");
		assert_eq!(pending.observation, Observation::Unconfirmed);
	}

	#[test]
	fn unverified_game_is_published_like_discord_and_confirmed_by_name() {
		// A user-added game has no application: Discord's client sends only name, type and start.
		let added = ActivityFields {
			timestamps: Some(Timestamps {
				start: Some(1_700_000_000_000),
				end: None,
			}),
			..Default::default()
		}
		.into_activity(Id(0), "My synthetic game".into())
		.unwrap();
		let mut pending = Pending::default();
		pending
			.update_presence(&OwnPresence {
				custom_status: "Synthetic status".into(),
				..Default::default()
			})
			.unwrap();
		pending.update(&Some(added)).unwrap();
		let sent: serde_json::Value =
			serde_json::from_str(pending.packet(Instant::now()).unwrap().to_text().unwrap())
				.unwrap();
		assert_eq!(sent["op"], 3);
		assert_eq!(
			sent["d"]["activities"],
			serde_json::json!([
				{"name":"My synthetic game","type":0,"timestamps":{"start":1_700_000_000_000_u64}},
				{"name":"Custom Status","type":4,"state":"Synthetic status"}
			])
		);
		// Discord lists it back without an application id; that still confirms the game.
		pending.observe(
			br#"[{"session_id":"all","activities":[{"name":"My synthetic game","type":0,"created_at":1},{"name":"Custom Status","type":4}]}]"#,
			"own",
		);
		assert_eq!(pending.observation, Observation::ServerListed);
		pending.observe(
			br#"[{"session_id":"own","activities":[{"name":"My synthetic game","type":0}]}]"#,
			"own",
		);
		assert_eq!(pending.observation, Observation::ServerReceived);
	}

	#[test]
	fn activity_coalesces_clears_reconnects_and_bounds_utf8() {
		let mut pending = Pending::default();
		let now = Instant::now();
		let packet = |frame: Message| -> serde_json::Value {
			serde_json::from_str(frame.to_text().unwrap()).unwrap()
		};
		pending.update(&Some(game("osu!"))).unwrap();
		assert_eq!(
			packet(pending.packet(now).unwrap()),
			serde_json::json!({"op":3,"d":{
				"since":null,"activities":[{"name":"osu!","type":0,"application_id":"42",
					"details":"Ranked match","state":"Round 2","timestamps":{"start":1_700_000_000_000_u64},
					"assets":{"large_image":"123"}}],"status":"online","afk":false
			}})
		);
		assert!(pending.deadline().is_none());
		pending.update(&Some(game("Intermediate"))).unwrap();
		pending.update(&None).unwrap();
		assert!(pending.packet(now + Duration::from_millis(4999)).is_none());
		assert_eq!(
			packet(pending.packet(now + Duration::from_secs(5)).unwrap())["d"]["activities"],
			serde_json::json!([])
		);
		assert!(pending.deadline().is_none());
		pending.reconnect();
		assert!(pending.packet(now + Duration::from_secs(9)).is_none());
		assert_eq!(
			packet(pending.packet(now + Duration::from_secs(10)).unwrap())["d"]["activities"],
			serde_json::json!([])
		);
		for name in [
			"x".repeat(129),
			"é".repeat(65),
			"game\n".into(),
			"\0".into(),
			" ".into(),
		] {
			let mut activity = game("Valid");
			activity.name = name;
			assert_eq!(pending.update(&Some(activity)), Err(Failure::Protocol));
		}
		pending.update(&Some(game(&"é".repeat(64)))).unwrap();
		assert!(pending.packet(now + Duration::from_secs(14)).is_none());
		assert!(pending.packet(now + Duration::from_secs(15)).is_some());
		assert!(pending.deadline().is_none());
		let mut changed = game(&"é".repeat(64));
		changed.details = Some("Next match".into());
		pending.update(&Some(changed.clone())).unwrap();
		changed.state = Some("x".repeat(129));
		assert_eq!(pending.update(&Some(changed)), Err(Failure::Protocol));
		assert_eq!(
			packet(pending.packet(now + Duration::from_secs(20)).unwrap())["d"]["activities"][0]["details"],
			"Next match"
		);
	}
}
