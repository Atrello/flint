//! Wire types for the Steel API and the messages passed between the UI and the network worker.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct User {
	pub id: String,
	pub username: String,
	#[serde(default)]
	pub global_name: Option<String>,
}

impl User {
	pub fn label(&self) -> &str {
		self.global_name
			.as_deref()
			.filter(|name| !name.is_empty())
			.unwrap_or(&self.username)
	}
}

pub mod channel_type {
	pub const DM: u8 = 1;
	pub const GROUP_DM: u8 = 3;
}

pub mod relationship_type {
	pub const FRIEND: u8 = 1;
	pub const INCOMING_REQUEST: u8 = 3;
	pub const OUTGOING_REQUEST: u8 = 4;
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Channel {
	pub id: String,
	#[serde(rename = "type")]
	pub kind: u8,
	#[serde(default)]
	pub name: Option<String>,
	#[serde(default)]
	pub recipients: Vec<User>,
}

impl Channel {
	/// Name shown in lists: the group name, or the other participants' names.
	pub fn title(&self, me: &str) -> String {
		if let Some(name) = self.name.as_deref().filter(|n| !n.is_empty()) {
			return name.to_string();
		}
		let others: Vec<&str> = self
			.recipients
			.iter()
			.filter(|u| u.id != me)
			.map(|u| u.label())
			.collect();
		if others.is_empty() {
			"Just you".to_string()
		} else {
			others.join(", ")
		}
	}

	/// Participant ids other than `me`. These are the peers a screen share goes to.
	pub fn peer_ids(&self, me: &str) -> Vec<String> {
		self.recipients
			.iter()
			.filter(|u| u.id != me)
			.map(|u| u.id.clone())
			.collect()
	}
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Message {
	pub id: String,
	pub channel_id: String,
	pub author: User,
	pub content: String,
	pub timestamp: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Relationship {
	pub id: String,
	#[serde(rename = "type")]
	pub kind: u8,
	pub user: User,
}

#[derive(Serialize)]
pub struct Credentials<'a> {
	pub username: &'a str,
	pub password: &'a str,
}

#[derive(Deserialize)]
pub struct TokenResponse {
	pub token: String,
	pub user: User,
}

/// Commands from the UI to the network worker.
#[derive(Debug)]
pub enum Cmd {
	Login {
		server: String,
		username: String,
		password: String,
		register: bool,
	},
	Resume {
		server: String,
		token: String,
	},
	Logout,
	OpenChannel(String),
	Send {
		channel_id: String,
		content: String,
	},
	AddFriend(String),
	AcceptFriend(String),
	RemoveRelationship(String),
	CreateDm(Vec<String>),
	ShareScreen(Vec<String>),
	StopShare,
	StartCall(Vec<String>),
	LeaveCall,
}

/// Events from the network worker to the UI.
#[derive(Debug)]
pub enum Ev {
	Error(String),
	Notice(String),
	LoggedIn {
		server: String,
		me: User,
	},
	LoggedOut,
	Channels(Vec<Channel>),
	ChannelUpsert(Channel),
	Relationships(Vec<Relationship>),
	History {
		channel_id: String,
		messages: Vec<Message>,
	},
	Message(Message),
	Share {
		outgoing: Vec<String>,
		incoming: Vec<String>,
	},
	/// Whether this client is in a voice call, and with whom it has a connection.
	Call {
		in_call: bool,
		peers: Vec<String>,
	},
	/// A remote speaker is being heard. `frames` counts decoded 20 ms frames so far.
	Voice {
		from: String,
		frames: u64,
	},
	Frame {
		from: String,
		width: u32,
		height: u32,
		rgba: Vec<u8>,
	},
}
