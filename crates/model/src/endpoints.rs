//! The Steel server this client talks to. The user enters one server address (for example
//! `https://chat.example.com`); every REST, gateway, media and cookie host is derived from it.
//! Nothing here points at a third-party service.
use std::sync::OnceLock;

/// Endpoints derived from one server address, set once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
	/// Scheme, host and optional port, with no trailing slash.
	pub origin: String,
	/// REST base, `{origin}/api/v10`. Paths keep the Discord-shaped layout Steel serves.
	pub api_origin: String,
	/// Gateway WebSocket address, `{ws}://{host}/gateway`, without query.
	pub gateway_url: String,
	/// Host name only, used for cookie scoping and link recognition.
	pub host: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EndpointError {
	#[error("server address must start with https:// (or http:// for localhost)")]
	Scheme,
	#[error("server address is not a valid URL")]
	Invalid,
	#[error("server address must not contain a path, query or credentials")]
	Shape,
	#[error("a different server is already configured for this session")]
	AlreadySet,
}

static ENDPOINTS: OnceLock<Endpoints> = OnceLock::new();

/// Parses a user-entered server address into its endpoints. Pure: does not touch the global.
pub fn parse(server: &str) -> Result<Endpoints, EndpointError> {
	let url = url::Url::parse(server.trim()).map_err(|_| EndpointError::Invalid)?;
	let host = url
		.host_str()
		.ok_or(EndpointError::Invalid)?
		.to_ascii_lowercase();
	let local = host == "localhost" || host == "127.0.0.1" || host == "[::1]";
	let ws = match url.scheme() {
		"https" => "wss",
		"http" if local => "ws",
		_ => return Err(EndpointError::Scheme),
	};
	if !url.username().is_empty()
		|| url.password().is_some()
		|| url.query().is_some()
		|| url.fragment().is_some()
		|| !matches!(url.path(), "" | "/")
	{
		return Err(EndpointError::Shape);
	}
	let authority = match url.port() {
		Some(port) => format!("{host}:{port}"),
		None => host.clone(),
	};
	let origin = format!("{}://{authority}", url.scheme());
	Ok(Endpoints {
		api_origin: format!("{origin}/api/v10"),
		gateway_url: format!("{ws}://{authority}/gateway"),
		origin,
		host,
	})
}

/// Sets the server for this process. Calling again with the same address is fine; a different
/// address is refused so one run cannot talk to two servers.
pub fn configure(server: &str) -> Result<&'static Endpoints, EndpointError> {
	let parsed = parse(server)?;
	if let Some(current) = ENDPOINTS.get() {
		return if *current == parsed {
			Ok(current)
		} else {
			Err(EndpointError::AlreadySet)
		};
	}
	Ok(ENDPOINTS.get_or_init(|| parsed))
}

/// The configured endpoints, or `None` before a server address has been set.
pub fn configured() -> Option<&'static Endpoints> {
	ENDPOINTS.get()
}

/// The configured endpoints. Panics if no server was configured: that is a startup bug, since
/// the login screen cannot run without an address.
pub fn get() -> &'static Endpoints {
	ENDPOINTS
		.get()
		.expect("server address is configured before any network use")
}

/// Discord's public media hosts. Their paths are served by Steel under `{origin}/cdn/`.
const DISCORD_MEDIA_HOSTS: [&str; 2] = ["cdn.discordapp.com", "media.discordapp.net"];

/// The path after a Discord media host, for example `attachments/1/2/a.png` for
/// `https://cdn.discordapp.com/attachments/1/2/a.png`. Matches the exact host only.
fn discord_media_path(value: &str) -> Option<&str> {
	let rest = value.strip_prefix("https://")?;
	DISCORD_MEDIA_HOSTS.iter().find_map(|host| {
		rest.strip_prefix(host)
			.and_then(|rest| rest.strip_prefix('/'))
	})
}

/// Makes a server-relative path such as `/cdn/attachments/…` absolute against the configured
/// origin. Discord media URLs move to the same path on the configured server. Other absolute
/// URLs pass through unchanged, and nothing here requires a configured server.
pub fn absolute_url(value: &str) -> String {
	match ENDPOINTS.get() {
		Some(endpoints) => {
			if let Some(path) = discord_media_path(value) {
				return format!("{}/cdn/{path}", endpoints.origin);
			}
			if value.starts_with('/') && !value.starts_with("//") {
				return format!("{}{value}", endpoints.origin);
			}
			value.to_owned()
		}
		None => value.to_owned(),
	}
}

/// Maps a media URL on the configured server (`{origin}/cdn/...`) back to the Discord-shaped
/// address its path came from, so validators keep one set of rules. Other URLs are unchanged.
pub fn discord_form(url: &str) -> std::borrow::Cow<'_, str> {
	if let Some(endpoints) = ENDPOINTS.get()
		&& let Some(rest) = url
			.strip_prefix(endpoints.origin.as_str())
			.and_then(|rest| rest.strip_prefix("/cdn/"))
	{
		return format!("https://{}/{rest}", DISCORD_MEDIA_HOSTS[0]).into();
	}
	url.into()
}

/// Whether `url` is media this client fetches from the configured server (or from a Discord
/// media host it has not yet rewritten).
pub fn is_cdn_url(url: &str) -> bool {
	discord_media_path(url).is_some()
		|| ENDPOINTS.get().is_some_and(|endpoints| {
			url.strip_prefix(endpoints.origin.as_str())
				.is_some_and(|rest| rest.starts_with("/cdn/"))
		})
}

/// Whether `host` is the configured server's host (case-insensitive).
pub fn is_server_host(host: &str) -> bool {
	ENDPOINTS
		.get()
		.is_some_and(|endpoints| endpoints.host.eq_ignore_ascii_case(host))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn derives_every_endpoint_from_one_address() {
		let parsed = parse("https://Chat.Example.com/").unwrap();
		assert_eq!(parsed.origin, "https://chat.example.com");
		assert_eq!(parsed.api_origin, "https://chat.example.com/api/v10");
		assert_eq!(parsed.gateway_url, "wss://chat.example.com/gateway");
		assert_eq!(parsed.host, "chat.example.com");
	}

	#[test]
	fn keeps_port_and_allows_plain_http_only_locally() {
		let local = parse("http://localhost:8080").unwrap();
		assert_eq!(local.api_origin, "http://localhost:8080/api/v10");
		assert_eq!(local.gateway_url, "ws://localhost:8080/gateway");
		assert_eq!(parse("http://chat.example.com"), Err(EndpointError::Scheme));
	}

	#[test]
	fn resolves_only_server_relative_paths() {
		configure("https://chat.example.com").unwrap();
		assert_eq!(
			absolute_url("/cdn/attachments/1/2/a.png"),
			"https://chat.example.com/cdn/attachments/1/2/a.png"
		);
		assert_eq!(
			absolute_url("https://other.example/a.png"),
			"https://other.example/a.png"
		);
		assert_eq!(
			absolute_url("//other.example/a.png"),
			"//other.example/a.png"
		);
	}

	#[test]
	fn moves_discord_media_to_the_configured_server() {
		configure("https://chat.example.com").unwrap();
		assert_eq!(
			absolute_url("https://cdn.discordapp.com/attachments/1/2/a.png?ex=1"),
			"https://chat.example.com/cdn/attachments/1/2/a.png?ex=1"
		);
		assert_eq!(
			absolute_url("https://media.discordapp.net/stickers/2.gif"),
			"https://chat.example.com/cdn/stickers/2.gif"
		);
		// Lookalike hosts are not Discord media and keep their address.
		assert_eq!(
			absolute_url("https://cdn.discordapp.com.evil.test/a.png"),
			"https://cdn.discordapp.com.evil.test/a.png"
		);
		assert!(is_cdn_url(
			"https://chat.example.com/cdn/attachments/1/2/a.png"
		));
		assert!(is_cdn_url(
			"https://cdn.discordapp.com/attachments/1/2/a.png"
		));
		assert!(!is_cdn_url("https://chat.example.com/api/v10/users/@me"));
		assert_eq!(
			discord_form("https://chat.example.com/cdn/emojis/9.gif?size=64"),
			"https://cdn.discordapp.com/emojis/9.gif?size=64"
		);
		assert_eq!(
			discord_form("https://chat.example.com/cdn.evil/x"),
			"https://chat.example.com/cdn.evil/x"
		);
	}

	#[test]
	fn refuses_paths_credentials_and_odd_schemes() {
		assert_eq!(
			parse("https://chat.example.com/x"),
			Err(EndpointError::Shape)
		);
		assert_eq!(
			parse("https://u:p@chat.example.com"),
			Err(EndpointError::Shape)
		);
		assert_eq!(
			parse("https://chat.example.com?x=1"),
			Err(EndpointError::Shape)
		);
		assert_eq!(parse("ftp://chat.example.com"), Err(EndpointError::Scheme));
		assert_eq!(parse("not a url"), Err(EndpointError::Invalid));
	}
}
