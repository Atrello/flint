//! Sign-in and account creation against the configured Steel server. The returned token is the
//! only credential Flint keeps; the password is sent once and never stored.
use client_core::auth::{Failure, SessionSecret};
use serde::Deserialize;
use std::time::Duration;
use wreq::{Client, StatusCode};

/// A token response is a token and a user object; anything larger is not a Steel reply.
const MAX_REPLY_BYTES: usize = 16 * 1024;

#[derive(Deserialize)]
struct TokenReply {
	token: String,
}

fn client() -> Result<Client, Failure> {
	crate::browser_transport(Client::builder())
		.redirect(wreq::redirect::Policy::none())
		.timeout(Duration::from_secs(15))
		.connect_timeout(Duration::from_secs(10))
		.build()
		.map_err(|_| Failure::Network)
}

/// `POST /auth/login` with the username or email and password.
pub async fn sign_in(login: &str, password: &str) -> Result<SessionSecret, Failure> {
	let body = serde_json::json!({ "login": login, "password": password });
	post("/auth/login", &body, Failure::ProtocolAt("Username or password is wrong")).await
}

/// `POST /auth/register` creates the account and returns its first token.
pub async fn register(
	username: &str,
	password: &str,
	email: Option<&str>,
) -> Result<SessionSecret, Failure> {
	let mut body = serde_json::json!({ "username": username, "password": password });
	if let Some(email) = email.filter(|email| !email.is_empty()) {
		body["email"] = serde_json::Value::String(email.to_owned());
	}
	post(
		"/auth/register",
		&body,
		Failure::ProtocolAt("Account could not be created; check the username and password"),
	)
	.await
}

async fn post(
	path: &str,
	body: &serde_json::Value,
	rejected: Failure,
) -> Result<SessionSecret, Failure> {
	let url = format!("{}{path}", crate::api_v10_base());
	let response = client()?
		.post(url)
		.json(body)
		.send()
		.await
		.map_err(|_| Failure::Network)?;
	let status = response.status();
	if status == StatusCode::UNAUTHORIZED || status == StatusCode::BAD_REQUEST {
		return Err(rejected);
	}
	if !status.is_success() {
		return Err(Failure::Protocol);
	}
	let bytes = response.bytes().await.map_err(|_| Failure::Protocol)?;
	if bytes.len() > MAX_REPLY_BYTES {
		return Err(Failure::Protocol);
	}
	let reply: TokenReply = serde_json::from_slice(&bytes).map_err(|_| Failure::Protocol)?;
	SessionSecret::from_owner_input(reply.token)
}
