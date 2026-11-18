use crate::{DiscordApi, Failure};
use model::Id;
use wreq::Method;

const MAX_RESPONSE: usize = 256 * 1024;

impl DiscordApi {
	/// Bot user ids of the applications this account has installed (Steel).
	pub async fn installed_applications(&self) -> Result<Vec<Id>, Failure> {
		let bytes = self
			.request_limited(
				Method::GET,
				"/users/@me/installed-applications",
				None,
				MAX_RESPONSE,
			)
			.await?;
		let rows: Vec<serde_json::Value> =
			serde_json::from_slice(&bytes).map_err(|_| Failure::Protocol)?;
		rows.iter()
			.map(|row| {
				row.get("bot_user_id")
					.and_then(serde_json::Value::as_str)
					.and_then(|id| id.parse::<Id>().ok())
					.ok_or(Failure::Protocol)
			})
			.collect()
	}

	/// Installs the application behind a bot user for this account, or removes it (Steel).
	pub async fn set_application_installed(&self, bot: Id, install: bool) -> Result<(), Failure> {
		let method = if install { Method::PUT } else { Method::DELETE };
		self.request_limited(
			method,
			&format!("/users/@me/applications/{bot}"),
			None,
			4096,
		)
		.await?;
		Ok(())
	}
}
