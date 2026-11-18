//! Installed Steel applications, keyed by their bot user, for the bot profile's install control.

use crate::{Command, State};
use model::Id;
use std::{
	collections::BTreeSet,
	time::{Duration, Instant},
};

const BASE_BACKOFF: Duration = Duration::from_secs(5);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

#[derive(Default)]
pub struct AppInstalls {
	/// Bot users whose applications this account has installed; `None` until loaded.
	pub installed: Option<BTreeSet<Id>>,
	pub loading: bool,
	/// Bot users with an install or removal in flight.
	pub pending: BTreeSet<Id>,
	/// The last install or removal failed.
	pub error: bool,
	/// Consecutive failed loads, and when the next load may run.
	failures: u32,
	retry_at: Option<Instant>,
	sequence: u64,
}

impl AppInstalls {
	/// A new session (READY, or a reconnect) reloads the list, so forget what was loaded.
	pub fn reset(&mut self) {
		self.installed = None;
		self.failures = 0;
		self.retry_at = None;
	}

	fn loaded(&mut self, ids: Vec<Id>) {
		self.installed = Some(ids.into_iter().collect());
		self.failures = 0;
		self.retry_at = None;
	}

	fn load_failed(&mut self, now: Instant) {
		self.failures = self.failures.saturating_add(1);
		let backoff = BASE_BACKOFF
			.checked_mul(2u32.saturating_pow(self.failures - 1))
			.unwrap_or(MAX_BACKOFF)
			.min(MAX_BACKOFF);
		self.retry_at = Some(now + backoff);
	}
}

impl State {
	fn next_app_install_request(&mut self) -> u64 {
		self.app_installs.sequence = self.app_installs.sequence.wrapping_add(1);
		self.app_installs.sequence
	}

	/// The request that loads the installed list, unless it is loaded, loading, or waiting out
	/// its retry backoff.
	pub fn request_app_installs(&mut self, now: Instant) -> Option<Command> {
		// The install control only shows in a conversation with a bot, so load nothing elsewhere.
		let bot_conversation = self
			.selected
			.and_then(|channel| self.channel(channel))
			.is_some_and(|channel| {
				channel
					.recipients
					.iter()
					.any(|user| user.kind == model::AccountKind::Bot)
			});
		if !bot_conversation {
			return None;
		}
		let installs = &self.app_installs;
		if installs.loading
			|| installs.installed.is_some()
			|| installs.retry_at.is_some_and(|at| now < at)
		{
			return None;
		}
		self.app_installs.loading = true;
		Some(Command::AppInstalls {
			request: self.next_app_install_request(),
		})
	}

	/// Applies the answer to a list request: loads the list, or schedules a retry.
	pub fn apply_app_installs(&mut self, result: Result<Vec<Id>, crate::auth::Failure>) {
		self.app_installs.loading = false;
		match result {
			Ok(ids) => self.app_installs.loaded(ids),
			Err(_) => self.app_installs.load_failed(Instant::now()),
		}
	}

	/// The request that installs or removes the application behind `bot`, if none is in flight.
	pub fn set_app_installed(&mut self, bot: Id, install: bool) -> Option<Command> {
		if !self.app_installs.pending.insert(bot) {
			return None;
		}
		self.app_installs.error = false;
		Some(Command::AppInstall {
			request: self.next_app_install_request(),
			bot,
			install,
		})
	}
}

impl AppInstalls {
	/// How long until a failed list load may be retried, for scheduling a repaint.
	pub fn retry_delay(&self, now: Instant) -> Option<Duration> {
		self.retry_at
			.filter(|_| self.installed.is_none() && !self.loading)
			.map(|at| at.saturating_duration_since(now))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn backoff_doubles_and_caps() {
		let mut installs = AppInstalls::default();
		let now = Instant::now();
		let mut delays = Vec::new();
		for _ in 0..8 {
			installs.load_failed(now);
			delays.push(installs.retry_at.unwrap().duration_since(now).as_secs());
		}
		assert_eq!(delays, [5, 10, 20, 40, 80, 160, 300, 300]);
	}

	#[test]
	fn reset_forgets_failures() {
		let mut installs = AppInstalls::default();
		installs.load_failed(Instant::now());
		installs.reset();
		assert!(installs.retry_at.is_none() && installs.failures == 0);
		installs.loaded(vec![]);
		assert!(installs.installed.is_some());
	}
}
