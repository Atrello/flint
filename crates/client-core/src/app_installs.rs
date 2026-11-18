//! Installed Steel applications, keyed by their bot user, for the bot profile's install control.

use crate::{Command, State};
use model::Id;
use std::collections::BTreeSet;

#[derive(Default)]
pub struct AppInstalls {
	/// Bot users whose applications this account has installed; `None` until first loaded.
	pub installed: Option<BTreeSet<Id>>,
	pub loading: bool,
	/// Bot users with an install or removal in flight.
	pub pending: BTreeSet<Id>,
	/// The last install, removal or list request failed.
	pub error: bool,
	sequence: u64,
}

impl State {
	fn next_app_install_request(&mut self) -> u64 {
		self.app_installs.sequence = self.app_installs.sequence.wrapping_add(1);
		self.app_installs.sequence
	}

	/// The request that loads the installed list, once: it is not retried after a failure.
	pub fn request_app_installs(&mut self) -> Option<Command> {
		if self.app_installs.loading
			|| self.app_installs.error
			|| self.app_installs.installed.is_some()
		{
			return None;
		}
		self.app_installs.loading = true;
		Some(Command::AppInstalls {
			request: self.next_app_install_request(),
		})
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
