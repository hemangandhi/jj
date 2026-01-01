use clap::Subcommand;

mod init;

use self::init::MultiRepoInitArgs;
use self::init::cmd_multi_repo_init;

use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::ui::Ui;

/// Commands for working in a repo backend by multiple different repos.
///
/// This can include both a git repo with git submodules or multiple
/// sibling directories, each with their own version-control.
#[derive(Subcommand, Clone, Debug)]
pub enum MultiRepoCommand {
    Init(MultiRepoInitArgs),
}


pub fn cmd_git(
    ui: &mut Ui,
    command: &CommandHelper,
    subcommand: &MultiRepoCommand,
) -> Result<(), CommandError> {
    match subcommand {
        MultiRepoCommand::Init(args) => cmd_multi_repo_init(ui, command, args),
    }
}
