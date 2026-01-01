use std::collections::HashSet;
use std::path::PathBuf;

use jj_lib::file_util;
use jj_lib::workspace::Workspace;

use crate::cli_util::CommandHelper;
use crate::command_error::CommandError;
use crate::command_error::cli_error;
use crate::command_error::user_error_with_message;
use crate::ui::Ui;

/// Create a new repo backed by various different version control systems.
#[derive(clap::Args, Clone, Debug)]
pub struct MultiRepoInitArgs {
    /// The destination directory where the `jj` repo will be created.
    /// This jj repo will encapsulate all the repos under the current directory.
    /// If the directory does not exist, it will be created.
    /// If no directory is given, the current directory is used.
    ///
    /// By default the repo is under `$destination/.jj`
    #[arg(long, default_value = ".", value_hint = clap::ValueHint::DirPath)]
    destination: String,

    /// Paths from the current directory to folders to exclude in the
    /// multi-repo.
    #[arg(value_name = "DIRS", value_hint = clap::ValueHint::DirPath)]
    exact: Vec<String>,
}

pub fn cmd_multi_repo_init(
    ui: &mut Ui,
    command: &CommandHelper,
    args: &MultiRepoInitArgs,
) -> Result<(), CommandError> {
    if command.global_args().ignore_working_copy {
        return Err(cli_error("--ignore-working-copy is not respected"));
    }
    if command.global_args().at_operation.is_some() {
        return Err(cli_error("--at-op is not respected"));
    }
    let cwd = command.cwd();
    let wc_path = cwd.join(&args.destination);
    let wc_path = file_util::create_or_reuse_dir(&wc_path)
        .and_then(|_| dunce::canonicalize(wc_path))
        .map_err(|e| user_error_with_message("Failed to create workspace", e))?;

    let (settings, _conf_env) = command.settings_for_new_workspace(ui, &wc_path)?;
    let (workspace, repo) = Workspace::init_multi_backend(
        &settings,
        &wc_path,
        cwd,
        &args
            .exact
            .iter()
            .map(|d| PathBuf::from(d))
            .collect::<HashSet<PathBuf>>(),
    )?;

    let mut workspace_command = command.for_workable_repo(ui, workspace, repo)?;
    workspace_command.maybe_snapshot(ui)?;
    Ok(())
}
