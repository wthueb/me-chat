mod commands;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;

use commands::{extract_fixtures, find_group_ids, gap_check};

#[derive(Parser)]
#[command(name = "me-chat", version)]
#[command(args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Args for the default `gap-check` command, used when no subcommand is given
    #[command(flatten)]
    gap_check: gap_check::Args,
}

#[derive(Subcommand)]
enum Command {
    /// Run the gap check and print per-user stats (default)
    GapCheck(gap_check::Args),
    /// Find group chats made up entirely of known users
    FindGroupIds(find_group_ids::Args),
    /// Extract sanitized message fixtures for tests
    ExtractFixtures,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Some(Command::GapCheck(args)) => gap_check::run(args),
        Some(Command::FindGroupIds(args)) => find_group_ids::run(args),
        Some(Command::ExtractFixtures) => extract_fixtures::run(),
        None => gap_check::run(cli.gap_check),
    }
}
