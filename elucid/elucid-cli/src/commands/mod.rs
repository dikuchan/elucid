mod entrypoint;
mod execute;
mod ingest;
mod repl;
mod schema;
mod validate;

use clap::Parser;

use self::entrypoint::Entrypoint;
use self::execute::ExecuteCommand;
use self::ingest::IngestCommand;
use self::repl::ReplCommand;
use self::schema::SchemaCommand;
use self::validate::ValidateCommand;

pub fn parse() -> Entrypoint {
    Entrypoint::parse()
}
