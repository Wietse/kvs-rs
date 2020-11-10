#![allow(unused_variables)]

use std::process;

#[macro_use]
extern crate clap;
use clap::App;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let app = App::from_yaml(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"));

    let matches = app.get_matches();

    if let Some(matches) = matches.subcommand_matches("get") {
        eprintln!("unimplemented")
    }

    if let Some(matches) = matches.subcommand_matches("set") {
        eprintln!("unimplemented")
    }

    if let Some(matches) = matches.subcommand_matches("rm") {
        eprintln!("unimplemented")
    }

    process::exit(-1);
}
