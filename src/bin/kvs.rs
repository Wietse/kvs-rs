use std::process::exit;

use structopt::StructOpt;


#[derive(StructOpt, Debug)]
enum Kvs {
    /// Get the VALUE associated with KEY
    Get {
        key: String
    },
    /// Set a KEY with associated VALUE
    Set {
        key: String,
        value: String,
    },
    /// Remove KEY
    Rm {
        key: String
    },
}


fn main() {
    let matches = Kvs::clap().get_matches();

    match matches.subcommand() {
        ("get", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(-1)
        }
        ("set", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(-1)
        }
        ("rm", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(-1)
        }
        _ => unreachable!(),
    }
}
