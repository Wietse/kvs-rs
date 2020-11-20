use std::env;
use structopt::StructOpt;
use kvs::{KvStore, Result};


// Command line argument parsing is done with structopt.
// This generates a clap::App which can then be used as such.
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


fn run() -> Result<()> {
    let matches = Kvs::clap().get_matches();
    let mut store = KvStore::open(env::current_dir()?)?;
    match matches.subcommand() {
        ("get", Some(m)) => {
            // values must be Some(_) else clap would have failed
            let key = m.value_of("key").unwrap();
            eprintln!("calling store.get({})", key);
            match store.get(key.to_owned())? {
                Some(v) => println!("{}", &v),
                None => println!("Key not found"),
            }
            Ok(())
        }
        ("set", Some(m)) => {
            // values must be Some(_) else clap would have failed
            let key = m.value_of("key").unwrap();
            let value = m.value_of("value").unwrap();
            store.set(key.to_owned(), value.to_owned())?;
            Ok(())
        }
        ("rm", Some(m)) => {
            // values must be Some(_) else clap would have failed
            let key = m.value_of("key").unwrap();
            store.remove(key.to_owned())?;
            Ok(())
        }
        _ => unreachable!(),
    }
}


fn main() {
    if let Err(ref err) = run() {
        use std::io::Write;
        let stderr = &mut ::std::io::stderr();
        writeln!(stderr, "error: {}", err).expect("Error writing to stderr");
        if err.is_key_not_found() {
            println!("{}", err);
        }
        ::std::process::exit(1);
    }
}
