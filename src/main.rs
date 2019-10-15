extern crate lmdb;
extern crate reqwest;
extern crate structopt;
#[macro_use]
extern crate warp;
extern crate tokio;

use lmdb::Cursor;
use lmdb::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use structopt::StructOpt;
mod serve;

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "lmdb-ipfs")]
enum Opt {
    #[structopt(name = "slurp")]
    Slurp,
    #[structopt(name = "serve")]
    Serve,
    #[structopt(name = "dump")]
    Dump,
}

type Cid = String;
type PinMap = BTreeMap<Cid, PinInfo>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
enum PinType {
    Indirect,
    Recursive,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct PinInfo {
    r#type: PinType,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct PinLsResponse {
    keys: PinMap,
}

fn slurp() -> Result<(), Box<dyn std::error::Error>> {
    let env = lmdb::Environment::new()
        .set_max_dbs(1)
        .set_map_size(10485760 * 1024)
        .open(std::path::Path::new("./test"))?;
    let db = env.create_db(Some("ipfsdump"), lmdb::DatabaseFlags::empty())?;
    let mut result = reqwest::get("http://localhost:5001/api/v0/pin/ls")?;
    let x: PinLsResponse = result.json()?;
    for key in x.keys.keys() {
        let url = format!("http://localhost:5001/api/v0/block/get?arg={}", key);
        println!("{}", key);
        let mut data = reqwest::get(url.as_str())?;
        let key = key.clone().into_bytes();
        let mut value: Vec<u8> = Vec::new();
        data.copy_to(&mut value)?;
        let mut txn = env.begin_rw_txn()?;
        txn.put(db, &key, &value, lmdb::WriteFlags::empty())?;
        txn.commit()?;
    }
    Ok(())
}

fn dump() -> Result<(), Box<dyn std::error::Error>> {
    let env = lmdb::Environment::new()
        .set_max_dbs(1)
        .set_map_size(10485760 * 1024)
        .open(std::path::Path::new("./test"))?;
    let db = env.create_db(Some("ipfsdump"), lmdb::DatabaseFlags::empty())?;
    let txn = env.begin_ro_txn()?;
    let mut cursor = txn.open_ro_cursor(db)?;
    for x in cursor.iter() {
        // x.1.to_vec().into_iter().sum::<u8>()
        println!(
            "{:?} {}",
            String::from_utf8(x.0.to_vec())?,
            x.1.to_vec().len()
        );
    }
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    match opt {
        Opt::Slurp => slurp(),
        Opt::Dump => dump(),
        Opt::Serve => serve::serve(),
    }
}
