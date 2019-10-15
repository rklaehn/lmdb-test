use lmdb::Cursor;
use lmdb::Transaction;
use serde::Deserialize;
use std::sync::Arc;
use tokio::runtime::Runtime;
use warp::filters::query::query;
use warp::Filter;

#[derive(Deserialize, Debug)]
struct PutArg {
    #[serde(rename = "arg")]
    cid: String,
}

#[derive(Debug)]
struct BlockStore {
    env: lmdb::Environment,
    db: lmdb::Database,
}

impl BlockStore {
    fn new() -> Result<BlockStore, lmdb::Error> {
        let env = lmdb::Environment::new()
            .set_max_dbs(1)
            .set_map_size(10485760 * 1024)
            .open(std::path::Path::new("./test"))?;
        let db = env.create_db(Some("ipfsdump"), lmdb::DatabaseFlags::empty())?;
        Ok(BlockStore { env, db })
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let key_bytes = key.as_bytes();
        let txn = self.env.begin_ro_txn()?;
        let mut cursor = txn.open_ro_cursor(self.db)?;
        println!("getting {}", key);
        if let Some((key, bytes)) = cursor.iter_from(key).next() {
            if key == key_bytes {
                Ok(bytes.to_vec())
            } else {
                Ok("not found".as_bytes().to_vec())
            }
        } else {
            Ok("not found".as_bytes().to_vec())
        }
    }
}

pub fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(BlockStore::new()?);
    let block_get = path!("api" / "v0" / "block" / "get")
        .and(query())
        .map(move |arg: PutArg| {
            println!("{:?}", arg);
            match store.as_ref().get(arg.cid.as_str()) {
                Ok(data) => data,
                Err(cause) => format!("Kaput {}", cause).as_bytes().to_vec(),
            }
        });
    let block_put = path!("api" / "v0" / "block" / "put").map(move || "OK");
    let block_api = block_get.or(block_put);

    let mut rt = Runtime::new().expect("Could not start tokio runtime");
    let http_server = warp::serve(block_api).bind(([0, 0, 0, 0], 5002));
    rt.block_on(http_server).unwrap();
    Ok(())
}
