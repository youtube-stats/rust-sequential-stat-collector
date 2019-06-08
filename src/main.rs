extern crate bytes;
extern crate postgres;
extern crate reqwest;
extern crate serde_json;
extern crate serde;
extern crate prost;
#[macro_use]
extern crate prost_derive;

use postgres::Connection;
use postgres::TlsMode;
use std::collections::HashMap;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct PageInfoType {
    #[allow(dead_code)]
    totalResults: u8,

    #[allow(dead_code)]
    resultsPerPage: u8
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct StatisticsType {
    viewCount: String,

    #[allow(dead_code)]
    commentCount: String,

    subscriberCount: String,

    #[allow(dead_code)]
    hiddenSubscriberCount: bool,

    videoCount: String
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct  ItemType {
    #[allow(dead_code)]
    kind: String,

    #[allow(dead_code)]
    etag: String,

    id: String,
    statistics: StatisticsType
}

#[allow(non_snake_case)]
#[derive(serde::Deserialize)]
struct YoutubeResponseType {
    #[allow(dead_code)]
    kind: String,

    #[allow(dead_code)]
    etag: String,

    #[allow(dead_code)]
    nextPageToken: String,

    #[allow(dead_code)]
    pageInfo: PageInfoType,

    items: Vec<ItemType>
}

pub mod types {
    #[derive(Clone, PartialEq, Message)]
    pub struct Subs {
        #[prost(int32, repeated, tag = "1")]
        pub time: Vec<i32>,
        #[prost(int32, repeated, tag = "2")]
        pub id: Vec<i32>,
        #[prost(int32, repeated, tag = "3")]
        pub subs: Vec<i32>
    }
}

pub mod statics {
    pub const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
}

pub fn get_client() -> postgres::Connection {
    let params: &'static str = POSTGRESQL_URL;
    let tls: postgres::TlsMode = postgres::TlsMode::None;

    postgres::Connection::connect(params, tls).unwrap()
}

fn main() {
    let addr: String = std::env::args().last()
        .expect("Could not retrieve args");
    let conn: Connection = {
        let params: &'static str = statics::POSTGRESQL_URL;
        let tls: TlsMode = TlsMode::None;

        Connection::connect(params, tls).expect("Could not connect to database")
    };

    let query: &'static str = "SELECT * FROM youtube.stats.channels ORDER BY id ASC";
    let rows: postgres::rows::Rows = conn.query(query, &[])
        .expect("Could not query");

    let mut hash: std::collections::HashMap<String, i32> = HashMap::new();
    for row in &rows {
        let v: i32 = row.get(0);
        let k: String = row.get(1);

        hash.insert(k, v);
    }

    let keys: Vec<&String> = {
        let mut keys: Vec<&String> = Vec::new();

        for value in hash.keys() {
            keys.push(value);
        }

        keys
    };
    let chunk_size: usize = 50;
    let chunky = keys.chunks(chunk_size);

    loop {
        for vec_id in chunky {
            let key: String = reqwest::get(addr.as_str())
                .expect("Could not get HTTP response").text()
                .expect("Could not retrieve HTTP body");
            println!("Using key {}", key);
            if key.is_empty() {
                println!("Detected empty key - waiting 1 minutes");
                let dur: std::time::Duration = std::time::Duration::from_secs(60);
                std::thread::sleep(dur);
                continue;
            }

            let vec: Vec<&String>  = vec_id.to_vec();
            let ids: String = vec.join(",");
            let url: String = format!("https://www.googleapis.com/youtube/v3/channels?part=statistics&key={}&id={}", key, ids);

            let mut resp: reqwest::Response = match reqwest::get(url.as_str()) {
                Ok(resp) => resp,
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    continue
                }
            };

            let body: String = match resp.text() {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    continue
                }
            };

            let response: YoutubeResponseType = match serde_json::from_str(body.as_str()) {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("{}", e.to_string());
                    continue
                }
            };

            for item in response.items {
                let channel_id: &String = match hash.get(item.id.as_str()) {
                    Some(text) => text,
                    None => {
                        eprintln!("Found no value for key {}", item.id);
                        continue
                    }
                };

                println!("{} {} {} {} {}",
                         item.id,
                         channel_id,
                         item.statistics.subscriberCount,
                         item.statistics.viewCount,
                         item.statistics.videoCount);

                let query: String =
                    format!("INSERT INTO youtube.stats.metrics (channel_id, subs, views, videos) VALUES ({}, {}, {}, {})",
                            channel_id,
                            item.statistics.subscriberCount,
                            item.statistics.viewCount,
                            item.statistics.videoCount);

                let n: u64 = match conn.execute(query.as_str(), &[]) {
                    Ok(size) => size,
                    Err(e) => {
                        eprintln!("{}", e.to_string());
                        continue
                    }
                };

                if n != 1 {
                    eprintln!("Row did not insert correctly");
                }
            }
        }
    }
}
