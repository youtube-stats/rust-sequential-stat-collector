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
use postgres::rows::Rows;
use std::slice::Chunks;

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
    let url: &'static str = "http://localhost:8080";
    let conn: Connection = {
        let params: &'static str = statics::POSTGRESQL_URL;
        let tls: TlsMode = TlsMode::None;

        Connection::connect(params, tls).expect("Could not connect to database")
    };

    let hash: HashMap<String, i32> = {
        let query: &'static str = "SELECT * FROM youtube.stats.channels ORDER BY id ASC";
        let rows: Rows = conn.query(query, &[])
            .expect("Could not query");

        let mut hash: HashMap<String, i32> = HashMap::new();
        for row in &rows {
            let v: i32 = row.get(0);
            let k: String = row.get(1);

            hash.insert(k, v);
        }

        hash
    };
    let keys: Vec<&String> = {
        let mut keys: Vec<&String> = Vec::new();

        for value in hash.keys() {
            keys.push(value);
        }

        keys
    };
    let chunky: Vec<_> = {
        let chunk_size: usize = 50;

        keys.chunks(chunk_size)
            .collect()
    };

    loop {
        let len: usize = chunky.len();

        for i in 0..len {
            let vec_id = chunky[i];
            if vec_id.len() == 0 {
                return;
            }

            let key: String = reqwest::get(url)
                .expect("Could not get HTTP response").text()
                .expect("Could not retrieve HTTP body");
            println!("Using key {}", key);
            if key.is_empty() {
                println!("Detected empty key - waiting 1 minutes");
                let dur: std::time::Duration = std::time::Duration::from_secs(60);
                std::thread::sleep(dur);
                return;
            }

            let capacity: usize = vec_id.len() * 24 + (vec_id.len() - 1);
            let ids: String = {
                let mut buffer: String = String::with_capacity(capacity);
                {
                    let string: &String = vec_id.first().expect("Vector is empty");
                    buffer.push_str(string)
                }

                for i in vec_id.iter().skip(1) {
                    let string: String = format!(",{}", i);
                    let string: &str = string.as_str();

                    buffer.push_str(string);
                }

                buffer
            };
            let url: String = format!("https://www.googleapis.com/youtube/v3/channels?part=statistics&key={}&id={}", key, ids);

            let body: String = reqwest::get(url.as_str())
                .expect("Could not get HTTP response").text()
                .expect("Could not retrieve HTTP body");

            let response: YoutubeResponseType = serde_json::from_str(body.as_str())
                .expect("Could not convert JSON obj");

            for item in response.items {
                let channel_id: &i32 = hash.get(item.id.as_str())
                    .expect("Could not find key");

                println!("{} {} {} {} {}",
                         item.id,
                         channel_id,
                         item.statistics.subscriberCount,
                         item.statistics.viewCount,
                         item.statistics.videoCount);

            }
        }
    }
}
