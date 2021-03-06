extern crate postgres;
extern crate reqwest;
extern crate serde_json;
extern crate serde;
extern crate quick_protobuf;

use postgres::Connection;
use postgres::TlsMode;
use postgres::rows::Rows;
use quick_protobuf::Writer;
use std::collections::HashMap;
use std::time::SystemTime;

pub mod message;
use message::Subs;

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
    #[allow(dead_code)]
    viewCount: String,

    #[allow(dead_code)]
    commentCount: String,

    subscriberCount: String,

    #[allow(dead_code)]
    hiddenSubscriberCount: bool,

    #[allow(dead_code)]
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

pub mod statics {
    pub const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
}

pub fn get_client() -> postgres::Connection {
    let params: &'static str = POSTGRESQL_URL;
    let tls: postgres::TlsMode = postgres::TlsMode::None;

    postgres::Connection::connect(params, tls).unwrap()
}

fn main() {
    let url: &'static str = "http://localhost:8080/get";
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
        let client = reqwest::Client::new();

        for i in 0..len {
            let vec_id = chunky[i];
            if vec_id.len() == 0 {
                return;
            }

            let key: String = reqwest::get(url)
                .expect("Could not get HTTP response").text()
                .expect("Could not retrieve HTTP body");
            println!("Using key {}", key);

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

            {
                let now: u64 = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Could not get timestamp")
                    .as_secs();
                let now: i32 = now as i32;

                let mut time: Vec<i32> = Vec::new();
                let mut ids: Vec<i32> = Vec::new();
                let mut subs: Vec<i32> = Vec::new();

                for item in response.items {
                    let k: &str = item.id.as_str();
                    let id: &i32 = hash.get(k)
                        .expect("Could not find key");
                    let value: i32 = item.statistics.subscriberCount.parse::<i32>()
                        .expect("Could not convert subs to i32");

                    time.push(now.clone());
                    ids.push(id.clone());
                    subs.push(value);
                }

                let subs: Subs = Subs {
                    time,
                    ids,
                    subs
                };

                println!("{:?}", subs);

                let out: Vec<u8> = {
                    let mut out: Vec<u8> = Vec::new();
                    {
                        let mut writer = Writer::new(&mut out);
                        writer
                            .write_message(&subs)
                            .expect("Cannot write message!");
                    }

                    out
                };

                let url: &'static str = "http://localhost:8081/post";
                let _ = client.post(url)
                    .body(out)
                    .send()
                    .expect("Could not send message");
            }
        }
    }
}
