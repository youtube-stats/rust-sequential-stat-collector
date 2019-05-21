extern crate postgres;
extern crate reqwest;
extern crate serde_json;
extern crate serde;

use std::collections::HashMap;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const QUERY: &'static str = "SELECT * FROM youtube.stats.channels ORDER BY RANDOM() LIMIT 50";

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

fn main() {
    let params: &'static str = POSTGRESQL_URL;
    let tls: postgres::TlsMode = postgres::TlsMode::None;

    let conn: postgres::Connection =
        postgres::Connection::connect(params, tls).unwrap();

    let key: String = std::env::var("YOUTUBE_KEY").unwrap();

    loop {
        let rows: postgres::rows::Rows = conn.query(QUERY, &[]).unwrap();

        let mut hash: std::collections::HashMap<String, String> = HashMap::new();
        for row in &rows {
            let k: String = row.get(1);
            let v: i32 = row.get(0);
            let v: String = v.to_string();

            hash.insert(k, v);
        }

        let mut vec_id: Vec<String> = Vec::new();
        for value in hash.keys().clone() {
            vec_id.push(value.to_string());
        }

        let ids: String = vec_id.join(",");
        let url: String = format!("https://www.googleapis.com/youtube/v3/channels?part=statistics&key={}&id={}", key, ids);

        let mut resp: reqwest::Response = match reqwest::get(url.as_str()) {
            Ok(resp) => resp,
            Err(e) => {
                panic!("{}", e.to_string());
                continue
            }
        };

        let body: String = match resp.text() {
            Ok(text) => text,
            Err(e) => {
                panic!("{}", e.to_string());
                continue
            }
        };

        let response: YoutubeResponseType = match serde_json::from_str(body.as_str()) {
            Ok(text) => text,
            Err(e) => {
                panic!("{}", e.to_string());
                continue
            }
        };

        for item in response.items {
            let channel_id: &String = match hash.get(item.id.as_str()) {
                Ok(text) => text,
                Err(e) => {
                    panic!("{}", e.to_string());
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
                    panic!("{}", e.to_string());
                    continue
                }
            };

            if n != 1 {
                panic!("Row did not insert correctly");
            }
        }
    }
}
