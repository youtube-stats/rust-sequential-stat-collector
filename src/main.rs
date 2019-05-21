extern crate postgres;
extern crate reqwest;
extern crate serde_json;
extern crate serde;

use std::collections::HashMap;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const QUERY: &'static str = "SELECT * FROM youtube.stats.channels ORDER BY RANDOM() LIMIT 50";
const INSERT: &'static str =
    "INSERT INTO youtube.stats.metrics (channel_id, subs, views, videos) VALUES ($1, $2, $3, $4)";

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

        let mut hash: std::collections::HashMap<String, i32> = HashMap::new();
        for row in &rows {
            let k: String = row.get(1);
            let v: i32 = row.get(0);

            hash.insert(k, v);
        }

        let mut vec_id: Vec<String> = Vec::new();
        for value in hash.keys().clone() {
            vec_id.push(value.to_string());
        }
        let ids: String = vec_id.join(",");

        let url: String = format!("https://www.googleapis.com/youtube/v3/channels?part=statistics&key={}&id={}", key, ids);
        let body: String = reqwest::get(url.as_str()).unwrap().text().unwrap();
        let response: YoutubeResponseType = serde_json::from_str(body.as_str()).unwrap();

        for item in response.items {
            let channel_id: &i32 = hash.get(item.id.as_str()).unwrap();
            let subs: String = item.statistics.subscriberCount;
            let views: String = item.statistics.viewCount;
            let videos: String = item.statistics.videoCount;

            println!("{} {} {} {} {}",
                     item.id,
                     channel_id,
                     subs,
                     views,
                     videos);
            conn.execute(INSERT, &[&channel_id, &subs, &views, &videos]).unwrap();
        }
    }
}
