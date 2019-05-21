extern crate postgres;
extern crate reqwest;
extern crate serde_json;
extern crate serde;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const QUERY: &'static str = "SELECT * FROM youtube.stats.channels ORDER BY RANDOM() LIMIT 50";
const INSERT: &'static str =
    "INSERT INTO youtube.stats.metrics (channel_id, subs, views, videos) VALUES ($1, $2, $3, $4)";

struct Channel {
    id: i32,
    serial: String
}

fn main() {
    let params: &'static str = POSTGRESQL_URL;
    let tls: postgres::TlsMode = postgres::TlsMode::None;

    let conn: postgres::Connection =
        postgres::Connection::connect(params, tls).unwrap();

    let key: String = std::env::var("YOUTUBE_KEY").unwrap();

    loop {
        let rows: postgres::rows::Rows = conn.query(QUERY, &[]).unwrap();
        for row in &rows {
            let channel: Channel = Channel {
                id: row.get(0),
                serial: row.get(1)
            };

            println!("{} {}", channel.id, channel.serial);
        }
    }
}
