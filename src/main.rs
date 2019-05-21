extern crate postgres;
extern crate csv;

const POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
const INSERT: &'static str =
    "INSERT INTO youtube.stats.channels (serial) VALUES ($1) ON CONFLICT DO NOTHING";

fn main() {
    let params: &'static str = POSTGRESQL_URL;
    let tls: postgres::TlsMode = postgres::TlsMode::None;

    let conn: postgres::Connection =
        postgres::Connection::connect(params, tls).unwrap();

    let rdr: std::io::Stdin = std::io::stdin();

    let mut rdr: csv::Reader<std::io::Stdin> =
        csv::Reader::from_reader(rdr);

    for result in rdr.records() {
        let record: csv::StringRecord = result.unwrap();
        let channel_serial: &str = record.get(0).unwrap();

        let result: u64 = conn.execute(INSERT, &[&channel_serial]).unwrap();
        if result == 1{
            println!("Inserting {}", channel_serial);
        } else  if result == 0{
            println!("Ignoring {}", channel_serial);
        } else {
            panic!("Something bad happened - could not insert {}", channel_serial);
        }
    }
}
