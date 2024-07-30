use chrono::prelude::*;
use polars::prelude::*;
use regex::Regex;
use std::error::Error;
use std::fs::File;
use ureq::get;
fn main() -> Result<(), Box<dyn Error>> {
    // Download log data
    let url = r#"https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/nginx_logs/nginx_logs"#;
    let response = get(url).call()?.into_string()?;
    // Parse log data with regex pattern
    let log_pattern = Regex::new(
        r#"(?P<ip>\S+) \S+ \S+ \[(?P<date>[^$\n]+)\] "(?P<method>\S+) (?P<url>\S+) (?P<proto>\S+)" (?P<status>\d+) (?P<bytes>\d+) "(?P<referer>[^\"]*)" "(?P<ua>[^\"]*)""#,
    )?;

    let mut ips = Vec::new();
    let mut dates = Vec::new();
    let mut methods = Vec::new();
    let mut urls = Vec::new();
    let mut protos = Vec::new();
    let mut statuses = Vec::new();
    let mut bytes_vec = Vec::new();
    let mut referers = Vec::new();
    let mut uas = Vec::new();

    for line in response.lines() {
        if let Some(caps) = log_pattern.captures(line) {
            ips.push(caps["ip"].to_string());
            let date = DateTime::parse_from_str(&caps["date"], "%d/%b/%Y:%H:%M:%S %z")?
                .with_timezone(&Utc);
            dates.push(date);
            methods.push(caps["method"].to_string());
            urls.push(caps["url"].to_string());
            protos.push(caps["proto"].to_string());
            statuses.push(caps["status"].parse::<u64>()?);
            bytes_vec.push(caps["bytes"].parse::<u64>()?);
            referers.push(caps["referer"].to_string());
            uas.push(caps["ua"].to_string());
        }
    }

    let date_series = polars::prelude::Series::new("date", dates.iter().map(|d| d.timestamp()).collect::<Vec<_>>());
    let status_series = polars::prelude::Series::new("status", statuses);
    let ip_series = polars::prelude::Series::new("ip", ips);
    let method_series = polars::prelude::Series::new("method", methods);
    let url_series = polars::prelude::Series::new("url", urls);
    let proto_series = polars::prelude::Series::new("proto", protos);
    let referer_series = polars::prelude::Series::new("referer", referers);
    let ua_series = polars::prelude::Series::new("ua", uas);
    let bytes_series = polars::prelude::Series::new("bytes", bytes_vec);

    let df = polars::prelude::DataFrame::new(vec![
        ip_series,
        date_series,
        method_series,
        url_series,
        proto_series,
        status_series,
        bytes_series,
        referer_series,
        ua_series,
    ])?;

    println!("Dataframe {:?}", df);

    let file = File::create("nginx_logs.parquet")?;
    ParquetWriter::new(file).finish(&mut df.clone())?;

    Ok(())
}