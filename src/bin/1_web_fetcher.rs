/// # Assigment 1: Concurrent Web Fetcher
///
/// Objective: Build a CLI tool that fetches multiple URLs
/// concurrently and reports results.
///
/// Requirements:
///
/// - Fetch all URLs concurrently.
/// - For each URL print (URL, HTTP Status Code, Time of request)
/// - Print total elapsed time
/// - Handle Errors

const URLS: [&str; 6] = [
    "https://httpbin.org",
    "https://jsonplaceholder.typicode.com",
    "https://api.ipify.org/",
    "https://quotes.toscrape.com/",
    "https://api.weather.gov/",
    "https://a.bad.url",
];

async fn fetch_url(url: &str) -> Result<(String, String, u128), String> {
    let start = Instant::now();

    let response = reqwest::get(url)
        .await
        .map_err(|_| format!("failed to fetch - {url}"))?;

    let time = Instant::now() - start;

    Ok((url.into(), response.status().to_string(), time.as_millis()))
}

use std::time::Instant;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let start = Instant::now();

    let (tx, mut rx) = mpsc::channel(URLS.len());

    for url in URLS {
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let res = fetch_url(url).await;
            tx_clone.send(res).await.unwrap();
        });
    }

    drop(tx);

    while let Some(res) = rx.recv().await {
        match res {
            Ok((url, status, time)) => {
                println!("Url: {} Status: {} Elapsed Time: {} ms", url, status, time)
            }
            Err(e) => println!("Failure: {}", e),
        }
    }

    println!(
        "Total elapsed time: {} ms",
        (Instant::now() - start).as_millis()
    )
}
