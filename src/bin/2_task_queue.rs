/// # Assigment 2: Rate-Limited Task Queue
///
/// Objective: Process a batch of work items concurrently, but never
/// exceed N tasks running at the same time.
///
/// Requirements:
///
/// - Define ~20 simulated works (sleep at random 100-500ms) and return a value.
/// - Process all items concurrently, but at most N simultaneously.
/// - Use tokio::sync::semaphore.
/// - For each task prin:
///     - Task {id} started
///     - Task {id} finished - {elapsed_time}
/// - Print total elapsed time.
/// - When a task starts, also print how many are currently in flight.

const MAX_TASKS: usize = 20;
const MAX_CONCURENT: usize = 10;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};

async fn simulated_task(id: usize, n: usize) -> String {
    let start = Instant::now();
    println!("Task {id} started - {n}/{MAX_CONCURENT} in flight");

    let sleep = rand::random_range(100..=500);
    tokio::time::sleep(Duration::from_millis(sleep)).await;

    let elapsed = Instant::now() - start;
    println!("Task {id} finished - {} ms", elapsed.as_millis());
    format!("Task {id} completed")
}

#[tokio::main]
async fn main() {
    let start = Instant::now();

    let (tx, mut rx) = mpsc::channel(MAX_TASKS);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURENT));

    for id in 1..=MAX_TASKS {
        let sem = semaphore.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Ok(_) = sem.acquire().await {
                let n = MAX_CONCURENT - sem.available_permits();
                let resp = simulated_task(id, n).await;
                let _ = tx.send(resp).await;
            }
        });
    }
    drop(tx);

    while let Some(resp) = rx.recv().await {
        println!("Response: {}", resp);
    }

    println!(
        "Total elapsed time: {} ms",
        (Instant::now() - start).as_millis()
    );
}
