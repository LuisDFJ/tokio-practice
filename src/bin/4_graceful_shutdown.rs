/// # Assigment 4: Graceful Shutdown Orchestrator
///
/// Objective: Extend the chat server with graceful shutdown.
/// Stop accepting new connections, drain in-flight work, and
/// exit cleanly.
///
/// Requirements:
///
/// - Start with chat server from previous addigment.
/// - When user presses <ctrl-c> the server should:
///   - Stop accepting new connections.
///   - Broadcast a shutdown message.
///   - Wait for all connected clients to disconnect with
///     a 5s timeout.
/// - Print "Server shutdown gracefully" on exit.

use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{self, Receiver, Sender};

fn new_error(e: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

async fn send_msg(user: String, content: String, tx: &Sender<Message>) -> io::Result<()> {
    tx.send(Message { user, content })
        .map_err(|_| new_error("Channel unavailable"))?;
    Ok(())
}

async fn create_user(id: usize, tx: &Sender<Message>) -> io::Result<String> {
    let user = format!("User {id}");
    send_msg(user.clone(), "joined the chat".into(), tx).await?;
    Ok(user)
}

async fn goodbye_user(user: String, tx: Sender<Message>) -> io::Result<()> {
    send_msg(user, "left the chat".into(), &tx).await
}

async fn task(
    id: usize,
    mut socket: TcpStream,
    addr: SocketAddr,
    tx: Sender<Message>,
    mut rx: Receiver<Message>,
    mut cancel: Receiver<()>,
) -> io::Result<()> {
    let user = create_user(id, &tx).await?;
    println!("{} joined from {}", user, addr);
    let (s_rx, mut writer) = socket.split();
    let mut reader = BufReader::new(s_rx).lines();
    loop {
        tokio::select! {
            line = reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if send_msg(user.clone(), line, &tx).await.is_err() {
                            break
                        }
                    }
                    Ok(None) | Err(_) => break,
                }

            }

            msg = rx.recv() => {
                match msg {
                    Ok(msg) => {
                        if msg.user != user && writer.write_all(msg.parse().as_bytes()).await.is_err() {
                                break
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        if writer.write_all(format!("INFO: {n} messages missed\n").as_bytes()).await.is_err() {
                            break
                        }
                    },
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }

            _ = cancel.recv() => {
                break
            }
        }
    }
    println!("{} left", user);
    goodbye_user(user, tx).await?;
    Ok(())
}

const SERVER_IP: &str = "127.0.0.1";
const SERVER_PORT: &str = "8080";
const CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
struct Message {
    user: String,
    content: String,
}

impl Message {
    fn parse(&self) -> String {
        if &self.user == "" {
            format!("{}\n", self.content)
        } else {
            format!("> {} - {}\n", self.user, self.content)
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind(format!("{}:{}", SERVER_IP, SERVER_PORT)).await?;
    let (tx, _): (Sender<Message>, Receiver<Message>) = broadcast::channel(CHANNEL_CAPACITY);
    let (cancel, _): (Sender<()>, Receiver<()>) = broadcast::channel(1);

    let mut id: usize = 0;

    println!("Listening at {}", listener.local_addr()?);

    let mut tasks = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((socket,addr)) => {
                        tasks.spawn(
                            task(id, socket, addr, tx.clone(), tx.subscribe(), cancel.subscribe())
                        );
                        id += 1;
                    }
                    Err(_) => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break
            }
        }
    }

    println!("Server shuting down");
    send_msg("".into(), "Server shuting down".into(), &tx).await?;
    cancel.send(())
        .map_err(|_| new_error("Cancel channel unavailable"))?;

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tasks.join_all()
    ).await?;

    println!("Graceful shutdown");
    Ok(())
}


