/// # Assigment 3: Chat Server with Channels
///
/// Objective: Build a TCP chat server where multiple clients connect and
/// messages are broadcast to all connected clients.
///
/// Requirements:
///
/// - Listen on 127.0.0.1:8080.
/// - When client connects, assign them a name.
/// - Broadcast a join message to all other clients.
/// - Broadcast all messages to connected clients.
/// - Broadcast a leave message to all other clients.
/// - Handle errors gracefully.
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
        format!("> {} - {}\n", self.user, self.content)
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind(format!("{}:{}", SERVER_IP, SERVER_PORT)).await?;
    let (tx, _): (Sender<Message>, Receiver<Message>) = broadcast::channel(CHANNEL_CAPACITY);
    let mut id: usize = 0;

    println!("Listening at {}", listener.local_addr()?);
    while let Ok((socket, addr)) = listener.accept().await {
        tokio::spawn(task(id, socket, addr, tx.clone(), tx.subscribe()));
        id += 1;
    }

    Ok(())
}
