extern crate clap;
use std::time::Duration;

use clap::Parser;
use kademlia::{Contact, Key, Result, RoutingTable, Server};
use tokio::{
  io::{stdin, AsyncBufReadExt, BufReader},
  signal::ctrl_c,
  time::sleep,
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
  addr: String,
  #[clap(short, long)]
  bootstrap: Option<String>,
  #[clap(short, long)]
  console: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
  let args = Args::parse();

  const N: usize = 16;
  let id = Key::<N>::random();
  let k = 3;
  let addr = args.addr.parse().unwrap();
  let routing_table = RoutingTable::new(id, k);
  let mut server = Server::new(id, addr, routing_table).await.unwrap();

  // ネットワークに参加する
  if let Some(bootstrap) = args.bootstrap {
    let bootstrap_addrs =
      bootstrap.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).map(|s| s.parse().unwrap()).collect::<Vec<_>>();
    'bootstrap: loop {
      if server.join(&bootstrap_addrs, Duration::from_secs(30)).await.is_ok() {
        break 'bootstrap;
      }
      eprintln!("join failure: empty contact");
      sleep(Duration::from_secs(3)).await;
    }
  }

  if args.console {
    let mut lines = BufReader::new(stdin()).lines();
    'input: loop {
      eprint!("INPUT> ");
      match lines.next_line().await {
        Ok(Some(line)) => {
          let cmd = line.split_whitespace().collect::<Vec<_>>();
          match cmd.first().map(|s| s.to_lowercase()).as_deref() {
            None => (),
            Some("exit") => break 'input,
            Some("find") if cmd.len() >= 2 => {
              let contacts = server.find_node(key(cmd[1]), Duration::from_secs(3)).await?;
              for (i, Contact { id, address }) in contacts.iter().enumerate() {
                eprintln!("[{:2}] {} = {}", i, id, address);
              }
            }
            Some(unknown) => eprintln!("ERROR: Unknown command: {:?}", unknown),
          }
        }
        _ => break,
      }
    }
  } else {
    ctrl_c().await.unwrap();
  }
  eprintln!("server shutting-down");
  Ok(())
}

fn key<const N: usize>(input: &str) -> Key<N> {
  let input = input.chars().collect::<Vec<_>>();
  let mut key = [0u8; N];
  for i in 0..N {
    let upper = if i * 2 >= input.len() { 0 } else { input[i * 2].to_digit(16).unwrap() };
    let lower = if i * 2 + 1 >= input.len() { 0 } else { input[i * 2 + 1].to_digit(16).unwrap() };
    key[i] = (((upper & 0x0F) << 4) | (lower & 0x0F)) as u8;
  }
  Key::from_bytes(&key)
}
