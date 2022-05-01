extern crate clap;
use clap::Parser;
use embed_doc_image::embed_doc_image;
use kademlia::{AppMsg, Contact, Key, NodeID, RoutingTable, Server};
use sha2::{Digest, Sha512};
use std::net::SocketAddr;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::signal::ctrl_c;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::sleep;

/// このアプリケーションで使用するキーのバイト長
///
const N: usize = 4;

const GET_REQ: u8 = 0;
const PUT_REQ: u8 = 1;
const GET_RES: u8 = 2;
const PUT_RES: u8 = 3;

const PUT_SUCCESS: u8 = 0;

/// [kademlia] ライブラリを使用した Key-Value ストアのリファレンス実装です。
///
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
  /// 0.0.0.0:29988 のような Listen アドレス.
  addr: String,
  /// ブートストラップノード.
  #[clap(short, long)]
  bootstrap: Option<String>,
  /// コンソール入力モード (CLI) で起動します.
  #[clap(short, long)]
  console: bool,
}

/// ## Component Chart
///
/// アプリケーションを `--console` オプション付きで実行すると対話型モードとして起動します。対話型モードでは
/// [Input Loop](input_loop()) がユーザ入力を解釈して ([`kademlia::Server`] を介して) `APP_MESSAGE` を送信します。
/// また `APP_MESSAGE` を受信した [`Server`](kademlia::Server) はメッセージを [Message Loop](message_loop())
/// に渡します。
///
/// [][component-chart]
///
/// ### Put Operation
///
/// [KVS Put Operation][kvs-put]
///
/// ### Get Operation
///
/// KVS GET 操作は値のペイロードが `GET_RES` メッセージに付属しているだけで全体の流れは PUT 操作と同じです。
///
/// [KVS Get Operation][kvs-get]
///
#[embed_doc_image("component-chart", "assets/ref-kms-component-chart.png")]
#[embed_doc_image("kvs-put", "assets/kvs-put.png")]
#[embed_doc_image("kvs-get", "assets/kvs-get.png")]
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
  let _ = env_logger::builder().is_test(true).try_init();
  let args = Args::parse();

  // ノードを起動する
  let addr = args.addr.parse().unwrap();
  let id = node_id(&addr);
  let k = 3;
  let routing_table = RoutingTable::new(id, k);
  let (app_tx, app_rx) = mpsc::channel(64);
  let mut server = Server::with_app_channel(id, addr, routing_table, app_tx).await.unwrap();
  eprintln!("{}", server.contact());

  // ネットワークに参加する
  if let Some(bootstrap) = args.bootstrap {
    let bootstrap_addrs =
      bootstrap.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).map(|s| s.parse().unwrap()).collect::<Vec<_>>();
    'bootstrap: loop {
      if server.join(&bootstrap_addrs, Duration::from_secs(30)).await.is_ok() {
        break 'bootstrap;
      }
      log::warn!("join failure: empty contact");
      sleep(Duration::from_secs(3)).await;
    }
  }

  // 別スレッドで APP_MESSAGE 受信処理を実行
  let server = Arc::new(Mutex::new(server));
  let (get_tx, get_rx) = mpsc::channel(64);
  let (put_tx, put_rx) = mpsc::channel(64);
  let shared_server = server.clone();
  tokio::spawn(async move { message_loop(shared_server, app_rx, put_tx, get_tx).await });

  if args.console {
    input_loop(server, put_rx, get_rx).await.unwrap();
  } else {
    ctrl_c().await.unwrap();
  }

  log::info!("server shutting-down");
  Ok(())
}

/// コマンドプロンプト入力の操作を実行します。
///
async fn input_loop(
  server: Arc<Mutex<Server<N>>>, mut put_rx: Receiver<u8>, mut get_rx: Receiver<String>,
) -> Result<()> {
  let mut lines = BufReader::new(stdin()).lines();
  'input: loop {
    eprint!("INPUT> ");
    match lines.next_line().await {
      Ok(Some(line)) => {
        let cmd = tokenize(&line);
        let result = match cmd.first().map(|s| s.to_lowercase()).as_deref() {
          None => Ok(()),
          Some("exit") | Some("quit") => break 'input,
          Some("find") => find(&server, cmd[1..].to_vec()).await,
          Some("put") => put(&server, cmd[1..].to_vec(), &mut put_rx).await,
          Some("get") => get(&server, cmd[1..].to_vec(), &mut get_rx).await,
          Some("help") => {
            eprintln!("find [key] ... key を担当するノードを検索して優先順に表示します");
            eprintln!("put [key] [value] ... key に対する値をネットワークに保存します");
            eprintln!("get [key] ... key に対する値をネットワークから参照します");
            eprintln!("exit ... インタラクティブ CLI を終了します");
            eprintln!("help ... このメッセージ");
            Ok(())
          }
          Some(unknown) => {
            eprintln!("ERROR: コマンドを認識できません: {:?}", unknown);
            Ok(())
          }
        };
        match result {
          Ok(_) => (),
          Err(Error::Cmd(msg)) => eprintln!("ERROR: {}", msg),
          Err(err) => eprintln!("ERROR: {:?}", err),
        }
      }
      _ => break,
    }
  }
  Ok(())
}

/// キーに対応するノードを検索して表示します。
///
async fn find(server: &Arc<Mutex<Server<N>>>, args: Vec<String>) -> Result<()> {
  for i in 0..args.len() {
    let key = str_to_key(&args[i]);
    let mut server = server.lock().await;
    match server.find_node(key, Duration::from_secs(3)).await {
      Ok(contacts) => {
        eprintln!("FIND: {}", key);
        for (i, Contact { id, address }) in contacts.iter().enumerate() {
          eprintln!("[{:2}] {} = {}", i + 1, id, address);
        }
      }
      Err(err) => {
        eprintln!("ERROR: {:?}", err);
      }
    }
  }
  Ok(())
}

/// キーに対する値を保存します。
///
async fn put(server: &Arc<Mutex<Server<N>>>, args: Vec<String>, rx: &mut Receiver<u8>) -> Result<()> {
  if args.len() != 2 {
    return Err(Error::Cmd(format!("put 引数が不正です: {:?}", args)));
  }
  let name = &args[0];
  let value = &args[1];
  let key = str_to_key(&name);
  let peer = {
    let mut server = server.lock().await;
    server.find_node(key, Duration::from_secs(3)).await?.remove(0)
  };
  let peer = {
    let mut payload = vec![PUT_REQ];
    payload.append(&mut (name.len() as u16).to_le_bytes().to_vec());
    payload.append(&mut name.as_bytes().to_vec());
    payload.append(&mut (value.len() as u16).to_le_bytes().to_vec());
    payload.append(&mut value.as_bytes().to_vec());
    let mut server = server.lock().await;
    server.app_call(peer, key, payload, Duration::from_secs(3)).await?
  };
  let code = rx.recv().await.unwrap();
  if code == PUT_SUCCESS {
    eprintln!("PUT [{}] {} = {:?} => {}", key, name, value, peer);
  } else {
    eprintln!("PUT FAILURE");
  }
  Ok(())
}

/// キーに対する値を参照します。
///
async fn get(server: &Arc<Mutex<Server<N>>>, args: Vec<String>, rx: &mut Receiver<String>) -> Result<()> {
  if args.len() != 1 {
    return Err(Error::Cmd(format!("get 引数が不正です: {:?}", args)));
  }
  let name = &args[0];
  let key = str_to_key(&name);
  let peer = {
    let mut server = server.lock().await;
    server.find_node(key, Duration::from_secs(3)).await?.remove(0)
  };
  let peer = {
    let mut payload = vec![GET_REQ];
    payload.append(&mut (name.len() as u16).to_le_bytes().to_vec());
    payload.append(&mut name.as_bytes().to_vec());
    let mut server = server.lock().await;
    server.app_call(peer, key, payload, Duration::from_secs(3)).await?
  };
  let value = rx.recv().await.unwrap();
  eprintln!("GOT [{}] {} = {:?} <= {}", key, name, value, peer);
  Ok(())
}

async fn message_loop(
  shared_server: Arc<Mutex<Server<N>>>, mut app_rx: Receiver<AppMsg<N>>, put_tx: Sender<u8>, get_tx: Sender<String>,
) {
  let mut kvs = HashMap::new();
  while let Some(AppMsg { source, key, value: payload }) = app_rx.recv().await {
    let result = match payload[0] {
      PUT_REQ => {
        // 受信した Key=Value を KVS に保存する
        let (len, name) = buf_to_string(&payload[1..]);
        let (_, value) = buf_to_string(&payload[(1 + 2 + len as usize)..]);
        kvs.insert(name.clone(), value.clone());
        log::info!("STORED   [{}] {} = {:?} ({})", key, name, value, source);
        let payload = vec![PUT_RES, PUT_SUCCESS];
        let mut server = shared_server.lock().await;
        server.app_call(source.clone(), source.id, payload, Duration::from_secs(3)).await.map(|_| ())
      }
      GET_REQ => {
        // 問い合わせのあった Key を問い合わせ元に送信する
        let (_, name) = buf_to_string(&payload[1..]);
        let mut payload = vec![GET_RES];
        if let Some(value) = kvs.get(&name) {
          payload.append(&mut (value.len() as u16).to_le_bytes().to_vec());
          payload.append(&mut value.as_bytes().to_vec());
          log::info!("REFERRED [{}] {} = {:?} ({})", key, name, value, source);
        } else {
          payload.append(&mut 0u16.to_le_bytes().to_vec());
          log::info!("REFERRED [{}] {} = <nonexistent> ({})", key, name, source);
        }
        let mut server = shared_server.lock().await;
        server.app_call(source.clone(), source.id, payload, Duration::from_secs(3)).await.map(|_| ())
      }
      PUT_RES => {
        // PUT 結果の受信
        let code = payload[1];
        put_tx.send(code).await.map_err(|e| kademlia::Error::Shutdown { message: format!("{:?}", e) })
      }
      GET_RES => {
        // GET 結果の受信
        let (_, value) = buf_to_string(&payload[1..]);
        get_tx.send(value).await.map_err(|e| kademlia::Error::Shutdown { message: format!("{:?}", e) })
      }
      unknown => {
        log::error!("unrecognized message id: 0x{:2X}", unknown);
        Ok(())
      }
    };
    if let Err(err) = result {
      log::error!("{:?}", err);
    }
  }
  drop(app_rx);
  log::info!("stopping message_loop");
}

fn buf_to_string(buf: &[u8]) -> (u16, String) {
  let len = [buf[0], buf[1]];
  let len = u16::from_le_bytes(len);
  let str = String::from_utf8_lossy(&buf[2..(2 + len as usize)]).to_string();
  (len, str)
}

/// 指定された文字列をハッシュ化して [Key<N>] に変換します。
///
fn str_to_key<const N: usize>(input: &str) -> Key<N> {
  let mut hasher = Sha512::new();
  hasher.update(input.as_bytes());
  let hash = hasher.finalize();
  bytes_to_key(&hash[..])
}

/// 指定された任意長のバイト配列からキーを生成します。入力値が `N` バイトに満たない場合、左桁の `'0'` が省略されていると
/// みなします。また入力値が `N` バイトを超える場合、左桁が削除されて丸めが行われます。
///
fn bytes_to_key<const N: usize>(input: &[u8]) -> Key<N> {
  let mut bytes = [0u8; N];
  for i in 0..std::cmp::min(N, input.len()) {
    bytes[bytes.len() - i - 1] = input[input.len() - i - 1];
  }
  Key::from_bytes(&bytes)
}

/// ソケットアドレスからノード ID を生成します。
///
fn node_id<const N: usize>(addr: &SocketAddr) -> NodeID<N> {
  let mut hasher = Sha512::new();
  hasher.update(format!("{}", addr).as_bytes());
  let hash = hasher.finalize();
  bytes_to_key(&hash[..])
}

/// プロンプト入力のために引用記号で囲まれた文字列を一つのトークンとして認識して文字列分割を行います。
///
fn tokenize(input: &str) -> Vec<String> {
  let input = input.chars().collect::<Vec<_>>();
  let mut tokens = Vec::new();
  let mut i = 0;
  let mut token = String::with_capacity(256);
  while i < input.len() {
    match input[i] {
      ch if ch.is_whitespace() => {
        i += 1;
        while i < input.len() && input[i].is_whitespace() {
          i += 1;
        }
      }
      quote if quote == '\"' || quote == '\'' => {
        i += 1;
        while i < input.len() {
          if input[i] == quote {
            i += 1;
            break;
          }
          if input[i] == '\\' && i + 1 < input.len() {
            i += 1;
            match input[i] {
              'b' => token.push('\x08'),
              'f' => token.push('\x0C'),
              'n' => token.push('\n'),
              'r' => token.push('\r'),
              't' => token.push('\t'),
              '\\' => token.push('\\'),
              '\"' => token.push('\"'),
              '\'' => token.push('\''),
              '0' => token.push('\0'),
              'x' | 'X' if i + 2 < input.len() => {
                if let Ok(ch) = u8::from_str_radix(&input[i + 1..i + 3].iter().collect::<String>(), 16) {
                  token.push(ch as char);
                  i += 2;
                } else {
                  token.push(input[i - 1]);
                  token.push(input[i]);
                }
              }
              _ => {
                token.push(input[i - 1]);
                token.push(input[i]);
              }
            }
          } else {
            token.push(input[i]);
          }
          i += 1;
        }
        tokens.push(token.to_string());
        token.truncate(0);
      }
      _ => {
        while i < input.len() && !input[i].is_whitespace() {
          token.push(input[i]);
          i += 1;
        }
        if !token.is_empty() {
          tokens.push(token.to_string());
          token.truncate(0);
        }
      }
    }
  }
  if !token.is_empty() {
    tokens.push(token);
  }
  tokens
}

type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
enum Error {
  #[error("{0}")]
  Cmd(String),
  #[error(transparent)]
  Kademlia(#[from] kademlia::Error),
}
