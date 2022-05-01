use crate::routing_table::test::set_expires;
use crate::server::{MessageLoop, Session};
use crate::{AppMsg, Contact, Error, Key, Message, NodeID, Result, RoutingTable, Server};
use rand::{thread_rng, Rng};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::mpsc::channel;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[tokio::test]
async fn join_empty_bootstrap_nodes() {
  const N: usize = 2;
  const K: usize = 10;
  let id = Key::<N>::random();
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut server = Server::new(id, addr, rt).await.unwrap();
  assert!(matches!(server.join(&[], Duration::from_secs(1)).await, Err(Error::Routing { .. })));
}

#[tokio::test]
async fn join_unable_to_connect_to_bootstrap_nodes() {
  const N: usize = 2;
  const K: usize = 10;
  let id = Key::<N>::random();
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut server = Server::new(id, addr, rt).await.unwrap();
  assert!(matches!(
    server.join(&["127.0.0.1:0".parse().unwrap()], Duration::from_secs(1)).await,
    Err(Error::Timeout { .. })
  ));
}

#[tokio::test]
async fn join_timeout() {
  init();
  const N: usize = 2;
  const K: usize = 10;

  // 応答しない UDP ポートを開く
  let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let bootstrap = socket.local_addr().unwrap();

  let id = Key::<N>::random();
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::new(id, addr, rt).await.unwrap();
  assert!(matches!(node.join(&[bootstrap], Duration::from_secs(0)).await, Err(Error::Timeout { .. })));
}

#[tokio::test]
async fn join_empty_contacts_response() {
  init();
  const N: usize = 1;
  const K: usize = 10;

  let id = Key::from_bytes(&[0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::new(id, addr, rt).await.unwrap();
  let port = node.bind_address().port();

  // FIND_NODE に 0 件で応答する UDP ポートを開く
  let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let bootstrap = socket.local_addr().unwrap();
  tokio::spawn(async move {
    let mut buf = [0u8; 0xFFFF];
    let len = socket.recv(&mut buf).await.unwrap();
    let msg = Message::<N>::from_bytes(&buf[..len]).unwrap();
    assert!(matches!(msg, Message::FindNode(..)));
    if let Message::FindNode(_peer_id, nonce, _target_id) = msg {
      let id = Key::from_bytes(&[0x7F]);
      let msg = Message::FoundNode(id, nonce, vec![]);
      socket.send_to(&msg.to_bytes(), format!("127.0.0.1:{}", port)).await.unwrap();
    }
  });

  assert!(matches!(node.join(&[bootstrap], Duration::from_secs(5)).await, Err(Error::Routing { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_empty_contacts_response_with_timeout() {
  init();
  const N: usize = 1;
  const K: usize = 10;

  let id = Key::from_bytes(&[0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::with(id, addr, rt, None, 3600.0).await.unwrap();
  let port = node.bind_address().port();

  // FIND_NODE に 0 件で応答する UDP ポートを開く
  let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let bootstrap = socket.local_addr().unwrap();
  let handler = tokio::spawn(async move {
    let mut buf = [0u8; 0xFFFF];
    let len = socket.recv(&mut buf).await.unwrap();
    let msg = Message::<N>::from_bytes(&buf[..len]).unwrap();
    assert!(matches!(msg, Message::FindNode(..)));
    if let Message::FindNode(_peer_id, nonce, _target_id) = msg {
      let id = Key::from_bytes(&[0x7F]);
      let msg = Message::FoundNode(id, nonce, vec![]);
      socket.send_to(&msg.to_bytes(), format!("127.0.0.1:{}", port)).await.unwrap();
      tokio::time::sleep(Duration::from_millis(1000)).await;
    }
  });

  assert!(matches!(node.join(&[bootstrap], Duration::from_secs(0)).await, Err(Error::Timeout { .. })));
  handler.await.unwrap();
  drop(node);
}

#[tokio::test]
async fn join_too_slow_response() {
  init();
  const N: usize = 1;
  const K: usize = 10;

  let id = Key::from_bytes(&[0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::with(id, addr, rt, None, 3600.0).await.unwrap();
  let port = node.bind_address().port();

  // FIND_NODE 受信 1 秒後に応答する UDP ポートを開く
  // 呼び出しは 0.5 秒後にタイムアウトでリターンし、サーバは 2 秒以内にタスクを purge する
  // CI のような遅延の大きい環境での実行は「セッションから取り出したタスクがすでにタイムアウトしているためリダイレクトを
  // 中止」という挙動にならないかもしれない。
  let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let bootstrap = socket.local_addr().unwrap();
  let handler = tokio::spawn(async move {
    let mut buf = [0u8; 0xFFFF];
    let len = socket.recv(&mut buf).await.unwrap();
    let msg = Message::<N>::from_bytes(&buf[..len]).unwrap();
    assert!(matches!(msg, Message::FindNode(..)));
    if let Message::FindNode(_peer_id, nonce, _target_id) = msg {
      let id = Key::from_bytes(&[0x7F]);
      let contacts =
        vec![Contact { id: Key::from_bytes(&[0x3F]), address: format!("127.0.0.1:{}", port).parse().unwrap() }];
      let msg = Message::FoundNode(id, nonce, contacts);
      tokio::time::sleep(Duration::from_millis(1000)).await;
      socket.send_to(&msg.to_bytes(), format!("127.0.0.1:{}", port)).await.unwrap();
      tokio::time::sleep(Duration::from_millis(1000)).await;
    }
  });

  assert!(matches!(node.join(&[bootstrap], Duration::from_millis(500)).await, Err(Error::Timeout { .. })));
  handler.await.unwrap();
  drop(node)
}

#[tokio::test]
async fn find_node_by_100nodes() {
  init();
  const N: usize = 4;
  const K: usize = 10;

  // 100 ノードを起動する
  let mut servers = boot_cluster(100, K).await;

  // FIND_NODE を実行
  // ただし問い合わせ先のノードが保持しているルーティングテーブルにより返される近傍ノードもその数も様々になる
  tokio::time::sleep(Duration::from_millis(500)).await;
  let mut rng = thread_rng();
  for _ in 0..100 {
    eprintln!("-------------");
    let server_idx = rng.gen_range(0..servers.len());
    let id = NodeID::<N>::random();
    assert!(matches!(servers.get_mut(server_idx), Some(_)));
    if let Some(server) = servers.get_mut(server_idx) {
      let contacts = server.find_node(id, Duration::from_secs(3)).await.unwrap();
      assert!(!contacts.is_empty() && contacts.len() <= K, "len={}", contacts.len());

      // id に近い順にソートされていることを確認
      let actual = contacts.iter().map(|c| c.id).collect::<Vec<_>>();
      let mut expected = actual.clone();
      expected.sort_by(|a, b| {
        let da = a ^ &id;
        let db = b ^ &id;
        da.cmp(&db)
      });
      assert_eq!(expected, actual);
    }
  }
}

#[tokio::test]
async fn find_node_full_k_bucket() {
  init();
  const N: usize = 1;
  const K: usize = 2;

  // 256 ノードを起動する
  let ids = (0x00..=0xFF).map(|i| Key::from_bytes(&[i])).collect::<Vec<_>>();
  let mut servers = boot_cluster_for::<N>(&ids, K).await;

  // すべてのノードがノード 0 に対して自身の問い合わせを行う
  for i in 0..servers.len() {
    let id = *servers[i].id();
    servers[0].find_node(id, Duration::from_secs(5)).await.unwrap();
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_node_respond_too_late() {
  init();
  const K: usize = 10;

  // ノードを起動
  let id = Key::from_bytes(&[0x00, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::with(id, addr, rt, None, 0.1).await.unwrap();

  // 別のノードを起動して JOIN 後に終了させ、タイムアウト後に応答する UDP ソケットで置き換える
  let bootstrap = boot_single_node(&[0x80, 0x00], K).await;
  let id = *bootstrap.id();
  let addr = *bootstrap.bind_address();
  node.join(&[addr], Duration::from_secs(3)).await.unwrap();
  bootstrap.shutdown().await.unwrap();
  let (_addr, handle) = open_service_at(id, addr, |id, addr, msg| {
    std::thread::sleep(Duration::from_secs(1));
    Some(Message::FoundNode(id, *nonce(&msg), vec![Contact::new(id, addr)]))
  })
  .await
  .unwrap();

  // FIND_NODE 要求を出してタイムアウトを待機する
  let id = Key::from_bytes(&[0xCF, 0xFF]);
  let result = node.find_node(id, Duration::from_secs(0)).await;
  assert!(matches!(result, Err(Error::Timeout(..))));
  tokio::time::sleep(Duration::from_secs(2)).await;
  handle.await.unwrap().unwrap();
  drop(node);
}

#[tokio::test]
async fn app_call_not_found() {
  init();
  const N: usize = 2;
  const K: usize = 10;

  // ノード1: APP_MSG を受信する処理を設定しない
  let node1 = boot_single_node_with_random_id::<N>(K).await;
  let id1 = node1.id();

  // ノード2: ノード 1 に対して APP_MSG を送信する
  let mut node2 = boot_single_node_with_random_id(K).await;
  node2.join(&[*node1.bind_address()], Duration::from_secs(3)).await.unwrap();
  assert!(matches!(
    node2.app_call(node1.contact().clone(), *id1, vec![0x00], Duration::from_secs(3)).await,
    Err(Error::AppNotFound { .. })
  ));
}

#[tokio::test]
async fn app_call_no_response() {
  init();
  const N: usize = 2;
  const K: usize = 10;

  // 応答しない UDP ポートを開く
  let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let port = socket.local_addr().unwrap().port();

  let id = Key::<N>::random();
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::new(id, addr, rt).await.unwrap();

  assert!(matches!(
    node
      .app_call(
        Contact { id: Key::<N>::random(), address: format!("127.0.0.1:{}", port).parse().unwrap() },
        id,
        vec![],
        Duration::from_secs(0),
      )
      .await,
    Err(Error::Timeout { .. })
  ));
}

#[tokio::test]
async fn app_call_redirected_echo() {
  init();
  const K: usize = 10;

  // ノード1: echo ノード
  let id = Key::from_bytes(&[0x00, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let (tx, mut rx) = channel(64);
  let mut echo = Server::with_app_channel(id, addr, rt, tx).await.unwrap();
  let contact = echo.contact().clone();
  let key = id;
  let handler1 = tokio::spawn(async move {
    let msg = rx.recv().await.unwrap();
    log::debug!("ECHO: received: {:?}", msg);
    let AppMsg { source, key: _key, value } = msg;
    echo.app_call(source.clone(), source.id, value, Duration::from_secs(60)).await.unwrap();
  });

  // ノード2: リダイレクトノード
  let id = Key::from_bytes(&[0x80, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut redirect = Server::new(id, addr, rt).await.unwrap();
  redirect.join(&[contact.address], Duration::from_secs(3)).await.unwrap();

  // ノード3: リダイレクトノードを介して echo ノードに対して APP_MSG を送信する
  let id = Key::from_bytes(&[0xC0, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let (tx, mut rx) = channel(64);
  let mut node = Server::with_app_channel(id, addr, rt, tx).await.unwrap();
  let peer = redirect.contact().clone();
  let handler3 = tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(500)).await;
    let msg = "hello, world".as_bytes().to_vec();
    node.app_call(peer, key, msg, Duration::from_secs(3)).await.unwrap();
    assert_eq!(
      Some(AppMsg { source: contact.clone(), key: id, value: "hello, world".as_bytes().to_vec() }),
      rx.recv().await,
    );
  });

  handler1.await.unwrap();
  handler3.await.unwrap();
}

#[tokio::test]
async fn app_call_redirected_timeout() {
  init();
  const K: usize = 10;

  // ノード1: echo ノード
  let id = Key::from_bytes(&[0x00, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let node1 = Server::new(id, addr, rt).await.unwrap();
  let contact = node1.contact().clone();

  // ノード2: リダイレクトノード
  let id = Key::from_bytes(&[0x80, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node2 = Server::new(id, addr, rt).await.unwrap();
  node2.join(&[contact.address], Duration::from_secs(3)).await.unwrap();

  // ノード3: リダイレクトノードを介してノード 1 に APP_MSG を送信する
  let id = Key::from_bytes(&[0xC0, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node3 = Server::with(id, addr, rt, None, 3600.0).await.unwrap();
  let msg = "hello, world".as_bytes().to_vec();
  let peer = node2.contact().clone();
  let key = *node1.id();
  tokio::time::sleep(Duration::from_millis(500)).await;
  let result = node3.app_call(peer, key, msg, Duration::from_secs(0)).await;
  assert!(matches!(result, Err(Error::Timeout(..))) || matches!(result, Err(Error::Shutdown { .. })), "{:?}", result);

  tokio::time::sleep(Duration::from_secs(2)).await;
  drop(node1);
  drop(node2);
  drop(node3);
}

#[tokio::test]
async fn app_call_redirected_with_empty_contacts() {
  init();
  const K: usize = 10;

  // 空のコンタクト情報付きの FOUND_NODE メッセージを返すソケットをオープン
  let id = Key::from_bytes(&[0x00, 0x00]);
  let (peer_addr, handler) = open_responding_with_found_node_with_empty_contacts(id).await.unwrap();

  // ソケットに対してAPP_MSG を送信する
  let id = Key::from_bytes(&[0xC0, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node3 = Server::new(id, addr, rt).await.unwrap();
  let msg = "hello, world".as_bytes().to_vec();
  let peer = Contact::new(id, peer_addr);
  let key = Key::from_bytes(&[0x80, 0x00]);
  assert!(matches!(node3.app_call(peer, key, msg, Duration::from_secs(3)).await, Err(Error::Routing { .. })));

  handler.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_call_responded_timeout() {
  init();
  const K: usize = 10;

  // 一定時間後に応答するソケットをオープン
  let id = Key::from_bytes(&[0x00, 0x00]);
  let (peer_addr, handler) = open_service(id, |id, _addr, msg| {
    std::thread::sleep(Duration::from_secs(2));
    Some(Message::AppResponse(id, *nonce(&msg), Message::<2>::APP_RESPONSE_OK))
  })
  .await
  .unwrap();

  // ソケットに対してAPP_MSG を送信する
  let id = Key::from_bytes(&[0xC0, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::new(id, addr, rt).await.unwrap();
  let msg = "hello, world".as_bytes().to_vec();
  let peer = Contact::new(id, peer_addr);
  let key = Key::from_bytes(&[0x80, 0x00]);
  assert!(matches!(node.app_call(peer, key, msg, Duration::from_millis(500)).await, Err(Error::Timeout { .. })));

  handler.await.unwrap().unwrap();
  tokio::time::sleep(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn app_call_responded_invalid_code() {
  init();
  const K: usize = 10;

  // 未定義の応答コードで応答するソケットをオープン
  let id = Key::from_bytes(&[0x00, 0x00]);
  let (peer_addr, handler) =
    open_service(id, |id, _addr, msg| Some(Message::AppResponse(id, *nonce(&msg), 0xFF))).await.unwrap();

  // ソケットに対してAPP_MSG を送信する
  let id = Key::from_bytes(&[0xC0, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::new(id, addr, rt).await.unwrap();
  let msg = "hello, world".as_bytes().to_vec();
  let peer = Contact::new(id, peer_addr);
  let key = Key::from_bytes(&[0x80, 0x00]);
  assert!(matches!(
    node.app_call(peer, key, msg, Duration::from_millis(500)).await,
    Err(Error::UnknowAppResponse { .. })
  ));

  handler.await.unwrap().unwrap();
}

#[tokio::test]
async fn app_call_responded_for_join() {
  init();
  const K: usize = 10;

  // APP_RESPONSE で応答するソケットをオープン
  let id = Key::from_bytes(&[0x00, 0x00]);
  let (peer_addr, handler) =
    open_service(id, |id, _addr, msg| Some(Message::AppResponse(id, *nonce(&msg), Message::<2>::APP_RESPONSE_OK)))
      .await
      .unwrap();

  // ソケットに対して JOIN (FIND_NODE) を実行する
  let id = Key::from_bytes(&[0xC0, 0x00]);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let mut node = Server::new(id, addr, rt).await.unwrap();
  let result = node.join(&[peer_addr], Duration::from_millis(500)).await;
  assert!(matches!(result, Err(Error::Timeout { .. })), "{:?}", result);

  handler.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown() {
  let mut servers = boot_cluster::<2>(4, 10).await;
  while !servers.is_empty() {
    let server = servers.remove(0);
    let addr = *server.bind_address();
    server.shutdown().await.unwrap();

    // シャットダウン実行後すぐに UDP ソケットが使用できることを確認
    let socket = UdpSocket::bind(addr).await.unwrap();
    drop(socket);
  }
}

#[tokio::test]
async fn command_between_server_and_message_loop() {
  const N: usize = 8;

  // 正常に応答が返ってくるケース
  let (mut ctx, mut crx) = channel(1);
  let (rtx, rrx) = channel(1);
  let rtx2 = rtx.clone();
  let handler = tokio::spawn(async move {
    let _cmd = crx.recv().await.unwrap();
    rtx2.send(Ok(())).await.unwrap();
  });
  Server::<N>::command("test", super::CommandOpt::Shutdown(rtx), Duration::from_secs(3), &mut ctx, rrx).await.unwrap();
  handler.await.unwrap();

  // 送信時点で MessageLoop が終了しているケース
  let (mut ctx, crx) = channel(1);
  let (rtx, rrx) = channel(1);
  drop(crx);
  assert!(matches!(
    Server::<N>::command("test", super::CommandOpt::Shutdown(rtx), Duration::from_secs(3), &mut ctx, rrx).await,
    Err(Error::Shutdown { .. })
  ));

  // 送信は成功したが受信前に MessageLoop が終了したケース
  let (mut ctx, mut crx) = channel(1);
  let (rtx, rrx) = channel::<Result<()>>(1);
  let (dtx, _drx) = channel(1);
  let handler = tokio::spawn(async move {
    let _cmd = crx.recv().await.unwrap();
    drop(rtx);
  });
  assert!(matches!(
    Server::<N>::command("test", super::CommandOpt::Shutdown(dtx), Duration::from_secs(3), &mut ctx, rrx).await,
    Err(Error::Shutdown { .. })
  ));
  handler.await.unwrap();

  // 応答がタイムアウトしたケース
  let (mut ctx, mut crx) = channel(1);
  let (rtx, rrx) = channel(1);
  let (xtx, mut xrx) = channel(1);
  let handler = tokio::spawn(async move {
    let _cmd = crx.recv().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    xrx.recv().await.unwrap();
  });
  assert!(matches!(
    Server::<N>::command("test", super::CommandOpt::Shutdown(rtx), Duration::from_millis(500), &mut ctx, rrx).await,
    Err(Error::Timeout { .. })
  ));
  xtx.send(()).await.unwrap();
  handler.await.unwrap();
}

#[tokio::test]
async fn bind_to_duplicated_port() {
  init();
  const N: usize = 4;
  const K: usize = 10;

  // 2 ノードを同じポート番号で起動
  let mut port = 0;
  let mut servers = Vec::with_capacity(2);
  for i in 0..servers.capacity() {
    let id = NodeID::<N>::random();
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let rt = RoutingTable::new(id, K);
    let server = match Server::new(id, addr, rt).await {
      Ok(server) => server,
      Err(Error::IO { .. }) => break,
      Err(err) => panic!("unexpected error: {:?}", err),
    };
    eprintln!("[{}] bind on {}", i + 1, server.bind_address());
    if port == 0 {
      port = server.bind_address().port();
    }
    servers.push(server);
  }
}

#[tokio::test]
async fn display() {
  init();
  const N: usize = 4;
  const K: usize = 10;
  let id = NodeID::<N>::random();
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, K);
  let server = Server::new(id, addr, rt).await.unwrap();
  let port = server.bind_address().port();
  assert_eq!(format!("{}@127.0.0.1:{}", id, port), format!("{}", server));
}

#[tokio::test]
async fn message_loop_replace_failure_node() {
  init();
  const N: usize = 1;
  const K: usize = 2;

  // 5 ノード起動: RoutingTable [[1,2], [3,4]] ← 5 となるように
  let ids = vec![[0b11111111], [0b01111111], [0b10111111], [0b00111111], [0b11010000]]
    .iter()
    .map(Key::from_bytes)
    .collect::<Vec<_>>();
  let nodes = boot_cluster_for(&ids, K).await;

  // ノード 0 役は MessageLoop で動作
  let id = Key::<N>::from_bytes(&0u8.to_be_bytes());
  let mut socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let routing_table = RoutingTable::new(id, K);
  let sessions = Session::new();
  let mut ml = MessageLoop { id, address: socket.local_addr().unwrap(), routing_table, sessions };

  // 5 ノードを "観測" し PING 要請の出たノードには何もせずタイムアウトさせる
  let mut replace = Vec::new();
  for node in nodes.iter() {
    let contact = node.contact().clone();
    if let Some(ping) = ml.routing_table.observed(contact.clone()) {
      let deadline = Instant::now() + Duration::from_millis(1);
      if ml.sessions.begin_ping(deadline, ping.id, contact.clone()).is_some() {
        replace.push((ping.id, contact.id));
      }
    }
  }
  log::debug!("replacing contacts: {:?}", replace);
  assert!(!replace.is_empty());
  tokio::time::sleep(Duration::from_millis(2)).await;

  for (exist, notexist) in replace.iter() {
    assert!(ml.routing_table.get_contact(exist).is_some());
    assert!(ml.routing_table.get_contact(notexist).is_none());
  }

  ml.purge(&mut socket).await.unwrap();

  // purge 後に置き換えが行われていることを確認
  for (notexist, exist) in replace.iter() {
    assert!(ml.routing_table.get_contact(exist).is_some());
    assert!(ml.routing_table.get_contact(notexist).is_none());
  }
}

#[tokio::test]
async fn message_loop_refresh_routing_table() {
  init();
  const N: usize = 1;
  const K: usize = 2;

  // UDP 受信確認のための 5 ソケットを起動
  let mut handlers = Vec::new();
  let mut contacts = Vec::new();
  let msgs = Arc::new(Mutex::new(Vec::new()));
  for i in 1u8..=5 {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    contacts.push(Contact::new(Key::<N>::from_bytes(&i.to_be_bytes()), socket.local_addr().unwrap()));
    let (tx, rx) = oneshot::channel::<()>();
    let msgs = msgs.clone();
    let handler = tokio::spawn(async move {
      log::debug!("begin {}", i);
      let mut buf = [0u8; 0xFFFF];
      select! {
        _ = rx => (),
        result = socket.recv(&mut buf) => {
          log::debug!("receive {}", i);
          let len = result.unwrap();
          msgs.lock().await.push(Message::<N>::from_bytes(&buf[..len]).unwrap());
        }
      }
      log::debug!("end {}", i);
    });
    handlers.push((tx, handler));
  }

  // ノード 0 役は MessageLoop で動作
  let id = Key::<N>::from_bytes(&0u8.to_be_bytes());
  let mut socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
  let routing_table = RoutingTable::new(id, K);
  let sessions = Session::new();
  let mut ml = MessageLoop { id, address: socket.local_addr().unwrap(), routing_table, sessions };
  for contact in contacts.iter() {
    ml.routing_table.observed(contact.clone());
  }

  // ルーティングテーブルの確認期限を超過させる
  set_expires(&mut ml.routing_table, Instant::now());
  tokio::time::sleep(Duration::from_millis(1)).await;

  ml.purge(&mut socket).await.unwrap();
  tokio::time::sleep(Duration::from_secs(1)).await;
  log::debug!("received messages: {}", msgs.lock().await.len());
  assert!(!msgs.lock().await.is_empty());

  // 受信しなかったノードも含めてすべてのスレッドを終了
  for (tx, handler) in handlers {
    let _ = tx.send(());
    handler.await.unwrap();
  }
}

#[tokio::test]
async fn check_and_get_first_contact() {
  // 空のコンタクト情報
  let key = Key::from_bytes(&[0x00, 0x00]);
  let peer = Contact::new(Key::from_bytes(&[0x80, 0x88]), "127.0.0.1:0".parse().unwrap());
  let (tx, mut rx) = channel::<Result<()>>(1);
  assert!(MessageLoop::check_and_get_first_contact(&peer, &key, &[], &tx).await.is_none());
  drop(tx);
  assert!(matches!(rx.recv().await, Some(Err(Error::Routing { .. }))));

  // ソートされているコンタクト情報
  let mut contacts = (0..=65535u16)
    .step_by(8)
    .map(|i| {
      let id = Key::from_bytes(&i.to_le_bytes());
      Contact::new(id, "127.0.0.1:0".parse().unwrap())
    })
    .collect::<Vec<_>>();
  contacts.sort_by(|c1, c2| c1.id.cmp(&c2.id));
  let (tx, mut rx) = channel::<Result<()>>(1);
  let first = contacts.first().unwrap();
  assert_eq!(Some(first.clone()), MessageLoop::check_and_get_first_contact(&peer, &key, &contacts, &tx).await);
  drop(tx);
  assert!(matches!(rx.recv().await, None));

  // ソートされていないコンタクト情報
  contacts.sort_by(|c1, c2| c1.id.cmp(&c2.id));
  let tmp = contacts[0].clone();
  contacts[0] = contacts[1].clone();
  contacts[1] = tmp;
  let (tx, mut rx) = channel::<Result<()>>(1);
  assert!(MessageLoop::check_and_get_first_contact(&peer, &key, &contacts, &tx).await.is_none());
  drop(tx);
  assert!(matches!(rx.recv().await, Some(Err(Error::Routing { .. }))));
}

#[tokio::test]
async fn check_max_hop_has_not_been_reached() {
  let (tx, mut rx) = channel::<Result<()>>(1);
  assert!(MessageLoop::<1>::check_max_hop_has_not_been_reached(1, &tx).await);
  drop(tx);
  assert!(matches!(rx.recv().await, None));
  eprintln!("11111111111111111");

  let (tx, mut rx) = channel::<Result<()>>(1);
  assert!(MessageLoop::<1>::check_max_hop_has_not_been_reached(MessageLoop::<1>::MAX_HOPS, &tx).await);
  drop(tx);
  assert!(matches!(rx.recv().await, None));
  eprintln!("222222222222222");

  let (tx, mut rx) = channel::<Result<()>>(10);
  assert!(!MessageLoop::<1>::check_max_hop_has_not_been_reached(MessageLoop::<1>::MAX_HOPS + 1, &tx).await);
  drop(tx);
  assert!(matches!(rx.recv().await, Some(Err(Error::Routing { .. }))));
  eprintln!("333333333333333");
}

async fn boot_single_node_with_random_id<const N: usize>(k: usize) -> Server<N> {
  use std::io::Write;
  let mut id = [0u8; N];
  (&mut id[..]).write_all(Key::<N>::random().as_bytes()).unwrap();
  boot_single_node(&id, k).await
}

async fn boot_single_node<const N: usize>(id: &[u8; N], k: usize) -> Server<N> {
  let id = Key::from_bytes(id);
  let addr = "127.0.0.1:0".parse().unwrap();
  let rt = RoutingTable::new(id, k);
  Server::new(id, addr, rt).await.unwrap()
}

async fn boot_cluster<const N: usize>(nodes: usize, k: usize) -> Vec<Server<N>> {
  boot_cluster_for(&(0..nodes).map(|_| NodeID::<N>::random()).collect::<Vec<_>>(), k).await
}

async fn boot_cluster_for<const N: usize>(ids: &[NodeID<N>], k: usize) -> Vec<Server<N>> {
  let mut servers = Vec::with_capacity(ids.len());
  let mut bootstrap = Vec::with_capacity(std::cmp::min(5, ids.len()));
  for (i, id) in ids.iter().enumerate() {
    let addr = "127.0.0.1:0".parse().unwrap();
    let rt = RoutingTable::new(*id, k);
    let mut server = Server::new(*id, addr, rt).await.unwrap();
    assert_ne!(0, server.bind_address().port());
    eprintln!("[{}] Kademlia Node {} boots at {}", i + 1, id, server.bind_address());
    if !bootstrap.is_empty() {
      server.join(&bootstrap, Duration::from_secs(3)).await.unwrap();
    }
    if i < bootstrap.capacity() {
      bootstrap.push(*server.bind_address());
    }
    servers.push(server);
  }
  servers
}

fn init() {
  let _ = env_logger::builder().is_test(true).try_init();
}

/// 任意のメッセージに対して空のコンタクト情報を持つ FOUND_NODE メッセージで応答するソケットをオープンします。
///
async fn open_responding_with_found_node_with_empty_contacts<const N: usize>(
  node_id: NodeID<N>,
) -> Result<(SocketAddr, JoinHandle<Result<()>>)> {
  open_service(node_id, |node_id, _addr: SocketAddr, msg: Message<N>| -> Option<Message<N>> {
    let nonce = nonce(&msg);
    Some(Message::FoundNode(node_id, *nonce, vec![]))
  })
  .await
}

/// 任意の処理を行うソケットをオープンします。
///
async fn open_service<const N: usize>(
  node_id: NodeID<N>, service: fn(NodeID<N>, SocketAddr, Message<N>) -> Option<Message<N>>,
) -> Result<(SocketAddr, JoinHandle<Result<()>>)> {
  open_service_at(node_id, "127.0.0.1:0".parse().unwrap(), service).await
}

/// 任意の処理を行うソケットをオープンします。
///
async fn open_service_at<const N: usize>(
  node_id: NodeID<N>, addr: SocketAddr, service: fn(NodeID<N>, SocketAddr, Message<N>) -> Option<Message<N>>,
) -> Result<(SocketAddr, JoinHandle<Result<()>>)> {
  let socket = UdpSocket::bind(addr).await.unwrap();
  let addr = socket.local_addr().unwrap();
  let handler = tokio::spawn(async move {
    let mut buf = [0u8; 0xFFFF];
    let (len, addr) = socket.recv_from(&mut buf).await.unwrap();
    let msg = Message::from_bytes(&buf[..len]).unwrap();
    if let Some(msg) = service(node_id, addr, msg) {
      socket.send_to(&msg.to_bytes(), addr).await.map_err(Error::IO).map(|_| ())
    } else {
      Ok(())
    }
  });
  Ok((addr, handler))
}

fn nonce<const N: usize>(msg: &Message<N>) -> &Key<N> {
  match msg {
    Message::Ping(_, nonce) => nonce,
    Message::Pong(_, nonce) => nonce,
    Message::FindNode(_, nonce, _) => nonce,
    Message::FoundNode(_, nonce, _) => nonce,
    Message::AppMessage(_, nonce, _, _) => nonce,
    Message::AppResponse(_, nonce, _) => nonce,
  }
}
