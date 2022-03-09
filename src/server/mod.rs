use crate::{AppCode, Contact, Error, Key, Message, NodeID, Result, RoutingTable};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::future::Future;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;
use tokio::{select, time};

#[cfg(test)]
mod test;

#[derive(Debug)]
struct Command<const N: usize> {
  deadline: Instant,
  opt: CommandOpt<N>,
}

#[derive(Debug)]
enum CommandOpt<const N: usize> {
  Join(Vec<SocketAddr>, Sender<Result<Vec<Contact<N>>>>),
  FindNode(NodeID<N>, Sender<Result<Vec<Contact<N>>>>),
  AppCall(Contact<N>, Key<N>, Vec<u8>, Sender<Result<()>>),
  Shutdown(Sender<Result<()>>),
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct AppMsg<const N: usize> {
  pub source: Contact<N>,
  pub key: Key<N>,
  pub value: Vec<u8>,
}

/// `Server` はそのスコープで Kademlia サーバの開始と終了を表します。
///
#[derive(Debug)]
pub struct Server<const N: usize> {
  /// この Kademlia ノードのコンタクト情報です。
  ///
  contact: Contact<N>,

  /// 実行中の `MessageLoop` にコマンドを送信するチャネル。
  ///
  tx: Sender<Command<N>>,
}

impl<const N: usize> Server<N> {
  /// 指定された ID を持つ Kademlia ノードを構築しサービスを開始します。
  ///
  pub fn new(
    id: NodeID<N>, bind_addr: SocketAddr, routing_table: RoutingTable<N>,
  ) -> impl Future<Output = Result<Server<N>>> {
    Self::with(id, bind_addr, routing_table, None, 2.0)
  }

  /// 指定された ID を持つ Kademlia ノードを構築しサービスを開始します。
  ///
  pub fn with_app_channel(
    id: NodeID<N>, bind_addr: SocketAddr, routing_table: RoutingTable<N>, app_tx: Sender<AppMsg<N>>,
  ) -> impl Future<Output = Result<Server<N>>> {
    Self::with(id, bind_addr, routing_table, Some(app_tx), 2.0)
  }

  /// 指定された ID を持つ Kademlia ノードを構築しサービスを開始します。
  ///
  async fn with(
    id: NodeID<N>, bind_addr: SocketAddr, mut routing_table: RoutingTable<N>, app_tx: Option<Sender<AppMsg<N>>>,
    purge_tick: f64,
  ) -> Result<Server<N>> {
    // bind UDP socket on the specified address
    let socket = UdpSocket::bind(bind_addr).await?;
    let addr = socket.local_addr()?;

    // 自身のコンタクト情報を追加しておく
    routing_table.observed(Contact { id, address: addr });

    // bind UDP socket on the specified address
    let (tx, rx) = channel(64);
    let sessions = Session::new();
    let mut runner = MessageLoop { id, address: addr, routing_table, sessions };
    let purge_tick = Duration::from_secs_f64(purge_tick);
    tokio::spawn(async move {
      if let Err(err) = runner.start(socket, rx, app_tx, purge_tick).await {
        log::error!("{:?}", err);
      }
    });

    let contact = Contact { id, address: addr };
    Ok(Server { contact, tx })
  }

  /// この Kademlia サーバのノード ID を参照します。
  ///
  pub fn id(&self) -> &NodeID<N> {
    &self.contact.id
  }

  /// この Kademlia ノードが使用しているアドレスを参照します。
  ///
  pub fn bind_address(&self) -> &SocketAddr {
    &self.contact.address
  }

  /// この Kademlia ノードのコンタクト情報を参照します。
  ///
  pub fn contact(&self) -> &Contact<N> {
    &self.contact
  }

  /// 指定されたブートストラップノードのアドレスを起点に、このノード自身を検索することで経路中継するノードにこのノードの
  /// コンタクト情報をキャッシュさせます。
  ///
  /// いずれのブートストラップノードからの検索もタイムアウトした場合に [`Timeout`](Error::Timeout) を返します。これは
  /// ブートストラップノードが応答しなかったケースだけではなく、経路を中継するいずれかのノードが応答しなかったケースも
  /// 含みます。
  ///
  /// # Return
  /// - `Vec<Contact<N>>` - 1 つ以上のブートストラップノードからこのサーバの検索が成功した場合、このサーバに近い順の別の
  ///   ノードのコンタクト情報。
  ///
  /// # Errors
  /// - [`Timeout`](Error::Timeout) - すべてのブートストラップノードからの応答がタイムアウトした。
  /// - [`Routing`](Error::Rouoting) - 指定されたブートストラップアドレスが空の場合、またはノード探索に失敗した場合。
  /// - [`Shutdown`](Error::Shutdown) - サーバはすでに[シャットダウン](Server::shutdown())されている。
  ///
  pub async fn join(&mut self, bootstrap_addrs: &[SocketAddr], timeout: Duration) -> Result<Vec<Contact<N>>> {
    if bootstrap_addrs.is_empty() {
      return Err(Error::Routing { message: String::from("The addresses of the Bootstrap node are empty") });
    }
    let (tx, rx) = channel(bootstrap_addrs.len());
    Self::command("join", CommandOpt::Join(bootstrap_addrs.to_vec(), tx), timeout, &mut self.tx, rx).await
  }

  pub async fn find_node(&mut self, id: NodeID<N>, timeout: Duration) -> Result<Vec<Contact<N>>> {
    let (tx, rx) = channel(1);
    Self::command("find_node", CommandOpt::FindNode(id, tx), timeout, &mut self.tx, rx).await
  }

  pub async fn app_call(&mut self, peer: Contact<N>, key: Key<N>, value: Vec<u8>, timeout: Duration) -> Result<()> {
    let (tx, rx) = channel(1);
    Self::command("find_node", CommandOpt::AppCall(peer, key, value, tx), timeout, &mut self.tx, rx).await
  }

  /// このサーバをシャットダウンしメッセージループを終了します。このメソッドの正常終了によって使用していた UDP ソケットが
  /// 開放されることを保証します (つまり、すぐに同一のポート番号で UDP ソケットを開くことができます)。シャットダウンされた
  /// サーバに [`FIND_NODE`](Server::find_node()) などを実行するとエラーが発生します。
  ///
  /// シャットダウンを行うことなくサーバが [Drop](Drop::drop()) されても UDP ソケットは正しく開放されますが、この場合、
  /// UDP ソケットのクローズは非同期で行われるため、すぐにポートが再利用できない可能性があります。
  ///
  pub async fn shutdown(mut self) -> Result<()> {
    let (tx, rx) = channel(1);
    match Self::command("shutdown", CommandOpt::Shutdown(tx), Duration::from_secs(5 * 60 * 60), &mut self.tx, rx).await
    {
      Err(Error::Shutdown { .. }) => Ok(()),
      result => result,
    }
  }

  async fn command<R>(
    name: &'static str, opt: CommandOpt<N>, timeout: Duration, tx: &mut Sender<Command<N>>, mut rx: Receiver<Result<R>>,
  ) -> Result<R> {
    let deadline = Instant::now() + timeout;
    let cmd = Command { deadline, opt };
    tx.send(cmd).await.map_err(|_| Error::server_has_been_shutdown())?;
    match time::timeout_at(deadline, rx.recv()).await {
      Ok(Some(result)) => result,
      Ok(None) => Err(Error::server_has_been_shutdown()),
      Err(_) => Err(Error::Timeout(name, timeout)),
    }
  }
}

impl<const N: usize> Display for Server<N> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}@{}", self.id(), self.bind_address())
  }
}

impl<const N: usize> Drop for Server<N> {
  fn drop(&mut self) {
    log::debug!("{}: server {} terminated", self.id(), self.bind_address());
  }
}

/// 独立したスレッドで実行される Kademlia サーバのメッセージループです。
/// メッセージの送受信、要求に対する応答、応答のためのセッション管理を行います。
///
struct MessageLoop<const N: usize> {
  id: NodeID<N>,
  address: SocketAddr,
  routing_table: RoutingTable<N>,

  /// 進行中のタスク状態を保持しておくためのセッション。
  ///
  sessions: Session<N>,
}

impl<const N: usize> MessageLoop<N> {
  /// `FIND_NODE` 要求の最大ホップ回数。
  ///
  const MAX_HOPS: usize = N * 8;

  /// `PING` 応答の最大待ち時間。
  ///
  const PING_TIMEOUT: Duration = Duration::from_secs(3);

  /// UDP パケットの送受信処理を開始します。
  ///
  /// # Parameters
  ///
  /// * `socket` - UDP ソケット
  /// * `cmd_rx` - コマンド受信のためのチャネル
  /// * `app_tx` - 受信したアプリケーションメッセージを通知するチャネル
  /// * `purge_tick` - セッション中のタイムアウトタスクを削除する間隔
  ///
  pub async fn start(
    &mut self, mut socket: UdpSocket, mut cmd_rx: Receiver<Command<N>>, mut app_tx: Option<Sender<AppMsg<N>>>,
    purge_tick: Duration,
  ) -> Result<()> {
    let mut interval = time::interval(purge_tick);
    let mut buffer = [0u8; 0xFFFF];
    let mut is_first_purge = true;
    let tx = 'polling: loop {
      select! {
        _ = interval.tick() => {
          // 一定時間おきに実行する処理 (直ちに実行される初回は無視; 実行が無駄であるのと、テストで purge を制御したいため)
          if ! is_first_purge {
            self.purge(&mut socket).await?;
          } else {
            is_first_purge = false;
          }
        }
        cmd = cmd_rx.recv() => {
          // コマンドが到達したときの処理
          if let Some(cmd) = cmd {
            if let Some(tx) = self.command(&mut socket, cmd).await? {
              self.debug("shutdown order received; start shutdown process");
              break 'polling Some(tx);
            }
          } else {
            self.debug("command channel closed; start shutdown process");
            break 'polling None;
          }
        }
        result = socket.recv_from(&mut buffer) => {
          // メッセージを受信したときの処理
          match result {
            Ok((len, addr)) => {
              self.react(&mut socket, addr, &buffer[..len], &mut app_tx).await?;
            }
            Err(err) => {
              log::error!("{}: {:?}", self.id, err);
            },
          }
        }
      }
    };
    drop(socket);
    if let Some(tx) = tx {
      let _ = tx.send(Ok(())).await;
    }
    self.debug("shutdown");
    Ok(())
  }

  /// 指定されたコマンドを実行します。
  ///
  async fn command(&mut self, socket: &mut UdpSocket, cmd: Command<N>) -> Result<Option<Sender<Result<()>>>> {
    log::debug!("{}: CMD: {:?}", self.id, cmd);
    match cmd.opt {
      CommandOpt::Join(bootstrap_addrs, tx) => {
        for addr in bootstrap_addrs {
          let tx = tx.clone();
          let nonce = self.sessions.begin_find_node(cmd.deadline, &self.id, 0, tx);
          let find_node = Message::<N>::FindNode(self.id, nonce, self.id);
          self.send(socket, find_node, &addr).await?;
        }
      }
      CommandOpt::FindNode(target_id, tx) => {
        let contacts = self.routing_table.order_of_distance_from(&target_id);
        assert!(!contacts.is_empty());
        if let Some(contact) = contacts.first() {
          let nonce = self.sessions.begin_find_node(cmd.deadline, &target_id, 0, tx);
          let find_node = Message::<N>::FindNode(self.id, nonce, target_id);
          self.send(socket, find_node, &contact.address).await?;
        }
      }
      CommandOpt::AppCall(contact, key, value, tx) => {
        let nonce = self.sessions.begin_app_call(cmd.deadline, key, value.clone(), 0, tx);
        let app_call = Message::<N>::AppMessage(self.id, nonce, key, value);
        self.send(socket, app_call, &contact.address).await?;
      }
      CommandOpt::Shutdown(tx) => {
        return Ok(Some(tx));
      }
    }
    Ok(None)
  }

  /// 指定されたバッファに格納されている受信メッセージに対する処理を行います。
  ///
  async fn react(
    &mut self, socket: &mut UdpSocket, addr: SocketAddr, buf: &[u8], app_tx: &mut Option<Sender<AppMsg<N>>>,
  ) -> Result<()> {
    let msg = Message::<N>::from_bytes(buf)?;
    self.msg_log(false, &addr, &msg);
    let peer_id = match msg {
      Message::Ping(peer_id, nonce) => {
        self.react_ping(socket, addr, nonce).await?;
        peer_id
      }
      Message::Pong(peer_id, nonce) => {
        self.react_pong(nonce).await?;
        peer_id
      }
      Message::FindNode(peer_id, nonce, target_id) => {
        self.react_find_node(socket, addr, nonce, target_id).await?;
        peer_id
      }
      Message::FoundNode(peer_id, nonce, contacts) => {
        self.react_found_node(socket, &addr, peer_id, nonce, contacts).await?;
        peer_id
      }
      Message::AppMessage(peer_id, nonce, key, value) => {
        let msg = AppMsg { source: Contact::new(peer_id, addr), key, value };
        self.react_app_message(socket, nonce, msg, app_tx).await?;
        peer_id
      }
      Message::AppResponse(peer_id, nonce, code) => {
        self.react_app_response(peer_id, nonce, code).await?;
        peer_id
      }
    };

    // 観測したピアのコンタクト情報を保存
    let contact = Contact { id: peer_id, address: addr };
    if let Some(target) = self.routing_table.observed(contact.clone()) {
      // k-bucket がいっぱいの場合は PING を送信し入れ替え動作を実行する
      let deadline = Instant::now() + Self::PING_TIMEOUT;
      if let Some(nonce) = self.sessions.begin_ping(deadline, target.id, contact) {
        let ping = Message::<N>::Ping(self.id, nonce);
        self.send(socket, ping, &target.address).await?;
      }
    }
    Ok(())
  }

  /// `PING` 要求に応答します。
  ///
  async fn react_ping(&mut self, socket: &mut UdpSocket, addr: SocketAddr, nonce: Key<N>) -> Result<()> {
    let pong = Message::<N>::Pong(self.id, nonce);
    self.send(socket, pong, &addr).await?;
    Ok(())
  }

  /// `PING` 応答
  ///
  async fn react_pong(&mut self, nonce: Key<N>) -> Result<()> {
    // PONG 応答を受けたら nonce に該当するタスクを削除する
    self.sessions.release(&nonce);
    Ok(())
  }

  /// `FIND_NODE` 要求の処理を行います。
  /// この呼出で行われる応答には、このノードのキャッシュに保存されているコンタクト情報が `target_id` に近い順で最大 `k` 件
  /// 含まれます。
  ///
  async fn react_find_node(
    &mut self, socket: &mut UdpSocket, addr: SocketAddr, nonce: Key<N>, target_id: NodeID<N>,
  ) -> Result<()> {
    // FIND_NODE を受けたら最も近いコンタクト情報を最大 k 件で応答する
    let contacts = self.routing_table.order_of_distance_from(&target_id);
    let found_node = Message::<N>::FoundNode(self.id, nonce, contacts);
    self.send(socket, found_node, &addr).await?;
    Ok(())
  }

  /// `FIND_NODE` 応答
  ///
  async fn react_found_node(
    &mut self, socket: &mut UdpSocket, addr: &SocketAddr, peer_id: NodeID<N>, nonce: Key<N>, contacts: Vec<Contact<N>>,
  ) -> Result<()> {
    // FIND_NODE 応答を受けたらさらにリダイレクトするかを判定する
    let peer = Contact::new(peer_id, *addr);
    match self.sessions.release(&nonce) {
      Some(Task(deadline, TaskSpecData::FindNode(task))) => {
        let FindNodeTask(_target_id, hop, tx) = &task;
        if Self::check_max_hop_has_not_been_reached(*hop, tx).await {
          self.react_found_node_for_find_node(socket, &peer, &nonce, contacts, deadline, task).await?;
        }
      }
      Some(Task(deadline, TaskSpecData::AppCall(task))) => {
        let AppCallTask(_key, _value, hop, tx) = &task;
        if Self::check_max_hop_has_not_been_reached(*hop, tx).await {
          self.react_found_node_for_app_call(socket, &peer, &nonce, contacts, deadline, task).await?;
        }
      }
      _ => {
        // すでにタイムアウトして purge されているか、不正な FIND_NODE 応答
        self.debug("find_node already timed out or is a invalid message");
      }
    }
    Ok(())
  }

  async fn react_found_node_for_find_node(
    &mut self, socket: &mut UdpSocket, peer: &Contact<N>, nonce: &Key<N>, contacts: Vec<Contact<N>>, deadline: Instant,
    task: FindNodeTask<N>,
  ) -> Result<()> {
    let FindNodeTask(target_id, hop, tx) = task;
    let contact = match Self::check_and_get_first_contact(peer, &target_id, &contacts, &tx).await {
      Some(contact) => contact,
      None => return Ok(()),
    };

    // 最も近いノードが問い合わせ先のノードだった場合、または FIND_NODE を送ったノードだった場合はここで終了
    if contact.id == peer.id || contact.id == target_id {
      self.debug(format!("found contact list: {:?}", contacts));
      if let Err(err) = tx.send(Ok(contacts)).await {
        self.debug(format!("contact list successfully retrieved but return channel closed; {:?}", err));
      }
      return Ok(());
    }

    // 時間制限を超過している場合は探索を中断
    if Instant::now() >= deadline {
      self.debug(format!(
        "find_node({}) for response {} received but has already been timed out in {} hop",
        target_id,
        nonce,
        hop + 1
      ));
      return Ok(());
    }

    // より近いノードに FIND_NODE を送信する
    log::info!("{}: redirect: {:?}", self.id, contact);
    let nonce = self.sessions.begin_find_node(deadline, &target_id, hop + 1, tx);
    let find_node = Message::<N>::FindNode(self.id, nonce, target_id);
    self.send(socket, find_node, &contact.address).await?;
    Ok(())
  }

  async fn react_found_node_for_app_call(
    &mut self, socket: &mut UdpSocket, peer: &Contact<N>, nonce: &Key<N>, contacts: Vec<Contact<N>>, deadline: Instant,
    task: AppCallTask<N>,
  ) -> Result<()> {
    let AppCallTask(key, value, hop, tx) = task;
    let contact = match Self::check_and_get_first_contact(peer, &key, &contacts, &tx).await {
      Some(contact) => contact,
      None => return Ok(()),
    };

    if Instant::now() >= deadline {
      // 時間制限を超過している場合は探索を中断
      self.debug(format!(
        "app_call({}, {}) for response {} received but has already been timed out in {} hop",
        key,
        value.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(""),
        nonce,
        hop + 1
      ));
      return Ok(());
    }

    // より近いノードに APP_MESSAGE を送信する
    log::info!("{}: redirect: {:?}", self.id, contact);
    let nonce = self.sessions.begin_app_call(deadline, key, value.clone(), hop + 1, tx);
    let app_msg = Message::<N>::AppMessage(self.id, nonce, key, value);
    self.send(socket, app_msg, &contact.address).await?;
    Ok(())
  }

  /// 最大ホップ数に達していないことを確認 (中継するノードの ID が変わって意図せず遠回りすることになったケース)
  ///
  async fn check_max_hop_has_not_been_reached<T: Debug>(hop: usize, tx: &Sender<Result<T>>) -> bool {
    if hop > MessageLoop::<N>::MAX_HOPS {
      let message = format!("exceeds max hops: {}/{}", hop, MessageLoop::<N>::MAX_HOPS);
      let _ = tx.send(Err(Error::Routing { message })).await; // 処理がタイムアウトしている場合は無視
      false
    } else {
      true
    }
  }

  /// コンタクト情報の中から最も近いコンタクト情報を取得します。リストが空であったりソートされていない場合は、中継した
  /// ノードの応答として `tx` にエラーを送信し `None` を返します。
  ///
  async fn check_and_get_first_contact<T>(
    peer: &Contact<N>, key: &Key<N>, contacts: &[Contact<N>], tx: &Sender<Result<T>>,
  ) -> Option<Contact<N>> {
    if let Some(contact) = contacts.first() {
      for i in 0..contacts.len() - 1 {
        if (&contacts[i].id ^ key) >= (&contacts[i + 1].id ^ key) {
          log::debug!("contacts aren't sorted: {:?}", contacts);
          let message = "incorrect response: contacts are not sorted by distance";
          let _ = tx.send(Err(Error::Routing { message: message.to_string() })).await;
          return None;
        }
      }
      Some(contact.clone())
    } else {
      // ピアの問題: 少なくとも自身のコンタクト情報は入っていなければならない
      let msg = format!("incorrect response: empty contacts was returned from node {}", peer);
      let _ = tx.send(Err(Error::Routing { message: msg })).await;
      None
    }
  }

  /// `APP_MESSAGE` 要求に応答します。
  ///
  async fn react_app_message(
    &mut self, socket: &mut UdpSocket, nonce: Key<N>, msg: AppMsg<N>, app_tx: &mut Option<Sender<AppMsg<N>>>,
  ) -> Result<()> {
    let contacts = self.routing_table.order_of_distance_from(&msg.key);
    let nearest = contacts.first().unwrap();

    // より近いノードが見つかった場合はリダイレクトする
    if nearest.id != self.id {
      let found_node = Message::<N>::FoundNode(self.id, nonce, contacts);
      self.send(socket, found_node, &msg.source.address).await?;
      return Ok(());
    }

    // アプリ呼び出し
    if let Some(tx) = app_tx {
      tx.send(msg.clone()).await.map_err(Error::client_has_been_dropped)?;
      let app_response = Message::<N>::AppResponse(self.id, nonce, Message::<N>::APP_RESPONSE_OK);
      self.send(socket, app_response, &msg.source.address).await?;
    } else {
      let app_response = Message::<N>::AppResponse(self.id, nonce, Message::<N>::APP_RESPONSE_NOT_FOUND);
      self.send(socket, app_response, &msg.source.address).await?;
    }
    Ok(())
  }

  /// `APP_RESPONSE` 応答の処理を行います。
  ///
  async fn react_app_response(&mut self, peer_id: NodeID<N>, nonce: Key<N>, code: AppCode) -> Result<()> {
    if let Some(Task(deadline, spec)) = self.sessions.get(&nonce) {
      if Instant::now() >= *deadline {
        // 時間制限を超過している場合は何もしない
        self.debug(format!("app_call() response for {} received but has already timed out", nonce));
      } else if let TaskSpecData::AppCall(task) = spec {
        // 呼び出しの応答を通知
        let AppCallTask(_key, _value, _hop, tx) = &task;
        tx.send(match code {
          Message::<N>::APP_RESPONSE_OK => Ok(()),
          Message::<N>::APP_RESPONSE_NOT_FOUND => Err(Error::AppNotFound { peer: peer_id.to_string() }),
          _ => Err(Error::UnknowAppResponse(code)),
        })
        .await
        .map_err(Error::client_has_been_dropped_oneshot)?;
      } else {
        // APP_CALL ではない要求に対して APP_CALL の応答が返ってきたか、nonce を引き当てた不正な応答 (キューから削除しない)
        self.debug("app_call response received but no corresponding request exists");
        return Ok(());
      }
    } else {
      // すでにタイムアウトして purge されているか、不正な APP_MESSAGE 応答
      self.debug("app_call already timed out or is a invalid message");
    }
    self.sessions.release(&nonce);
    Ok(())
  }

  /// 応答待ちでタイムアウトしているタスクを除去しタスクの終了処理を行います。
  ///
  async fn purge(&mut self, socket: &mut UdpSocket) -> Result<()> {
    self.debug("purge()");

    // 応答待ちでタイムアウトしたジョブを除去
    let tasks = self.sessions.purge();
    if !tasks.is_empty() {
      log::warn!("{}: task purged: {}", self.id, tasks.len());
    }

    // PING 確認に失敗したコンタクト情報を置き換え
    for task in tasks {
      if let TaskSpecData::AliveCheck(target, replace) = task.1 {
        self.routing_table.replace(&target, replace);
      }
    }

    // リフレッシュの必要なルーティングテーブルのランダムなノード ID を取得して検索を実行
    let deadline = Instant::now() + Duration::from_secs((N * 8) as u64);
    let ids = self.routing_table.refresh();
    for target_id in ids {
      let (tx, _) = channel(1);
      let nonce = self.sessions.begin_find_node(deadline, &target_id, 0, tx);
      let find_node = Message::<N>::FindNode(self.id, nonce, target_id);
      let addr = self.routing_table.order_of_distance_from(&target_id).first().unwrap().address;
      self.send(socket, find_node, &addr).await?;
    }
    Ok(())
  }

  /// 指定された UDP ソケットにメッセージを送信します。
  ///
  async fn send(&self, socket: &UdpSocket, msg: Message<N>, addr: &SocketAddr) -> Result<usize> {
    self.msg_log(true, addr, &msg);
    Ok(socket.send_to(&msg.to_bytes(), &addr).await?)
  }

  fn msg_log(&self, send: bool, addr: &SocketAddr, msg: &Message<N>) {
    if log::log_enabled!(log::Level::Debug) {
      let sign = if send { "<<" } else { ">>" };
      let msg = match msg {
        Message::Ping(peer_id, nonce) => format!("Ping({}, {})", peer_id, nonce),
        Message::Pong(peer_id, nonce) => format!("Pong({}, {})", peer_id, nonce),
        Message::FindNode(peer_id, nonce, target_id) => format!("FindNode({}, {}, {})", peer_id, nonce, target_id),
        Message::FoundNode(peer_id, nonce, contacts) => {
          let contacts = contacts.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
          format!("FoundNode({}, {}, [{}])", peer_id, nonce, contacts)
        }
        Message::AppMessage(peer_id, nonce, key, value) => {
          let hex = value.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join("");
          let hex = if value.len() > 24 { format!("{}...", &hex[0..24]) } else { hex };
          let hex = format!("{}({}B)", hex, value.len());
          format!("AppMessage({}, {}, {}, {})", peer_id, nonce, key, hex)
        }
        Message::AppResponse(peer_id, nonce, code) => format!("AppRespopnse({}, {}, {})", peer_id, nonce, code),
      };
      self.debug(format!("{} {} {}", addr, sign, msg));
    }
  }

  /// このノードを含めたデバッグログを出力します。
  ///
  fn debug<T: Display>(&self, msg: T) {
    log::debug!("{}@{}: {}", self.id, self.address, msg);
  }
}

/// 進行中の [`Task`] を保存するセッションです。
/// タイムアウトした順に [`Task`] を取り出すことができます。
///
struct Session<const N: usize> {
  alive_checkings: HashSet<NodeID<N>>,
  sessions: HashMap<Key<N>, (Instant, Task<N>)>,
  queue: BTreeMap<Instant, HashSet<Key<N>>>,
}

impl<const N: usize> Session<N> {
  pub fn new() -> Self {
    Session { alive_checkings: HashSet::new(), sessions: HashMap::new(), queue: BTreeMap::new() }
  }

  /// `FIND_NODE` タスクを開始します。
  ///
  /// # Return
  ///
  /// * `Key<N>` - セッションに登録されたユニークな nonce 値。
  ///
  pub fn begin_find_node(
    &mut self, deadline: Instant, target_id: &NodeID<N>, hop: usize, tx: Sender<Result<Vec<Contact<N>>>>,
  ) -> Key<N> {
    let task = Task(deadline, TaskSpecData::FindNode(FindNodeTask(*target_id, hop, tx)));
    self.register(deadline, task)
  }

  /// `PING` タスクを開始します。指定された `target_id` に対してすでに `PING` タスクが進行中の場合はタスクを登録しないで
  /// `None` を返します。
  ///
  /// # Return
  ///
  /// * `Key<N>` - セッションに登録されたユニークな nonce 値。すでに進行中の場合は `None`。
  ///
  pub fn begin_ping(&mut self, deadline: Instant, target_id: NodeID<N>, replace: Contact<N>) -> Option<Key<N>> {
    if self.alive_checkings.contains(&target_id) {
      // すでに AliveCheck が実行中であれば何もしない
      None
    } else {
      self.alive_checkings.insert(target_id);
      let task = Task(deadline, TaskSpecData::AliveCheck(target_id, replace));
      Some(self.register(deadline, task))
    }
  }

  pub fn begin_app_call(
    &mut self, deadline: Instant, key: Key<N>, value: Vec<u8>, hop: usize, tx: Sender<Result<()>>,
  ) -> Key<N> {
    let task = Task(deadline, TaskSpecData::AppCall(AppCallTask(key, value, hop, tx)));
    self.register(deadline, task)
  }

  /// 指定されたセッション情報を登録します。
  /// このセッションの nonce を返します。
  ///
  fn register(&mut self, deadline: Instant, task: Task<N>) -> Key<N> {
    // ユニークな nonce 値の取得
    let mut nonce = Key::random();
    while self.sessions.contains_key(&nonce) {
      nonce = Key::random();
    }

    self.sessions.insert(nonce, (deadline, task));

    match self.queue.get_mut(&deadline) {
      Some(nonces) => {
        nonces.insert(nonce);
      }
      None => {
        self.queue.insert(deadline, HashSet::from([nonce]));
      }
    }
    nonce
  }

  /// 指定された nonce 値を持つタスクを参照します。
  ///
  pub fn get(&self, nonce: &Key<N>) -> Option<&Task<N>> {
    self.sessions.get(nonce).map(|(_, task)| task)
  }

  /// 指定された nonce 値を持つタスクを削除して返します。
  ///
  pub fn release(&mut self, nonce: &Key<N>) -> Option<Task<N>> {
    self.sessions.remove(nonce).map(|(tm, session)| {
      if let Some(nonces) = self.queue.get_mut(&tm) {
        nonces.remove(nonce);
        if nonces.is_empty() {
          self.queue.remove(&tm);
        }
      }
      session
    })
  }

  /// タイムアウトしているセッションをキューから削除して返します。
  ///
  pub fn purge(&mut self) -> Vec<Task<N>> {
    // タイムアウトしている時刻と nonce を参照
    let now = Instant::now();
    let mut tms = Vec::with_capacity(16);
    let mut nonces = Vec::with_capacity(16);
    for (deadline, ncs) in self.queue.iter() {
      if deadline > &now {
        break;
      }
      tms.push(*deadline);
      for nonce in ncs.iter() {
        nonces.push(*nonce);
      }
    }
    for tm in tms {
      self.queue.remove(&tm);
    }
    let mut sessions = Vec::with_capacity(nonces.len());
    for nonce in &nonces {
      if let Some((_, session)) = self.sessions.remove(nonce) {
        sessions.push(session);
      }
    }
    sessions
  }
}

/// 現在進行中のタスク。
///
/// # Parameters
/// 1. タスクの期限
/// 2. タスク固有のデータ
///
struct Task<const N: usize>(Instant, TaskSpecData<N>);

enum TaskSpecData<const N: usize> {
  AliveCheck(NodeID<N>, Contact<N>),
  FindNode(FindNodeTask<N>),
  AppCall(AppCallTask<N>),
}

struct FindNodeTask<const N: usize>(NodeID<N>, usize, Sender<Result<Vec<Contact<N>>>>);
struct AppCallTask<const N: usize>(Key<N>, Vec<u8>, usize, Sender<Result<()>>);
