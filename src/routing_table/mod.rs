use std::{collections::BTreeMap, time::Duration};

use tokio::time::Instant;

use crate::{Contact, NodeID, XorDistance};

#[cfg(test)]
pub mod test;

/// Kademlia ノードが持つルーティングテーブルです。
/// 8 × `N` bit のノード ID に対して[コンタクト情報](Contact)をマップします。
///
/// ノード ID のビットに対する二分木 (トライ木) を構成します。
/// コンタクト情報は二分木の葉に位置する k-bucket に保存されます。
///
#[derive(Debug)]
pub struct RoutingTable<const N: usize> {
  id: NodeID<N>,
  prefix: NodeID<N>,
  prefix_bit_length: usize,
  node: Box<Node<N>>,
  k: usize,
  /// このルーティングテーブルのエントリのいずれかが最後に観測された時点
  /// このルーティングテーブルが古くなる時点
  expires: Instant,
}

impl<const N: usize> RoutingTable<N> {
  /// k-bucket 内のエントリが最後に観測されてから[更新対象](RoutingTable::refresh())となるまでの時間です。
  ///
  pub const TIMEOUT_REFRESH: Duration = Duration::from_secs(60 * 60);

  /// 指定されたノード ID に対するルーティングテーブルを構築します。
  ///
  /// # Parameters
  ///
  /// * `id` - このルーティングテーブルを保有するノードの ID
  /// * `k` - k-bucket に保存する最大数
  ///
  pub fn new(id: NodeID<N>, k: usize) -> Self {
    Self {
      id,
      prefix: NodeID::from_bytes(&[0u8; N]),
      prefix_bit_length: 0,
      node: Box::new(Node::KBucket(Vec::new())),
      k,
      expires: Instant::now() + RoutingTable::<N>::TIMEOUT_REFRESH,
    }
  }

  /// このルーティングテーブルを保有するノード ID を参照します。
  ///
  pub fn node_id(&self) -> &NodeID<N> {
    &self.id
  }

  /// このルーティングテーブルが保持する k-bucket に格納可能なコンタクト情報の数 (k 値) を参照します。
  ///
  pub fn k(&self) -> usize {
    self.k
  }

  /// 指定されたノード ID のコンタクト情報をルーティングテーブル内から参照します。
  ///
  /// # Paramteters
  ///
  /// * `id` - 参照するコンタクト情報のノード ID
  ///
  /// # Return
  ///
  /// * `Option<&Contact<N>>` - `id` に対応するコンタクト情報、または `id` に対応するコンタクト情報がキャッシュされて
  ///   いない場合は `None`
  ///
  pub fn get_contact(&self, id: &NodeID<N>) -> Option<&Contact<N>> {
    match self.node.as_ref() {
      Node::Branch(b0, b1) => {
        let position = (N * 8) - self.prefix_bit_length - 1;
        let branch = if id.get_bit(position) == 0 { b0 } else { b1 };
        branch.get_contact(id)
      }
      Node::KBucket(contacts) => contacts.iter().find(|c| &c.id == id),
    }
  }

  /// 指定されたコンタクト先を発見したときに呼び出します。ルーティングテーブルは必要に応じてコンタクト先を k-bucket に
  /// 保存します。
  ///
  /// コンタクト情報に対する k-bucket がいっぱいだった場合、メソッドは入れ替え候補のノードのコンタクト情報を返します。
  /// メソッドの呼び出し元は返値のノードに `PING` を実行し、応答がなかった場合は [`RoutingTable::replace()`] メソッドで
  /// コンタクト情報の入れ替えを行う必要があります。
  ///
  /// # Parameters
  ///
  /// * `contact` - 観測されたコンタクト情報
  ///
  /// # Return
  ///
  /// * `Option<Contact<N>>` - 入れ替え候補のノードのコンタクト情報
  ///
  pub fn observed(&mut self, contact: Contact<N>) -> Option<Contact<N>> {
    debug_assert!(self.prefix.prefix_match_bits(&contact.id) >= self.prefix_bit_length);

    let position = (N * 8) - self.prefix_bit_length - 1;
    match self.node.as_mut() {
      Node::Branch(ref mut b0, ref mut b1) => {
        // 分割されている場合は再帰的に実行
        let branch = if contact.id.get_bit(position) == 0 { b0 } else { b1 };
        branch.observed(contact)
      }
      Node::KBucket(ref mut contacts) => {
        self.expires = Instant::now() + RoutingTable::<N>::TIMEOUT_REFRESH;

        // すでに存在していれば一番うしろに移動する (アドレスが変わっている可能性を考慮して最新のコンタクト情報で更新する)
        for i in 0..contacts.len() {
          if contacts[i].id == contact.id {
            contacts.remove(i);
            contacts.push(contact);
            return None;
          }
        }

        // k-bucket が k 個に達していなければ単に追加するのみ
        if contacts.len() < self.k {
          contacts.push(contact);
          return None;
        }

        // このノード ID が k-bucket に含まれていれば分割して再試行
        if self.id.prefix_match_bits(&contact.id) >= self.prefix_bit_length {
          let (c0, c1) = Self::split(contacts, position);
          let rt0 = self.delve(0, c0);
          let rt1 = self.delve(1, c1);
          let node = Node::Branch(rt0, rt1);
          self.node = Box::new(node);
          return self.observed(contact);
        }

        // このノード ID が含まれていなければ k-bucket 内の最後に確認されたノード (つまり末尾のノード) に PING 要求
        contacts.last().cloned()
      }
    }
  }

  /// 指定された ID のコンタクト情報が存在する場合にそれを削除して末尾に新しいコンタクト情報 `newbie` を追加します。
  /// これは `id` のノードが `PING` に応答しなかったときの標準動作です。
  ///
  /// `id` に該当するコンタクト情報が存在しない場合は何も行いません。
  /// `newbie` がこのルーティングテーブルに含まれない場合、つまり、`PING` を発行してから未応答を検出するまでに k-bucket の
  /// 分割が起きた場合には、`PING` に応答しなかった `id` のコンタクト情報を削除するのみで新しいコンタクト情報の追加は
  /// 行われません。
  ///
  /// # Parameters
  ///
  /// * `id` - 削除するコンタクト情報の ID
  /// * `newbie` - `id` のコンタクト情報と置き換える新しいコンタクト情報
  ///
  /// # Return
  ///
  /// 新しいコンタクト情報 `newbie` が追加されたとき true。
  ///
  pub fn replace(&mut self, id: &NodeID<N>, newbie: Contact<N>) -> bool {
    match self.node.as_mut() {
      Node::Branch(ref mut b0, ref mut b1) => {
        // 分割されている場合は再帰的に実行
        let position = (N * 8) - self.prefix_bit_length - 1;
        let branch = if id.get_bit(position) == 0 { b0 } else { b1 };
        branch.replace(id, newbie)
      }
      Node::KBucket(ref mut contacts) => {
        // コンタクト情報の中から該当する ID を削除
        for i in 0..contacts.len() {
          if &contacts[i].id == id {
            let old = contacts.remove(i);
            debug_assert!(contacts.len() < self.k, "{} < {}", contacts.len(), self.k);
            if self.prefix.prefix_matches(&newbie.id, self.prefix_bit_length) {
              log::debug!("entry replaced: {:?} -> {:?}", old, newbie);
              contacts.push(newbie);
              return true;
            } else {
              log::debug!("entry removed: {:?}", old);
            }
            break;
          }
        }
        false
      }
    }
  }

  /// ルーティングテーブルのリフレッシュ動作のためのノード ID を参照します。
  ///
  /// 一定時間検索のなかった (つまり観測のなかった) ルーティングテーブルをリフレッシュするために、そのルーティングテーブル
  /// の範囲に属するランダムなノード ID を検索しコンタクト情報を強制的に更新する必要があります。このメソッドはリフレッシュ
  /// 検索を行う必要のあるノード ID を参照します。
  ///
  pub fn refresh(&self) -> Vec<NodeID<N>> {
    fn rec<const N: usize>(rt: &RoutingTable<N>, ids: &mut Vec<NodeID<N>>) {
      match rt.node.as_ref() {
        Node::Branch(b0, b1) => {
          // 分割されている場合は再帰的に実行
          rec(b0, ids);
          rec(b1, ids);
        }
        Node::KBucket { .. } => {
          // 一定時間検索がなければランダムなノード ID を候補に追加
          if rt.expires < Instant::now() {
            let mut random_id = rt.prefix;
            let mut rand = rand::thread_rng();
            let position = (N * 8) - rt.prefix_bit_length - 1;
            random_id.set_random_bits_lower_than(position, &mut rand);
            ids.push(random_id);
          }
        }
      }
    }
    let mut ids = Vec::new();
    rec(self, &mut ids);
    ids
  }

  /// このルーティングテーブルに保存されているコンタクト情報の中から、指定されたノード ID に近い順で最大
  /// [k](RoutingTable::k()) 個を参照します。
  ///
  pub fn order_of_distance_from(&self, id: &NodeID<N>) -> Vec<Contact<N>> {
    let mut buffer = BTreeMap::new(); // BTreeMap is used as sorted collection
    self.nearest(id, &mut buffer, self.k);
    buffer.into_iter().map(|(_, c)| c).collect()
  }

  /// 最大で指定されたリターンバッファのキャパシティまでのコンタクト情報を格納します。
  /// `buffer` には XOR 距離の近い順にコンタクト情報が保存されます。
  ///
  fn nearest(&self, id: &NodeID<N>, buffer: &mut BTreeMap<XorDistance<N>, Contact<N>>, max: usize) {
    match self.node.as_ref() {
      Node::Branch(rt0, rt1) => {
        let position = (N * 8) - self.prefix_bit_length - 1;
        let (rt0, rt1) = if id.get_bit(position) == 0 { (rt0, rt1) } else { (rt1, rt0) };
        rt0.nearest(id, buffer, max);
        if buffer.len() < max {
          rt1.nearest(id, buffer, max)
        }
      }
      Node::KBucket(contacts) => {
        assert_ne!(max, buffer.len());
        let size = max - buffer.len();
        let mut b = BTreeMap::new();
        for contact in contacts.iter() {
          let distance = &contact.id ^ id;
          b.insert(distance, contact.clone());
        }
        for (distance, contact) in b.into_iter().take(size) {
          buffer.insert(distance, contact);
        }
      }
    }
  }

  /// ルーティングテーブル分割のためにこのルーティングテーブルより 1-bit 深いノードを構築します。
  ///
  fn delve(&self, bit: usize, contacts: Vec<Contact<N>>) -> Self {
    debug_assert!(self.prefix_bit_length < N * 8, "{:?}", self);

    let mut prefix = self.prefix;
    prefix.set_bit((N * 8) - self.prefix_bit_length - 1, bit);
    Self {
      id: self.id,
      prefix,
      prefix_bit_length: self.prefix_bit_length + 1,
      node: Box::new(Node::KBucket(contacts)),
      k: self.k,
      expires: self.expires, // TODO: 正しくは contacts 内の最新観測時点になるはず
    }
  }

  /// 指定されたコンタクト情報の左から `prefix_bit` ビット目 (右から (N×8)-`prefix_bit`-1 ビット目) が 0 か 1 かで分割
  /// したリストを返します。
  ///
  fn split(contacts: &mut Vec<Contact<N>>, position: usize) -> (Vec<Contact<N>>, Vec<Contact<N>>) {
    debug_assert!(position < N * 8, "{} < {} × 8", position, N);
    let mut c0 = Vec::new();
    let mut c1 = Vec::new();
    while !contacts.is_empty() {
      let c = contacts.remove(0);
      let contacts = if c.id.get_bit(position) == 0 { &mut c0 } else { &mut c1 };
      contacts.push(c);
    }
    (c0, c1)
  }
}

#[derive(Debug)]
enum Node<const N: usize> {
  Branch(RoutingTable<N>, RoutingTable<N>),
  KBucket(Vec<Contact<N>>),
}
