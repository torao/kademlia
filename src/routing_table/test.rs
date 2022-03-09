use crate::{routing_table::Node, Contact, Key, NodeID, RoutingTable};
use rand::{prelude::SliceRandom, SeedableRng};
use rand_chacha::ChaCha12Rng;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::time::Instant;

#[test]
fn routing_table() {
  // プロパティ
  const N: usize = 2;
  const K: usize = 10;
  let id: NodeID<N> = Key([0, 0]);
  let mut rt = RoutingTable::new(id, K);
  assert_eq!(&id, rt.node_id());
  assert_eq!(K, rt.k());
  assert_eq!(1, depth(&rt));

  // 0x0000-0xFFFF まですべての空間に渡って観測されたコンタクト情報を追加
  let mut random = ChaCha12Rng::seed_from_u64(198482);
  let mut ids = (0..0xFFFFu16).collect::<Vec<_>>();
  ids.shuffle(&mut random);
  for i in ids {
    rt.observed(local_contact(&i.to_be_bytes(), i));
  }
  assert_eq!((N * 8) - (f64::log2(K as f64) as usize) + 1, depth(&rt));
}

#[test]
fn get_contact() {
  const N: usize = 2;
  const K: usize = 10;
  let id: NodeID<N> = Key::from_bytes(&0u16.to_be_bytes());
  let mut rt = RoutingTable::new(id, K);

  let mut rejected = HashSet::new();
  for i in 1u16..=0xFFFFu16 {
    let id = Key::from_bytes(&i.to_be_bytes());
    assert!(rt.get_contact(&id).is_none());
    if let Some(p) = rt.observed(local_contact(&i.to_be_bytes(), i)) {
      assert!(rt.get_contact(&p.id).is_some());
      assert!(rt.get_contact(&id).is_none());
      rejected.insert(i);
    } else {
      assert!(rt.get_contact(&id).is_some());
    }
  }

  for i in 1u16..=0xFFFFu16 {
    let id = Key::from_bytes(&i.to_be_bytes());
    assert_eq!(rejected.contains(&i), rt.get_contact(&id).is_none());
  }
}

#[test]
fn observed_append_in_sequential_only() {
  const N: usize = 2;
  const K: usize = 5;
  let id: NodeID<N> = Key([0, 0]);
  let mut rt = RoutingTable::new(id, K);
  assert_eq!(&id, rt.node_id());
  assert_eq!(K, rt.k());

  // 古いコンタクト情報から順に保持されていることを確認
  for i in 0..K {
    rt.observed(local_contact(&[i as u8, 0x00], i as u16));
  }
  if let Node::KBucket(contacts) = rt.node.as_ref() {
    assert_eq!(local_contact(&[0x00, 0x00], 0), contacts[0]);
    assert_eq!(local_contact(&[0x01, 0x00], 1), contacts[1]);
    assert_eq!(local_contact(&[0x02, 0x00], 2), contacts[2]);
    assert_eq!(local_contact(&[0x03, 0x00], 3), contacts[3]);
    assert_eq!(local_contact(&[0x04, 0x00], 4), contacts[4]);
  } else {
    panic!();
  }

  // 既存のコンタクト情報を観測すると末尾に移動することを確認
  rt.observed(local_contact(&[0x01, 0x00], 100));
  if let Node::KBucket(contacts) = rt.node.as_ref() {
    assert_eq!(local_contact(&[0x00, 0x00], 0), contacts[0]);
    assert_eq!(local_contact(&[0x02, 0x00], 2), contacts[1]);
    assert_eq!(local_contact(&[0x03, 0x00], 3), contacts[2]);
    assert_eq!(local_contact(&[0x04, 0x00], 4), contacts[3]);
    assert_eq!(local_contact(&[0x01, 0x00], 100), contacts[4]);
  } else {
    panic!();
  }
}

#[test]
fn observed_split() {
  const N: usize = 2;
  const K: usize = 2;
  let id: NodeID<N> = Key([0, 0]);
  let mut rt = RoutingTable::new(id, K);
  assert_eq!(&id, rt.node_id());
  assert_eq!(K, rt.k());
  assert_eq!(1, depth(&rt));

  // 想定通りに分割されること確認
  rt.observed(local_contact(&[0b11111111, 0x00], 0));
  rt.observed(local_contact(&[0b11011111, 0x00], 1));
  rt.observed(local_contact(&[0b10011111, 0x00], 2));
  rt.observed(local_contact(&[0b10111111, 0x00], 3));
  rt.observed(local_contact(&[0b01111111, 0x00], 4));
  rt.observed(local_contact(&[0b00111111, 0x00], 5));
  rt.observed(local_contact(&[0b10011111, 0x00], 6));
  assert_eq!(2, depth(&rt));
  if let Node::Branch(b0, b1) = rt.node.as_ref() {
    assert_eq!(Key::from_bytes(&[0b00000000, 0b00000000]), b0.prefix);
    assert_eq!(Key::from_bytes(&[0b10000000, 0b00000000]), b1.prefix);
    assert_eq!(1, b0.prefix_bit_length);
    assert_eq!(1, b1.prefix_bit_length);
    if let (Node::KBucket(c0), Node::KBucket(c1)) = (b0.node.as_ref(), b1.node.as_ref()) {
      assert_eq!(2, c0.len());
      assert_eq!(Key::from_bytes(&[0b01111111, 0]), c0[0].id);
      assert_eq!(Key::from_bytes(&[0b00111111, 0]), c0[1].id);
      assert_eq!(2, c1.len());
      assert_eq!(Key::from_bytes(&[0b11111111, 0]), c1[0].id);
      assert_eq!(Key::from_bytes(&[0b11011111, 0]), c1[1].id);
    } else {
      panic!();
    }
  } else {
    panic!();
  }
}

#[test]
fn replace() {
  const N: usize = 2;
  const K: usize = 3;
  let id: NodeID<N> = Key([0, 0]);
  let mut rt = RoutingTable::new(id, K);
  rt.observed(local_contact(&[0b11111111, 0x00], 0));
  rt.observed(local_contact(&[0b11011111, 0x00], 1));
  rt.observed(local_contact(&[0b10111111, 0x00], 2));
  rt.observed(local_contact(&[0b01111111, 0x00], 4));
  rt.observed(local_contact(&[0b01011111, 0x00], 5));
  rt.observed(local_contact(&[0b00111111, 0x00], 6));
  assert_eq!(2, depth(&rt));

  // コンタクト情報の中から該当する ID が置き換えられることを確認
  let target_id = Key::from_bytes(&[0b01011111, 0x00]);
  let replace = local_contact(&[0b00011111, 0x00], 7);
  if let Node::Branch(b0, ..) = rt.node.as_ref() {
    if let Node::KBucket(contacts) = b0.node.as_ref() {
      assert_eq!(K, contacts.len());
      assert!(contacts.iter().any(|c| c.id == target_id));
    } else {
      panic!();
    }
  } else {
    panic!();
  }
  rt.replace(&target_id, replace.clone());
  if let Node::Branch(b0, ..) = rt.node.as_ref() {
    if let Node::KBucket(contacts) = b0.node.as_ref() {
      assert_eq!(K, contacts.len());
      assert!(!contacts.iter().any(|c| c.id == target_id));
      assert!(contacts.iter().any(|c| *c == replace));
    } else {
      panic!();
    }
  } else {
    panic!();
  }

  // 該当する ID のコンタクト情報は削除されるが、すでに k-bucket の分割が起きていて置き換え対象のコンタクト情報を含む
  // k-bucket ではない場合に、削除のみが行われることを確認
  let target_id = Key::from_bytes(&[0b11011111, 0x00]);
  let replace = local_contact(&[0b00011111, 0x00], 8);
  if let Node::Branch(_, b1) = rt.node.as_ref() {
    if let Node::KBucket(contacts) = b1.node.as_ref() {
      assert_eq!(K, contacts.len());
      assert!(contacts.iter().any(|c| c.id == target_id));
    } else {
      panic!();
    }
  } else {
    panic!();
  }
  rt.replace(&target_id, replace.clone());
  if let Node::Branch(_, b1) = rt.node.as_ref() {
    if let Node::KBucket(contacts) = b1.node.as_ref() {
      assert_eq!(K - 1, contacts.len());
      assert!(!contacts.iter().any(|c| c.id == target_id));
      assert!(!contacts.iter().any(|c| *c == replace));
    } else {
      panic!();
    }
  } else {
    panic!();
  }
}

#[test]
fn refresh() {
  const N: usize = 2;
  const K: usize = 2;
  let id: NodeID<N> = Key([0, 0]);
  let mut rt = RoutingTable::new(id, K);
  rt.observed(local_contact(&[0b00111111, 0x00], 0));
  rt.observed(local_contact(&[0b01111111, 0x00], 1));
  rt.observed(local_contact(&[0b10111111, 0x00], 2));
  assert_eq!(2, depth(&rt));

  // リフレッシュタイムアウトを経過させた状態ですべての k-bucket がランダムな ID 検索を要求することを確認
  let now = Instant::now();
  if let Node::Branch(b0, b1) = rt.node.as_mut() {
    b0.expires = now - Duration::from_nanos(1);
    b1.expires = now - Duration::from_nanos(1);
  } else {
    panic!();
  }
  let ids = rt.refresh();
  assert_eq!(2, ids.len());

  // 新しいコンタクト情報が追加された k-bucket は取得から除外されることを確認
  rt.observed(local_contact(&[0b11111111, 0x00], 7));
  if let Node::Branch(b0, b1) = rt.node.as_mut() {
    assert!(b0.expires < now);
    assert!(b1.expires > now);
  } else {
    panic!();
  }
  let ids = rt.refresh();
  assert_eq!(1, ids.len());
}

#[test]
fn order_of_distance_from_fixed() {
  const N: usize = 2;
  const K: usize = 4;
  let id = Key::<N>::zero();
  let ids = (0..=7).map(|i| [(i << 5) | 0b11111, 0x00]).collect::<Vec<_>>();
  let mut rt = RoutingTable::new(id, K);
  for (i, id) in ids.iter().enumerate() {
    rt.observed(local_contact(id, i as u16));
  }

  // 3-bit の変化に対して既知の順序で取得できることを確認
  for (i, x) in
    vec![[1, 2, 3], [0, 3, 2], [3, 0, 1], [2, 1, 0], [5, 6, 7], [4, 7, 6], [7, 4, 5], [6, 5, 4]].iter().enumerate()
  {
    let contacts = rt.order_of_distance_from(&Key::from_bytes(&ids[i]));
    assert_eq!(K, contacts.len());
    assert_eq!(&ids[i], contacts[0].id.as_bytes());
    assert_eq!(&ids[x[0]], contacts[1].id.as_bytes());
    assert_eq!(&ids[x[1]], contacts[2].id.as_bytes());
    assert_eq!(&ids[x[2]], contacts[3].id.as_bytes());
  }
}

#[test]
fn order_of_distance_from_sparse() {
  const N: usize = 32;
  const K: usize = 10;
  let id = Key::<N>::zero();
  let ids = (0..1000).map(|_| Key::<N>::random().0).collect::<Vec<_>>();
  let mut rt = RoutingTable::new(id, K);
  for (i, id) in ids.iter().enumerate() {
    rt.observed(local_contact(id, i as u16));
  }

  // ルーティングテーブルが保持しているコンタクト情報の中から XOR 距離の近い順に取得できることを確認
  for _ in 0..100 {
    let id = Key::random();
    let contacts = rt.order_of_distance_from(&id);
    assert_eq!(K, contacts.len());
    let mut cs = all_contacts(&rt);
    cs.sort_by(|a, b| {
      let da = &a.id ^ &id;
      let db = &b.id ^ &id;
      da.cmp(&db)
    });
    for (expected, actual) in cs.iter().take(K).zip(contacts.iter()) {
      assert_eq!(
        expected,
        actual,
        "{:?}: {:?} != {:?}",
        id,
        ids.iter().take(K).map(Key::from_bytes).collect::<Vec<_>>(),
        contacts
      );
    }
  }
}

#[test]
fn delve() {
  const N: usize = 2;
  const K: usize = 2;
  let id: NodeID<N> = Key([0, 0]);
  let mut rt = RoutingTable::new(id, K);
  rt.prefix = Key::from_bytes(&[0b10000000, 00000000]);
  rt.prefix_bit_length = 1;

  let sub = rt.delve(1, vec![local_contact(&[0b11000000, 0b00000000], 0), local_contact(&[0b11100000, 0b00000000], 1)]);
  assert_eq!(rt.id, sub.id);
  assert_eq!(Key::from_bytes(&[0b11000000, 00000000]), sub.prefix);
  assert_eq!(2, sub.prefix_bit_length);
  assert_eq!(K, sub.k());
  assert_eq!(rt.expires, sub.expires);
  if let Node::KBucket(contacts) = sub.node.as_ref() {
    assert_eq!(
      &vec![local_contact(&[0b11000000, 0b00000000], 0), local_contact(&[0b11100000, 0b00000000], 1),],
      contacts
    );
  } else {
    panic!();
  }
}

#[test]
fn split() {
  const N: usize = 2;
  // 右から 1 ビット目で分割
  let (c0, c1) = RoutingTable::<N>::split(
    &mut vec![
      local_contact(&[0b00000000, 0b00000000], 0),
      local_contact(&[0b01000000, 0b00000000], 1),
      local_contact(&[0b10000000, 0b00000000], 2),
      local_contact(&[0b11000000, 0b00000000], 3),
    ],
    (N * 8) - 1,
  );
  assert_eq!(vec![local_contact(&[0b00000000, 0b00000000], 0), local_contact(&[0b01000000, 0b00000000], 1)], c0,);
  assert_eq!(vec![local_contact(&[0b10000000, 0b00000000], 2), local_contact(&[0b11000000, 0b00000000], 3)], c1,);

  // 右から 2 ビット目で分割
  let (c0, c1) = RoutingTable::<2>::split(
    &mut vec![
      local_contact(&[0b10000000, 0b00000000], 0),
      local_contact(&[0b10100000, 0b00000000], 1),
      local_contact(&[0b11000000, 0b00000000], 2),
      local_contact(&[0b11100000, 0b00000000], 3),
    ],
    (N * 8) - 2,
  );
  assert_eq!(vec![local_contact(&[0b10000000, 0b00000000], 0), local_contact(&[0b10100000, 0b00000000], 1)], c0,);
  assert_eq!(vec![local_contact(&[0b11000000, 0b00000000], 2), local_contact(&[0b11100000, 0b00000000], 3)], c1,);
}

fn local_contact<const N: usize>(id: &[u8; N], port: u16) -> Contact<N> {
  Contact::new(Key::from_bytes(id), local_addr(port))
}

fn local_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn depth<const N: usize>(rt: &RoutingTable<N>) -> usize {
  match rt.node.as_ref() {
    super::Node::Branch(b0, b1) => std::cmp::max(depth(b0), depth(b1)) + 1,
    super::Node::KBucket { .. } => 1,
  }
}

/// ルーティングテーブル内のすべてのコンタクト情報を取得します。
///
fn all_contacts<const N: usize>(rt: &RoutingTable<N>) -> Vec<Contact<N>> {
  match rt.node.as_ref() {
    Node::Branch(b0, b1) => {
      let mut c0 = all_contacts(b0);
      let mut c1 = all_contacts(b1);
      c0.append(&mut c1);
      c0
    }
    Node::KBucket(contacts) => contacts.clone(),
  }
}

/// 指定されたルーティングテーブルが refresh を必要とするまでの時点を設定します。
///
pub fn set_expires<const N: usize>(rt: &mut RoutingTable<N>, deadline: Instant) {
  rt.expires = deadline;
  if let Node::Branch(b0, b1) = rt.node.as_mut() {
    set_expires(b0, deadline);
    set_expires(b1, deadline);
  }
}

#[allow(dead_code)]
fn dump<const N: usize>(rt: &RoutingTable<N>) {
  fn dump_bucket<const N: usize>(rt: &RoutingTable<N>, depth: usize) {
    match rt.node.as_ref() {
      super::Node::Branch(b0, b1) => {
        let mut prefix = rt.prefix;
        eprintln!("{}[{}]", " ".repeat(depth), prefix);
        dump_bucket(b0, depth + 2);
        prefix.set_bit((N * 8) - rt.prefix_bit_length - 1, 1);
        eprintln!("{}[{}]", " ".repeat(depth), prefix);
        dump_bucket(b1, depth + 2);
      }
      super::Node::KBucket(contacts) => {
        if contacts.is_empty() {
          eprintln!("{}---", " ".repeat(depth));
        }
        for c in contacts {
          eprintln!("{}#{:?}", " ".repeat(depth), c);
        }
      }
    }
  }
  dump_bucket(rt, 0);
}
