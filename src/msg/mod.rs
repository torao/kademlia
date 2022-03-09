//! メッセージ。

use crate::{Contact, Error, Key, NodeID, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[cfg(test)]
mod test;

pub const MSG_ID_PING: u8 = 0x01;
pub const MSG_ID_PONG: u8 = (1 << 7) | MSG_ID_PING;
pub const MSG_ID_FIND_NODE: u8 = 0x02;
pub const MSG_ID_FOUND_NODE: u8 = (1 << 7) | MSG_ID_FIND_NODE;
pub const MSG_ID_APP_MESSAGE: u8 = 0x03;
pub const MSG_ID_APP_RESPONSE: u8 = (1 << 7) | MSG_ID_APP_MESSAGE;

/// この Kademlia 実装で使用するメッセージの列挙型です。
///
/// すべてのメッセージは先頭のフィールドに (送信元ノードID, nonce, ...) を持ちます。
///
/// [`Message::to_bytes()`] によってシリアライズされたすべてのメッセージの先頭 1 バイトはメッセージ識別子を表します。
///
/// 応答メッセージの `nonce` フィールドには、対応する要求メッセージの `nonce` フィールドに設定されていた値がそのまま設定
/// される。したがって応答メッセージの受信者は `nonce` 値を参照することでどの要求に対する応答かを知ることができる。
///
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum Message<const N: usize> {
  /// `PING` はノードが機能していることを確認するための問い合わせメッセージです。
  ///
  /// [`Ping`](Message::Ping) メッセージは任意のノードが任意のタイミングで任意のノードに対して送信することができます。
  /// [`Ping`](Message::Ping) メッセージを受信したノードは時間を置かずに、そのメッセージの送信元 IP アドレスとポートに
  /// 対して [`Pong`](Message::Pong) メッセージで応答する必要があります。
  ///
  /// # Serialize Format by [`Message::to_bytes()`]
  ///
  /// | No. | Field       | Length | Value |
  /// |----:|:------------|-------:|:------|
  /// |   1 | `msg_id`    |      1 | [`MSG_ID_PING`] (0x01) |
  /// |   2 | `sender_id` |    `N` |       |
  /// |   3 | `nonce`     |    `N` |       |
  ///
  /// ```
  /// use kademlia::{NodeID, Key, Message, MSG_ID_PING};
  ///
  /// const N: usize = 2;
  /// let sender_id = NodeID::from_bytes(&[0xAB, 0xCD]);
  /// let nonce = Key::random();
  /// let ping = Message::Ping(sender_id, nonce);
  /// let bytes = ping.to_bytes();
  /// assert_eq!(1 + N + N,           bytes.len());
  /// assert_eq!(MSG_ID_PING,         bytes[0]);
  /// assert_eq!(0xAB,                bytes[1]);
  /// assert_eq!(0xCD,                bytes[2]);
  /// assert_eq!(nonce.as_bytes()[0], bytes[3]);
  /// assert_eq!(nonce.as_bytes()[1], bytes[4]);
  /// ```
  ///
  Ping(NodeID<N>, Key<N>),

  /// `PING` に対する応答メッセージです。
  ///
  /// # Serialize Format by [`Message::to_bytes()`]
  ///
  /// `msg_id` フィールド値が異なるだけで [`Ping`](Message::Ping) と同じです。
  ///
  /// | No. | Field       | Length | Value |
  /// |----:|:------------|-------:|:------|
  /// |   1 | `msg_id`    |      1 | [`MSG_ID_PONG`] (0x81) |
  /// |   2 | `sender_id` |    `N` |       |
  /// |   3 | `nonce`     |    `N` |       |
  ///
  /// ```
  /// use kademlia::{NodeID, Key, Message, MSG_ID_PONG};
  ///
  /// const N: usize = 2;
  /// let sender_id = NodeID::from_bytes(&[0xEF, 0xFE]);
  /// let nonce = Key::random();
  /// let pong = Message::Pong(sender_id, nonce);
  /// let bytes = pong.to_bytes();
  /// assert_eq!(1 + N + N,           bytes.len());
  /// assert_eq!(MSG_ID_PONG,         bytes[0]);
  /// assert_eq!(0xEF,                bytes[1]);
  /// assert_eq!(0xFE,                bytes[2]);
  /// assert_eq!(nonce.as_bytes()[0], bytes[3]);
  /// assert_eq!(nonce.as_bytes()[1], bytes[4]);
  /// ```
  ///
  Pong(NodeID<N>, Key<N>),

  /// `FIND_NODE` メッセージは宛先のノードに対して `node_id` のコンタクト情報を問い合わせます。[`FindNode`](Message::FindNode)
  /// を受信したノードは [`FoundNode`](Message::FoundNode) で応答する必要があります。
  ///
  /// # Serialize Format by [`Message::to_bytes()`]
  ///
  /// | No. | Field       | Length | Value |
  /// |----:|:------------|-------:|:------|
  /// |   1 | `msg_id`    |      1 | [`MSG_ID_FIND_NODE`] (0x02) |
  /// |   2 | `sender_id` |    `N` |       |
  /// |   3 | `nonce`     |    `N` |       |
  /// |   4 | `node_id`   |    `N` |       |
  ///
  /// ```
  /// use kademlia::{NodeID, Key, Message, MSG_ID_FIND_NODE};
  ///
  /// const N: usize = 2;
  /// let sender_id = NodeID::from_bytes(&[0x3E, 0xE3]);
  /// let nonce = Key::random();
  /// let node_id = NodeID::from_bytes(&[0x3F, 0xF3]);
  /// let find_node = Message::FindNode(sender_id, nonce, node_id);
  /// let bytes = find_node.to_bytes();
  /// assert_eq!(1 + N + N + N,       bytes.len());
  /// assert_eq!(MSG_ID_FIND_NODE,    bytes[0]);
  /// assert_eq!(0x3E,                bytes[1]);
  /// assert_eq!(0xE3,                bytes[2]);
  /// assert_eq!(nonce.as_bytes()[0], bytes[3]);
  /// assert_eq!(nonce.as_bytes()[1], bytes[4]);
  /// assert_eq!(0x3F,                bytes[5]);
  /// assert_eq!(0xF3,                bytes[6]);
  /// ```
  ///
  FindNode(NodeID<N>, Key<N>, NodeID<N>),

  /// `FIND_NODE` に対応する応答メッセージです。`FIND_NODE` メッセージを受信したノードが知っているコンタクト情報の中で、
  /// 問い合わせのあった `node_id` に近い順に最大 255 件のコンタクト情報を送信することができます。
  ///
  /// このメッセージはフィールドに `node_id` を含みません。この応答メッセージがどの `node_id` に対するものであるかは、
  /// `FIND_NODE` で頭囲合わせを行ったノードが `nonce` 値と併せて保持しておく必要があります。
  ///
  /// # Serialize Format by [`Message::to_bytes()`]
  ///
  /// | No. | Field             | Length | Value |
  /// |----:|:------------------|-------:|:------|
  /// |   1 | `msg_id`          |      1 | [`MSG_ID_FOUND_NODE`] (0x82) |
  /// |   2 | `sender_id`       |    `N` |       |
  /// |   3 | `nonce`           |    `N` |       |
  /// |   4 | `num_of_contacts` |      1 |       |
  /// |   5 | `contacts`        | Contact × `num_of_contacts` | |
  ///
  /// `contacts` は [`Contact`] を保存する可変長のリスト構造であり、その要素数は `num_of_contacts` フィールドに保存されて
  /// います。[`Contact`] は以下のフィールド構造を持ちます。
  ///
  /// |No. | Field       | Length     | Value   |
  /// |---:|:------------|-----------:|:--------|
  /// |  1 | `node_id`   |        `N` |         |
  /// |  2 | `addr_len`  |          1 | 4 or 16 |
  /// |  3 | `address`   | `addr_len` |         |
  /// |  4 | `port`      |          2 |         |
  ///
  /// アドレスとして IPv4 保存する場合、`addr_len` フィールド値は 4 であり `address` フィールドは 4 バイトの長さを持ちます。
  /// IPv6 を保存する場合、`addr_len` フィールド値は 6 であり `address` フィールドは 16 バイトの長さを持ちます。
  ///
  /// ```
  /// use kademlia::{Contact, Key, Message, NodeID, MSG_ID_FOUND_NODE};
  ///
  /// const N: usize = 2;
  /// let sender_id = NodeID::from_bytes(&[0x01, 0x23]);
  /// let nonce = Key::random();
  /// let contacts = vec![
  ///   Contact::new(NodeID::from_bytes(&[0xA0, 0xB0]), "127.0.0.1:9000".parse().unwrap()),
  /// ];
  /// let find_node = Message::FoundNode(sender_id, nonce, contacts);
  /// let bytes = find_node.to_bytes();
  /// assert_eq!(1 + N + N + 1 + N + 1 + 4 + 2, bytes.len());
  /// assert_eq!(MSG_ID_FOUND_NODE,   bytes[0]);
  /// assert_eq!(0x01,                bytes[1]);
  /// assert_eq!(0x23,                bytes[2]);
  /// assert_eq!(nonce.as_bytes()[0], bytes[3]);
  /// assert_eq!(nonce.as_bytes()[1], bytes[4]);
  /// assert_eq!(1,                   bytes[5]);
  /// assert_eq!(0xA0,                bytes[6]);
  /// assert_eq!(0xB0,                bytes[7]);
  /// assert_eq!(4,                   bytes[8]);
  /// assert_eq!(&[127, 0, 0, 1],     &bytes[9..13]);
  /// assert_eq!(&[0x23, 0x28],       &bytes[13..15]);
  /// ```
  ///
  FoundNode(NodeID<N>, Key<N>, Vec<Contact<N>>),

  /// Kademlia の論文は `STORE` と `FIND_VALUE` を使用していましたが、このような KVS 操作はアプリケーション定義で「キーに
  /// 対する任意のバイト列を送信し、受信側のアプリケーションがそれを解釈する」と変更します。もちろん、このメッセージを使用
  /// して `STORE` や `FIND_VALUE` に相当する機能を実装することができます。
  ///
  AppMessage(NodeID<N>, Key<N>, Key<N>, Vec<u8>),

  AppResponse(NodeID<N>, Key<N>, AppCode),
}

pub type AppCode = u8;

impl<const N: usize> Message<N> {
  pub const APP_RESPONSE_OK: AppCode = 0;
  pub const APP_RESPONSE_NOT_FOUND: AppCode = 1;

  /// このメッセージをバイト列に変換します。各メッセージがどのようなバイト列に変換されるかはそれぞれのメッセージの定義を
  /// 参照してください。
  ///
  pub fn to_bytes(&self) -> Vec<u8> {
    let mut buffer = BytesMut::with_capacity(1024);
    match self {
      Message::Ping(peer_id, nonce) => {
        buffer.put_u8(MSG_ID_PING);
        buffer.put_slice(&peer_id.0[..]);
        buffer.put_slice(&nonce.0[..]);
      }
      Message::Pong(peer_id, nonce) => {
        buffer.put_u8(MSG_ID_PONG);
        buffer.put_slice(&peer_id.0[..]);
        buffer.put_slice(&nonce.0[..]);
      }
      Message::FindNode(peer_id, nonce, node_id) => {
        buffer.put_u8(MSG_ID_FIND_NODE);
        buffer.put_slice(&peer_id.0[..]);
        buffer.put_slice(&nonce.0[..]);
        buffer.put_slice(&node_id.0[..]);
      }
      Message::FoundNode(peer_id, nonce, contacts) => {
        // MaxPacketSize: 1 + N + N + 1 + (N + 1 + 16 + 2) * 255; e.g., N=64 -> 21,292
        buffer.put_u8(MSG_ID_FOUND_NODE);
        buffer.put_slice(&peer_id.0[..]);
        buffer.put_slice(&nonce.0[..]);
        buffer.put_u8(contacts.len() as u8);
        for c in contacts.iter() {
          buffer.put_slice(&c.id.0[..]);
          match c.address.ip() {
            IpAddr::V4(addr) => {
              buffer.put_u8(4);
              buffer.put_slice(&addr.octets()[..]);
            }
            IpAddr::V6(addr) => {
              buffer.put_u8(16);
              buffer.put_slice(&addr.octets()[..]);
            }
          }
          buffer.put_u16(c.address.port());
        }
      }
      Message::AppMessage(peer_id, nonce, key, value) => {
        buffer.put_u8(MSG_ID_APP_MESSAGE);
        buffer.put_slice(&peer_id.0[..]);
        buffer.put_slice(&nonce.0[..]);
        buffer.put_slice(&key.0[..]);
        buffer.put_u16(value.len() as u16);
        buffer.put_slice(value);
      }
      Message::AppResponse(peer_id, nonce, code) => {
        buffer.put_u8(MSG_ID_APP_RESPONSE);
        buffer.put_slice(&peer_id.0[..]);
        buffer.put_slice(&nonce.0[..]);
        buffer.put_u8(*code);
      }
    }
    buffer.to_vec()
  }

  /// [`to_bytes()`](Message::to_bytes()) によって生成されたバイト列からメッセージを復元します。
  ///
  pub fn from_bytes(buffer: &[u8]) -> Result<Message<N>> {
    let mut buffer = Bytes::from(buffer.to_vec());

    ensure_readable(&buffer, 1)?;
    match buffer.get_u8() {
      MSG_ID_PING => {
        let peer_id = to_key(&mut buffer)?;
        let nonce = to_key(&mut buffer)?;
        ensure_empty(&buffer)?;
        Ok(Message::Ping(peer_id, nonce))
      }
      MSG_ID_PONG => {
        let peer_id = to_key(&mut buffer)?;
        let nonce = to_key(&mut buffer)?;
        ensure_empty(&buffer)?;
        Ok(Message::Pong(peer_id, nonce))
      }
      MSG_ID_FIND_NODE => {
        let peer_id = to_key(&mut buffer)?;
        let nonce = to_key(&mut buffer)?;
        let node_id = to_key(&mut buffer)?;
        ensure_empty(&buffer)?;
        Ok(Message::FindNode(peer_id, nonce, node_id))
      }
      MSG_ID_FOUND_NODE => {
        let peer_id = to_key(&mut buffer)?;
        let nonce = to_key(&mut buffer)?;
        ensure_readable(&buffer, 1)?;
        let len = buffer.get_u8() as usize;
        let mut contacts = Vec::with_capacity(len);
        for _ in 0..len {
          let id = to_key(&mut buffer)?;
          let addr = Self::address(&mut buffer)?;
          ensure_readable(&buffer, 2)?;
          let port = buffer.get_u16();
          let address = match addr {
            IpAddr::V4(addr) => SocketAddr::V4(SocketAddrV4::new(addr, port)),
            IpAddr::V6(addr) => SocketAddr::V6(SocketAddrV6::new(addr, port, 0, 0)),
          };
          contacts.push(Contact { id, address });
        }
        ensure_empty(&buffer)?;
        Ok(Message::FoundNode(peer_id, nonce, contacts))
      }
      MSG_ID_APP_MESSAGE => {
        let peer_id = to_key(&mut buffer)?;
        let nonce = to_key(&mut buffer)?;
        let key = to_key(&mut buffer)?;
        ensure_readable(&buffer, 2)?;
        let len = buffer.get_u16() as usize;
        ensure_readable(&buffer, len)?;
        let mut value = vec![0u8; len];
        buffer.copy_to_slice(&mut value);
        ensure_empty(&buffer)?;
        Ok(Message::AppMessage(peer_id, nonce, key, value))
      }
      MSG_ID_APP_RESPONSE => {
        let peer_id = to_key(&mut buffer)?;
        let nonce = to_key(&mut buffer)?;
        ensure_readable(&buffer, 1)?;
        let code = buffer.get_u8();
        ensure_empty(&buffer)?;
        Ok(Message::AppResponse(peer_id, nonce, code))
      }
      unknown => Err(Error::InvalidMessageID { id: unknown }),
    }
  }

  /// 指定されたバッファからアドレスを復元します。
  ///
  fn address(buffer: &mut Bytes) -> Result<IpAddr> {
    ensure_readable(buffer, 1)?;
    match buffer.get_u8() {
      4 => {
        ensure_readable(buffer, 4)?;
        let mut addr = [0u8; 4];
        buffer.copy_to_slice(&mut addr);
        Ok(IpAddr::V4(Ipv4Addr::from(addr)))
      }
      16 => {
        ensure_readable(buffer, 16)?;
        let mut addr = [0u8; 16];
        buffer.copy_to_slice(&mut addr);
        Ok(IpAddr::V6(Ipv6Addr::from(addr)))
      }
      len => Err(Error::InvalidIPAddressLength { actual: len as usize }),
    }
  }
}

/// 指定されたバッファからキーを生成します。
///
fn to_key<const N: usize>(buffer: &mut Bytes) -> Result<Key<N>> {
  ensure_readable(buffer, N)?;
  let mut key = Key([0u8; N]);
  buffer.copy_to_slice(&mut key.0[..]);
  Ok(key)
}

fn ensure_readable(buf: &Bytes, len: usize) -> Result<()> {
  if buf.remaining() < len {
    Err(Error::InvalidMessageLength { expected: buf.remaining() + len, actual: buf.len() })
  } else {
    Ok(())
  }
}

fn ensure_empty(buf: &Bytes) -> Result<()> {
  if buf.remaining() > 0 {
    Err(Error::InvalidMessageLength { expected: buf.len() - buf.remaining(), actual: buf.len() })
  } else {
    Ok(())
  }
}
