//!
//! この文書は [github.com/torao/kademlia](https://github.com/torao/kademlia) の API リファレンスです。
//!
//! `kademlia` ライブラリはあるキーに対応するノードを大規模な P2P ネットワークから探索するための分散ハッシュテーブル
//! (DHT) アルゴリズムを実装しています。このアルゴリズムは、キー空間の大きさを $n$ としたときに最大 $\log_2 n$ 回の問い
//! 合わせで目的のノードに到達することができます。これは、例えば IPv4 のような 32-bit キー空間を使用するケースで最悪でも
//! 32 回のホップで目的のノードに到達することを意味します。
//!
//! ```toml
//! [dependencies]
//! kademlia = { git = "https://github.com/torao/kademlia" }
//! tokio = { version = "1", features = ["full"] }
//! ```
//!
//! このライブラリは Kademlia のリファレンス実装として構造や特性を学ぶことを目的としています。目的のための機能は十分に
//! テストしていますが、構成のわかりやすさと Kademlia 論文の方針を優先しており、最高の効率を得るためにはさらなる改良が
//! 必要と考えられます。したがって今のところ [crates.io](https://crates.io) に登録する予定はなく github から参照します。
//!
//! ### Build the Reference Implementation of KVS
//!
//! `main.rs` は `kademlia` ライブラリを使用した KVS のリファレンス実装です。
//!
//! ### Build Document
//!
//! ```
//! $ RUSTDOCFLAGS="--html-in-header katex-header.html" cargo doc --no-deps --open
//! ```
//!
//! ## About Kademlia
//!
//! **Kademlia** は Maymounkov と Mazières による論文 ["Kademlia: A Peer-to-peer Information System Based on the XOR Metric"](chrome-extension://efaidnbmnnnibpcajpcglclefindmkaj/https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)
//! (2002) で提案された分散ハッシュテーブル (DHT) アルゴリズムです。この `kademlia` ライブラリは Rust による Kademlia
//! ルーティングの実装です。
//!
//! `kademlia` ライブラリは論文と以下の点で異なります。
//!
//! * キーや nonce 値の空間は 160-bit 固定ではなく (コード上に定数ジェネリクス `const N:usize` として現れる) 任意のバイト
//!   長を使用できます。
//! * KVS 実装を想定した `STORE` と `FIND_VALUE` メッセージを廃止し、より汎用化されたアプリケーション仕様のメッセージ
//!   `APP_MESSAGE` を導入しています。アプリケーションは `APP_MESSAGE` を使って KVS を含む任意の RPC を実装することが
//!   できます。このため KVS の動作として言及されていた "値が消えないように 1 時間おきに `STORE` メッセージを送信する"
//!   といった動作は実装していません。
//!
#[cfg_attr(doc, katexit::katexit)]
use rand::{thread_rng, Rng, RngCore};
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::ops::BitXor;
use std::time::Duration;

mod msg;
mod routing_table;
mod server;

#[cfg(test)]
mod test;

pub use msg::*;
pub use routing_table::*;
pub use server::*;

/// 8 × `N` bit の空間上の点を示します。
/// この空間はノード ID と共用しています。
/// 値の比較や演算は Little Endian、つまり `key[0]` が最も大きい桁を格納し `key[key.len()-1]` が最も小さい桁を格納する
/// 多倍長整数として評価します。
///
#[derive(Eq, PartialEq, PartialOrd, Ord, Clone, Copy, Hash)]
pub struct Key<const N: usize>([u8; N]);

impl<const N: usize> Key<N> {
  /// 指定された `N` バイト列からキーを構築します。
  ///
  /// ```
  /// use kademlia::Key;
  ///
  /// let key = Key::from_bytes(&[0xAB, 0xCD, 0xEF]);
  /// assert_eq!(&[0xAB, 0xCD, 0xEF], key.as_bytes());
  /// ```
  ///
  pub fn from_bytes(bytes: &[u8; N]) -> Self {
    Self(*bytes)
  }

  /// このキーを固定長 `N` バイト列として参照します。
  ///
  pub fn as_bytes(&self) -> &[u8; N] {
    &self.0
  }

  /// このキーを可変な固定長 `N` バイト列として参照します。
  ///
  pub fn as_bytes_mut(&mut self) -> &mut [u8; N] {
    &mut self.0
  }

  /// ランダムな値を持つキーを作成します。この値はメッセージの nonce に使用することを意図しています。
  ///
  pub fn random() -> Self {
    let mut key = Self([0u8; N]);
    thread_rng().fill_bytes(&mut key.0[..]);
    key
  }

  /// 指定された位置のビットを参照します。返値は 0 または 1 のいずれかです。
  ///
  /// ```
  /// use kademlia::Key;
  ///
  /// let key = Key::from_bytes(&[0b00001000, 0b00000000]);
  /// assert_eq!(1, key.get_bit(11));
  /// ```
  ///
  pub fn get_bit(&self, position: usize) -> usize {
    let (bytes, bits) = Self::bit_position(position);
    ((self.0[bytes] >> bits) & 0x01) as usize
  }

  /// 指定されたビットに 1 または 0 を設定します。
  ///
  /// ```
  /// use kademlia::Key;
  ///
  /// let mut key = Key::from_bytes(&[0b00000000, 0b00000000]);
  /// assert_eq!(0, key.get_bit(11));
  /// key.set_bit(11, 1);
  /// assert_eq!(1, key.get_bit(11));
  /// assert_eq!(&[0b00001000, 0b00000000], key.as_bytes());
  /// ```
  ///
  pub fn set_bit(&mut self, position: usize, bit: usize) {
    assert!(bit == 0 || bit == 1);
    let (bytes, bits) = Self::bit_position(position);
    self.0[bytes] = (self.0[bytes] & !(1 << bits)) | ((bit as u8) << bits);
  }

  /// 指定されたビット位置より下のすべてのビットをランダムなビットに置き換えます。
  /// `position` に相当する位置は置き換えられません。
  ///
  pub fn set_random_bits_lower_than<R: Rng + ?Sized>(&mut self, position: usize, rand: &mut R) {
    if position == N * 8 {
      rand.fill_bytes(&mut self.0[..]);
      return;
    }
    let (bytes, bits) = Self::bit_position(position);
    if bytes + 1 < N {
      rand.fill_bytes(&mut self.0[bytes + 1..]);
    }
    let mask = (1 << bits) - 1;
    let r = (rand.next_u32() & 0xFF) as u8;
    self.0[bytes] = (self.0[bytes] & !mask) | (r & mask);
  }

  /// このキーと指定されたキーをビットレベルで比較し、上位の一致しているビット数を返します。
  ///
  /// ```
  /// use kademlia::Key;
  ///
  /// let key1 = Key::from_bytes(&[0b10010101, 0b01101111]);
  /// let key2 = Key::from_bytes(&[0b10011000, 0b11101011]);
  /// let key3 = Key::from_bytes(&[0b10010101, 0b01101111]);
  /// let key4 = Key::from_bytes(&[0b01100010, 0b11001110]);
  /// assert_eq!( 4, key1.prefix_match_bits(&key2));
  /// assert_eq!(16, key1.prefix_match_bits(&key3));
  /// assert_eq!( 0, key1.prefix_match_bits(&key4));
  /// ```
  ///
  pub fn prefix_match_bits(&self, other: &Key<N>) -> usize {
    let mut bits = 0;
    for i in 0..N {
      if self.0[i] == other.0[i] {
        bits += 8;
      } else {
        for j in 0..8 {
          let mask = 0b10000000 >> j;
          if self.0[i] & mask == other.0[i] & mask {
            bits += 1;
          } else {
            return bits;
          }
        }
      }
    }
    bits
  }

  /// 指定されたキーの上位 `bit_length` ビットがこのキーの上位ビットと一致しているかを判定します。
  ///
  pub fn prefix_matches(&self, other: &Key<N>, bit_length: usize) -> bool {
    self.prefix_match_bits(other) >= bit_length
  }

  /// ビット指定の位置をバイト位置とビット位置に変換します。
  ///
  #[inline]
  fn bit_position(position: usize) -> (usize, usize) {
    assert!(position < N * 8, "{} < {} × 8", position, N);
    (N - (position >> 3) - 1, position & 0x07)
  }

  /// すべてのビットが 0 から成るキーを生成します。
  ///
  pub fn zero() -> Self {
    Key([0u8; N])
  }

  /// このキーのすべてのビットが 0 のとき true を返します。
  ///
  pub fn is_zero(&self) -> bool {
    for i in 0..N {
      if self.0[i] != 0 {
        return false;
      }
    }
    true
  }
}

/// キーに対してビット OR 演算子 `^` を用いて XOR 距離を算出できるようにします。
///
impl<const N: usize> BitXor for &Key<N> {
  type Output = XorDistance<N>;

  fn bitxor(self, rhs: Self) -> Self::Output {
    let mut output = Key([0u8; N]);
    for i in 0..N {
      output.0[i] = self.0[i] ^ rhs.0[i];
    }
    output
  }
}

impl<const N: usize> Debug for Key<N> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Display::fmt(&self, f)
  }
}

impl<const N: usize> Display for Key<N> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for i in 0..N {
      write!(f, "{:02X}", self.0[i])?;
    }
    Ok(())
  }
}

/// ノード ID。
pub type NodeID<const N: usize> = Key<N>;

/// ノード ID やキーとの距離を表す XOR 距離。
pub type XorDistance<const N: usize> = Key<N>;

/// ルーティングテーブルに保存されるノードのアクセス先情報です。
///
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct Contact<const N: usize> {
  pub id: NodeID<N>,
  pub address: SocketAddr,
}

impl<const N: usize> Contact<N> {
  /// 指定されたノード ID とアドレスを持つ新しいコンタクト情報を構築します。
  ///
  pub fn new(id: NodeID<N>, address: SocketAddr) -> Self {
    Self { id, address }
  }
}

impl<const N: usize> Display for Contact<N> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}@{}", self.id, self.address)
  }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("Invalid message byte length; {expected} bytes expected, but {actual}")]
  InvalidMessageLength { expected: usize, actual: usize },
  #[error("Invalid IP address length; 4 or 16 bytes expected, but {actual}")]
  InvalidIPAddressLength { actual: usize },
  #[error("Invalid message id: 0x{id:02X}")]
  InvalidMessageID { id: u8 },
  #[error("Operation timed out: {0} ({1:?})")]
  Timeout(&'static str, Duration),
  #[error("{message}")]
  Shutdown { message: String },
  #[error("{message}")]
  Routing { message: String },
  #[error("Application not found: {peer}")]
  AppNotFound { peer: String },
  #[error("Unknown application response: {0}")]
  UnknowAppResponse(AppCode),

  #[error(transparent)]
  AddrParse(#[from] std::net::AddrParseError),
  #[error(transparent)]
  IO(#[from] std::io::Error),
  #[error(transparent)]
  ChannelRecv(#[from] tokio::sync::oneshot::error::RecvError),
  #[error(transparent)]
  Elapsed(#[from] tokio::time::error::Elapsed),
  #[error(transparent)]
  Join(#[from] tokio::task::JoinError),
}

impl Error {
  fn server_has_been_shutdown() -> Error {
    Error::Shutdown { message: String::from("The server has already been shutdown") }
  }
  fn client_has_been_dropped_oneshot<T>(_err: T) -> Error {
    Error::Shutdown { message: String::from("The client-receiver has already been dropped") }
  }
}
