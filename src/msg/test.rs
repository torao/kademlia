use bytes::Bytes;

use crate::{Contact, Error, Key, Message};

#[test]
fn to_and_from_bytes() {
  for msg in vec![
    Message::Ping(Key([0x00, 0x01]), Key::random()),
    Message::Pong(Key([0x00, 0x01]), Key::random()),
    Message::FindNode(Key([0x00, 0x02]), Key::random(), Key([0x00, 0x03])),
    Message::FoundNode(
      Key([0x00, 0x04]),
      Key::random(),
      vec![
        Contact { id: Key([0x01, 0x00]), address: "127.0.0.1:0".parse().unwrap() },
        Contact { id: Key([0x01, 0x01]), address: "127.0.0.1:1".parse().unwrap() },
        Contact { id: Key([0x01, 0x02]), address: "[0:0:0:0:0:0:0:1]:2".parse().unwrap() },
        Contact { id: Key([0x01, 0x03]), address: "[::1]:3".parse().unwrap() },
      ],
    ),
    Message::AppMessage(Key([0x00, 0x03]), Key::random(), Key([0x00, 0x00]), vec![0x01, 0x02]),
  ] {
    let bytes = msg.to_bytes();

    // 正しく復元できることを確認
    assert_eq!(msg, Message::from_bytes(&bytes).unwrap());

    // 後方が欠落している場合はエラーとなることを確認
    for i in 0..bytes.len() {
      assert!(Message::<2>::from_bytes(&bytes[..i]).is_err());
    }

    // 想定より長い場合はエラーとなることを確認
    let mut bytes = bytes.clone();
    bytes.push(0x00);
    assert!(Message::<2>::from_bytes(&bytes).is_err());
  }
}

#[test]
fn invalid_message_id() {
  let bytes = &[0xFF, 0xFE, 0xFD, 0xFC];
  if let Err(Error::InvalidMessageID { .. }) = Message::<1>::from_bytes(bytes) {
    /* */
  } else {
    panic!();
  }
}

#[test]
fn invalid_address_size() {
  let mut bytes = Bytes::copy_from_slice(&[0x00, 0x01, 0x02, 0x03]);
  if let Err(Error::InvalidIPAddressLength { .. }) = Message::<1>::address(&mut bytes) {
    /* */
  } else {
    panic!();
  }
}
