use crate::{Error, Key, XorDistance};
use rand::seq::SliceRandom;
use rand::{self, SeedableRng};
use rand_chacha::ChaCha12Rng;
use std::io::Write;

#[test]
fn xor_distance_nature() {
  // XOR 距離の前提を確認
  for x in random_distances(100) {
    // 1. ∀x: d(x, x) = 0
    assert_eq!(XorDistance::zero(), &x ^ &x);

    for y in random_distances(100) {
      // 2. ∀x, y: d(x, y) > 0 if x ≠ y
      if x != y {
        assert!(&x ^ &y > XorDistance::zero());
      } else {
        assert_eq!(XorDistance::zero(), &x ^ &y);
      }

      // 3. ∀x, y: d(x, y) = d(y, x)
      assert_eq!(&x ^ &y, &y ^ &x);

      // 4. ∀x, y, z: d(x, z) ≦ d(x, y) + d(y, z)
      for z in random_distances(100) {
        let mut lhs = Key([0x00, 0x00, 0x00]);
        let rhs = add::<2, 3>(&(&x ^ &y), &(&y ^ &z));
        (&mut lhs.0[1..]).write_all(&(&x ^ &z).0).unwrap();
        assert!(lhs <= rhs, "{} > {} (= {} + {})", lhs, rhs, &x ^ &y, &y ^ &z);
      }
    }
  }
  fn random_distances(len: usize) -> Vec<Key<2>> {
    (0..len).map(|_| Key::random()).collect::<Vec<_>>()
  }
}

#[test]
fn key_from_bytes() {
  let bytes = [0xAB, 0xCD, 0xEF];
  let key = Key::from_bytes(&bytes);
  assert_eq!(&bytes, &key.0);
  assert_eq!(&bytes, key.as_bytes());

  let mut key = Key::from_bytes(&bytes);
  key.as_bytes_mut()[1] = 0x00;
  assert_eq!(&[0xAB, 0x00, 0xEF], key.as_bytes());
}

#[test]
fn key_display() {
  let key = Key::from_bytes(&[0x01, 0x23, 0x45, 0xAB, 0xCD, 0xEF]);
  assert_eq!("012345ABCDEF", format!("{}", key)); // Display
  assert_eq!("012345ABCDEF", format!("{:?}", key)); // Debug
}

#[test]
fn key_zero() {
  let key = Key::<2>::zero();
  assert_eq!("0000", format!("{}", key));
  assert!(key.is_zero());

  let key = Key::from_bytes(&[0x00, 0x70]);
  assert!(!key.is_zero());
}

#[test]
fn bit_operation() {
  assert_eq!((1, 0), Key::<2>::bit_position(0));
  assert_eq!((1, 1), Key::<2>::bit_position(1));
  assert_eq!((1, 2), Key::<2>::bit_position(2));
  assert_eq!((1, 7), Key::<2>::bit_position(7));
  assert_eq!((0, 0), Key::<2>::bit_position(8));
  assert_eq!((0, 1), Key::<2>::bit_position(9));
  assert_eq!((0, 7), Key::<2>::bit_position(15));

  // ビット位置指定でのビット値の参照と設定
  let mut key = Key::from_bytes(&[0b00000000, 0b00000000]);
  assert_eq!(0, key.get_bit(3));
  key.set_bit(3, 1);
  assert_eq!(1, key.get_bit(3));
  assert_eq!(&[0b00000000, 0b00001000], key.as_bytes());
  key.set_bit(3, 0);
  assert_eq!(0, key.get_bit(3));
  assert_eq!(&[0b00000000, 0b00000000], key.as_bytes());
}

#[test]
fn set_random_bits_lower_than() {
  // position より下のビットが乱数となることを確認
  let mut rng = ChaCha12Rng::seed_from_u64(4987512);
  let mut key = Key::from_bytes(&[0b00000000, 0b00000000]);
  key.set_random_bits_lower_than(11, &mut rng);
  assert_eq!(0, key.get_bit(11));
  assert_eq!(&[0b00000100, 0b11010100], key.as_bytes());

  // 繰り返しビット論理和を取ることで position 以上のビットがすべて 0、position より下のビットがすべて 1 になることを確認
  const N: usize = 2;
  let mut rng = ChaCha12Rng::seed_from_u64(4987512);
  for i in 0..=(N * 8) {
    let keys = (0..20)
      .map(|_| {
        let mut key = Key::<N>::zero();
        key.set_random_bits_lower_than(i, &mut rng);
        key
      })
      .collect::<Vec<_>>();
    let key = bit_or(keys);
    for j in 0..N * 8 {
      assert_eq!(if j < i { 1 } else { 0 }, key.get_bit(j));
    }
  }
}

#[test]
fn prefix_match_bits() {
  let k0 = Key::from_bytes(&[0b00000000, 0b00000000]);
  let k1 = Key::from_bytes(&[0b10000000, 0b00000000]);
  assert_eq!(0, k0.prefix_match_bits(&k1));

  let k0 = Key::from_bytes(&[0b00000000, 0b00000000]);
  let k1 = Key::from_bytes(&[0b00000000, 0b00000000]);
  assert_eq!(16, k0.prefix_match_bits(&k1));

  let k0 = Key::from_bytes(&[0b11111111, 0b11111111]);
  let k1 = Key::from_bytes(&[0b11111111, 0b11111111]);
  assert_eq!(16, k0.prefix_match_bits(&k1));

  let k0 = Key::from_bytes(&[0b10101010, 0b10101010]);
  let k1 = Key::from_bytes(&[0b10101010, 0b10001010]);
  assert_eq!(10, k0.prefix_match_bits(&k1));
}

#[test]
fn xor_distance() {
  let distances = vec![
    Key([0x00, 0x00]),
    Key([0x00, 0x01]),
    Key([0x00, 0x02]),
    Key([0x01, 0x00]),
    Key([0x01, 0x01]),
    Key([0x01, 0x02]),
    Key([0x02, 0x00]),
    Key([0x02, 0x01]),
    Key([0x02, 0x02]),
  ];

  // 距離またはキーの大小関係が正しく解釈されていることを確認
  for i in 0..distances.len() - 1 {
    assert!(distances[i] < distances[i + 1]);
  }

  // 正しくソートされることを確認
  let mut sorted = distances.clone();
  let mut rng = rand::thread_rng();
  sorted.shuffle(&mut rng);
  assert_ne!(distances, sorted);
  sorted.sort();
  assert_eq!(distances, sorted);
}

#[test]
fn key_to_string() {
  let key = Key::from_bytes(&[]);
  assert_eq!("", key.to_string());

  let key = Key::from_bytes(&[0xAB, 0xCD, 0xEF]);
  assert_eq!("ABCDEF", key.to_string().to_uppercase());
}

fn add<const N: usize, const M: usize>(x: &XorDistance<N>, y: &XorDistance<N>) -> XorDistance<M> {
  let mut overflow = 0;
  let mut z = XorDistance::zero();
  for i in 0..std::cmp::min(N, M) {
    let i_n = N - i - 1;
    let i_m = M - i - 1;
    let r = x.0[i_n] as u16 + y.0[i_n] as u16 + overflow;
    z.0[i_m] = (r & 0xFF) as u8;
    overflow = (r >> 8) & 0xFF;
  }
  if M > N {
    z.0[M - N - 1] = (overflow & 0xFF) as u8;
  }
  z
}

fn bit_or<const N: usize>(ks: Vec<Key<N>>) -> Key<N> {
  let mut k = Key::zero();
  for kk in ks {
    for i in 0..N {
      k.0[i] |= kk.0[i];
    }
  }
  k
}

#[test]
fn x_add() {
  let d0000 = Key([0x00, 0x00]);
  let d0001 = Key([0x00, 0x01]);
  let d0002 = Key([0x00, 0x02]);
  assert_eq!(d0001, add(&d0000, &d0001));
  assert_eq!(Key([0x00, 0x03]), add(&d0001, &d0002));
}

#[test]
fn error() {
  let errors = vec![Error::server_has_been_shutdown(), Error::client_has_been_dropped_oneshot("")];
  for err in &errors {
    assert!(matches!(err, Error::Shutdown { .. }));
  }
}
