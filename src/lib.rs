// (C)opyleft 2013-2021 Frank Denis

//! HyperLogLog implementation for Rust
#![crate_name = "hyperloglog"]
#![warn(non_camel_case_types, non_upper_case_globals, unused_qualifications)]
#![allow(non_snake_case)]
#![allow(clippy::unreadable_literal)]

mod weights;
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use weights::{BIAS_DATA, RAW_ESTIMATE_DATA, THRESHOLD_DATA};

/// A HyperLogLog counter. Length must be a power of 2 from 2^4 to 2^18, inclusive.
pub trait HyperLogLog {
    /// In the range `4..=18``.
    const PRECISION: u8;
    /// `2^Self::PRECISION``.
    const REGISTERS: usize;
    //const ERROR_RATE: f32;

    /// Length is `Self::REGISTERS``.
    fn registers(&self) -> &[u8];
    /// Length is `Self::REGISTERS``.
    fn registers_mut(&mut self) -> &mut [u8];

    /// Insert a new value into the `HyperLogLog` counter.
    fn insert<V: Hash>(&mut self, value: &V) {
        let mut sip = SipHasher13::new_with_keys(0x1337_1337, 0x123456789);
        value.hash(&mut sip);
        let x = sip.finish();

        // Insert by hash values.
        let j = x as usize & (Self::REGISTERS - 1);
        let w = x >> Self::PRECISION;
        let rho = get_rho(w, 64 - Self::PRECISION);
        let mjr = &mut self.registers_mut()[j];
        if rho > *mjr {
            *mjr = rho;
        }
    }

    fn estimate(&self) -> f64 {
        let registers = self.registers();
        let number_of_zero_registers = bytecount::count(registers, 0);
        if number_of_zero_registers > 0 {
            let estimate = Self::REGISTERS as f64
                * (Self::REGISTERS as f64 / number_of_zero_registers as f64).ln();
            if estimate <= get_threshold(Self::PRECISION) {
                return estimate;
            }
        }

        // ep
        let sum: f64 = registers.iter().map(|&x| 2.0f64.powi(-(x as i32))).sum();
        let estimate = get_alpha(Self::PRECISION) * Self::REGISTERS.pow(2) as f64 / sum;
        if estimate <= (5 * registers.len()) as f64 {
            estimate - estimate_bias(estimate, Self::PRECISION)
        } else {
            estimate
        }
    }

    /// Merge another `HyperLogLog` counter into the current one.
    fn merge(&mut self, src: &Self) {
        let src_registers = src.registers();
        for (i, mir) in self.registers_mut().iter_mut().enumerate() {
            *mir = (*mir).max(src_registers[i]);
        }
    }

    /// Wipe the `HyperLogLog` counter.
    fn clear(&mut self) {
        self.registers_mut().fill(0);
    }
}

macro_rules! impl_array {
    ($precision:literal, $registers:literal) => {
        impl HyperLogLog for [u8; $registers] {
            const PRECISION: u8 = $precision;
            const REGISTERS: usize = $registers;

            #[inline(always)]
            fn registers(&self) -> &[u8] {
                self
            }

            #[inline(always)]
            fn registers_mut(&mut self) -> &mut [u8] {
                self
            }
        }
    };
}

impl_array!(4, 16);
impl_array!(5, 32);
impl_array!(6, 64);
impl_array!(7, 128);
impl_array!(8, 256);
/*
impl_array!(9, 512);
impl_array!(10, 1024);
impl_array!(11, 2048);
impl_array!(12, 4096);
impl_array!(13, 8192);
impl_array!(14, 16384);
impl_array!(15, 32768);
impl_array!(16, 65536);
impl_array!(17, 131072);
impl_array!(18, 262144);
*/

/*
impl HyperLogLog for [u32; 4] {
    const PRECISION: u8 = 4;
    const REGISTERS: usize = 16;

    fn registers(&self) -> &[u8] {
        bytemuck::cast_slice(self)
    }

    fn registers_mut(&mut self) -> &mut [u8] {
        bytemuck::cast_slice_mut(self)
    }
}

impl HyperLogLog for u128 {
    const PRECISION: u8 = 4;
    const REGISTERS: usize = 16;

    fn registers(&self) -> &[u8] {
        bytemuck::cast_slice(std::slice::from_ref(self))
    }

    fn registers_mut(&mut self) -> &mut [u8] {
        bytemuck::cast_slice_mut(std::slice::from_mut(self))
    }
}
*/

fn get_threshold(p: u8) -> f64 {
    THRESHOLD_DATA[p as usize - 4]
}

fn get_alpha(p: u8) -> f64 {
    assert!(p >= 4);
    assert!(p <= 18);
    match p {
        4 => 0.673,
        5 => 0.697,
        6 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / (1usize << (p as usize)) as f64),
    }
}

fn bit_length(x: u64) -> u8 {
    (64 - x.leading_zeros()) as u8
}

fn get_rho(w: u64, max_width: u8) -> u8 {
    let rho = max_width - bit_length(w) + 1;
    assert!(rho > 0);
    rho
}

fn estimate_bias(estimate: f64, p: u8) -> f64 {
    let bias_vector = BIAS_DATA[(p - 4) as usize];
    let estimate_vector = RAW_ESTIMATE_DATA[(p - 4) as usize];

    // Since the estimates are sorted, we can use a partition point to find the nearest neighbors
    let partition_point = estimate_vector.partition_point(|&x| x < estimate);

    let mut min = if partition_point > 6 {
        partition_point - 6
    } else {
        0
    };
    let mut max = core::cmp::min(partition_point + 6, estimate_vector.len());

    while max - min != 6 {
        let (min_val, max_val) = (estimate_vector[min], estimate_vector[max - 1]);
        if 2.0 * estimate - min_val > max_val {
            min += 1;
        } else {
            max -= 1;
        }
    }

    (min..max).map(|i| bias_vector[i]).sum::<f64>() / 6.0
}

/*
assert!(error_rate > 0.0 && error_rate < 1.0);
let p = (f64::log2(1.04 / error_rate) * 2.0).ceil() as u8;
*/

#[test]
fn hyperloglog_test_simple() {
    let mut hll = [0u8; 16];
    let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
    for k in &keys {
        hll.insert(k);
    }
    assert!((hll.estimate().round() - 3.0).abs() < std::f64::EPSILON);
    hll.clear();
    assert!(hll.estimate() == 0.0);
}

#[test]
fn hyperloglog_test_merge() {
    let mut hll = [0u8; 64];
    let keys = ["test1", "test2", "test3", "test2", "test2", "test2"];
    for k in &keys {
        hll.insert(k);
    }
    assert!((hll.estimate().round() - 3.0).abs() < std::f64::EPSILON);

    let mut hll2 = [0u8; 64];
    let keys2 = ["test3", "test4", "test4", "test4", "test4", "test1"];
    for k in &keys2 {
        hll2.insert(k);
    }
    assert!(
        (hll2.estimate().round() - 3.0).abs() < std::f64::EPSILON,
        "{}",
        hll2.estimate().round()
    );

    hll.merge(&hll2);
    assert!((hll.estimate().round() - 4.0).abs() < std::f64::EPSILON);
}
