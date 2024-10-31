// (C)opyleft 2013-2021 Frank Denis

//! HyperLogLog implementation for Rust
#![crate_name = "hyperloglog"]
#![warn(non_camel_case_types, non_upper_case_globals, unused_qualifications)]
#![allow(non_snake_case)]
#![allow(clippy::unreadable_literal)]

mod weights;
use arcode::{
    bitbit::{BitReader, BitWriter, MSB},
    ArithmeticDecoder, ArithmeticEncoder, EOFKind, Model,
};
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use std::{fmt::Debug, io::Cursor};
use weights::{BIAS_DATA, RAW_ESTIMATE_DATA, THRESHOLD_DATA};

/// An approximate counter for distinct elements.
#[derive(Clone, PartialEq, Eq)]
pub struct HyperLogLog<R>(R);

impl<R> Debug for HyperLogLog<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog").finish_non_exhaustive()
    }
}

impl<R: Registers> Default for HyperLogLog<R> {
    fn default() -> Self {
        Self(R::zero())
    }
}

impl<R: Registers> HyperLogLog<R> {
    /// Count an item if it is distinct.
    pub fn insert<V: Hash>(&mut self, v: &V) {
        self.0.insert(v);
    }

    /// Estimate the number of distinct items inserted.
    pub fn estimate(&self) -> u64 {
        self.0.estimate().round() as u64
    }

    /// Forgets previous insertions.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

#[cfg(feature = "serde")]
thread_local! {
    static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::default();
}

#[cfg(feature = "serde")]
impl<R: Registers> serde::Serialize for HyperLogLog<R> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let compressed = self.0.compress();
        if serializer.is_human_readable() {
            use base64::prelude::*;

            BUFFER.with(|buffer| {
                let mut buffer = buffer.borrow_mut();
                buffer.clear();
                // SAFETY: An empty `Vec<u8>` is always valid UTF-8.
                let mut string =
                    unsafe { String::from_utf8_unchecked(std::mem::take(&mut *buffer)) };
                BASE64_STANDARD_NO_PAD.encode_string(&compressed, &mut string);
                let result = serializer.serialize_str(&string);
                *buffer = string.into_bytes();
                result
            })
        } else {
            serializer.serialize_bytes(&compressed)
        }
    }
}

#[cfg(feature = "serde")]
impl<'de, R: Registers> serde::Deserialize<'de> for HyperLogLog<R> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor<R>(std::marker::PhantomData<R>);
        impl<'de, R: Registers> serde::de::Visitor<'de> for Visitor<R> {
            type Value = HyperLogLog<R>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("hyperloglog base64 str or bytes")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use base64::prelude::*;
                BUFFER.with(|buffer| {
                    let mut buffer = buffer.borrow_mut();
                    buffer.clear();
                    BASE64_STANDARD_NO_PAD
                        .decode_vec(v, &mut *buffer)
                        .map_err(|_| serde::de::Error::custom("hyperloglog invaild base64"))?;
                    let mut ret = HyperLogLog::<R>::default();
                    ret.0
                        .decompress(&buffer)
                        .map_err(|_| serde::de::Error::custom("hyperloglog bytes too short"))?;
                    Ok(ret)
                })
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let mut ret = HyperLogLog::<R>::default();
                ret.0
                    .decompress(v)
                    .map_err(|_| serde::de::Error::custom("hyperloglog bytes too short"))?;
                Ok(ret)
            }
        }
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(Visitor::<R>(std::marker::PhantomData))
        } else {
            deserializer.deserialize_bytes(Visitor::<R>(std::marker::PhantomData))
        }
    }
}

/// Storage for [`HyperLogLog`]. Larger ones are more precise.
pub trait Registers: Clone + PartialEq + Eq {
    /// In the range `4..=18`.
    const PRECISION: u8;
    /// `2^Self::PRECISION`.
    const REGISTERS: usize;
    //const ERROR_RATE: f32;

    fn zero() -> Self;
    /// Length is [`Self::REGISTERS`].
    fn registers(&self) -> &[u8];
    /// Length is [`Self::REGISTERS`].
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

    fn compress(&self) -> Vec<u8> {
        let data = self.registers();

        let mut model = Model::builder()
            .num_symbols(compression_symbols(Self::PRECISION))
            .eof(EOFKind::None)
            .build();
        let compressed = Cursor::new(Vec::new());
        let mut compressed_writer = BitWriter::new(compressed);
        let mut encoder = ArithmeticEncoder::new(COMPRESSION_PRECISION);

        for &sym in data {
            encoder
                .encode(
                    sym.min(64 - Self::PRECISION) as u32,
                    &model,
                    &mut compressed_writer,
                )
                .unwrap();
            model.update_symbol(sym as u32);
        }

        // encoder.encode(model.eof(), &model, &mut compressed_writer).unwrap();
        encoder.finish_encode(&mut compressed_writer).unwrap();
        compressed_writer.pad_to_byte().unwrap();

        compressed_writer.get_ref().get_ref().clone()
    }

    fn decompress(&mut self, data: &[u8]) -> Result<(), ()> {
        let mut model = Model::builder()
            .num_symbols(compression_symbols(Self::PRECISION))
            .eof(EOFKind::None)
            .build();

        let mut input_reader = BitReader::<_, MSB>::new(data);
        let mut decoder = ArithmeticDecoder::new(COMPRESSION_PRECISION);

        for decompressed in self.registers_mut() {
            let sym = decoder.decode(&model, &mut input_reader).map_err(|_| ())?;
            model.update_symbol(sym);
            *decompressed = sym as u8;
        }

        Ok(())
    }
}

const COMPRESSION_PRECISION: u64 = 48;

fn compression_symbols(precision: u8) -> u32 {
    64 + 1 - precision as u32
}

macro_rules! impl_u8_array {
    ($precision:literal, $registers:literal) => {
        impl Registers for [u8; $registers] {
            const PRECISION: u8 = $precision;
            const REGISTERS: usize = $registers;

            fn zero() -> Self {
                [0; $registers]
            }

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

impl_u8_array!(4, 16);
impl_u8_array!(5, 32);
impl_u8_array!(6, 64);
impl_u8_array!(7, 128);
impl_u8_array!(8, 256);
impl_u8_array!(9, 512);
impl_u8_array!(10, 1024);
impl_u8_array!(11, 2048);

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
