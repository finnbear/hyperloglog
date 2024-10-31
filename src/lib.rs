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
use std::io::{Cursor, Result};
use std::{
    hash::{Hash, Hasher},
    io::ErrorKind,
};
use weights::{BIAS_DATA, RAW_ESTIMATE_DATA, THRESHOLD_DATA};

pub enum DecompressionError {
    UnexpectedEof,
    ExpectedEof,
}

/// A HyperLogLog counter. Length must be a power of 2 from 2^4 to 2^18, inclusive.
pub trait HyperLogLog {
    /// In the range `4..=18``.
    const PRECISION: u8;
    /// `2^Self::PRECISION``.
    #[doc(hidden)]
    const REGISTERS: usize;
    //const ERROR_RATE: f32;

    /// Length is `Self::REGISTERS``.
    #[doc(hidden)]
    fn registers(&self) -> &[u8];
    /// Length is `Self::REGISTERS``.
    #[doc(hidden)]
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
        let compressed = Cursor::new(vec![]);
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

    fn decompress(&mut self, data: &[u8]) -> Result<()> {
        let mut model = Model::builder()
            .num_symbols(compression_symbols(Self::PRECISION))
            .eof(EOFKind::None)
            .build();

        let mut input_reader = BitReader::<_, MSB>::new(data);
        let mut decoder = ArithmeticDecoder::new(COMPRESSION_PRECISION);

        for decompressed in self.registers_mut() {
            let sym = decoder
                .decode(&model, &mut input_reader)
                .map_err(|_| std::io::Error::new(ErrorKind::UnexpectedEof, "unexpected EOF"))?;
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

impl_u8_array!(4, 16);
impl_u8_array!(5, 32);
impl_u8_array!(6, 64);
impl_u8_array!(7, 128);
impl_u8_array!(8, 256);
impl_u8_array!(9, 512);
impl_u8_array!(10, 1024);

macro_rules! impl_uint_array {
    ($precision:literal, $registers:literal, $uint:ident, $cells:literal) => {
        impl HyperLogLog for [$uint; $cells] {
            const PRECISION: u8 = $precision;
            const REGISTERS: usize = $registers;

            fn registers(&self) -> &[u8] {
                debug_assert_eq!(
                    $registers,
                    std::mem::size_of_val(&self[0]) * $cells,
                    "size = {}, cells = {}",
                    std::mem::size_of_val(self),
                    $cells
                );
                bytemuck::must_cast_slice(self)
            }

            fn registers_mut(&mut self) -> &mut [u8] {
                bytemuck::must_cast_slice_mut(self)
            }
        }
    };
}

impl_uint_array!(4, 16, u32, 4);
impl_uint_array!(5, 32, u32, 8);
impl_uint_array!(6, 64, u32, 16);
impl_uint_array!(7, 128, u32, 32);
impl_uint_array!(8, 256, u32, 64);
impl_uint_array!(9, 512, u32, 128);
impl_uint_array!(10, 1024, u32, 256);

impl_uint_array!(4, 16, u64, 2);
impl_uint_array!(5, 32, u64, 4);
impl_uint_array!(6, 64, u64, 8);
impl_uint_array!(7, 128, u64, 16);
impl_uint_array!(8, 256, u64, 32);
impl_uint_array!(9, 512, u64, 64);
impl_uint_array!(10, 1024, u64, 128);

impl_uint_array!(5, 32, u128, 2);
impl_uint_array!(6, 64, u128, 4);
impl_uint_array!(7, 128, u128, 8);
impl_uint_array!(8, 256, u128, 16);
impl_uint_array!(9, 512, u128, 32);
impl_uint_array!(10, 1024, u128, 64);

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
