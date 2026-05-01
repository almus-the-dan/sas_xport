//! IBM HFP ↔ IEEE-754 conversion throughput.
//!
//! - `ibm_to_ieee`: head-to-head between `sas_xport::ibm::IbmFloat64` and `ibmfloat::F64`.
//! - `ieee_to_ibm`: `IbmFloat64` only — `ibmfloat` 0.1.1 does not implement this direction.
//!   This baseline establishes the cost of the writer hot path.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use sas_xport::ibm::IbmFloat64;

const N: usize = 10_000;

/// Deterministic 64-bit PRNG (splitmix64) so the input set is identical across runs
/// and across the two implementations.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Build a fixed input set: hand-picked edge cases first, then a pseudo-random tail.
/// Every 8-byte pattern is a valid IBM HFP value (no NaN/inf in the format), so we
/// can feed raw random bytes without filtering.
fn build_inputs() -> Vec<[u8; 8]> {
    let mut out: Vec<[u8; 8]> = Vec::with_capacity(N);

    // Edge cases that exercise distinct code paths in both implementations:
    // signed zero (early-exit), unit values, sign-flip, near-MAX/MIN, the
    // smallest normal and smallest denormalized values, and a couple of
    // well-known reference patterns (π, 0.1).
    let edge_cases: &[[u8; 8]] = &[
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], // +0
        [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], // -0
        [0x41, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], //  1.0
        [0xC1, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], // -1.0
        [0x42, 0x76, 0xA0, 0x00, 0x00, 0x00, 0x00, 0x00], //  118.625
        [0x41, 0x32, 0x43, 0xF6, 0xA8, 0x88, 0x5A, 0x30], //  π
        [0x40, 0x19, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A], //  0.1
        [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], //  MAX_VALUE
        [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], //  MIN_VALUE
        [0x01, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], //  smallest normal
        [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01], //  smallest denormal
    ];
    out.extend_from_slice(edge_cases);

    let mut state = 0xDEAD_BEEF_CAFE_BABE_u64;
    while out.len() < N {
        out.push(splitmix64(&mut state).to_be_bytes());
    }

    out
}

/// Build a fixed input set of f64 values guaranteed to be in the IBM-representable
/// range, so `IbmFloat64::try_from` never hits the over/underflow or NaN branches.
/// Edge cases first, pseudo-random tail clamped by exponent.
fn build_f64_inputs() -> Vec<f64> {
    // IBM HFP 64-bit covers roughly 5.4e-79 .. 7.2e75. The corresponding biased
    // IEEE exponent range is ~[763, 1275]; clamp into [768, 1278) for a safe margin.
    // This produces random sign + random mantissa, never zero/inf/NaN.
    const EXP_LO: u64 = 768;
    const EXP_RANGE: u64 = 510;

    let mut out: Vec<f64> = Vec::with_capacity(N);

    let edge_cases: &[f64] = &[
        0.0,
        -0.0,
        1.0,
        -1.0,
        118.625,
        std::f64::consts::PI,
        0.1,
        1.0e75,
        -1.0e75,
        1.0e-78,
        -1.0e-78,
    ];
    out.extend_from_slice(edge_cases);

    let mut state = 0xDEAD_BEEF_CAFE_BABE_u64;
    while out.len() < N {
        let bits = splitmix64(&mut state);
        let exp = (splitmix64(&mut state) % EXP_RANGE) + EXP_LO;
        let f_bits = (bits & 0x800F_FFFF_FFFF_FFFF) | (exp << 52);
        out.push(f64::from_bits(f_bits));
    }

    out
}

fn ibm_to_ieee(c: &mut Criterion) {
    let inputs = build_inputs();

    let mut group = c.benchmark_group("ibm_to_ieee");
    group.throughput(Throughput::Elements(inputs.len() as u64));

    group.bench_function("sas_xport", |b| {
        b.iter(|| {
            inputs.iter().fold(0u64, |acc, &bytes| {
                let ibm = IbmFloat64::from_be_bytes(bytes);
                acc ^ f64::from(ibm).to_bits()
            })
        });
    });

    group.bench_function("ibmfloat", |b| {
        b.iter(|| {
            inputs.iter().fold(0u64, |acc, &bytes| {
                let ibm = ibmfloat::F64::from_be_bytes(bytes);
                acc ^ f64::from(ibm).to_bits()
            })
        });
    });

    group.finish();
}

fn ieee_to_ibm(c: &mut Criterion) {
    let inputs = build_f64_inputs();

    let mut group = c.benchmark_group("ieee_to_ibm");
    group.throughput(Throughput::Elements(inputs.len() as u64));

    group.bench_function("sas_xport", |b| {
        b.iter(|| {
            inputs.iter().fold(0u64, |acc, &f| {
                let ibm = IbmFloat64::try_from(f).expect("input is in IBM range");
                acc ^ u64::from_be_bytes(ibm.to_be_bytes())
            })
        });
    });

    group.finish();
}

criterion_group!(benches, ibm_to_ieee, ieee_to_ibm);
criterion_main!(benches);
