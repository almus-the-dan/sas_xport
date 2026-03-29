use std::fmt::{self, Display, Formatter};

/// Represents a 64-bit IBM floating point value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct IbmFloat64 {
    bytes: [u8; 8],
}

impl IbmFloat64 {
    /// Gets the maximum representable value.
    pub const MAX_VALUE: Self = Self::from_be_bytes([
        0x7Fu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8,
    ]);
    /// Gets the minimum representable value.
    pub const MIN_VALUE: Self = Self::from_be_bytes([0xFFu8; 8]);

    /// Initializes a new IBM floating point value, with a value of 0.0.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: [0; 8] }
    }

    /// Initializes a new IBM floating point value from the given byte array,
    /// which is stored in big-endian order.
    #[inline]
    #[must_use]
    pub const fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self { bytes }
    }

    /// Initializes a new IBM floating point value from the given byte array,
    /// which is stored in little-endian order.
    #[inline]
    #[must_use]
    pub const fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self {
            bytes: [
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0],
            ],
        }
    }

    /// Converts the IBM floating point value to a byte array, with the bytes stored
    /// in big-endian order.
    #[inline]
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.bytes
    }

    /// Converts the IBM floating point value to a byte array, with the bytes stored
    /// in little-endian order.
    #[inline]
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 8] {
        [
            self.bytes[7],
            self.bytes[6],
            self.bytes[5],
            self.bytes[4],
            self.bytes[3],
            self.bytes[2],
            self.bytes[1],
            self.bytes[0],
        ]
    }

    /// Indicates whether the value is positive.
    #[inline]
    #[must_use]
    pub const fn is_sign_positive(self) -> bool {
        self.bytes[0] & 0x80u8 == 0
    }

    /// Indicates whether the value is negative.
    #[inline]
    #[must_use]
    pub const fn is_sign_negative(self) -> bool {
        self.bytes[0] & 0x80u8 != 0
    }
}

impl TryFrom<f64> for IbmFloat64 {
    type Error = ();

    /// Converts an `f64` to an `IbmFloat64`.
    ///
    /// - If the magnitude is too small to represent (underflow), zero is returned
    ///   (with the original sign preserved).
    /// - If the magnitude is too large to represent (overflow), the maximum or
    ///   minimum representable value is returned based on sign.
    /// - If the value is NaN, an error is returned.
    /// - If the value is infinite, the maximum or minimum representable value
    ///   is returned based on sign.
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_nan() {
            return Err(());
        }
        if value.is_infinite() {
            return Ok(if value.is_sign_positive() {
                Self::MAX_VALUE
            } else {
                Self::MIN_VALUE
            });
        }

        let ieee8 = u64::from_be_bytes(value.to_be_bytes());
        let ieee1 = (ieee8 >> 32) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let ieee2 = ieee8 as u32;

        // Check for negative by extracting sign bit
        let is_negative = (ieee1 & 0x8000_0000u32) != 0;

        // For zero, return early
        if ieee1 == 0 && ieee2 == 0 {
            return Ok(Self::new());
        }

        // Extract and check exponent bounds before computing potentially-wrapping values
        #[allow(clippy::cast_possible_wrap)]
        let high = ieee1 as i32 >> 16;
        let exponent = ((high & 0x7FF0) >> 4) - 1023;

        // Underflow: magnitude too small, return signed zero
        if exponent < -260 {
            return Ok(if is_negative {
                Self::from_be_bytes([0x80, 0, 0, 0, 0, 0, 0, 0])
            } else {
                Self::new()
            });
        }

        // Overflow: magnitude too large, saturate to max/min based on sign
        if exponent > 248 {
            return Ok(if is_negative {
                Self::MIN_VALUE
            } else {
                Self::MAX_VALUE
            });
        }

        // Now safe to compute IBM format values - exponent is in valid range
        let mut xport1 = ieee1 & 0x000F_FFFFu32;
        let mut xport2 = ieee2;

        let shift = exponent & 0x03;
        xport1 |= 0x0010_0000u32;
        if shift != 0 {
            xport1 <<= shift;
            xport1 |= ((ieee2 >> 24) & 0xE0) >> (5 + (3 - shift));
            xport2 <<= shift;
        }

        // exponent is now guaranteed to be in range [-260, 248], so this won't wrap
        #[allow(clippy::cast_sign_loss)]
        let ibm_exponent = ((exponent >> 2) + 65) as u32;

        // Debug assertion to verify IBM exponent is in valid 7-bit range
        debug_assert!(
            ibm_exponent <= 127,
            "IBM exponent out of valid range: {ibm_exponent} (from IEEE exponent {exponent})"
        );

        xport1 |= (ibm_exponent | ((ieee1 >> 24) & 0x80)) << 24;

        let temp = (u64::from(xport1) << 32) | u64::from(xport2);
        Ok(Self {
            bytes: temp.to_be_bytes(),
        })
    }
}

impl From<IbmFloat64> for f64 {
    /// Convert an `IbmFloat64` to an `f64`.
    fn from(value: IbmFloat64) -> f64 {
        let temp = u64::from_be_bytes(value.bytes);
        let sign = temp & 0x8000_0000_0000_0000u64; // Leave unshifted
        let ibm_fraction = temp & 0x00FF_FFFF_FFFF_FFFFu64;

        // Quick return for zeros.
        if ibm_fraction == 0 {
            return f64::from_bits(sign);
        }

        #[allow(clippy::cast_possible_wrap)]
        let shift = ibm_fraction.leading_zeros() as i32 - 8;
        let ibm_exponent = (temp & 0x7F00_0000_0000_0000u64) >> 56;
        #[allow(clippy::cast_possible_truncation)]
        let ibm_exponent = (ibm_exponent << 2) as i32 - shift;
        let ibm_fraction = ibm_fraction << shift;

        let ieee_exponent = ibm_exponent + 765;

        // Right-shift by 3 bits (the difference between the IBM and IEEE significand lengths)
        let ieee_fraction = ibm_fraction >> 3;

        // Debug assertions to catch unexpected values that would cause wrapping
        debug_assert!(
            (0..=2_046).contains(&ieee_exponent),
            "IEEE exponent out of valid range: {} (IBM bytes: {:02X?})",
            ieee_exponent,
            value.bytes
        );
        debug_assert!(
            ieee_fraction < (1u64 << 53),
            "IEEE fraction exceeds 53 bits: {:016X} (IBM bytes: {:02X?})",
            ieee_fraction,
            value.bytes
        );

        #[allow(clippy::cast_sign_loss)]
        let ieee = sign
            .wrapping_add((ieee_exponent as u64) << 52)
            .wrapping_add(ieee_fraction);
        f64::from_bits(ieee)
    }
}

impl Display for IbmFloat64 {
    /// Displays the `IbmFloat64` by converting it to an `f64` and formatting it.
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&f64::from(*self), formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use float_cmp::assert_approx_eq;

    #[test]
    fn new_as_bytes_returns_slice() {
        let x = IbmFloat64::new();
        assert_eq!([0u8; 8], x.to_be_bytes());
    }

    #[test]
    fn round_trip_zero() {
        let bytes = [0u8; 8];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 0f64, f);

        assert!(x.is_sign_positive());

        let reversed = IbmFloat64::try_from(f).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_one() {
        let bytes = [
            0x41u8, 0x10u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8,
        ];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 1f64, f);

        assert!(x.is_sign_positive());

        let reversed = IbmFloat64::try_from(f).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_negative_one() {
        let bytes = [
            0xC1u8, 0x10u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8, 0x00u8,
        ];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, -1f64, f);

        assert!(x.is_sign_negative());

        let reversed = IbmFloat64::try_from(f).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_negative_118_625() {
        let bytes = [
            0b1100_0010u8,
            0b0111_0110u8,
            0b1010_0000u8,
            0x00u8,
            0x00u8,
            0x00u8,
            0x00u8,
            0x00u8,
        ];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, -118.625, f);

        assert!(x.is_sign_negative());

        let reversed = IbmFloat64::try_from(f).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_7_2370051e75() {
        let bytes = [
            0x7Fu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8,
        ];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        // Due to the ridiculous scale of the number, we divide by 1.0e75 to
        // eliminate most of the precision. Then we divide by another 6 digits
        // to put everything on the left-side of the decimal. Then we truncate.
        let truncated = f64::trunc(f / 1.0e69);
        assert_approx_eq!(f64, 7_237_005.0, truncated);

        assert!(x.is_sign_positive());

        let reversed = IbmFloat64::try_from(f).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_5_397605e78() {
        let bytes = [
            0x00u8, 0x9Fu8, 0xFFu8, 0xFFu8, 0x53u8, 0x76u8, 0x2Eu8, 0x78u8,
        ];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 5.397_605e-78, f);

        assert!(x.is_sign_positive());

        let reversed = IbmFloat64::try_from(5.397_605e-78).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn from_nan_fails() {
        let result = IbmFloat64::try_from(f64::NAN);
        assert!(result.is_err());
    }

    #[test]
    fn from_negative_infinity_translates() {
        let ibm = IbmFloat64::try_from(f64::NEG_INFINITY).unwrap();
        assert_eq!(IbmFloat64::MIN_VALUE, ibm);
    }

    #[test]
    fn from_positive_infinity_translates() {
        let ibm = IbmFloat64::try_from(f64::INFINITY).unwrap();
        assert_eq!(IbmFloat64::MAX_VALUE, ibm);
    }

    #[test]
    fn underflow_positive_returns_positive_zero() {
        // Very small positive number that underflows IBM range
        let tiny = 1.0e-310;
        let ibm = IbmFloat64::try_from(tiny).unwrap();
        assert!(ibm.is_sign_positive());
        assert_approx_eq!(f64, 0.0, f64::from(ibm));
    }

    #[test]
    fn underflow_negative_returns_negative_zero() {
        // Very small negative number that underflows IBM range
        let tiny = -1.0e-310;
        let ibm = IbmFloat64::try_from(tiny).unwrap();
        assert!(ibm.is_sign_negative());
        assert_approx_eq!(f64, 0.0, f64::from(ibm));
    }

    #[test]
    fn overflow_positive_returns_max_value() {
        // Large positive number that overflows IBM range
        let huge = 1.0e300;
        let ibm = IbmFloat64::try_from(huge).unwrap();
        assert_eq!(IbmFloat64::MAX_VALUE, ibm);
    }

    #[test]
    fn overflow_negative_returns_min_value() {
        // Large negative number that overflows IBM range
        let huge = -1.0e300;
        let ibm = IbmFloat64::try_from(huge).unwrap();
        assert_eq!(IbmFloat64::MIN_VALUE, ibm);
    }

    #[test]
    fn positive_zero_round_trips() {
        let ibm = IbmFloat64::try_from(0.0f64).unwrap();
        assert!(ibm.is_sign_positive());
        let f = f64::from(ibm);
        assert_approx_eq!(f64, 0.0, f);
        assert!(f.is_sign_positive());
    }

    #[test]
    fn negative_zero_round_trips() {
        let ibm = IbmFloat64::try_from(-0.0f64).unwrap();
        // Note: An IBM format preserves signed zeros
        assert!(ibm.is_sign_negative());
        let f = f64::from(ibm);
        assert_approx_eq!(f64, 0.0, f);
        // When converted back to IEEE, the sign should be preserved
        assert!(f.is_sign_negative());
    }

    #[test]
    fn near_max_ibm_range_positive() {
        // A value well within the IBM range (max is ~7.2e75)
        let near_max = 1.0e70;
        let ibm = IbmFloat64::try_from(near_max).unwrap();
        let f = f64::from(ibm);
        // IBM double has 53-56 bits precision, so the relative error should be small
        let relative_error = (f - near_max).abs() / near_max;
        assert!(relative_error < 1e-13, "relative error: {relative_error}");
    }

    #[test]
    fn near_min_ibm_range_positive() {
        // A value near the minimum IBM range (~5.4e-79)
        let near_min = 6.0e-79;
        let ibm = IbmFloat64::try_from(near_min).unwrap();
        let f = f64::from(ibm);
        // Should round-trip approximately
        assert!((f - near_min).abs() / near_min < 1e-10);
    }

    // Additional test vectors from MathWorks and IBM documentation

    #[test]
    fn round_trip_0_1() {
        // 0.1 is a repeating fraction in both binary and hex
        // From MathWorks: 0.1 → 401999999999999A
        let bytes = [0x40, 0x19, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 0.1, f, epsilon = 1e-15);

        // Round-trip from IEEE
        let reversed = IbmFloat64::try_from(0.1).unwrap();
        let f2 = f64::from(reversed);
        assert_approx_eq!(f64, 0.1, f2, epsilon = 1e-15);
    }

    #[test]
    fn round_trip_pi() {
        // π from MathWorks: C13243F6A8885A30 (negative), so positive is 413243F6A8885A30
        let bytes = [0x41, 0x32, 0x43, 0xF6, 0xA8, 0x88, 0x5A, 0x30];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, std::f64::consts::PI, f, epsilon = 1e-14);

        assert!(x.is_sign_positive());

        // Round-trip from IEEE
        let reversed = IbmFloat64::try_from(std::f64::consts::PI).unwrap();
        let f2 = f64::from(reversed);
        assert_approx_eq!(f64, std::f64::consts::PI, f2, epsilon = 1e-14);
    }

    #[test]
    fn round_trip_negative_pi() {
        // -π from MathWorks: C13243F6A8885A30
        let bytes = [0xC1, 0x32, 0x43, 0xF6, 0xA8, 0x88, 0x5A, 0x30];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, -std::f64::consts::PI, f, epsilon = 1e-14);

        assert!(x.is_sign_negative());
    }

    #[test]
    fn round_trip_two() {
        // 2.0 = 0.2 × 16^1 = 0x42 0x20 ...
        let bytes = [0x41, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 2.0, f);

        let reversed = IbmFloat64::try_from(2.0).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_half() {
        // 0.5 = 0.8 × 16^0 = 0x40 0x80 ...
        let bytes = [0x40, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 0.5, f);

        let reversed = IbmFloat64::try_from(0.5).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_quarter() {
        // 0.25 = 0.4 × 16^0 = 0x40 0x40 ...
        let bytes = [0x40, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 0.25, f);

        let reversed = IbmFloat64::try_from(0.25).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_sixteen() {
        // 16.0 = 0.1 × 16^2 = exponent 66 = 0x42
        let bytes = [0x42, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let x = IbmFloat64::from_be_bytes(bytes);
        let f = f64::from(x);
        assert_approx_eq!(f64, 16.0, f);

        let reversed = IbmFloat64::try_from(16.0).unwrap();
        assert_eq!(bytes, reversed.bytes);
    }

    #[test]
    fn round_trip_one_tenth() {
        // Verify 0.1 round-trips correctly (it's a repeating fraction)
        let original = 0.1f64;
        let ibm = IbmFloat64::try_from(original).unwrap();
        let back = f64::from(ibm);
        // Should be very close but may have small representation error
        assert_approx_eq!(f64, original, back, epsilon = 1e-15);
    }

    #[test]
    fn round_trip_large_integer() {
        // Test a large integer value: 1_000_000
        let original = 1_000_000.0f64;
        let ibm = IbmFloat64::try_from(original).unwrap();
        let back = f64::from(ibm);
        assert_approx_eq!(f64, original, back);
    }

    #[test]
    fn round_trip_small_fraction() {
        // Test a small fraction: 0.000001
        let original = 0.000_001f64;
        let ibm = IbmFloat64::try_from(original).unwrap();
        let back = f64::from(ibm);
        assert_approx_eq!(f64, original, back, epsilon = 1e-20);
    }

    #[test]
    #[ignore = "Prints constant values for verification"]
    fn verify_constants() {
        println!("\n=== IbmFloat64 Constants ===");
        println!(
            "MAX_VALUE bytes: {:02X?}",
            IbmFloat64::MAX_VALUE.to_be_bytes()
        );
        println!("MAX_VALUE as f64: {:e}", f64::from(IbmFloat64::MAX_VALUE));
        println!(
            "MIN_VALUE bytes: {:02X?}",
            IbmFloat64::MIN_VALUE.to_be_bytes()
        );
        println!("MIN_VALUE as f64: {:e}", f64::from(IbmFloat64::MIN_VALUE));

        // Smallest positive denormalized
        let smallest_denorm =
            IbmFloat64::from_be_bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
        println!("\nSmallest positive (denormalized):");
        println!("  Bytes: {:02X?}", smallest_denorm.to_be_bytes());
        println!("  As f64: {:e}", f64::from(smallest_denorm));

        // Smallest normalized
        let smallest_norm =
            IbmFloat64::from_be_bytes([0x01, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        println!("\nSmallest positive (normalized):");
        println!("  Bytes: {:02X?}", smallest_norm.to_be_bytes());
        println!("  As f64: {:e}", f64::from(smallest_norm));
    }

    #[test]
    #[ignore = "Performance benchmark"]
    fn benchmark_conversions() {
        const ITERATIONS: usize = 1_000_000;

        use std::time::Instant;

        // Test data: various IBM float byte patterns
        let test_bytes: Vec<[u8; 8]> = vec![
            [0x41, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], // 1.0
            [0xC1, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], // -1.0
            [0x42, 0x76, 0xA0, 0x00, 0x00, 0x00, 0x00, 0x00], // 118.625
            [0x41, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF], // arbitrary
            [0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01], // small positive
        ];

        // Benchmark IBM -> IEEE conversion
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            for bytes in &test_bytes {
                let ibm = IbmFloat64::from_be_bytes(*bytes);
                let _ = std::hint::black_box(f64::from(ibm));
            }
        }
        let ibm_to_ieee_duration = start.elapsed();

        // Benchmark IEEE -> IBM conversion
        let test_f64s: Vec<f64> = test_bytes
            .iter()
            .map(|b| f64::from(IbmFloat64::from_be_bytes(*b)))
            .collect();

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            for &f in &test_f64s {
                let _ = std::hint::black_box(IbmFloat64::try_from(f));
            }
        }
        let ieee_to_ibm_duration = start.elapsed();

        println!(
            "IBM -> IEEE: {:?} ({} conversions)",
            ibm_to_ieee_duration,
            ITERATIONS * test_bytes.len()
        );
        println!(
            "IEEE -> IBM: {:?} ({} conversions)",
            ieee_to_ibm_duration,
            ITERATIONS * test_f64s.len()
        );
    }
}
