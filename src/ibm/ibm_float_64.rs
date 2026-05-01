use std::fmt::{self, Display, Formatter, LowerExp, UpperExp};
use std::str::FromStr;

use super::{IbmFloat64Error, ParseIbmFloat64Error};

/// Represents a 64-bit IBM hexadecimal floating point value.
///
/// Equality, ordering, and hashing are over the underlying `[u8; 8]` bit pattern,
/// not the numeric value the bytes represent. Multiple byte patterns can encode
/// the same numeric value (notably any byte with a zero mantissa is numerically
/// zero); this type treats them as distinct. Convert to `f64` for numeric
/// comparison.
///
/// `Ord`/`PartialOrd` derive lexicographic byte order, which is *not* numeric
/// order: negative values sort after positive values because the sign bit is set.
/// Convert to `f64` for numeric comparison.
///
/// See `src/ibm/README.md` for the IBM-to-IEEE rounding-mode rationale and other
/// crate-level conventions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    type Error = IbmFloat64Error;

    /// Strictly converts an `f64` to an `IbmFloat64`.
    ///
    /// Any input that cannot be faithfully represented returns an error
    /// describing the failure mode. Callers that want saturating semantics
    /// should match on the error variants and substitute `MAX_VALUE`,
    /// `MIN_VALUE`, or signed zero as appropriate.
    ///
    /// - NaN → [`IbmFloat64Error::NotANumber`].
    /// - ±Infinity → [`IbmFloat64Error::PositiveInfinity`] / [`IbmFloat64Error::NegativeInfinity`].
    /// - Magnitude exceeds `MAX_VALUE` / falls below `MIN_VALUE` → the
    ///   corresponding `Overflow` variant.
    /// - Nonzero magnitude smaller than the smallest representable IBM value
    ///   → the corresponding `Underflow` variant.
    /// - `+0.0` and `-0.0` succeed and preserve sign.
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_nan() {
            return Err(IbmFloat64Error::NotANumber);
        }
        if value.is_infinite() {
            return Err(if value.is_sign_positive() {
                IbmFloat64Error::PositiveInfinity
            } else {
                IbmFloat64Error::NegativeInfinity
            });
        }

        // Both signed zeros are representable IBM HFP values; handle them
        // before the bit extraction so the underflow path doesn't see them.
        if value == 0.0 {
            return Ok(if value.is_sign_negative() {
                Self::from_be_bytes([0x80, 0, 0, 0, 0, 0, 0, 0])
            } else {
                Self::new()
            });
        }

        let ieee8 = u64::from_be_bytes(value.to_be_bytes());
        let ieee1 = (ieee8 >> 32) as u32;
        #[allow(clippy::cast_possible_truncation)]
        let ieee2 = ieee8 as u32;

        let is_negative = (ieee1 & 0x8000_0000u32) != 0;

        // Extract and check exponent bounds before computing potentially-wrapping values
        #[allow(clippy::cast_possible_wrap)]
        let high = ieee1 as i32 >> 16;
        let exponent = ((high & 0x7FF0) >> 4) - 1023;

        if exponent < -260 {
            return Err(if is_negative {
                IbmFloat64Error::NegativeUnderflow
            } else {
                IbmFloat64Error::PositiveUnderflow
            });
        }

        // Overflow: largest representable IBM characteristic is 127, which
        // corresponds to IEEE exponent 251 = (127 - 65) << 2 + 3. Anything
        // strictly above 251 cannot fit in 7 bits of IBM characteristic.
        if exponent > 251 {
            return Err(if is_negative {
                IbmFloat64Error::NegativeOverflow
            } else {
                IbmFloat64Error::PositiveOverflow
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

        // exponent is now guaranteed to be in range [-260, 251], so this won't wrap
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

impl LowerExp for IbmFloat64 {
    /// Formats the `IbmFloat64` in lowercase scientific notation by converting it
    /// to an `f64` and forwarding to `f64`'s `LowerExp` impl.
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        LowerExp::fmt(&f64::from(*self), formatter)
    }
}

impl UpperExp for IbmFloat64 {
    /// Formats the `IbmFloat64` in uppercase scientific notation by converting it
    /// to an `f64` and forwarding to `f64`'s `UpperExp` impl.
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        UpperExp::fmt(&f64::from(*self), formatter)
    }
}

impl FromStr for IbmFloat64 {
    type Err = ParseIbmFloat64Error;

    /// Parses an `IbmFloat64` by first parsing the input as an `f64` and then
    /// converting via `TryFrom<f64>`. Failures from either step are surfaced
    /// via the corresponding [`ParseIbmFloat64Error`] variant.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = s.parse::<f64>()?;
        let value = Self::try_from(value)?;
        Ok(value)
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

        // f64 → IBM is lossy: IBM HFP's 56-bit mantissa cannot survive a
        // round-trip through f64's 53-bit mantissa (3 low bits truncated).
        // The conversion succeeds, but the bottom 3 bits zero out — so we
        // assert f64-level round-trip rather than byte equality.
        let reversed = IbmFloat64::try_from(f).unwrap();
        assert_eq!(f.to_bits(), f64::from(reversed).to_bits());
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
    fn from_nan_errors() {
        let result = IbmFloat64::try_from(f64::NAN);
        assert_eq!(Err(IbmFloat64Error::NotANumber), result);
    }

    #[test]
    fn from_negative_infinity_errors() {
        let result = IbmFloat64::try_from(f64::NEG_INFINITY);
        assert_eq!(Err(IbmFloat64Error::NegativeInfinity), result);
    }

    #[test]
    fn from_positive_infinity_errors() {
        let result = IbmFloat64::try_from(f64::INFINITY);
        assert_eq!(Err(IbmFloat64Error::PositiveInfinity), result);
    }

    #[test]
    fn underflow_positive_errors() {
        // Very small positive number, smaller than the smallest representable IBM HFP value.
        let tiny = 1.0e-310;
        let result = IbmFloat64::try_from(tiny);
        assert_eq!(Err(IbmFloat64Error::PositiveUnderflow), result);
    }

    #[test]
    fn underflow_negative_errors() {
        let tiny = -1.0e-310;
        let result = IbmFloat64::try_from(tiny);
        assert_eq!(Err(IbmFloat64Error::NegativeUnderflow), result);
    }

    #[test]
    fn overflow_positive_errors() {
        let huge = 1.0e300;
        let result = IbmFloat64::try_from(huge);
        assert_eq!(Err(IbmFloat64Error::PositiveOverflow), result);
    }

    #[test]
    fn overflow_negative_errors() {
        let huge = -1.0e300;
        let result = IbmFloat64::try_from(huge);
        assert_eq!(Err(IbmFloat64Error::NegativeOverflow), result);
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
    fn hash_is_consistent_with_eq() {
        use std::collections::HashSet;

        let bytes = [0x41, 0x10, 0, 0, 0, 0, 0, 0];
        let a = IbmFloat64::from_be_bytes(bytes);
        let b = IbmFloat64::from_be_bytes(bytes);

        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn hash_distinguishes_signed_zeros() {
        use std::collections::HashSet;

        let plus_zero = IbmFloat64::from_be_bytes([0; 8]);
        let minus_zero = IbmFloat64::from_be_bytes([0x80, 0, 0, 0, 0, 0, 0, 0]);

        // Bit-exact equality: +0 and -0 are different bit patterns.
        assert_ne!(plus_zero, minus_zero);

        let mut set = HashSet::new();
        set.insert(plus_zero);
        set.insert(minus_zero);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn lower_exp_formats_via_f64() {
        let ibm = IbmFloat64::try_from(118.625).unwrap();
        assert_eq!(format!("{ibm:e}"), format!("{:e}", f64::from(ibm)));
        assert_eq!(format!("{ibm:.3e}"), format!("{:.3e}", f64::from(ibm)));
    }

    #[test]
    fn upper_exp_formats_via_f64() {
        let ibm = IbmFloat64::try_from(118.625).unwrap();
        assert_eq!(format!("{ibm:E}"), format!("{:E}", f64::from(ibm)));
        assert_eq!(format!("{ibm:.3E}"), format!("{:.3E}", f64::from(ibm)));
    }

    #[test]
    fn from_str_parses_one() {
        let parsed: IbmFloat64 = "1.0".parse().unwrap();
        let expected = IbmFloat64::from_be_bytes([0x41, 0x10, 0, 0, 0, 0, 0, 0]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn from_str_parses_negative_one() {
        let parsed: IbmFloat64 = "-1.0".parse().unwrap();
        let expected = IbmFloat64::from_be_bytes([0xC1, 0x10, 0, 0, 0, 0, 0, 0]);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn from_str_parses_zero() {
        let parsed: IbmFloat64 = "0".parse().unwrap();
        assert_eq!(parsed, IbmFloat64::new());
    }

    #[test]
    fn from_str_parses_scientific_notation() {
        let parsed: IbmFloat64 = "1.18625e2".parse().unwrap();
        assert_approx_eq!(f64, 118.625, f64::from(parsed));
    }

    #[test]
    fn from_str_rejects_nan() {
        let result: Result<IbmFloat64, _> = "nan".parse();
        assert_eq!(
            Err(ParseIbmFloat64Error::Conversion(
                IbmFloat64Error::NotANumber
            )),
            result
        );
    }

    #[test]
    fn from_str_rejects_garbage() {
        let result: Result<IbmFloat64, _> = "abc".parse();
        assert!(matches!(
            result,
            Err(ParseIbmFloat64Error::InvalidFloat(_))
        ));
    }

    #[test]
    fn from_str_rejects_positive_infinity() {
        let result: Result<IbmFloat64, _> = "inf".parse();
        assert_eq!(
            Err(ParseIbmFloat64Error::Conversion(
                IbmFloat64Error::PositiveInfinity
            )),
            result
        );
    }

    #[test]
    fn from_str_rejects_negative_infinity() {
        let result: Result<IbmFloat64, _> = "-inf".parse();
        assert_eq!(
            Err(ParseIbmFloat64Error::Conversion(
                IbmFloat64Error::NegativeInfinity
            )),
            result
        );
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

    #[test]
    #[ignore = "Bit-exact agreement check against the ibmfloat crate (run with --nocapture)"]
    fn agreement_with_ibmfloat() {
        const RANDOM_N: usize = 5_000_000;

        use ibmfloat::F64;

        fn check(bytes: [u8; 8]) -> Option<(u64, u64)> {
            let ours = f64::from(IbmFloat64::from_be_bytes(bytes)).to_bits();
            let theirs = f64::from(F64::from_be_bytes(bytes)).to_bits();
            if ours == theirs {
                None
            } else {
                Some((ours, theirs))
            }
        }

        fn report(label: &str, total: usize, disagreements: &[([u8; 8], u64, u64)]) {
            println!(
                "{label}: {total} inputs, {} disagreements",
                disagreements.len()
            );
            for (bytes, ours, theirs) in disagreements.iter().take(10) {
                println!("  in={bytes:02X?}  ours={ours:016X}  ibmfloat={theirs:016X}");
            }
        }

        // ----- Curated set: every (sign, exponent) byte × representative mantissa shapes,
        //       plus SAS missing-value sentinels and explicit extremes.
        let mantissa_shapes: &[[u8; 7]] = &[
            [0x00; 7],                                  // mantissa = 0 (signed zero regardless of exp)
            [0xFF; 7],                                  // mantissa = all-ones
            [0x80, 0, 0, 0, 0, 0, 0],                   // leading hex digit 0x8 (full normalized)
            [0xF0, 0, 0, 0, 0, 0, 0],                   // leading hex digit 0xF
            [0x10, 0, 0, 0, 0, 0, 0],                   // leading hex digit 0x1 (least normalized)
            [0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], // leading 0x1 + full tail
            [0x01, 0, 0, 0, 0, 0, 0],                   // 1 leading zero hex digit (subnormal)
            [0x00, 0x10, 0, 0, 0, 0, 0],                // 2 leading zero hex digits
            [0x00, 0x01, 0, 0, 0, 0, 0],                // 3 leading zero hex digits
            [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE], // arbitrary mixed
        ];
        let mut curated: Vec<[u8; 8]> = Vec::new();
        for byte0 in 0u8..=0xFF {
            for shape in mantissa_shapes {
                let mut b = [0u8; 8];
                b[0] = byte0;
                b[1..].copy_from_slice(shape);
                curated.push(b);
            }
        }
        for &c in b".ABCDEFGHIJKLMNOPQRSTUVWXYZ_" {
            curated.push([c, 0, 0, 0, 0, 0, 0, 0]);
        }
        curated.push([0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]); // MAX_VALUE
        curated.push([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]); // MIN_VALUE
        curated.push([0x01, 0x10, 0, 0, 0, 0, 0, 0]); // smallest normal
        curated.push([0x00, 0, 0, 0, 0, 0, 0, 0x01]); // smallest denormal

        let curated_disagreements: Vec<([u8; 8], u64, u64)> = curated
            .iter()
            .filter_map(|&b| check(b).map(|(o, t)| (b, o, t)))
            .collect();
        report("curated", curated.len(), &curated_disagreements);

        // ----- Random set: splitmix64 over the full [u8; 8] space.
        let mut state: u64 = 0xCAFE_F00D_BAAD_F00D;
        let mut random_disagreements: Vec<([u8; 8], u64, u64)> = Vec::new();
        for _ in 0..RANDOM_N {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            let bytes = (z ^ (z >> 31)).to_be_bytes();
            if let Some((o, t)) = check(bytes) {
                random_disagreements.push((bytes, o, t));
            }
        }
        report("random", RANDOM_N, &random_disagreements);

        let total = curated_disagreements.len() + random_disagreements.len();
        assert_eq!(total, 0, "implementations disagree on {total} inputs");
    }
}
