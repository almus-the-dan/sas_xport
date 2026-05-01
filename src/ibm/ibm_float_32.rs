use std::fmt::{self, Display, Formatter, LowerExp, UpperExp};
use std::str::FromStr;

use super::{IbmFloat64, IbmFloatError, ParseIbmFloatError};

/// Represents a 32-bit IBM hexadecimal floating point value.
///
/// The 4-byte big-endian layout is sign (1 bit) + characteristic (7 bits,
/// biased by 64) + mantissa (24 bits, base-16 fraction). Same numeric range
/// as [`IbmFloat64`](super::IbmFloat64) (~5.4e-79 to ~7.2e75 — both formats
/// share the same 7-bit characteristic) but with a 24-bit mantissa instead of
/// 56 bits.
///
/// Equality, ordering, and hashing operate on the underlying `[u8; 4]` bit
/// pattern, mirroring the conventions of [`IbmFloat64`]. Convert to `f64` for
/// numeric comparison.
///
/// Public conversions split by direction:
/// - **Lossless out**: `From<IbmFloat32> for f64`, `From<IbmFloat32> for IbmFloat64`.
/// - **Lossy in**: explicit `_lossy`-suffixed inherent methods —
///   [`Self::try_from_f64_lossy`], [`Self::try_from_f32_lossy`], and
///   [`Self::from_ibm_float_64_lossy`]. No `TryFrom`/`From` trait impls for
///   these directions because the precision-truncation loss can't be
///   surfaced through any error type; the suffix is the warning label.
/// - **String**: `FromStr` parses decimal strings via the f64 path.
///
/// See `src/ibm/README.md` for crate-level conventions and rationale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IbmFloat32 {
    bytes: [u8; 4],
}

impl IbmFloat32 {
    /// Gets the maximum representable value.
    pub const MAX_VALUE: Self = Self::from_be_bytes([0x7F, 0xFF, 0xFF, 0xFF]);
    /// Gets the minimum representable value.
    pub const MIN_VALUE: Self = Self::from_be_bytes([0xFF, 0xFF, 0xFF, 0xFF]);

    /// Initializes a new IBM floating point value, with a value of 0.0.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self { bytes: [0; 4] }
    }

    /// Initializes a new IBM floating point value from the given byte array,
    /// which is stored in big-endian order.
    #[inline]
    #[must_use]
    pub const fn from_be_bytes(bytes: [u8; 4]) -> Self {
        Self { bytes }
    }

    /// Initializes a new IBM floating point value from the given byte array,
    /// which is stored in little-endian order.
    #[inline]
    #[must_use]
    pub const fn from_le_bytes(bytes: [u8; 4]) -> Self {
        Self {
            bytes: [bytes[3], bytes[2], bytes[1], bytes[0]],
        }
    }

    /// Converts the IBM floating point value to a byte array, with the bytes
    /// stored in big-endian order.
    #[inline]
    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 4] {
        self.bytes
    }

    /// Converts the IBM floating point value to a byte array, with the bytes
    /// stored in little-endian order.
    #[inline]
    #[must_use]
    pub const fn to_le_bytes(self) -> [u8; 4] {
        [self.bytes[3], self.bytes[2], self.bytes[1], self.bytes[0]]
    }

    /// Indicates whether the value is positive.
    #[inline]
    #[must_use]
    pub const fn is_sign_positive(self) -> bool {
        self.bytes[0] & 0x80 == 0
    }

    /// Indicates whether the value is negative.
    #[inline]
    #[must_use]
    pub const fn is_sign_negative(self) -> bool {
        self.bytes[0] & 0x80 != 0
    }

    /// Narrows an [`IbmFloat64`] to an `IbmFloat32` by truncating the
    /// bottom 32 bits of the 56-bit IBM HFP mantissa (dropping the
    /// trailing 4 bytes). The conversion is infallible — both formats
    /// share the same range — but discards up to 32 bits of mantissa
    /// precision.
    ///
    /// **The `_lossy` suffix is the warning label.** No `From`/`TryFrom`
    /// trait impl is provided for this direction: trait conversions tend to
    /// suggest "free" or "strict" semantics, and neither is true here. The
    /// dual `From<IbmFloat32> for IbmFloat64` widening *is* a `From` impl
    /// because that direction is lossless.
    #[inline]
    #[must_use]
    pub fn from_ibm_float_64_lossy(ibm64: IbmFloat64) -> Self {
        let b = ibm64.to_be_bytes();
        Self::from_be_bytes([b[0], b[1], b[2], b[3]])
    }

    /// Converts an `f64` to an `IbmFloat32` by delegating to
    /// [`IbmFloat64::try_from`] and then narrowing via
    /// [`Self::from_ibm_float_64_lossy`].
    ///
    /// Errors on NaN/±Infinity and on values outside the IBM HFP range, but
    /// **the `_lossy` suffix flags a third lossy mode that the error type
    /// can't surface**: precision truncation from f64's 53-bit mantissa
    /// down to IBM32's 24-bit mantissa is silent. We don't expose this as
    /// a public `TryFrom<f64>` because trait conversions suggest a strict
    /// guarantee that doesn't fully hold here.
    #[inline]
    pub fn try_from_f64_lossy(value: f64) -> Result<Self, IbmFloatError> {
        let value = IbmFloat64::try_from(value)?;
        Ok(Self::from_ibm_float_64_lossy(value))
    }

    /// Converts an `f32` to an `IbmFloat32` via the lossless `f32 → f64`
    /// widening cast, then delegates to [`Self::try_from_f64_lossy`].
    ///
    /// Errors only on NaN/±Infinity (every finite f32 fits inside IBM
    /// HFP's range). **The `_lossy` suffix flags binary-to-hex alignment
    /// loss**: even though both f32 and IBM32 carry 24 mantissa bits, the
    /// alignment can silently truncate up to 3 bits when the IBM32
    /// mantissa's leading hex digit is small ("wobbling precision"). No
    /// `TryFrom<f32>` trait impl for the same reason as f64.
    #[inline]
    pub fn try_from_f32_lossy(value: f32) -> Result<Self, IbmFloatError> {
        Self::try_from_f64_lossy(f64::from(value))
    }
}

impl FromStr for IbmFloat32 {
    type Err = ParseIbmFloatError;

    /// Parses an `IbmFloat32` by first parsing the input as an `f64` and
    /// then narrowing to IBM HFP 32-bit format.
    ///
    /// The f64 intermediate (rather than f32) preserves IBM32's full numeric
    /// range: strings like `"1e50"` are well within IBM32's range
    /// (~5.4e-79 to ~7.2e75) but would saturate to infinity going through
    /// f32 (~3.4e38 max), causing a misleading `Infinite` error for a value
    /// that's actually representable.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = s.parse::<f64>()?;
        let value = Self::try_from_f64_lossy(value)?;
        Ok(value)
    }
}

impl From<IbmFloat32> for IbmFloat64 {
    /// Losslessly widens an `IbmFloat32` to an `IbmFloat64`.
    ///
    /// Both formats share the same byte 0 (sign + 7-bit characteristic
    /// biased by 64) and IBM HFP mantissas are byte-prefix compatible —
    /// trailing zero bytes don't change the represented value. Widening is
    /// therefore a four-byte zero-pad with no arithmetic.
    #[inline]
    fn from(value: IbmFloat32) -> Self {
        let b = value.to_be_bytes();
        Self::from_be_bytes([b[0], b[1], b[2], b[3], 0, 0, 0, 0])
    }
}

impl From<IbmFloat32> for f64 {
    /// Converts an `IbmFloat32` to an `f64`. The conversion is bit-exact:
    /// IBM HFP 32-bit's 24-bit mantissa fits inside f64's 53-bit significand,
    /// and `16^k` is exactly representable in f64 across the entire IBM HFP
    /// exponent range, so no precision is lost.
    fn from(value: IbmFloat32) -> f64 {
        let temp = u32::from_be_bytes(value.bytes);
        let sign = u64::from(temp & 0x8000_0000) << 32;
        let ibm_fraction = temp & 0x00FF_FFFF;

        // Quick return for zeros.
        if ibm_fraction == 0 {
            return f64::from_bits(sign);
        }

        #[allow(clippy::cast_possible_wrap)]
        let shift = ibm_fraction.leading_zeros() as i32 - 8;
        #[allow(clippy::cast_possible_wrap)]
        let ibm_characteristic = ((temp & 0x7F00_0000) >> 24) as i32;

        // Normalize so the leading 1 of the 24-bit IBM mantissa is at bit 23,
        // then shift left by 29 to land at bit 52 (the f64 implicit-1 position).
        // The implicit bit then carries into the exponent during the
        // wrapping_add below, which is why the offset is 765 and not 766.
        let ieee_fraction = u64::from(ibm_fraction << shift) << 29;

        // Same +765 offset as IbmFloat64: combines IEEE bias (+1023), IBM
        // characteristic bias (-256 = 4 * -64), and -1 for the implicit-1 carry.
        // The mantissa-width difference between the two formats is captured
        // entirely in the `<< 29` above (vs. the `>> 3` in IbmFloat64).
        let ieee_exponent = 4 * ibm_characteristic + 765 - shift;

        #[allow(clippy::cast_sign_loss)]
        let ieee_bits = sign
            .wrapping_add((ieee_exponent as u64) << 52)
            .wrapping_add(ieee_fraction);
        f64::from_bits(ieee_bits)
    }
}

impl Display for IbmFloat32 {
    /// Displays the `IbmFloat32` by converting it to an `f64` and formatting it.
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&f64::from(*self), formatter)
    }
}

impl LowerExp for IbmFloat32 {
    /// Formats the `IbmFloat32` in lowercase scientific notation by converting
    /// it to an `f64` and forwarding to `f64`'s `LowerExp` impl.
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        LowerExp::fmt(&f64::from(*self), formatter)
    }
}

impl UpperExp for IbmFloat32 {
    /// Formats the `IbmFloat32` in uppercase scientific notation by converting
    /// it to an `f64` and forwarding to `f64`'s `UpperExp` impl.
    #[inline]
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        UpperExp::fmt(&f64::from(*self), formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ibm::IbmFloat64;
    use float_cmp::assert_approx_eq;

    #[test]
    fn new_returns_zero_bytes() {
        assert_eq!([0u8; 4], IbmFloat32::new().to_be_bytes());
    }

    #[test]
    fn positive_zero_round_trips() {
        let x = IbmFloat32::from_be_bytes([0; 4]);
        assert!(x.is_sign_positive());
        assert_eq!(0.0_f64.to_bits(), f64::from(x).to_bits());
    }

    #[test]
    fn negative_zero_round_trips() {
        let x = IbmFloat32::from_be_bytes([0x80, 0, 0, 0]);
        assert!(x.is_sign_negative());
        let f = f64::from(x);
        assert_eq!((-0.0_f64).to_bits(), f.to_bits());
        assert!(f.is_sign_negative());
    }

    #[test]
    fn one_converts_exactly() {
        // 1.0 = 0.1 × 16^1 → characteristic 65, mantissa 0x100000.
        let x = IbmFloat32::from_be_bytes([0x41, 0x10, 0, 0]);
        assert_eq!(1.0_f64.to_bits(), f64::from(x).to_bits());
        assert!(x.is_sign_positive());
    }

    #[test]
    fn negative_one_converts_exactly() {
        let x = IbmFloat32::from_be_bytes([0xC1, 0x10, 0, 0]);
        assert_eq!((-1.0_f64).to_bits(), f64::from(x).to_bits());
        assert!(x.is_sign_negative());
    }

    #[test]
    fn two_converts_exactly() {
        let x = IbmFloat32::from_be_bytes([0x41, 0x20, 0, 0]);
        assert_eq!(2.0_f64.to_bits(), f64::from(x).to_bits());
    }

    #[test]
    fn half_converts_exactly() {
        let x = IbmFloat32::from_be_bytes([0x40, 0x80, 0, 0]);
        assert_eq!(0.5_f64.to_bits(), f64::from(x).to_bits());
    }

    #[test]
    fn quarter_converts_exactly() {
        let x = IbmFloat32::from_be_bytes([0x40, 0x40, 0, 0]);
        assert_eq!(0.25_f64.to_bits(), f64::from(x).to_bits());
    }

    #[test]
    fn sixteen_converts_exactly() {
        let x = IbmFloat32::from_be_bytes([0x42, 0x10, 0, 0]);
        assert_eq!(16.0_f64.to_bits(), f64::from(x).to_bits());
    }

    #[test]
    fn one_hundred_eighteen_point_six_two_five() {
        // 118.625 = 76.A × 16^1 → characteristic 0x42, mantissa 0x76A000.
        let x = IbmFloat32::from_be_bytes([0x42, 0x76, 0xA0, 0x00]);
        assert_eq!(118.625_f64.to_bits(), f64::from(x).to_bits());
    }

    #[test]
    fn pi_within_24_bit_precision() {
        let x = IbmFloat32::from_be_bytes([0x41, 0x32, 0x43, 0xF6]);
        let f = f64::from(x);
        assert_approx_eq!(f64, std::f64::consts::PI, f, epsilon = 1e-6);
    }

    #[test]
    fn max_value_magnitude() {
        let f = f64::from(IbmFloat32::MAX_VALUE);
        // ~7.2370051e75. Same trick as the IbmFloat64 max test: divide out
        // most of the precision and assert on the remaining digits.
        let truncated = f64::trunc(f / 1.0e69);
        assert_approx_eq!(f64, 7_237_005.0, truncated);
        assert!(IbmFloat32::MAX_VALUE.is_sign_positive());
    }

    #[test]
    fn min_value_is_negative_max() {
        let f = f64::from(IbmFloat32::MIN_VALUE);
        assert!(f.is_sign_negative());
        assert!(IbmFloat32::MIN_VALUE.is_sign_negative());
        // Magnitude equal to MAX_VALUE.
        assert_eq!(
            f64::from(IbmFloat32::MAX_VALUE).to_bits(),
            (-f).to_bits()
        );
    }

    #[test]
    fn smallest_normalized_positive() {
        // characteristic 0, leading hex digit 1: smallest normalized.
        let x = IbmFloat32::from_be_bytes([0x00, 0x10, 0, 0]);
        let f = f64::from(x);
        assert!(f > 0.0);
        // Should be 16^-64 × 1/16 = 16^-65 ≈ 5.39761e-79.
        assert_approx_eq!(f64, 5.397_605e-79, f, epsilon = 1e-84);
    }

    #[test]
    fn smallest_denormalized_positive() {
        // characteristic 0, mantissa 1: smallest representable IBM32.
        let x = IbmFloat32::from_be_bytes([0x00, 0x00, 0x00, 0x01]);
        let f = f64::from(x);
        assert!(f > 0.0);
    }

    #[test]
    fn matches_ibm_float_64_for_padded_bytes() {
        // For any IBM HFP 32-bit byte pattern, treating it as the high 4
        // bytes of an IBM HFP 64-bit value (with trailing zeros) must yield
        // the same numeric value when each is converted to f64. This is the
        // strongest cross-check we have on the 32-bit conversion math.
        let cases: &[[u8; 4]] = &[
            [0x00, 0x00, 0x00, 0x00], // +0
            [0x80, 0x00, 0x00, 0x00], // -0
            [0x41, 0x10, 0x00, 0x00], // 1.0
            [0xC1, 0x10, 0x00, 0x00], // -1.0
            [0x41, 0x20, 0x00, 0x00], // 2.0
            [0x40, 0x80, 0x00, 0x00], // 0.5
            [0x40, 0x40, 0x00, 0x00], // 0.25
            [0x42, 0x10, 0x00, 0x00], // 16.0
            [0x42, 0x76, 0xA0, 0x00], // 118.625
            [0x41, 0x32, 0x43, 0xF6], // ≈ π
            [0x40, 0x19, 0x99, 0x99], // ≈ 0.1
            [0x7F, 0xFF, 0xFF, 0xFF], // MAX_VALUE
            [0xFF, 0xFF, 0xFF, 0xFF], // MIN_VALUE
            [0x01, 0x10, 0x00, 0x00], // smallest normalized positive
            [0x00, 0x10, 0x00, 0x00], // smallest with characteristic 0
            [0x00, 0x00, 0x00, 0x01], // smallest denormalized positive
            [0x12, 0x34, 0x56, 0x78], // arbitrary mid-range
            [0xC2, 0x76, 0xA0, 0x00], // -118.625
        ];
        for &bytes32 in cases {
            let mut bytes64 = [0u8; 8];
            bytes64[..4].copy_from_slice(&bytes32);
            let from32 = f64::from(IbmFloat32::from_be_bytes(bytes32));
            let from64 = f64::from(IbmFloat64::from_be_bytes(bytes64));
            assert_eq!(
                from32.to_bits(),
                from64.to_bits(),
                "IBM32 {bytes32:02X?} → {from32:e} differs from IBM64 {bytes64:02X?} → {from64:e}"
            );
        }
    }

    #[test]
    fn from_be_bytes_to_be_bytes_round_trips() {
        let bytes = [0x42, 0x76, 0xA0, 0x00];
        assert_eq!(bytes, IbmFloat32::from_be_bytes(bytes).to_be_bytes());
    }

    #[test]
    fn from_le_bytes_to_le_bytes_round_trips() {
        let le = [0x00, 0xA0, 0x76, 0x42];
        let x = IbmFloat32::from_le_bytes(le);
        assert_eq!(le, x.to_le_bytes());
        // Big-endian view should be the byte-reversal.
        assert_eq!([0x42, 0x76, 0xA0, 0x00], x.to_be_bytes());
    }

    #[test]
    fn display_formats_via_f64() {
        let x = IbmFloat32::from_be_bytes([0x42, 0x76, 0xA0, 0x00]);
        assert_eq!(format!("{x}"), format!("{}", f64::from(x)));
    }

    #[test]
    fn lower_exp_formats_via_f64() {
        let x = IbmFloat32::from_be_bytes([0x42, 0x76, 0xA0, 0x00]);
        assert_eq!(format!("{x:e}"), format!("{:e}", f64::from(x)));
        assert_eq!(format!("{x:.3e}"), format!("{:.3e}", f64::from(x)));
    }

    #[test]
    fn upper_exp_formats_via_f64() {
        let x = IbmFloat32::from_be_bytes([0x42, 0x76, 0xA0, 0x00]);
        assert_eq!(format!("{x:E}"), format!("{:E}", f64::from(x)));
    }

    #[test]
    fn hash_is_consistent_with_eq() {
        use std::collections::HashSet;

        let bytes = [0x41, 0x10, 0, 0];
        let a = IbmFloat32::from_be_bytes(bytes);
        let b = IbmFloat32::from_be_bytes(bytes);

        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
    }

    #[test]
    fn hash_distinguishes_signed_zeros() {
        use std::collections::HashSet;

        let plus_zero = IbmFloat32::from_be_bytes([0; 4]);
        let minus_zero = IbmFloat32::from_be_bytes([0x80, 0, 0, 0]);

        assert_ne!(plus_zero, minus_zero);

        let mut set = HashSet::new();
        set.insert(plus_zero);
        set.insert(minus_zero);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn try_from_f64_lossy_one() {
        let x = IbmFloat32::try_from_f64_lossy(1.0).unwrap();
        assert_eq!([0x41, 0x10, 0, 0], x.to_be_bytes());
    }

    #[test]
    fn try_from_f64_lossy_negative_one() {
        let x = IbmFloat32::try_from_f64_lossy(-1.0).unwrap();
        assert_eq!([0xC1, 0x10, 0, 0], x.to_be_bytes());
    }

    #[test]
    fn try_from_f64_lossy_zero_preserves_sign() {
        assert_eq!(
            [0u8; 4],
            IbmFloat32::try_from_f64_lossy(0.0).unwrap().to_be_bytes()
        );
        assert_eq!(
            [0x80, 0, 0, 0],
            IbmFloat32::try_from_f64_lossy(-0.0).unwrap().to_be_bytes()
        );
    }

    #[test]
    fn try_from_f64_lossy_nan_errors() {
        assert_eq!(
            Err(IbmFloatError::NotANumber),
            IbmFloat32::try_from_f64_lossy(f64::NAN)
        );
    }

    #[test]
    fn try_from_f64_lossy_infinity_errors() {
        assert_eq!(
            Err(IbmFloatError::PositiveInfinity),
            IbmFloat32::try_from_f64_lossy(f64::INFINITY)
        );
        assert_eq!(
            Err(IbmFloatError::NegativeInfinity),
            IbmFloat32::try_from_f64_lossy(f64::NEG_INFINITY)
        );
    }

    #[test]
    fn try_from_f64_lossy_overflow_errors() {
        assert_eq!(
            Err(IbmFloatError::PositiveOverflow),
            IbmFloat32::try_from_f64_lossy(1.0e300)
        );
        assert_eq!(
            Err(IbmFloatError::NegativeOverflow),
            IbmFloat32::try_from_f64_lossy(-1.0e300)
        );
    }

    #[test]
    fn try_from_f64_lossy_underflow_errors() {
        assert_eq!(
            Err(IbmFloatError::PositiveUnderflow),
            IbmFloat32::try_from_f64_lossy(1.0e-310)
        );
        assert_eq!(
            Err(IbmFloatError::NegativeUnderflow),
            IbmFloat32::try_from_f64_lossy(-1.0e-310)
        );
    }

    #[test]
    fn try_from_f64_lossy_matches_ibm_float_64_truncated() {
        // For any in-range f64, the IBM32 result must equal the top 4 bytes
        // of the IBM64 result. This is the input-direction analogue of
        // `matches_ibm_float_64_for_padded_bytes`.
        let cases: &[f64] = &[
            1.0,
            -1.0,
            2.0,
            0.5,
            0.25,
            16.0,
            118.625,
            -118.625,
            std::f64::consts::PI,
            0.1,
            1.0e10,
            -1.0e10,
            1.0e50,
            -1.0e50,
            1.0e-30,
            -1.0e-30,
        ];
        for &f in cases {
            let ibm32 = IbmFloat32::try_from_f64_lossy(f).unwrap();
            let ibm64 = IbmFloat64::try_from(f).unwrap();
            let b64 = ibm64.to_be_bytes();
            assert_eq!(
                [b64[0], b64[1], b64[2], b64[3]],
                ibm32.to_be_bytes(),
                "IBM32(f={f}) bytes should equal top-4 of IBM64(f={f}) bytes"
            );
        }
    }

    #[test]
    fn from_ibm_float_64_lossy_truncates_top_four_bytes() {
        let ibm64 = IbmFloat64::from_be_bytes([0x42, 0x76, 0xA0, 0x12, 0x34, 0x56, 0x78, 0x9A]);
        let ibm32 = IbmFloat32::from_ibm_float_64_lossy(ibm64);
        assert_eq!([0x42, 0x76, 0xA0, 0x12], ibm32.to_be_bytes());
    }

    #[test]
    fn from_ibm_float_64_lossy_preserves_signed_zero() {
        let pos = IbmFloat32::from_ibm_float_64_lossy(IbmFloat64::from_be_bytes([0; 8]));
        assert_eq!([0u8; 4], pos.to_be_bytes());
        let neg = IbmFloat32::from_ibm_float_64_lossy(IbmFloat64::from_be_bytes([
            0x80, 0, 0, 0, 0, 0, 0, 0,
        ]));
        assert_eq!([0x80, 0, 0, 0], neg.to_be_bytes());
    }

    #[test]
    fn widen_then_narrow_is_identity() {
        // For any IbmFloat32, widening to IbmFloat64 and narrowing back
        // must reproduce the original bytes — the widening only adds zero
        // bytes that the narrowing then discards.
        let cases: &[[u8; 4]] = &[
            [0x41, 0x10, 0, 0],
            [0xC1, 0x10, 0, 0],
            [0x42, 0x76, 0xA0, 0x00],
            [0x41, 0x32, 0x43, 0xF6],
            [0x7F, 0xFF, 0xFF, 0xFF],
            [0xFF, 0xFF, 0xFF, 0xFF],
            [0x00, 0x00, 0x00, 0x01],
            [0x80, 0x00, 0x00, 0x00],
        ];
        for &bytes in cases {
            let original = IbmFloat32::from_be_bytes(bytes);
            let widened: IbmFloat64 = original.into();
            let narrowed = IbmFloat32::from_ibm_float_64_lossy(widened);
            assert_eq!(original.to_be_bytes(), narrowed.to_be_bytes());
        }
    }

    #[test]
    fn widening_to_ibm_float_64_zero_pads_mantissa() {
        let x = IbmFloat32::from_be_bytes([0x42, 0x76, 0xA0, 0x00]);
        let widened: IbmFloat64 = x.into();
        assert_eq!(
            [0x42, 0x76, 0xA0, 0x00, 0x00, 0x00, 0x00, 0x00],
            widened.to_be_bytes()
        );
    }

    #[test]
    fn widening_preserves_signed_zero() {
        let pos: IbmFloat64 = IbmFloat32::from_be_bytes([0; 4]).into();
        assert_eq!([0u8; 8], pos.to_be_bytes());
        let neg: IbmFloat64 = IbmFloat32::from_be_bytes([0x80, 0, 0, 0]).into();
        assert_eq!([0x80, 0, 0, 0, 0, 0, 0, 0], neg.to_be_bytes());
    }

    #[test]
    fn widening_preserves_max_value() {
        let widened: IbmFloat64 = IbmFloat32::MAX_VALUE.into();
        // IBM32 MAX_VALUE bytes [0x7F, 0xFF, 0xFF, 0xFF] become IBM64 with
        // trailing zeros — strictly less than IBM64::MAX_VALUE (which has
        // trailing 0xFF bytes), but with the same numeric magnitude up to
        // IBM32's 24-bit mantissa precision.
        assert_eq!(
            [0x7F, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00],
            widened.to_be_bytes()
        );
    }

    #[test]
    fn widening_then_to_f64_equals_direct() {
        // Cross-check: f64::from(IbmFloat64::from(ibm32)) must equal
        // f64::from(ibm32) — the widening shouldn't change the numeric
        // value. This complements `matches_ibm_float_64_for_padded_bytes`
        // by going through the new From impl rather than manual padding.
        let cases: &[[u8; 4]] = &[
            [0x41, 0x10, 0, 0],       // 1.0
            [0xC1, 0x10, 0, 0],       // -1.0
            [0x42, 0x76, 0xA0, 0x00], // 118.625
            [0x41, 0x32, 0x43, 0xF6], // ≈ π
            [0x7F, 0xFF, 0xFF, 0xFF], // MAX_VALUE
            [0xFF, 0xFF, 0xFF, 0xFF], // MIN_VALUE
            [0x00, 0x00, 0x00, 0x01], // smallest denormal positive
            [0x80, 0x00, 0x00, 0x00], // -0
        ];
        for &bytes in cases {
            let ibm32 = IbmFloat32::from_be_bytes(bytes);
            let direct = f64::from(ibm32);
            let via_widening = f64::from(IbmFloat64::from(ibm32));
            assert_eq!(
                direct.to_bits(),
                via_widening.to_bits(),
                "widening of {bytes:02X?} should not change numeric value"
            );
        }
    }

    #[test]
    fn try_from_f32_lossy_one() {
        let x = IbmFloat32::try_from_f32_lossy(1.0_f32).unwrap();
        assert_eq!([0x41, 0x10, 0, 0], x.to_be_bytes());
    }

    #[test]
    fn try_from_f32_lossy_negative_one() {
        let x = IbmFloat32::try_from_f32_lossy(-1.0_f32).unwrap();
        assert_eq!([0xC1, 0x10, 0, 0], x.to_be_bytes());
    }

    #[test]
    fn try_from_f32_lossy_zero_preserves_sign() {
        assert_eq!(
            [0u8; 4],
            IbmFloat32::try_from_f32_lossy(0.0_f32).unwrap().to_be_bytes()
        );
        assert_eq!(
            [0x80, 0, 0, 0],
            IbmFloat32::try_from_f32_lossy(-0.0_f32).unwrap().to_be_bytes()
        );
    }

    #[test]
    fn try_from_f32_lossy_nan_errors() {
        assert_eq!(
            Err(IbmFloatError::NotANumber),
            IbmFloat32::try_from_f32_lossy(f32::NAN)
        );
    }

    #[test]
    fn try_from_f32_lossy_infinity_errors() {
        assert_eq!(
            Err(IbmFloatError::PositiveInfinity),
            IbmFloat32::try_from_f32_lossy(f32::INFINITY)
        );
        assert_eq!(
            Err(IbmFloatError::NegativeInfinity),
            IbmFloat32::try_from_f32_lossy(f32::NEG_INFINITY)
        );
    }

    #[test]
    fn try_from_f32_lossy_matches_try_from_f64_lossy_bridge() {
        // Bridge invariant: try_from_f32_lossy(x) must equal try_from_f64_lossy(f64::from(x))
        // since f32 → f64 is exact.
        let cases: &[f32] = &[
            1.0,
            -1.0,
            2.0,
            0.5,
            0.1,
            std::f32::consts::PI,
            118.625,
            f32::MAX,
            f32::MIN,
            f32::MIN_POSITIVE,
            1.0e-30,
            -1.0e-30,
        ];
        for &x in cases {
            let direct = IbmFloat32::try_from_f32_lossy(x).unwrap();
            let widened = IbmFloat32::try_from_f64_lossy(f64::from(x)).unwrap();
            assert_eq!(
                widened.to_be_bytes(),
                direct.to_be_bytes(),
                "try_from_f32_lossy({x}) must equal try_from_f64_lossy(f64::from({x}))"
            );
        }
    }

    #[test]
    fn from_str_parses_one() {
        let x: IbmFloat32 = "1.0".parse().unwrap();
        assert_eq!([0x41, 0x10, 0, 0], x.to_be_bytes());
    }

    #[test]
    fn from_str_parses_negative_one() {
        let x: IbmFloat32 = "-1.0".parse().unwrap();
        assert_eq!([0xC1, 0x10, 0, 0], x.to_be_bytes());
    }

    #[test]
    fn from_str_parses_zero() {
        let x: IbmFloat32 = "0".parse().unwrap();
        assert_eq!([0u8; 4], x.to_be_bytes());
    }

    #[test]
    fn from_str_parses_scientific() {
        let x: IbmFloat32 = "1.18625e2".parse().unwrap();
        assert_approx_eq!(f64, 118.625, f64::from(x));
    }

    #[test]
    fn from_str_preserves_full_ibm_range_via_f64() {
        // 1e50 sits well inside IBM32's range (~7.2e75) but is well above
        // f32's max (~3.4e38). Going through f64 must succeed; an f32 detour
        // would saturate to infinity and error.
        let x: IbmFloat32 = "1e50".parse().unwrap();
        let f = f64::from(x);
        assert!(f > 0.0);
        assert_approx_eq!(f64, 1.0e50, f, epsilon = 1.0e44);
    }

    #[test]
    fn from_str_rejects_nan() {
        let result: Result<IbmFloat32, _> = "nan".parse();
        assert_eq!(
            Err(ParseIbmFloatError::Conversion(
                IbmFloatError::NotANumber
            )),
            result
        );
    }

    #[test]
    fn from_str_rejects_positive_infinity() {
        let result: Result<IbmFloat32, _> = "inf".parse();
        assert_eq!(
            Err(ParseIbmFloatError::Conversion(
                IbmFloatError::PositiveInfinity
            )),
            result
        );
    }

    #[test]
    fn from_str_rejects_negative_infinity() {
        let result: Result<IbmFloat32, _> = "-inf".parse();
        assert_eq!(
            Err(ParseIbmFloatError::Conversion(
                IbmFloatError::NegativeInfinity
            )),
            result
        );
    }

    #[test]
    fn from_str_rejects_overflow() {
        let result: Result<IbmFloat32, _> = "1e300".parse();
        assert_eq!(
            Err(ParseIbmFloatError::Conversion(
                IbmFloatError::PositiveOverflow
            )),
            result
        );
    }

    #[test]
    fn from_str_rejects_garbage() {
        let result: Result<IbmFloat32, _> = "abc".parse();
        assert!(matches!(result, Err(ParseIbmFloatError::InvalidFloat(_))));
    }
}
