use std::fmt::{self, Display, Formatter, LowerExp, UpperExp};

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
/// Currently read-only: only `From<IbmFloat32> for f64` is implemented. The
/// conversion is bit-exact — IBM32's 24-bit mantissa fits inside f64's 53-bit
/// significand with 29 bits to spare, and `16^k` is exactly representable in
/// f64 across the full IBM HFP exponent range, so no precision is lost.
///
/// See `src/ibm/README.md` for crate-level conventions.
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
}
