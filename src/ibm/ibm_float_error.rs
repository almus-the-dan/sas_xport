use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Error returned when an `f64` cannot be faithfully represented as either
/// [`IbmFloat32`](super::IbmFloat32) or [`IbmFloat64`](super::IbmFloat64).
///
/// Both formats share the same 7-bit characteristic and the same range
/// thresholds, so a single error type covers both. Reached publicly via
/// `<IbmFloat64 as TryFrom<f64>>::try_from` (and indirectly via
/// [`ParseIbmFloatError`](super::ParseIbmFloatError) for both `FromStr`
/// impls).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IbmFloatError {
    /// Input was NaN. IBM HFP has no NaN encoding.
    NotANumber,
    /// Input was positive infinity. IBM HFP has no infinity encoding.
    PositiveInfinity,
    /// Input was negative infinity. IBM HFP has no infinity encoding.
    NegativeInfinity,
    /// Input is a finite positive value larger than the IBM HFP maximum
    /// (~7.2e75; the same boundary applies to both `IbmFloat32` and
    /// `IbmFloat64`).
    PositiveOverflow,
    /// Input is a finite negative value smaller than the IBM HFP minimum.
    NegativeOverflow,
    /// Input is a finite positive value too small to represent (smaller than
    /// the smallest positive IBM HFP value).
    PositiveUnderflow,
    /// Input is a finite negative value too close to zero to represent.
    NegativeUnderflow,
}

impl Display for IbmFloatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::NotANumber => "input is NaN, which has no IBM HFP encoding",
            Self::PositiveInfinity => {
                "input is positive infinity, which is not representable in IBM HFP"
            }
            Self::NegativeInfinity => {
                "input is negative infinity, which is not representable in IBM HFP"
            }
            Self::PositiveOverflow => "input exceeds the IBM HFP positive maximum",
            Self::NegativeOverflow => "input exceeds the IBM HFP negative minimum",
            Self::PositiveUnderflow => {
                "input is too small to represent in IBM HFP (positive underflow)"
            }
            Self::NegativeUnderflow => {
                "input is too small to represent in IBM HFP (negative underflow)"
            }
        };
        formatter.write_str(msg)
    }
}

impl Error for IbmFloatError {}
