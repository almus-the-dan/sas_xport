use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::ParseFloatError;

use super::IbmFloatError;

/// Error returned by `<IbmFloat64 as FromStr>::from_str` and
/// `<IbmFloat32 as FromStr>::from_str` when parsing fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseIbmFloatError {
    /// The input could not be parsed as an `f64`.
    InvalidFloat(ParseFloatError),
    /// The input parsed as `f64` but the value could not be converted into
    /// the target IBM HFP format (NaN, infinite, overflow, or underflow).
    Conversion(IbmFloatError),
}

impl Display for ParseIbmFloatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFloat(e) => Display::fmt(e, formatter),
            Self::Conversion(e) => Display::fmt(e, formatter),
        }
    }
}

impl Error for ParseIbmFloatError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidFloat(e) => Some(e),
            Self::Conversion(e) => Some(e),
        }
    }
}

impl From<ParseFloatError> for ParseIbmFloatError {
    fn from(value: ParseFloatError) -> Self {
        Self::InvalidFloat(value)
    }
}

impl From<IbmFloatError> for ParseIbmFloatError {
    fn from(value: IbmFloatError) -> Self {
        Self::Conversion(value)
    }
}
