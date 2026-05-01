use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::ParseFloatError;

use super::IbmFloat64Error;

/// Error returned by `<IbmFloat64 as FromStr>::from_str` when parsing fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseIbmFloat64Error {
    /// The input could not be parsed as an `f64`.
    InvalidFloat(ParseFloatError),
    /// The input parsed as `f64` but the value could not be converted to
    /// `IbmFloat64` (NaN, infinite, overflow, or underflow).
    Conversion(IbmFloat64Error),
}

impl Display for ParseIbmFloat64Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFloat(e) => Display::fmt(e, formatter),
            Self::Conversion(e) => Display::fmt(e, formatter),
        }
    }
}

impl Error for ParseIbmFloat64Error {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidFloat(e) => Some(e),
            Self::Conversion(e) => Some(e),
        }
    }
}

impl From<ParseFloatError> for ParseIbmFloat64Error {
    fn from(value: ParseFloatError) -> Self {
        Self::InvalidFloat(value)
    }
}

impl From<IbmFloat64Error> for ParseIbmFloat64Error {
    fn from(value: IbmFloat64Error) -> Self {
        Self::Conversion(value)
    }
}
