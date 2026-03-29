use crate::sas::SasVariableType;
use encoding_rs::Encoding;

/// Controls whether the writer reports an error when a value must be
/// truncated to fit its designated field width.
#[derive(Copy, Clone, Debug, Default, Hash, PartialEq, Eq)]
pub enum TruncationPolicy {
    /// Silently truncate the value (current default behavior).
    #[default]
    Silent,
    /// Return an error if truncation would occur.
    Report,
}

/// Options to control the behavior of writing a SAS® transport file.
#[derive(Clone, Debug)]
pub struct XportWriterOptions {
    encoding: &'static Encoding,
    character_truncation_policy: TruncationPolicy,
    numeric_truncation_policy: TruncationPolicy,
}

impl XportWriterOptions {
    /// Creates a builder for constructing `XportWriterOptions`. By default, the
    /// builder uses UTF-8 encoding and silent truncation for both character and
    /// numeric values.
    #[inline]
    #[must_use]
    pub fn builder() -> XportWriterOptionsBuilder {
        XportWriterOptionsBuilder::new()
    }

    /// Gets the encoding used for writing character values.
    #[inline]
    #[must_use]
    pub fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    /// Gets the truncation policy for the given variable type.
    #[inline]
    #[must_use]
    pub fn truncation_policy(&self, variable_type: SasVariableType) -> TruncationPolicy {
        match variable_type {
            SasVariableType::Character => self.character_truncation_policy,
            SasVariableType::Numeric => self.numeric_truncation_policy,
        }
    }
}

impl Default for XportWriterOptions {
    fn default() -> Self {
        XportWriterOptionsBuilder::new().build_into()
    }
}

/// A builder for configuring `XportWriterOptions`.
#[derive(Clone, Debug)]
pub struct XportWriterOptionsBuilder {
    encoding: &'static Encoding,
    character_truncation_policy: TruncationPolicy,
    numeric_truncation_policy: TruncationPolicy,
}

impl XportWriterOptionsBuilder {
    fn new() -> Self {
        Self {
            encoding: encoding_rs::UTF_8,
            character_truncation_policy: TruncationPolicy::Silent,
            numeric_truncation_policy: TruncationPolicy::Silent,
        }
    }

    /// Sets the encoding used for writing character values.
    #[inline]
    pub fn set_encoding(&mut self, encoding: &'static Encoding) -> &mut Self {
        self.encoding = encoding;
        self
    }

    /// Sets the truncation policy for the given variable type.
    #[inline]
    pub fn set_truncation_policy(
        &mut self,
        variable_type: SasVariableType,
        policy: TruncationPolicy,
    ) -> &mut Self {
        match variable_type {
            SasVariableType::Character => self.character_truncation_policy = policy,
            SasVariableType::Numeric => self.numeric_truncation_policy = policy,
        }
        self
    }

    /// Builds an `XportWriterOptions` from the current configuration.
    #[inline]
    #[must_use]
    pub fn build(&self) -> XportWriterOptions {
        self.clone().build_into()
    }

    /// Builds an `XportWriterOptions` from the current configuration,
    /// consuming the builder.
    #[must_use]
    pub fn build_into(self) -> XportWriterOptions {
        XportWriterOptions {
            encoding: self.encoding,
            character_truncation_policy: self.character_truncation_policy,
            numeric_truncation_policy: self.numeric_truncation_policy,
        }
    }
}

impl From<XportWriterOptionsBuilder> for XportWriterOptions {
    fn from(builder: XportWriterOptionsBuilder) -> Self {
        builder.build_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_encoding_is_utf8() {
        let options = XportWriterOptions::default();
        assert_eq!(options.encoding(), encoding_rs::UTF_8);
    }

    #[test]
    fn default_character_truncation_policy_is_silent() {
        let options = XportWriterOptions::default();
        assert_eq!(
            options.truncation_policy(SasVariableType::Character),
            TruncationPolicy::Silent,
        );
    }

    #[test]
    fn default_numeric_truncation_policy_is_silent() {
        let options = XportWriterOptions::default();
        assert_eq!(
            options.truncation_policy(SasVariableType::Numeric),
            TruncationPolicy::Silent,
        );
    }

    #[test]
    fn builder_default_matches_default() {
        let from_default = XportWriterOptions::default();
        let from_builder = XportWriterOptions::builder().build();
        assert_eq!(from_default.encoding(), from_builder.encoding());
        assert_eq!(
            from_default.truncation_policy(SasVariableType::Character),
            from_builder.truncation_policy(SasVariableType::Character),
        );
        assert_eq!(
            from_default.truncation_policy(SasVariableType::Numeric),
            from_builder.truncation_policy(SasVariableType::Numeric),
        );
    }

    #[test]
    fn set_encoding() {
        let options = XportWriterOptions::builder()
            .set_encoding(encoding_rs::WINDOWS_1252)
            .build();
        assert_eq!(options.encoding(), encoding_rs::WINDOWS_1252);
    }

    #[test]
    fn set_character_truncation_policy() {
        let options = XportWriterOptions::builder()
            .set_truncation_policy(SasVariableType::Character, TruncationPolicy::Report)
            .build();
        assert_eq!(
            options.truncation_policy(SasVariableType::Character),
            TruncationPolicy::Report,
        );
        assert_eq!(
            options.truncation_policy(SasVariableType::Numeric),
            TruncationPolicy::Silent,
        );
    }

    #[test]
    fn set_numeric_truncation_policy() {
        let options = XportWriterOptions::builder()
            .set_truncation_policy(SasVariableType::Numeric, TruncationPolicy::Report)
            .build();
        assert_eq!(
            options.truncation_policy(SasVariableType::Numeric),
            TruncationPolicy::Report,
        );
        assert_eq!(
            options.truncation_policy(SasVariableType::Character),
            TruncationPolicy::Silent,
        );
    }

    #[test]
    fn set_both_truncation_policies() {
        let options = XportWriterOptions::builder()
            .set_truncation_policy(SasVariableType::Character, TruncationPolicy::Report)
            .set_truncation_policy(SasVariableType::Numeric, TruncationPolicy::Report)
            .build();
        assert_eq!(
            options.truncation_policy(SasVariableType::Character),
            TruncationPolicy::Report,
        );
        assert_eq!(
            options.truncation_policy(SasVariableType::Numeric),
            TruncationPolicy::Report,
        );
    }

    #[test]
    fn build_does_not_consume_builder() {
        let mut builder = XportWriterOptions::builder();
        builder.set_encoding(encoding_rs::WINDOWS_1252);
        let first = builder.build();
        let second = builder.build();
        assert_eq!(first.encoding(), second.encoding());
    }

    #[test]
    fn build_into_consumes_builder() {
        let mut builder = XportWriterOptions::builder();
        builder.set_encoding(encoding_rs::SHIFT_JIS);
        let options = builder.build_into();
        assert_eq!(options.encoding(), encoding_rs::SHIFT_JIS);
    }

    #[test]
    fn from_builder() {
        let mut builder = XportWriterOptions::builder();
        builder.set_encoding(encoding_rs::EUC_JP);
        let options: XportWriterOptions = builder.into();
        assert_eq!(options.encoding(), encoding_rs::EUC_JP);
    }
}
