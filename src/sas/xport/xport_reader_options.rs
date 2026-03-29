use encoding_rs::Encoding;
use std::collections::HashSet;

/// Options to control the behavior of reading a SAS® transport file.
#[derive(Clone, Debug)]
pub struct XportReaderOptions {
    encoding: &'static Encoding,
    fallback_encodings: Vec<&'static Encoding>,
}

impl XportReaderOptions {
    /// Create a builder for constructing `XportReaderOptions`. By default, the
    /// builder defaults to using UTF-8 as the primary, and only, encoding.
    #[inline]
    #[must_use]
    pub fn builder() -> XportReaderOptionsBuilder {
        XportReaderOptionsBuilder::new()
    }

    /// Gets the primary encoding of the file.
    #[inline]
    #[must_use]
    pub fn encoding(&self) -> &'static Encoding {
        self.encoding
    }

    /// Gets the fallback encodings, in the order they will be attempted.
    #[inline]
    #[must_use]
    pub fn fallback_encodings(&self) -> &[&'static Encoding] {
        &self.fallback_encodings
    }
}

impl Default for XportReaderOptions {
    fn default() -> Self {
        let builder = XportReaderOptionsBuilder::new();
        builder.build_into()
    }
}

/// A builder for configuring an `XportReaderOptions`.
#[derive(Clone, Debug)]
pub struct XportReaderOptionsBuilder {
    encoding: &'static Encoding,
    fallback_encodings: Vec<&'static Encoding>,
}

impl XportReaderOptionsBuilder {
    fn new() -> Self {
        Self {
            encoding: encoding_rs::UTF_8,
            fallback_encodings: Vec::new(),
        }
    }

    /// Sets the primary encoding of the file.
    #[inline]
    pub fn set_encoding(&mut self, encoding: &'static Encoding) -> &mut Self {
        self.encoding = encoding;
        self
    }

    /// Adds a fallback encoding to try when the primary encoding fails.
    /// Fallback encodings are attempted in the order they are added.
    #[inline]
    pub fn add_fallback_encoding(&mut self, encoding: &'static Encoding) -> &mut Self {
        self.fallback_encodings.push(encoding);
        self
    }

    /// Builds an `XportReaderOptions` from the current configuration.
    #[inline]
    #[must_use]
    pub fn build(&self) -> XportReaderOptions {
        self.clone().build_into()
    }

    /// Build an `XportReaderOptions` from the current configuration, consuming the builder.
    ///
    /// Fallback encodings that duplicate the primary encoding or that appear
    /// more than once are silently removed to avoid redundant decode attempts.
    #[must_use]
    pub fn build_into(self) -> XportReaderOptions {
        let mut seen = HashSet::with_capacity(self.fallback_encodings.len() + 1);
        seen.insert(self.encoding);
        let mut fallback_encodings = Vec::with_capacity(self.fallback_encodings.len());
        for encoding in &self.fallback_encodings {
            if seen.insert(*encoding) {
                fallback_encodings.push(*encoding);
            }
        }
        XportReaderOptions {
            encoding: self.encoding,
            fallback_encodings,
        }
    }
}

impl From<XportReaderOptionsBuilder> for XportReaderOptions {
    fn from(builder: XportReaderOptionsBuilder) -> Self {
        builder.build_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_encoding_is_utf8() {
        let options = XportReaderOptions::default();
        assert_eq!(options.encoding(), encoding_rs::UTF_8);
    }

    #[test]
    fn test_default_fallback_encodings_is_empty() {
        let options = XportReaderOptions::default();
        assert!(options.fallback_encodings().is_empty());
    }

    #[test]
    fn test_builder_default_matches_default() {
        let from_default = XportReaderOptions::default();
        let from_builder = XportReaderOptions::builder().build();
        assert_eq!(from_default.encoding(), from_builder.encoding());
        assert_eq!(
            from_default.fallback_encodings(),
            from_builder.fallback_encodings()
        );
    }

    #[test]
    fn test_set_encoding() {
        let options = XportReaderOptions::builder()
            .set_encoding(encoding_rs::WINDOWS_1252)
            .build();
        assert_eq!(options.encoding(), encoding_rs::WINDOWS_1252);
    }

    #[test]
    fn test_add_fallback_encoding() {
        let options = XportReaderOptions::builder()
            .add_fallback_encoding(encoding_rs::WINDOWS_1252)
            .build();
        assert_eq!(options.fallback_encodings(), &[encoding_rs::WINDOWS_1252]);
    }

    #[test]
    fn test_multiple_fallback_encodings_preserve_order() {
        let options = XportReaderOptions::builder()
            .add_fallback_encoding(encoding_rs::WINDOWS_1252)
            .add_fallback_encoding(encoding_rs::SHIFT_JIS)
            .build();
        assert_eq!(
            options.fallback_encodings(),
            &[encoding_rs::WINDOWS_1252, encoding_rs::SHIFT_JIS]
        );
    }

    #[test]
    fn test_build_does_not_consume_builder() {
        let mut builder = XportReaderOptions::builder();
        builder.set_encoding(encoding_rs::WINDOWS_1252);
        let first = builder.build();
        let second = builder.build();
        assert_eq!(first.encoding(), second.encoding());
    }

    #[test]
    fn test_build_into_consumes_builder() {
        let mut builder = XportReaderOptions::builder();
        builder.set_encoding(encoding_rs::SHIFT_JIS);
        let options = builder.build_into();
        assert_eq!(options.encoding(), encoding_rs::SHIFT_JIS);
    }

    #[test]
    fn test_from_builder() {
        let mut builder = XportReaderOptions::builder();
        builder.set_encoding(encoding_rs::EUC_JP);
        let options: XportReaderOptions = builder.into();
        assert_eq!(options.encoding(), encoding_rs::EUC_JP);
    }

    #[test]
    fn test_duplicate_of_primary_is_dropped() {
        let options = XportReaderOptions::builder()
            .set_encoding(encoding_rs::WINDOWS_1252)
            .add_fallback_encoding(encoding_rs::WINDOWS_1252)
            .build();
        assert!(options.fallback_encodings().is_empty());
    }

    #[test]
    fn test_duplicate_of_default_primary_is_dropped() {
        let options = XportReaderOptions::builder()
            .add_fallback_encoding(encoding_rs::UTF_8)
            .build();
        assert!(options.fallback_encodings().is_empty());
    }

    #[test]
    fn test_duplicate_fallbacks_are_deduplicated() {
        let options = XportReaderOptions::builder()
            .add_fallback_encoding(encoding_rs::WINDOWS_1252)
            .add_fallback_encoding(encoding_rs::WINDOWS_1252)
            .build();
        assert_eq!(options.fallback_encodings(), &[encoding_rs::WINDOWS_1252]);
    }

    #[test]
    fn test_distinct_fallback_is_preserved() {
        let options = XportReaderOptions::builder()
            .set_encoding(encoding_rs::UTF_8)
            .add_fallback_encoding(encoding_rs::WINDOWS_1252)
            .build();
        assert_eq!(options.fallback_encodings(), &[encoding_rs::WINDOWS_1252]);
    }
}
