use super::xport_buffer::XportBuffer;
use super::{Result, XportDataset, XportMetadata, XportReaderOptions};
use std::fs::File;
use std::io::{BufRead, BufReader};

/// The result of opening a SAS® transport file. A valid transport file always
/// contains metadata, but may or may not contain datasets.
#[derive(Debug)]
pub struct XportReader<R> {
    buffer: XportBuffer<R>,
    metadata: XportMetadata,
}

impl<R> XportReader<R> {
    /// Gets the file metadata.
    #[inline]
    #[must_use]
    pub fn metadata(&self) -> &XportMetadata {
        &self.metadata
    }
}

impl XportReader<BufReader<File>> {
    /// Opens a SAS® transport file using the given options. The file will
    /// be buffered using the default buffer size.
    ///
    /// # Errors
    /// An error is returned if:
    /// * An I/O error occurs while reading the file
    /// * An encoding error occurs while reading metadata or the schema
    pub fn from_file(file: File, options: &XportReaderOptions) -> Result<Self> {
        Self::from_reader(BufReader::new(file), options)
    }
}

impl<R: BufRead> XportReader<R> {
    /// Opens a SAS® transport file from the given reader using the given options.
    ///
    /// # Errors
    /// An error is returned if:
    /// * An I/O error occurs while reading the file
    /// * An encoding error occurs while reading metadata or the schema
    pub fn from_reader(reader: R, options: &XportReaderOptions) -> Result<Self> {
        let mut buffer = XportBuffer::from_reader(reader, options);
        let metadata = buffer.read_metadata()?;
        let reader = XportReader { buffer, metadata };
        Ok(reader)
    }

    /// Reads the next dataset schema. If there are no more datasets, `None`
    /// is returned and the reader is consumed.
    ///
    /// # Errors
    /// An error is returned if:
    /// * An I/O error occurs while reading the schema
    /// * An encoding error occurs while parsing the schema
    pub fn next_dataset(mut self) -> Result<Option<XportDataset<R>>> {
        let Some(schema) = self.buffer.read_schema(self.metadata.file_version())? else {
            return Ok(None);
        };
        let record_offset = self.buffer.position();
        let dataset = XportDataset::new(self.buffer, self.metadata, schema, record_offset);
        Ok(Some(dataset))
    }
}
