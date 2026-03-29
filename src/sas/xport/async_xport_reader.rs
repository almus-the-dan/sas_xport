use super::async_xport_buffer::AsyncXportBuffer;
use super::{AsyncXportDataset, Result, XportMetadata, XportReaderOptions};
use tokio::fs::File;
use tokio::io::{AsyncBufRead, BufReader};

/// The result of opening a SAS® transport file asynchronously. A valid
/// transport file always contains metadata, but may or may not contain datasets.
#[derive(Debug)]
pub struct AsyncXportReader<R> {
    buffer: AsyncXportBuffer<R>,
    metadata: XportMetadata,
}

impl<R> AsyncXportReader<R> {
    /// Gets the file metadata.
    #[inline]
    #[must_use]
    pub fn metadata(&self) -> &XportMetadata {
        &self.metadata
    }
}

impl AsyncXportReader<BufReader<File>> {
    /// Opens a SAS® transport file using the given options. The file will
    /// be buffered using the default buffer size.
    ///
    /// # Errors
    /// An error is returned if:
    /// * An I/O error occurs while reading the file
    /// * An encoding error occurs while reading metadata or the schema
    pub async fn from_file(file: File, options: &XportReaderOptions) -> Result<Self> {
        Self::from_reader(BufReader::new(file), options).await
    }
}

impl<R: AsyncBufRead + Unpin> AsyncXportReader<R> {
    /// Opens a SAS® transport file from the given reader using the given options.
    ///
    /// # Errors
    /// An error is returned if:
    /// * An I/O error occurs while reading the file
    /// * An encoding error occurs while reading metadata or the schema
    pub async fn from_reader(reader: R, options: &XportReaderOptions) -> Result<Self> {
        let mut buffer = AsyncXportBuffer::from_reader(reader, options);
        let metadata = buffer.read_metadata().await?;
        let reader = AsyncXportReader { buffer, metadata };
        Ok(reader)
    }

    /// Reads the next dataset schema. If there are no more datasets, `None`
    /// is returned and the reader is consumed.
    ///
    /// # Errors
    /// An error is returned if:
    /// * An I/O error occurs while reading the schema
    /// * An encoding error occurs while parsing the schema
    pub async fn next_dataset(mut self) -> Result<Option<AsyncXportDataset<R>>> {
        let Some(schema) = self
            .buffer
            .read_schema(self.metadata.file_version())
            .await?
        else {
            return Ok(None);
        };
        let record_offset = self.buffer.position();
        let dataset = AsyncXportDataset::new(self.buffer, self.metadata, schema, record_offset);
        Ok(Some(dataset))
    }
}
