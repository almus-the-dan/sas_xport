# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-16

### Added

- `XportReader::from_path` and `AsyncXportReader::from_path` open a file by path with default options.
- `XportWriter::from_path` and `AsyncXportWriter::from_path` create a file by path with default options.
- `XportReader::options()` / `XportWriter::options()` return an options builder with terminal methods (`from_file`, `from_reader`/`from_writer`, `from_path`) that construct the reader or writer directly.
- `AsyncXportReader::options()` / `AsyncXportWriter::options()` return the same builder, with async terminal methods (`from_tokio_file`, `from_tokio_reader`/`from_tokio_writer`, `from_tokio_path`).
- `TruncationPolicy` is now in its own module (`truncation_policy.rs`).

### Changed

- **Breaking:** `XportReader::from_file` and `from_reader` no longer take an `&XportReaderOptions` parameter. They use default options. Use `XportReader::options()` for custom encoding settings.
- **Breaking:** `AsyncXportReader::from_file` and `from_reader` no longer take an `&XportReaderOptions` parameter. Use `AsyncXportReader::options()` for custom encoding settings.
- **Breaking:** `XportWriter::from_file` and `from_writer` no longer take an `XportWriterOptions` parameter. They use default options. Use `XportWriter::options()` for custom encoding or truncation settings.
- **Breaking:** `AsyncXportWriter::from_file` and `from_writer` no longer take an `XportWriterOptions` parameter. Use `AsyncXportWriter::options()` for custom encoding or truncation settings.
- **Breaking:** `XportReaderOptions` is now the builder (previously `XportReaderOptionsBuilder`). The built options struct is now internal.
- **Breaking:** `XportWriterOptions` is now the builder (previously `XportWriterOptionsBuilder`). The built options struct is now internal.
- **Breaking:** `build()` and `build_into()` are removed from the public API. Use the terminal methods (`from_file`, `from_reader`, `from_path`, etc.) on the builder instead.
- **Breaking:** `XportDataset::read_to_end` renamed to `skip_to_end`.
- **Breaking:** `AsyncXportDataset::read_to_end` renamed to `skip_to_end`.
- **Breaking:** Builder setter methods drop the `set_` prefix (e.g., `set_encoding` becomes `encoding`, `set_dataset_name` becomes `dataset_name`). `clear_*` methods are unchanged.

### Removed

- `XportReaderOptionsBuilder` — use `XportReaderOptions` directly.
- `XportWriterOptionsBuilder` — use `XportWriterOptions` directly.

### Migration guide

#### Reading with default options

```rust
// 0.1
let options = XportReaderOptions::builder().build();
let reader = XportReader::from_file(file, &options)?;

// 0.2
let reader = XportReader::from_file(file)?;
```

#### Reading with custom options

```rust
// 0.1
let options = XportReaderOptions::builder()
    .set_encoding(encoding_rs::WINDOWS_1252)
    .build();
let reader = XportReader::from_file(file, &options)?;

// 0.2
let reader = XportReader::options()
    .encoding(encoding_rs::WINDOWS_1252)
    .from_file(file)?;
```

#### Opening by path (new)

```rust
let reader = XportReader::from_path("data.xpt")?;
```

#### Writing with default options

```rust
// 0.1
let writer = XportWriter::from_file(file, metadata, XportWriterOptions::default())?;

// 0.2
let writer = XportWriter::from_file(file, metadata)?;
```

#### Writing with custom options

```rust
// 0.1
let options = XportWriterOptions::builder()
    .set_truncation_policy(SasVariableType::Character, TruncationPolicy::Report)
    .build();
let writer = XportWriter::from_writer(writer, metadata, options)?;

// 0.2
let writer = XportWriter::options()
    .truncation_policy(SasVariableType::Character, TruncationPolicy::Report)
    .from_writer(writer, metadata)?;
```

#### Async with custom options

```rust
// 0.1
let options = XportReaderOptions::builder()
    .set_encoding(encoding_rs::WINDOWS_1252)
    .build();
let reader = AsyncXportReader::from_file(file, &options).await?;

// 0.2
let reader = AsyncXportReader::options()
    .encoding(encoding_rs::WINDOWS_1252)
    .from_tokio_file(file).await?;
```

#### Builder setter methods

```rust
// 0.1
let mut v = XportVariable::builder();
v.set_short_name("AGE")
    .set_value_type(SasVariableType::Numeric)
    .set_value_length(8);

// 0.2
let mut v = XportVariable::builder();
v.short_name("AGE")
    .value_type(SasVariableType::Numeric)
    .value_length(8);
```

#### Skipping remaining records

```rust
// 0.1
dataset.read_to_end()?;

// 0.2
dataset.skip_to_end()?;
```

## [0.1.0] - 2026-04-10

Initial release.
