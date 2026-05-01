# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-05-01

### Changed

- **Breaking (behavior):** `SasFloat64::try_from(f64)` underflow handling changed for negative finite inputs. Tiny negative values (smaller in magnitude than the smallest representable IBM HFP value) previously round-tripped through `From<SasFloat64> for f64` as NaN — the saturating converter wrote the IBM HFP `-0` byte pattern (`[0x80, 0, 0, 0, 0, 0, 0, 0]`), which collides with the SAS missing-value sentinel encoding. They now round-trip as `+0.0`. Fix to a latent bug; users relying on the NaN behavior will see a difference.
- **Internal:** IBM hexadecimal floating point types (`IbmFloat32`, `IbmFloat64`, `IbmFloatError`, `ParseIbmFloatError`) extracted to a dedicated [`ibm_hfp` crate](https://crates.io/crates/ibm_hfp) (added as a dependency). Public trait impls on `SasFloat64` that reference `IbmFloat64` now resolve to `ibm_hfp::IbmFloat64`; downstream code that wants to construct or match on these types should add `ibm_hfp` as a direct dependency. Most existing users will not notice — `IbmFloat64` was unreachable through `sas_xport`'s public API in 0.3.0.

### Fixed

- `tests/async_xport_reader_test.rs` now correctly gates on `feature = "tokio"`. `cargo test` (default features) previously failed to compile this test file; only `cargo test --features tokio` worked. Both modes now build and pass.

## [0.3.0] - 2026-04-18

### Changed

- **Breaking:** `XportWriter::finish` and `AsyncXportWriter::finish` (on the schema-stage writers) now return `Result<W>` instead of `Result<()>` — the inner writer/sink is returned to the caller, so the underlying `File` (or other `Write`/`AsyncWrite` impl) can be reused after the XPORT trailer is committed.

### Removed

- **Breaking:** The `Drop` impl on writer types is gone. Trailing-record commitment used to happen implicitly when the writer was dropped, which silently swallowed write errors. Callers must now explicitly call `finish()` to commit the trailer (and observe any I/O error).

### Migration guide

```rust
// 0.2
let mut writer = XportWriter::from_file(file, metadata)?
    .write_schema(schema)?;
writer.write_record(&record)?;
// (Drop committed the trailer — silently)

// 0.3
let mut writer = XportWriter::from_file(file, metadata)?
    .write_schema(schema)?;
writer.write_record(&record)?;
let file = writer.finish()?; // explicit commit, returns the inner File
```

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
