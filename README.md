# sas_xport

A Rust library for reading and writing SAS® Transport (XPORT) files, supporting both V5 and V8 formats (V8 is also used in SAS® V9+).

## Features

- Read file-level metadata (SAS version, OS, creation date)
- Read dataset schemas with full variable definitions (name, type, length, label, format)
- Two record-reading APIs:
  - **Iterator API** (`records()`) — fully owned values, simple to use
  - **Zero-copy API** (`next_record()`) — borrows from an internal buffer, avoids per-record allocations
- Write XPORT files with the `XportWriter` API
- Optional async support via the `tokio` feature
- Optional `chrono` feature for date/time conversions

## Usage

### Using the record iterator

```rust
use std::fs::File;
use sas_xport::sas::xport::{XportReader, XportReaderOptions, Result};

pub fn read_xport_file(file: File) -> Result<()> {
    let options = XportReaderOptions::builder().build();
    let reader = XportReader::from_file(file, &options)?;
    let metadata = reader.metadata().clone();
    let Some(mut dataset) = reader.next_dataset()? else {
        println!("File contains no datasets");
        return Ok(());
    };
    loop {
        let schema = dataset.schema();
        println!("Dataset: {} (SAS® {})", schema.dataset_name(), metadata.sas_version());
        println!("Variable count: {}", schema.variables().len());

        for record in dataset.records() {
            let _record = record?;
        }
        println!("Record count: {}", dataset.record_number());
        let Some(next) = dataset.next_dataset()? else {
            break;
        };
        dataset = next;
    }
    Ok(())
}
```

### Using `next_record` for zero-copy access

```rust
use std::fs::File;
use sas_xport::sas::xport::{XportReader, XportReaderOptions, Result};

pub fn read_xport_file(file: File) -> Result<()> {
    let options = XportReaderOptions::builder().build();
    let reader = XportReader::from_file(file, &options)?;
    let Some(mut dataset) = reader.next_dataset()? else {
        println!("File contains no datasets");
        return Ok(());
    };
    loop {
        println!("Dataset: {}", dataset.schema().dataset_name());

        let mut buffer = Vec::new();
        while let Some(record) = dataset.next_record(&mut buffer)? {
            // Values in `record` borrow from `buffer` and are
            // invalidated on the next call to `next_record`.
        }
        println!("Record count: {}", dataset.record_number());
        let Some(next) = dataset.next_dataset()? else {
            break;
        };
        dataset = next;
    }
    Ok(())
}
```

### Writing records

```rust
use std::fs::File;
use sas_xport::sas::SasVariableType;
use sas_xport::sas::xport::{
    Result, XportMetadata, XportSchema, XportValue, XportVariable, XportWriter,
};

pub fn write_xport_file(file: File) -> Result<()> {
    let metadata = XportMetadata::builder().build();

    let mut study_id = XportVariable::builder();
    study_id
        .set_short_name("STUDYID")
        .set_value_type(SasVariableType::Character)
        .set_value_length(20);

    let mut age = XportVariable::builder();
    age.set_short_name("AGE")
        .set_value_type(SasVariableType::Numeric)
        .set_value_length(8);

    let schema = XportSchema::builder()
        .set_dataset_name("DM")
        .add_variable(study_id)
        .add_variable(age)
        .try_build()?;

    let writer = XportWriter::from_file(file, metadata, encoding_rs::UTF_8)?;
    let mut writer = writer.write_schema(schema)?;
    writer.write_record(&[XportValue::from("STUDY-001"), XportValue::from(35.0)])?;
    writer.write_record(&[XportValue::from("STUDY-001"), XportValue::from(42.5)])?;
    writer.finish()
}
```

## About

In my recent line of work, I became very familiar with the SAS® Transport (XPORT) file format. I took a personal interest in Rust about 5 years ago and decided I needed an engaging side project to help me learn it better. That's how this project was born. Fortunately, I was able to claim this project, among several of my other pre-existing open source projects, as belonging to me when joining my last company.

Rust still has a young ecosystem, and a great deal of statistical software requires working with SAS® XPORT files, especially in the clinical industry. I wanted to provide a pure Rust implementation. My goal was to make a flexible reader and writer that operated at breakneck speeds. I modeled it after other wonderful libraries like [BurntSushi/csv](https://github.com/BurntSushi/rust-csv) and [tafia/quick-xml](https://github.com/tafia/quick-xml).

On a personal note, please do not treat this project as a source of free work. It started as a fun side project. I do take the quality of this project very seriously and want to provide a valuable resource to the community, but my personal time is precious and rare. Please do not create issues to submit feature requests – only submit legitimate bugs – and allow me the flexibility to address them at my own pace. I am happy to entertain high-quality pull requests on my own schedule. That said, do not get angry if I close issues and pull requests not following these guidelines. 

If you want to support me, please share this repository to help it grow.

## Benchmarks

The benchmark suite measures record-reading throughput using a synthetic 100 thousand-record XPT V5 file with 20 variables (8 numeric, 12 character). The test file is auto-generated on the first run.

Three benchmarks are included:

| Benchmark | Description |
|---|---|
| `read_all_zerocopy` | `next_record()` API reading from disk |
| `read_all_iterator` | `records()` iterator reading from disk |
| `read_all_cursor` | `next_record()` from an in-memory cursor (isolates parsing from I/O) |

### Running benchmarks

Run all benchmarks:

```sh
cargo bench -p sas_xport
```

Run a specific benchmark by name:

```sh
cargo bench -p sas_xport -- read_all_cursor
```

After the first run, Criterion saves baseline results in `target/criterion/`. Subsequent runs compare against the baseline and report regressions or improvements. To update the baseline:

```sh
cargo bench -p sas_xport -- --save-baseline main
```

HTML reports are generated in `target/criterion/report/index.html`.

## Profiling

One-time setup: `cargo install --locked samply`

### Wall-clock summary

Generates a synthetic file with the writer and reads it back, printing throughput for each phase:

```sh
cargo run --example profile --profile profiling -p sas_xport
cargo run --example profile --profile profiling -p sas_xport -- --records 500000 --phase write
```

### Function-level profiling

Uses `samply` to sample both the write and read phases, then reports the top functions from this crate by inclusive and self-time percentage:

```sh
# Linux / macOS
./profile.sh
./profile.sh --records 500000 --top 20

# Windows (PowerShell)
.\profile.ps1
.\profile.ps1 -Records 500000 -Top 20
```

## Code Coverage

One-time setup: `cargo install cargo-llvm-cov`

Generate a coverage summary:

```sh
cargo llvm-cov --package sas_xport --all-features
```

Generate an HTML report:

```sh
cargo llvm-cov --package sas_xport --all-features --html
open target/llvm-cov/html/index.html
```