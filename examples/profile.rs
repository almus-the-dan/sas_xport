use clap::{Parser, ValueEnum};
use sas_xport::sas::SasVariableType;
#[cfg(feature = "tokio")]
use sas_xport::sas::xport::{AsyncXportReader, AsyncXportWriter};
use sas_xport::sas::xport::{
    XportMetadata, XportReader, XportSchema, XportValue, XportVariable, XportWriter,
};
use std::fs::File;
use std::time::Instant;
use tempfile::NamedTempFile;

#[derive(Parser)]
#[command(about = "Profile sas_xport read/write throughput")]
struct Cli {
    /// Which phase to run
    #[arg(long, short, default_value = "sync-both")]
    phase: Phase,

    /// Number of records to generate
    #[arg(long, short, default_value_t = 1_000_000)]
    records: usize,
}

#[derive(Clone, PartialEq, ValueEnum)]
enum Phase {
    SyncWrite,
    SyncRead,
    SyncBoth,
    #[cfg(feature = "tokio")]
    AsyncWrite,
    #[cfg(feature = "tokio")]
    AsyncRead,
    #[cfg(feature = "tokio")]
    AsyncBoth,
    #[cfg(feature = "tokio")]
    All,
}

fn build_bench_schema() -> XportSchema {
    let mut builder = XportSchema::builder();
    builder.dataset_name("BENCH");

    let variables: &[(&str, SasVariableType, u16)] = &[
        ("STUDYID", SasVariableType::Character, 20),
        ("USUBJID", SasVariableType::Character, 40),
        ("SUBJID", SasVariableType::Character, 8),
        ("SITEID", SasVariableType::Character, 8),
        ("TRTA", SasVariableType::Character, 40),
        ("AGE", SasVariableType::Numeric, 8),
        ("SEX", SasVariableType::Character, 8),
        ("RACE", SasVariableType::Character, 40),
        ("ETHNIC", SasVariableType::Character, 40),
        ("COUNTRY", SasVariableType::Character, 8),
        ("DMDTC", SasVariableType::Character, 20),
        ("WEIGHT", SasVariableType::Numeric, 8),
        ("HEIGHT", SasVariableType::Numeric, 8),
        ("BMI", SasVariableType::Numeric, 8),
        ("AVAL", SasVariableType::Numeric, 8),
        ("BASE", SasVariableType::Numeric, 8),
        ("CHG", SasVariableType::Numeric, 8),
        ("PCHG", SasVariableType::Numeric, 8),
        ("AVALC", SasVariableType::Character, 40),
        ("PARAM", SasVariableType::Character, 40),
    ];

    for &(name, var_type, length) in variables {
        let mut v = XportVariable::builder();
        v.short_name(name).value_type(var_type).value_length(length);
        builder.add_variable(v);
    }

    builder.try_build().unwrap()
}

fn build_template_record() -> Vec<XportValue<'static>> {
    vec![
        XportValue::from("STUDY-ABC-001"),
        XportValue::from("STUDY-ABC-001-SUBJ-00001"),
        XportValue::from("00001"),
        XportValue::from("SITE01"),
        XportValue::from("Placebo"),
        XportValue::from(65.0),
        XportValue::from("F"),
        XportValue::from("WHITE"),
        XportValue::from("NOT HISPANIC OR LATINO"),
        XportValue::from("USA"),
        XportValue::from("2024-01-15"),
        XportValue::from(72.5),
        XportValue::from(170.0),
        XportValue::from(25.1),
        XportValue::from(120.0),
        XportValue::from(115.0),
        XportValue::from(5.0),
        XportValue::from(4.35),
        XportValue::from("Normal"),
        XportValue::from("Systolic Blood Pressure"),
    ]
}

fn run_write(tmp: &NamedTempFile, record_count: usize) -> (std::time::Duration, u64) {
    let schema = build_bench_schema();
    let template = build_template_record();
    let file = File::create(tmp.path()).unwrap();

    let start = Instant::now();
    let metadata = XportMetadata::builder().build();
    let writer = XportWriter::from_file(file, metadata).unwrap();
    let mut writer = writer.write_schema(schema).unwrap();
    for _ in 0..record_count {
        writer.write_record(&template).unwrap();
    }
    writer.finish().unwrap();
    let elapsed = start.elapsed();

    let file_size = std::fs::metadata(tmp.path()).unwrap().len();
    (elapsed, file_size)
}

fn run_read(tmp: &NamedTempFile) -> (std::time::Duration, u64, u64) {
    let file_size = std::fs::metadata(tmp.path()).unwrap().len();
    let file = File::open(tmp.path()).unwrap();

    let start = Instant::now();
    let reader = XportReader::from_file(file).unwrap();
    let Some(mut dataset) = reader.next_dataset().unwrap() else {
        panic!("No dataset found");
    };
    let mut record_count: u64 = 0;
    loop {
        while dataset.next_record().unwrap().is_some() {
            record_count += 1;
        }
        match dataset.next_dataset().unwrap() {
            Some(next) => dataset = next,
            None => break,
        }
    }
    let elapsed = start.elapsed();

    (elapsed, record_count, file_size)
}

#[cfg(feature = "tokio")]
async fn run_async_write(tmp: &NamedTempFile, record_count: usize) -> (std::time::Duration, u64) {
    let schema = build_bench_schema();
    let template = build_template_record();
    let file = tokio::fs::File::create(tmp.path()).await.unwrap();

    let start = Instant::now();
    let metadata = XportMetadata::builder().build();
    let writer = AsyncXportWriter::from_file(file, metadata).await.unwrap();
    let mut writer = writer.write_schema(schema).await.unwrap();
    for _ in 0..record_count {
        writer.write_record(&template).await.unwrap();
    }
    writer.finish().await.unwrap();
    let elapsed = start.elapsed();

    let file_size = std::fs::metadata(tmp.path()).unwrap().len();
    (elapsed, file_size)
}

#[cfg(feature = "tokio")]
async fn run_async_read(tmp: &NamedTempFile) -> (std::time::Duration, u64, u64) {
    let file_size = std::fs::metadata(tmp.path()).unwrap().len();
    let file = tokio::fs::File::open(tmp.path()).await.unwrap();

    let start = Instant::now();
    let reader = AsyncXportReader::from_file(file).await.unwrap();
    let Some(mut dataset) = reader.next_dataset().await.unwrap() else {
        panic!("No dataset found");
    };
    let mut record_count: u64 = 0;
    loop {
        while dataset.next_record().await.unwrap().is_some() {
            record_count += 1;
        }
        match dataset.next_dataset().await.unwrap() {
            Some(next) => dataset = next,
            None => break,
        }
    }
    let elapsed = start.elapsed();

    (elapsed, record_count, file_size)
}

#[allow(clippy::cast_precision_loss)]
fn print_row(phase: &str, records: u64, size_bytes: u64, elapsed: std::time::Duration) {
    let size_mb = size_bytes as f64 / (1024.0 * 1024.0);
    let secs = elapsed.as_secs_f64();
    let records_per_sec = records as f64 / secs;
    let mb_per_sec = size_mb / secs;
    println!(
        "{phase:<12} {records:>12} {size_mb:>10.1} {secs:>10.3} {records_per_sec:>12.0} {mb_per_sec:>10.1}",
    );
}

fn main() {
    let cli = Cli::parse();

    println!("Records: {}", cli.records);
    println!();
    println!(
        "{:<12} {:>12} {:>10} {:>10} {:>12} {:>10}",
        "Phase", "Records", "Size (MB)", "Time (s)", "Records/s", "MB/s"
    );
    println!("{}", "-".repeat(70));

    let tmp = NamedTempFile::new().unwrap();

    let run_sync_write =
        matches!(cli.phase, Phase::SyncWrite | Phase::SyncBoth) || cfg_all(&cli.phase);
    let run_sync_read =
        matches!(cli.phase, Phase::SyncRead | Phase::SyncBoth) || cfg_all(&cli.phase);

    // Ensure a file exists before any read-only phase.
    if needs_seed_file(&cli.phase) && !run_sync_write {
        run_write(&tmp, cli.records);
    }

    if run_sync_write {
        let (elapsed, file_size) = run_write(&tmp, cli.records);
        print_row("SyncWrite", cli.records as u64, file_size, elapsed);
    }

    if run_sync_read {
        let (elapsed, records_read, file_size) = run_read(&tmp);
        print_row("SyncRead", records_read, file_size, elapsed);
    }

    #[cfg(feature = "tokio")]
    run_async_phases(&cli, &tmp, run_sync_write);
}

/// Returns true for the `All` variant (only exists with tokio).
fn cfg_all(_phase: &Phase) -> bool {
    #[cfg(feature = "tokio")]
    if *_phase == Phase::All {
        return true;
    }
    false
}

/// Returns true if any read phase is selected that needs a pre-existing file.
fn needs_seed_file(phase: &Phase) -> bool {
    let sync_read = matches!(phase, Phase::SyncRead | Phase::SyncBoth);
    #[cfg(feature = "tokio")]
    let async_read = matches!(phase, Phase::AsyncRead | Phase::AsyncBoth | Phase::All);
    #[cfg(not(feature = "tokio"))]
    let async_read = false;
    sync_read || async_read
}

#[cfg(feature = "tokio")]
fn run_async_phases(cli: &Cli, tmp: &NamedTempFile, sync_wrote: bool) {
    let should_write = matches!(cli.phase, Phase::AsyncWrite | Phase::AsyncBoth | Phase::All);
    let should_read = matches!(cli.phase, Phase::AsyncRead | Phase::AsyncBoth | Phase::All);

    if !should_write && !should_read {
        return;
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        if should_write {
            let (elapsed, file_size) = run_async_write(tmp, cli.records).await;
            print_row("AsyncWrite", cli.records as u64, file_size, elapsed);
        }

        if should_read {
            if !sync_wrote && !should_write {
                run_write(tmp, cli.records);
            }
            let (elapsed, records_read, file_size) = run_async_read(tmp).await;
            print_row("AsyncRead", records_read, file_size, elapsed);
        }
    });
}
