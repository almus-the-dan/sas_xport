use criterion::{Criterion, criterion_group, criterion_main};
use sas_xport::sas::SasVariableType;
use sas_xport::sas::xport::{
    XportMetadata, XportReader, XportSchema, XportValue, XportVariable, XportWriter,
};
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::path::Path;

// ---------------------------------------------------------------------------
// Synthetic XPT V5 file generator (using XportWriter)
// ---------------------------------------------------------------------------

const BENCH_FILE: &str = "benches/data/bench_large.xpt";
const RECORD_COUNT: usize = 100_000;

/// Build the ADaM-style schema: 8 numeric + 12 character = 20 variables.
fn build_bench_schema() -> XportSchema {
    let mut builder = XportSchema::builder();
    builder.set_dataset_name("BENCH");

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
        v.set_short_name(name)
            .set_value_type(var_type)
            .set_value_length(length);
        builder.add_variable(v);
    }

    builder.try_build().unwrap()
}

/// Build one representative record matching the schema variable order.
fn build_template_record() -> Vec<XportValue<'static>> {
    vec![
        XportValue::from("STUDY-ABC-001"),            // STUDYID
        XportValue::from("STUDY-ABC-001-SUBJ-00001"), // USUBJID
        XportValue::from("00001"),                    // SUBJID
        XportValue::from("SITE01"),                   // SITEID
        XportValue::from("Placebo"),                  // TRTA
        XportValue::from(65.0),                       // AGE
        XportValue::from("F"),                        // SEX
        XportValue::from("WHITE"),                    // RACE
        XportValue::from("NOT HISPANIC OR LATINO"),   // ETHNIC
        XportValue::from("USA"),                      // COUNTRY
        XportValue::from("2024-01-15"),               // DMDTC
        XportValue::from(72.5),                       // WEIGHT
        XportValue::from(170.0),                      // HEIGHT
        XportValue::from(25.1),                       // BMI
        XportValue::from(120.0),                      // AVAL
        XportValue::from(115.0),                      // BASE
        XportValue::from(5.0),                        // CHG
        XportValue::from(4.35),                       // PCHG
        XportValue::from("Normal"),                   // AVALC
        XportValue::from("Systolic Blood Pressure"),  // PARAM
    ]
}

/// Generate a synthetic V5 XPT file at the given path using `XportWriter`.
fn generate_xpt(path: &Path) {
    let file = File::create(path).expect("Failed to create benchmark file");
    let metadata = XportMetadata::builder().build();
    let schema = build_bench_schema();
    let template = build_template_record();

    let writer = XportWriter::from_file(file, metadata).unwrap();
    let mut writer = writer.write_schema(schema).unwrap();

    for _ in 0..RECORD_COUNT {
        writer.write_record(&template).unwrap();
    }

    writer.finish().unwrap();
}

/// Ensure the synthetic file exists, regenerating if needed.
fn ensure_bench_file() -> std::path::PathBuf {
    let path = Path::new(BENCH_FILE);
    if !path.exists() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("Failed to create benches/data directory");
        }
        eprintln!("Generating synthetic benchmark file ({RECORD_COUNT} records)...");
        generate_xpt(path);
        eprintln!(
            "Generated: {} ({} bytes)",
            path.display(),
            std::fs::metadata(path).unwrap().len()
        );
    }
    path.to_path_buf()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// End-to-end throughput using the zero-copy `next_record` API.
fn read_all_zerocopy(c: &mut Criterion) {
    let path = ensure_bench_file();
    c.bench_function("read_all_zerocopy", |b| {
        b.iter(|| {
            let file = File::open(&path).unwrap();
            let reader = XportReader::from_file(file).unwrap();
            let Some(mut dataset) = reader.next_dataset().unwrap() else {
                panic!("No dataset found");
            };
            let mut record_count = 0u64;
            loop {
                while dataset.next_record().unwrap().is_some() {
                    record_count += 1;
                }
                match dataset.next_dataset().unwrap() {
                    Some(next) => dataset = next,
                    None => break,
                }
            }
            assert_eq!(record_count, RECORD_COUNT as u64);
        });
    });
}

/// End-to-end throughput using the owning `records()` iterator.
fn read_all_iterator(c: &mut Criterion) {
    let path = ensure_bench_file();
    c.bench_function("read_all_iterator", |b| {
        b.iter(|| {
            let file = File::open(&path).unwrap();
            let reader = XportReader::from_file(file).unwrap();
            let Some(mut dataset) = reader.next_dataset().unwrap() else {
                panic!("No dataset found");
            };
            let mut record_count = 0u64;
            loop {
                for record in dataset.records() {
                    let _record = record.unwrap();
                    record_count += 1;
                }
                match dataset.next_dataset().unwrap() {
                    Some(next) => dataset = next,
                    None => break,
                }
            }
            assert_eq!(record_count, RECORD_COUNT as u64);
        });
    });
}

/// Parsing throughput from in-memory buffer, isolating from filesystem I/O.
fn read_all_cursor(c: &mut Criterion) {
    let path = ensure_bench_file();
    let file_bytes = std::fs::read(&path).unwrap();

    c.bench_function("read_all_cursor", |b| {
        b.iter(|| {
            let cursor = Cursor::new(file_bytes.as_slice());
            let buf_reader = BufReader::new(cursor);
            let reader = XportReader::from_reader(buf_reader).unwrap();
            let Some(mut dataset) = reader.next_dataset().unwrap() else {
                panic!("No dataset found");
            };
            let mut record_count = 0u64;
            loop {
                while dataset.next_record().unwrap().is_some() {
                    record_count += 1;
                }
                match dataset.next_dataset().unwrap() {
                    Some(next) => dataset = next,
                    None => break,
                }
            }
            assert_eq!(record_count, RECORD_COUNT as u64);
        });
    });
}

criterion_group!(
    benches,
    read_all_zerocopy,
    read_all_iterator,
    read_all_cursor
);
criterion_main!(benches);
