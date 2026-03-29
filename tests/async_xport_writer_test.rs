#[cfg(test)]
#[cfg(feature = "tokio")]
mod tests {
    use float_cmp::assert_approx_eq;
    use sas_xport::sas::SasVariableType;
    use sas_xport::sas::xport::{
        AsyncXportWriter, TruncationPolicy, XportDatasetVersion, XportFileVersion, XportMetadata,
        XportReader, XportReaderOptions, XportSchema, XportValue, XportVariable,
        XportWriterOptions,
    };
    use std::io::{BufReader, Cursor as StdCursor};

    fn assert_character(expected: &str, value: &XportValue<'_>) {
        match value {
            XportValue::Character(cow) => assert_eq!(expected, cow.as_ref()),
            XportValue::Number(n) => panic!("expected Character(\"{expected}\"), got Number({n})"),
        }
    }

    fn assert_number(expected: f64, value: &XportValue<'_>) {
        match value {
            XportValue::Number(n) => assert_approx_eq!(f64, expected, *n),
            XportValue::Character(s) => {
                panic!("expected Number({expected}), got Character(\"{s}\")")
            }
        }
    }

    #[tokio::test]
    async fn round_trips_v5_metadata() {
        use sas_xport::sas::{SasDateTime, SasMonth};

        let created = SasDateTime::builder()
            .day(15)
            .month(SasMonth::March)
            .year(26)
            .hour(10)
            .minute(30)
            .second(45)
            .build();
        let modified = SasDateTime::builder()
            .day(16)
            .month(SasMonth::March)
            .year(26)
            .hour(11)
            .minute(0)
            .second(0)
            .build();
        let metadata = XportMetadata::builder()
            .set_symbol1("SAS")
            .set_symbol2("SAS")
            .set_library("MYLIB")
            .set_sas_version("9.4")
            .set_operating_system("Linux")
            .set_created(created)
            .set_modified(modified)
            .build();

        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer = AsyncXportWriter::from_writer(
            &mut buffer,
            metadata.clone(),
            XportWriterOptions::default(),
        )
        .await
        .unwrap();
        writer.finish().await.unwrap();
        let bytes = buffer.into_inner();

        let reader_options = XportReaderOptions::builder().build();
        let reader =
            XportReader::from_reader(BufReader::new(StdCursor::new(bytes)), &reader_options)
                .unwrap();
        let read_metadata = reader.metadata();

        assert_eq!(XportFileVersion::V5, read_metadata.file_version());
        assert_eq!("SAS", read_metadata.symbol1());
        assert_eq!("SAS", read_metadata.symbol2());
        assert_eq!("MYLIB", read_metadata.library());
        assert_eq!("9.4", read_metadata.sas_version());
        assert_eq!("Linux", read_metadata.operating_system());
        assert_eq!(created, read_metadata.created());
        assert_eq!(modified, read_metadata.modified());
    }

    #[tokio::test]
    async fn round_trips_v8_metadata() {
        let metadata = XportMetadata::builder()
            .set_xport_file_version(XportFileVersion::V8)
            .set_symbol1("TST")
            .set_symbol2("XPT")
            .set_library("SASLIB")
            .set_sas_version("9.1")
            .set_operating_system("WinXP")
            .build();

        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer = AsyncXportWriter::from_writer(
            &mut buffer,
            metadata.clone(),
            XportWriterOptions::default(),
        )
        .await
        .unwrap();
        writer.finish().await.unwrap();
        let bytes = buffer.into_inner();

        let reader_options = XportReaderOptions::builder().build();
        let reader =
            XportReader::from_reader(BufReader::new(StdCursor::new(bytes)), &reader_options)
                .unwrap();
        let read_metadata = reader.metadata();

        assert_eq!(XportFileVersion::V8, read_metadata.file_version());
        assert_eq!("TST", read_metadata.symbol1());
        assert_eq!("XPT", read_metadata.symbol2());
        assert_eq!("SASLIB", read_metadata.library());
        assert_eq!("9.1", read_metadata.sas_version());
        assert_eq!("WinXP", read_metadata.operating_system());
    }

    #[tokio::test]
    async fn round_trips_v5_single_dataset_with_records() {
        let schema = XportSchema::builder()
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("STUDYID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(8);
                v
            })
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("AGE")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder().build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();
        writer
            .write_record(&[XportValue::from("ABC-001"), XportValue::from(35.0)])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("ABC-002"), XportValue::from(42.5)])
            .await
            .unwrap();
        writer.set_count_and_finish().await.unwrap();

        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!("DM", dataset.schema().dataset_name());
        assert_eq!(None, dataset.schema().record_count());

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("ABC-001", &record[0]);
        assert_number(35.0, &record[1]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("ABC-002", &record[0]);
        assert_number(42.5, &record[1]);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn round_trips_v5_single_dataset_with_records_wide() {
        let schema = XportSchema::builder()
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("STUDYID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(40);
                v
            })
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("USUBJID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(40);
                v
            })
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("AGE")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();
        // Record width: 40 + 40 + 8 = 88 (>= 80)

        let metadata = XportMetadata::builder().build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();
        writer
            .write_record(&[
                XportValue::from("STUDY-001"),
                XportValue::from("STUDY-001-SUBJ-001"),
                XportValue::from(35.0),
            ])
            .await
            .unwrap();
        writer
            .write_record(&[
                XportValue::from("STUDY-001"),
                XportValue::from("STUDY-001-SUBJ-002"),
                XportValue::from(42.5),
            ])
            .await
            .unwrap();
        writer.set_count_and_finish().await.unwrap();

        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!("DM", dataset.schema().dataset_name());

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("STUDY-001", &record[0]);
        assert_character("STUDY-001-SUBJ-001", &record[1]);
        assert_number(35.0, &record[2]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("STUDY-001", &record[0]);
        assert_character("STUDY-001-SUBJ-002", &record[1]);
        assert_number(42.5, &record[2]);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn round_trips_v8_single_dataset_with_count() {
        let schema = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("AE")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_full_name("ADVERSE_EVENT_TERM")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(40);
                v
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder()
            .set_xport_file_version(XportFileVersion::V8)
            .build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();
        writer
            .write_record(&[XportValue::from("Headache")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("Nausea")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("Fatigue")])
            .await
            .unwrap();
        writer.set_count_and_finish().await.unwrap();

        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!(Some(3), dataset.schema().record_count());
        assert_eq!(
            "ADVERSE_EVENT_TERM",
            dataset.schema().variables()[0].full_name()
        );

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("Headache", &record[0]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("Nausea", &record[0]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("Fatigue", &record[0]);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn round_trips_v8_single_dataset_with_count_wide() {
        let schema = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("AE")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_full_name("ADVERSE_EVENT_TERM")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(80);
                v
            })
            .try_build()
            .unwrap();
        // Record width: 80 (>= 80)

        let metadata = XportMetadata::builder()
            .set_xport_file_version(XportFileVersion::V8)
            .build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();
        writer
            .write_record(&[XportValue::from("Headache")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("Nausea")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("Fatigue")])
            .await
            .unwrap();
        writer.set_count_and_finish().await.unwrap();

        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!(Some(3), dataset.schema().record_count());

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("Headache", &record[0]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("Nausea", &record[0]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("Fatigue", &record[0]);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn round_trips_v5_multiple_datasets() {
        let schema1 = XportSchema::builder()
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("SUBJID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let schema2 = XportSchema::builder()
            .set_dataset_name("AE")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("AETERM")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(20);
                v
            })
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("AESEQ")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder().build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();

        // First dataset
        let mut writer = writer.write_schema(schema1).await.unwrap();
        writer
            .write_record(&[XportValue::from("SUBJ-01")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("SUBJ-02")])
            .await
            .unwrap();
        let writer = writer.next_dataset().await.unwrap();

        // Second dataset
        let mut writer = writer.write_schema(schema2).await.unwrap();
        writer
            .write_record(&[XportValue::from("Headache"), XportValue::from(1.0)])
            .await
            .unwrap();
        writer.finish().await.unwrap();

        // Read back
        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();

        let mut dataset1 = reader.next_dataset().unwrap().expect("expected dataset 1");
        assert_eq!("DM", dataset1.schema().dataset_name());
        let record = dataset1.next_record().unwrap().unwrap();
        assert_character("SUBJ-01", &record[0]);
        let record = dataset1.next_record().unwrap().unwrap();
        assert_character("SUBJ-02", &record[0]);
        assert!(dataset1.next_record().unwrap().is_none());

        let mut dataset2 = dataset1
            .next_dataset()
            .unwrap()
            .expect("expected dataset 2");
        assert_eq!("AE", dataset2.schema().dataset_name());
        let record = dataset2.next_record().unwrap().unwrap();
        assert_character("Headache", &record[0]);
        assert_number(1.0, &record[1]);
        assert!(dataset2.next_record().unwrap().is_none());

        assert!(dataset2.next_dataset().unwrap().is_none());
    }

    #[tokio::test]
    async fn round_trips_v8_multiple_datasets_with_count() {
        // Use a character variable with length >= 80 to avoid the narrow-record
        // ambiguity in the reader's blank-row look-ahead.
        let schema1 = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("SUBJID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(80);
                v
            })
            .try_build()
            .unwrap();

        let schema2 = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("VSTEST")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(80);
                v
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder()
            .set_xport_file_version(XportFileVersion::V8)
            .build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();

        let mut writer = writer.write_schema(schema1).await.unwrap();
        writer
            .write_record(&[XportValue::from("SUBJ-001")])
            .await
            .unwrap();
        let writer = writer.set_count_and_next_dataset().await.unwrap();

        let mut writer = writer.write_schema(schema2).await.unwrap();
        writer
            .write_record(&[XportValue::from("WEIGHT")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("HEIGHT")])
            .await
            .unwrap();
        writer.set_count_and_finish().await.unwrap();

        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();

        let mut dataset1 = reader.next_dataset().unwrap().expect("expected dataset 1");
        assert_eq!("DM", dataset1.schema().dataset_name());
        assert_eq!(Some(1), dataset1.schema().record_count());
        let record = dataset1.next_record().unwrap().unwrap();
        assert_character("SUBJ-001", &record[0]);
        assert!(dataset1.next_record().unwrap().is_none());

        let mut dataset2 = dataset1
            .next_dataset()
            .unwrap()
            .expect("expected dataset 2");
        assert_eq!("VS", dataset2.schema().dataset_name());
        assert_eq!(Some(2), dataset2.schema().record_count());
        let record = dataset2.next_record().unwrap().unwrap();
        assert_character("WEIGHT", &record[0]);
        let record = dataset2.next_record().unwrap().unwrap();
        assert_character("HEIGHT", &record[0]);
        assert!(dataset2.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn round_trips_v8_multiple_datasets_with_count_narrow() {
        // Sub-80-byte record width to exercise the narrow-record path.
        let schema1 = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("SUBJID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let schema2 = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("VSTEST")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(20);
                v
            })
            .try_build()
            .unwrap();
        // Record widths: DS1 = 8, DS2 = 20 (both < 80)

        let metadata = XportMetadata::builder()
            .set_xport_file_version(XportFileVersion::V8)
            .build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, metadata, XportWriterOptions::default())
                .await
                .unwrap();

        let mut writer = writer.write_schema(schema1).await.unwrap();
        writer
            .write_record(&[XportValue::from("SUBJ-001")])
            .await
            .unwrap();
        let writer = writer.set_count_and_next_dataset().await.unwrap();

        let mut writer = writer.write_schema(schema2).await.unwrap();
        writer
            .write_record(&[XportValue::from("WEIGHT")])
            .await
            .unwrap();
        writer
            .write_record(&[XportValue::from("HEIGHT")])
            .await
            .unwrap();
        writer.set_count_and_finish().await.unwrap();

        let reader_options = XportReaderOptions::builder().build();
        let reader = XportReader::from_reader(
            BufReader::new(StdCursor::new(buffer.into_inner())),
            &reader_options,
        )
        .unwrap();

        let mut dataset1 = reader.next_dataset().unwrap().expect("expected dataset 1");
        assert_eq!("DM", dataset1.schema().dataset_name());
        assert_eq!(Some(1), dataset1.schema().record_count());
        let record = dataset1.next_record().unwrap().unwrap();
        assert_character("SUBJ-001", &record[0]);
        assert!(dataset1.next_record().unwrap().is_none());

        let mut dataset2 = dataset1
            .next_dataset()
            .unwrap()
            .expect("expected dataset 2");
        assert_eq!("VS", dataset2.schema().dataset_name());
        assert_eq!(Some(2), dataset2.schema().record_count());
        let record = dataset2.next_record().unwrap().unwrap();
        assert_character("WEIGHT", &record[0]);
        let record = dataset2.next_record().unwrap().unwrap();
        assert_character("HEIGHT", &record[0]);
        assert!(dataset2.next_record().unwrap().is_none());
    }

    #[tokio::test]
    async fn reports_character_truncation_for_multibyte_value() {
        let schema = XportSchema::builder()
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("RACE")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let options = XportWriterOptions::builder()
            .set_truncation_policy(SasVariableType::Character, TruncationPolicy::Report)
            .build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, XportMetadata::builder().build(), options)
                .await
                .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();

        // "日本語テスト" is 18 bytes in UTF-8, won't fit in 8 bytes.
        let result = writer
            .write_record(&[XportValue::from("日本語テスト")])
            .await;
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("truncated"),
            "unexpected message: {message}"
        );
    }

    #[tokio::test]
    async fn silent_character_truncation_succeeds() {
        let schema = XportSchema::builder()
            .set_dataset_name("DM")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("RACE")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer = AsyncXportWriter::from_writer(
            &mut buffer,
            XportMetadata::builder().build(),
            XportWriterOptions::default(),
        )
        .await
        .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();

        // Same value, but Silent policy (the default) — no error.
        writer
            .write_record(&[XportValue::from("日本語テスト")])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reports_numeric_truncation() {
        let schema = XportSchema::builder()
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("VSSTRES")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(3);
                v
            })
            .try_build()
            .unwrap();

        let options = XportWriterOptions::builder()
            .set_truncation_policy(SasVariableType::Numeric, TruncationPolicy::Report)
            .build();
        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer =
            AsyncXportWriter::from_writer(&mut buffer, XportMetadata::builder().build(), options)
                .await
                .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();

        // 75.3 needs precision beyond 3 bytes.
        let result = writer.write_record(&[XportValue::from(75.3)]).await;
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("truncated"),
            "unexpected message: {message}"
        );
    }

    #[tokio::test]
    async fn silent_numeric_truncation_succeeds() {
        let schema = XportSchema::builder()
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("VSSTRES")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(3);
                v
            })
            .try_build()
            .unwrap();

        let mut buffer = std::io::Cursor::new(Vec::new());
        let writer = AsyncXportWriter::from_writer(
            &mut buffer,
            XportMetadata::builder().build(),
            XportWriterOptions::default(),
        )
        .await
        .unwrap();
        let mut writer = writer.write_schema(schema).await.unwrap();

        // Same value, but Silent policy (the default) — no error.
        writer
            .write_record(&[XportValue::from(75.3)])
            .await
            .unwrap();
    }
}
