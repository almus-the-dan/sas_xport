use float_cmp::assert_approx_eq;
use sas_xport::sas::xport::{
    TruncationPolicy, XportDatasetVersion, XportFileVersion, XportMetadata, XportReader,
    XportSchema, XportValue, XportVariable, XportWriter,
};
use sas_xport::sas::{SasDateTime, SasJustification, SasMonth, SasVariableType};
use std::io::{BufReader, Cursor};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_v5_metadata() {
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

        let mut buf = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buf, metadata.clone()).unwrap();
        drop(writer);
        let bytes = buf.into_inner();

        let reader = XportReader::from_reader(BufReader::new(Cursor::new(bytes))).unwrap();
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

    #[test]
    fn round_trips_v8_metadata() {
        let metadata = XportMetadata::builder()
            .set_xport_file_version(XportFileVersion::V8)
            .set_symbol1("TST")
            .set_symbol2("XPT")
            .set_library("SASLIB")
            .set_sas_version("9.1")
            .set_operating_system("WinXP")
            .build();

        let mut buf = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buf, metadata.clone()).unwrap();
        drop(writer);
        let bytes = buf.into_inner();

        let reader = XportReader::from_reader(BufReader::new(Cursor::new(bytes))).unwrap();
        let read_metadata = reader.metadata();

        assert_eq!(XportFileVersion::V8, read_metadata.file_version());
        assert_eq!("TST", read_metadata.symbol1());
        assert_eq!("XPT", read_metadata.symbol2());
        assert_eq!("SASLIB", read_metadata.library());
        assert_eq!("9.1", read_metadata.sas_version());
        assert_eq!("WinXP", read_metadata.operating_system());
    }

    #[test]
    fn round_trips_default_metadata() {
        let metadata = XportMetadata::builder().build();

        let mut buf = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buf, metadata.clone()).unwrap();
        drop(writer);
        let bytes = buf.into_inner();

        let reader = XportReader::from_reader(BufReader::new(Cursor::new(bytes))).unwrap();
        let read_metadata = reader.metadata();

        assert_eq!(XportFileVersion::V5, read_metadata.file_version());
        assert_eq!(XportMetadata::DEFAULT_SYMBOL1, read_metadata.symbol1());
        assert_eq!(XportMetadata::DEFAULT_SYMBOL2, read_metadata.symbol2());
        assert_eq!(XportMetadata::DEFAULT_LIBRARY, read_metadata.library());
        assert_eq!(
            XportMetadata::DEFAULT_SAS_VERSION_V5,
            read_metadata.sas_version()
        );
    }

    #[test]
    fn writes_exactly_240_bytes() {
        let metadata = XportMetadata::builder().build();
        let mut buf = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buf, metadata).unwrap();
        drop(writer);
        assert_eq!(240, buf.into_inner().len());
    }

    // -------------------------------------------------------------------
    // Schema round-trip tests
    // -------------------------------------------------------------------

    /// Writes metadata + schema to a buffer, reads it back, and returns
    /// the schema from the first dataset.
    fn round_trip_schema(file_version: XportFileVersion, schema: XportSchema) -> XportSchema {
        let metadata = XportMetadata::builder()
            .set_xport_file_version(file_version)
            .build();
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let writer = writer.write_schema(schema).unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
        let dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        dataset.schema().clone()
    }

    #[test]
    fn round_trips_v5_schema_with_one_numeric_variable() {
        let schema = XportSchema::builder()
            .set_dataset_name("AE")
            .set_dataset_label("Adverse Events")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("AESEQ")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let read = round_trip_schema(XportFileVersion::V5, schema);

        assert_eq!(XportDatasetVersion::V5, read.xport_dataset_version());
        assert_eq!("AE", read.dataset_name());
        assert_eq!("Adverse Events", read.dataset_label());
        assert_eq!(1, read.variables().len());

        let variable = read.variables().first().unwrap();
        assert_eq!("AESEQ", variable.short_name());
        assert_eq!(SasVariableType::Numeric, variable.value_type());
        assert_eq!(8, variable.value_length());
        assert_eq!(1, variable.number());
        assert_eq!(0, variable.position());
    }

    #[test]
    fn round_trips_v5_schema_with_mixed_variables() {
        let schema = XportSchema::builder()
            .set_dataset_name("DM")
            .set_dataset_label("Demographics")
            .set_version("9.4")
            .set_operating_system("Linux")
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("STUDYID")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(20)
                    .set_short_label("Study ID")
                    .set_short_format("$CHAR")
                    .set_format_length(20)
                    .set_justification(SasJustification::Left);
                variable
            })
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("AGE")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8)
                    .set_short_label("Age")
                    .set_justification(SasJustification::Right);
                variable
            })
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("SEX")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(1)
                    .set_short_label("Sex");
                variable
            })
            .try_build()
            .unwrap();

        let read = round_trip_schema(XportFileVersion::V5, schema);

        assert_eq!("DM", read.dataset_name());
        assert_eq!("Demographics", read.dataset_label());
        assert_eq!("9.4", read.version());
        assert_eq!("Linux", read.operating_system());
        assert_eq!(3, read.variables().len());

        let study_id = read.variables().first().unwrap();
        assert_eq!("STUDYID", study_id.short_name());
        assert_eq!(SasVariableType::Character, study_id.value_type());
        assert_eq!(20, study_id.value_length());
        assert_eq!("Study ID", study_id.short_label());
        assert_eq!("$CHAR", study_id.short_format());
        assert_eq!(20, study_id.format_length());
        assert_eq!(SasJustification::Left, study_id.justification());
        assert_eq!(0, study_id.position());

        let age = read.variables().get(1).unwrap();
        assert_eq!("AGE", age.short_name());
        assert_eq!(SasVariableType::Numeric, age.value_type());
        assert_eq!(8, age.value_length());
        assert_eq!(SasJustification::Right, age.justification());
        assert_eq!(20, age.position());

        let sex = read.variables().get(2).unwrap();
        assert_eq!("SEX", sex.short_name());
        assert_eq!(1, sex.value_length());
        assert_eq!(28, sex.position());
    }

    #[test]
    fn round_trips_v8_schema_with_long_name_and_label() {
        let schema = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("ADVERSE_EVENTS_DATASET")
            .set_dataset_label("Adverse Events Long Label")
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_full_name("ADVERSE_EVENT_SEQUENCE_NUMBER")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8)
                    .set_long_label("Adverse Event Sequence Number Within Subject");
                variable
            })
            .try_build()
            .unwrap();

        let read = round_trip_schema(XportFileVersion::V8, schema);

        assert_eq!(XportDatasetVersion::V8, read.xport_dataset_version());
        assert_eq!("ADVERSE_EVENTS_DATASET", read.dataset_name());
        assert_eq!("Adverse Events Long Label", read.dataset_label());
        assert_eq!(1, read.variables().len());

        let variable = read.variables().first().unwrap();
        assert_eq!("ADVERSE_EVENT_SEQUENCE_NUMBER", variable.full_name());
        assert_eq!(
            "Adverse Event Sequence Number Within Subject",
            variable.full_label()
        );
        assert_eq!(SasVariableType::Numeric, variable.value_type());
    }

    #[test]
    fn round_trips_v8_schema_with_short_names_no_extensions() {
        let schema = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("VS")
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("WEIGHT")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8)
                    .set_short_label("Weight");
                variable
            })
            .try_build()
            .unwrap();

        let read = round_trip_schema(XportFileVersion::V8, schema);

        assert_eq!(XportDatasetVersion::V8, read.xport_dataset_version());
        assert_eq!("VS", read.dataset_name());
        assert_eq!(1, read.variables().len());
        assert_eq!("WEIGHT", read.variables()[0].short_name());
    }

    #[test]
    fn round_trips_v5_schema_record_count_is_zero() {
        let schema = XportSchema::builder()
            .set_dataset_name("EMPTY")
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("X")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                variable
            })
            .try_build()
            .unwrap();

        let read = round_trip_schema(XportFileVersion::V5, schema);
        assert_eq!(None, read.record_count());
    }

    #[test]
    fn round_trips_v8_schema_record_count_is_zero() {
        let schema = XportSchema::builder()
            .set_xport_dataset_version(XportDatasetVersion::V8)
            .set_dataset_name("EMPTY")
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("X")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                variable
            })
            .try_build()
            .unwrap();

        let read = round_trip_schema(XportFileVersion::V8, schema);
        assert_eq!(Some(0), read.record_count());
    }

    #[test]
    fn output_length_is_multiple_of_80() {
        let schema = XportSchema::builder()
            .set_dataset_name("LB")
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("LBTEST")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(40);
                variable
            })
            .add_variable({
                let mut variable = XportVariable::builder();
                variable
                    .set_short_name("LBORRES")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(200);
                variable
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let writer = writer.write_schema(schema).unwrap();
        writer.set_count_and_finish().unwrap();
        let len = buffer.into_inner().len();
        assert_eq!(0, len % 80, "output length {len} is not a multiple of 80");
    }

    // -------------------------------------------------------------------
    // Record round-trip tests
    // -------------------------------------------------------------------

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

    fn assert_number_approx(expected: f64, value: &XportValue<'_>, epsilon: f64) {
        match value {
            XportValue::Number(n) => assert_approx_eq!(f64, expected, *n, epsilon = epsilon),
            XportValue::Character(s) => {
                panic!("expected Number({expected}), got Character(\"{s}\")")
            }
        }
    }

    #[test]
    fn round_trips_v5_single_dataset_with_records() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer
            .write_record(&[XportValue::from("ABC-001"), XportValue::from(35.0)])
            .unwrap();
        writer
            .write_record(&[XportValue::from("ABC-002"), XportValue::from(42.5)])
            .unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
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

    #[test]
    fn round_trips_v5_single_dataset_without_count() {
        let schema = XportSchema::builder()
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("WEIGHT")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(8);
                v
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer.write_record(&[XportValue::from(75.3)]).unwrap();
        writer.finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!(None, dataset.schema().record_count());

        let record = dataset.next_record().unwrap().unwrap();
        assert_number(75.3, &record[0]);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[test]
    fn round_trips_v8_single_dataset_with_count() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer
            .write_record(&[XportValue::from("Headache")])
            .unwrap();
        writer.write_record(&[XportValue::from("Nausea")]).unwrap();
        writer.write_record(&[XportValue::from("Fatigue")]).unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
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

    #[test]
    fn round_trips_v5_multiple_datasets() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();

        // First dataset
        let mut writer = writer.write_schema(schema1).unwrap();
        writer.write_record(&[XportValue::from("SUBJ-01")]).unwrap();
        writer.write_record(&[XportValue::from("SUBJ-02")]).unwrap();
        let writer = writer.next_dataset().unwrap();

        // Second dataset
        let mut writer = writer.write_schema(schema2).unwrap();
        writer
            .write_record(&[XportValue::from("Headache"), XportValue::from(1.0)])
            .unwrap();
        writer.finish().unwrap();

        // Read back
        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();

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

    #[test]
    fn round_trips_v8_multiple_datasets_with_count() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();

        let mut writer = writer.write_schema(schema1).unwrap();
        writer
            .write_record(&[XportValue::from("SUBJ-001")])
            .unwrap();
        let writer = writer.set_count_and_next_dataset().unwrap();

        let mut writer = writer.write_schema(schema2).unwrap();
        writer.write_record(&[XportValue::from("WEIGHT")]).unwrap();
        writer.write_record(&[XportValue::from("HEIGHT")]).unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();

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

    #[test]
    fn round_trips_v5_single_dataset_with_records_wide() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer
            .write_record(&[
                XportValue::from("STUDY-001"),
                XportValue::from("STUDY-001-SUBJ-001"),
                XportValue::from(35.0),
            ])
            .unwrap();
        writer
            .write_record(&[
                XportValue::from("STUDY-001"),
                XportValue::from("STUDY-001-SUBJ-002"),
                XportValue::from(42.5),
            ])
            .unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
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

    #[test]
    fn round_trips_v5_single_dataset_without_count_wide() {
        let schema = XportSchema::builder()
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("VSTESTCD")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(80);
                v
            })
            .try_build()
            .unwrap();
        // Record width: 80 (>= 80)

        let metadata = XportMetadata::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer.write_record(&[XportValue::from("WEIGHT")]).unwrap();
        writer.finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!(None, dataset.schema().record_count());

        let record = dataset.next_record().unwrap().unwrap();
        assert_character("WEIGHT", &record[0]);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[test]
    fn round_trips_v8_single_dataset_with_count_wide() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer
            .write_record(&[XportValue::from("Headache")])
            .unwrap();
        writer.write_record(&[XportValue::from("Nausea")]).unwrap();
        writer.write_record(&[XportValue::from("Fatigue")]).unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
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

    #[test]
    fn round_trips_v5_multiple_datasets_wide() {
        let schema1 = XportSchema::builder()
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
            .set_dataset_name("AE")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("AETERM")
                    .set_value_type(SasVariableType::Character)
                    .set_value_length(80);
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
        // Record widths: DS1 = 80, DS2 = 80 + 8 = 88 (both >= 80)

        let metadata = XportMetadata::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();

        let mut writer = writer.write_schema(schema1).unwrap();
        writer.write_record(&[XportValue::from("SUBJ-01")]).unwrap();
        writer.write_record(&[XportValue::from("SUBJ-02")]).unwrap();
        let writer = writer.next_dataset().unwrap();

        let mut writer = writer.write_schema(schema2).unwrap();
        writer
            .write_record(&[XportValue::from("Headache"), XportValue::from(1.0)])
            .unwrap();
        writer.finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();

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

    #[test]
    fn round_trips_v8_multiple_datasets_with_count_narrow() {
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
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();

        let mut writer = writer.write_schema(schema1).unwrap();
        writer
            .write_record(&[XportValue::from("SUBJ-001")])
            .unwrap();
        let writer = writer.set_count_and_next_dataset().unwrap();

        let mut writer = writer.write_schema(schema2).unwrap();
        writer.write_record(&[XportValue::from("WEIGHT")]).unwrap();
        writer.write_record(&[XportValue::from("HEIGHT")]).unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();

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

    #[test]
    fn round_trips_truncated_numeric_value_length_5() {
        let schema = XportSchema::builder()
            .set_dataset_name("VS")
            .add_variable({
                let mut v = XportVariable::builder();
                v.set_short_name("WEIGHT")
                    .set_value_type(SasVariableType::Numeric)
                    .set_value_length(5);
                v
            })
            .try_build()
            .unwrap();

        let metadata = XportMetadata::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::from_writer(&mut buffer, metadata).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();
        writer.write_record(&[XportValue::from(75.3)]).unwrap();
        writer.write_record(&[XportValue::from(0.0)]).unwrap();
        writer.write_record(&[XportValue::from(-123.456)]).unwrap();
        writer.set_count_and_finish().unwrap();

        let reader =
            XportReader::from_reader(BufReader::new(Cursor::new(buffer.into_inner()))).unwrap();
        let mut dataset = reader.next_dataset().unwrap().expect("expected a dataset");
        assert_eq!(5, dataset.schema().variables()[0].value_length());

        // Truncating from 8 to 5 bytes loses mantissa precision.
        // Use a wider epsilon to account for the reduced precision.
        let epsilon = 1e-5;
        let record = dataset.next_record().unwrap().unwrap();
        assert_number_approx(75.3, &record[0], epsilon);

        let record = dataset.next_record().unwrap().unwrap();
        assert_number(0.0, &record[0]);

        let record = dataset.next_record().unwrap().unwrap();
        assert_number_approx(-123.456, &record[0], epsilon);

        assert!(dataset.next_record().unwrap().is_none());
    }

    #[test]
    fn reports_character_truncation_for_multibyte_value() {
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

        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::options()
            .set_truncation_policy(SasVariableType::Character, TruncationPolicy::Report)
            .from_writer(&mut buffer, XportMetadata::builder().build())
            .unwrap();
        let mut writer = writer.write_schema(schema).unwrap();

        // "日本語テスト" is 18 bytes in UTF-8, won't fit in 8 bytes.
        let result = writer.write_record(&[XportValue::from("日本語テスト")]);
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("truncated"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn silent_character_truncation_succeeds() {
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

        let mut buffer = Cursor::new(Vec::new());
        let writer =
            XportWriter::from_writer(&mut buffer, XportMetadata::builder().build()).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();

        // Same value, but Silent policy (the default) — no error.
        writer
            .write_record(&[XportValue::from("日本語テスト")])
            .unwrap();
    }

    #[test]
    fn reports_numeric_truncation() {
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

        let mut buffer = Cursor::new(Vec::new());
        let writer = XportWriter::options()
            .set_truncation_policy(SasVariableType::Numeric, TruncationPolicy::Report)
            .from_writer(&mut buffer, XportMetadata::builder().build())
            .unwrap();
        let mut writer = writer.write_schema(schema).unwrap();

        // 75.3 needs precision beyond 3 bytes.
        let result = writer.write_record(&[XportValue::from(75.3)]);
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("truncated"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn silent_numeric_truncation_succeeds() {
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

        let mut buffer = Cursor::new(Vec::new());
        let writer =
            XportWriter::from_writer(&mut buffer, XportMetadata::builder().build()).unwrap();
        let mut writer = writer.write_schema(schema).unwrap();

        // Same value, but Silent policy (the default) — no error.
        writer.write_record(&[XportValue::from(75.3)]).unwrap();
    }
}
