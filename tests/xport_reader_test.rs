use sas_xport::sas::xport::{XportDataset, XportReader, XportValue};
use std::fs::File;
use std::io::BufReader;

fn open_sync(path: &str) -> XportDataset<BufReader<File>> {
    let file = File::open(path).unwrap();
    let reader = XportReader::from_file(file).unwrap();
    reader
        .next_dataset()
        .unwrap()
        .unwrap_or_else(|| panic!("Expected a dataset in {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_records_single_dataset() {
        let mut dataset = open_sync("tests/resources/v5/ae.xpt");
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        while let Some(record) = dataset.next_record().unwrap() {
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(16, record_count);
    }

    #[test]
    fn read_records_with_iterator() {
        let mut dataset = open_sync("tests/resources/v5/ae.xpt");
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        for record in dataset.records() {
            let record = record.unwrap();
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(16, record_count);
    }

    #[test]
    fn read_records_with_iterator_narrow() {
        let mut dataset = open_sync("tests/resources/narrow/relrec.xpt");
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        for record in dataset.records() {
            let record = record.unwrap();
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(2, record_count);
    }

    #[test]
    fn read_records_with_iterator_multiple_datasets() {
        let mut adsl = open_sync("tests/resources/multi/multiple-datasets.xpt");
        let adsl_variable_count = adsl.schema().variables().len();
        assert_eq!("ADSL", adsl.schema().dataset_name());

        let mut adsl_count = 0;
        for record in adsl.records() {
            let record = record.unwrap();
            assert_eq!(adsl_variable_count, record.len());
            adsl_count += 1;
        }
        assert_eq!(254, adsl_count);

        let mut adae = adsl.next_dataset().unwrap().unwrap();
        let adae_variable_count = adae.schema().variables().len();
        assert_eq!("ADAE", adae.schema().dataset_name());

        let mut adae_count = 0;
        for record in adae.records() {
            let record = record.unwrap();
            assert_eq!(adae_variable_count, record.len());
            adae_count += 1;
        }
        assert_eq!(1_191, adae_count);

        let mut qs = adae.next_dataset().unwrap().unwrap();
        let qs_variable_count = qs.schema().variables().len();
        assert_eq!("ADQSADAS", qs.schema().dataset_name());

        let mut qs_count = 0;
        for record in qs.records() {
            let record = record.unwrap();
            assert_eq!(qs_variable_count, record.len());
            qs_count += 1;
        }
        assert_eq!(12_463, qs_count);

        let no_more = qs.next_dataset().unwrap();
        assert!(no_more.is_none());
    }

    #[test]
    fn read_records_narrow_dataset() {
        let mut dataset = open_sync("tests/resources/narrow/relrec.xpt");
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        while let Some(record) = dataset.next_record().unwrap() {
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(2, record_count);
    }

    #[test]
    fn read_records_multiple_datasets() {
        let mut adsl = open_sync("tests/resources/multi/multiple-datasets.xpt");
        assert_eq!("ADSL", adsl.schema().dataset_name());
        let adsl_variable_count = adsl.schema().variables().len();
        let mut adsl_count = 0;
        while let Some(record) = adsl.next_record().unwrap() {
            assert_eq!(adsl_variable_count, record.len());
            adsl_count += 1;
        }
        assert_eq!(254, adsl_count);

        let mut adae = adsl.next_dataset().unwrap().unwrap();
        assert_eq!("ADAE", adae.schema().dataset_name());
        let adae_variable_count = adae.schema().variables().len();
        let mut adae_count = 0;
        while let Some(record) = adae.next_record().unwrap() {
            assert_eq!(adae_variable_count, record.len());
            adae_count += 1;
        }
        assert_eq!(1_191, adae_count);

        let mut qs = adae.next_dataset().unwrap().unwrap();
        assert_eq!("ADQSADAS", qs.schema().dataset_name());
        let qs_variable_count = qs.schema().variables().len();
        let mut qs_count = 0;
        while let Some(record) = qs.next_record().unwrap() {
            assert_eq!(qs_variable_count, record.len());
            qs_count += 1;
        }
        assert_eq!(12_463, qs_count);

        let no_more = qs.next_dataset().unwrap();
        assert!(no_more.is_none());
    }

    #[test]
    fn seek_record_multiple_datasets_can_seek_first_record() {
        let mut adsl = open_sync("tests/resources/multi/multiple-datasets.xpt");
        adsl.skip_to_end().unwrap();

        let mut adae = adsl.next_dataset().unwrap().unwrap();
        assert_eq!("ADAE", adae.schema().dataset_name());

        let initial_values: Vec<String> = adae
            .next_record()
            .unwrap()
            .unwrap()
            .iter()
            .map(|v| match v {
                XportValue::Character(c) => c.to_string(),
                XportValue::Number(n) => n.to_string(),
            })
            .collect();

        adae.seek(0).unwrap();

        let seek_values: Vec<String> = adae
            .next_record()
            .unwrap()
            .unwrap()
            .iter()
            .map(|v| match v {
                XportValue::Character(c) => c.to_string(),
                XportValue::Number(n) => n.to_string(),
            })
            .collect();
        assert_eq!(initial_values.len(), seek_values.len());
        for i in 0..initial_values.len() {
            assert_eq!(initial_values[i], seek_values[i]);
        }
    }

    #[test]
    fn seek_to_skip_dataset() {
        let mut adsl = open_sync("tests/resources/v9/adsl.xpt");
        let adsl_record_count = adsl.schema().record_count().unwrap();
        adsl.seek(adsl_record_count).unwrap();

        assert!(adsl.next_record().unwrap().is_none());
    }
}

#[cfg(test)]
#[cfg(feature = "chrono")]
mod chrono_tests {
    use super::*;
    use chrono::{Local, NaiveDate, TimeZone};
    use sas_xport::sas::xport::{XportFileVersion, XportMetadata, XportSchema, XportVariable};
    use sas_xport::sas::{SasDateTime, SasJustification, SasVariableType};

    fn assert_variable(
        schema: &XportSchema,
        index: usize,
        full_name: &str,
        expected: &XportVariable,
    ) {
        assert_eq!(index, schema.variable_ordinal(full_name).unwrap());
        let variable = schema.variable_at(index).unwrap();
        assert_eq!(expected.value_type(), variable.value_type());
        assert_eq!(expected.hash(), variable.hash());
        assert_eq!(expected.value_length(), variable.value_length());
        assert_eq!(expected.number(), variable.number());
        assert_eq!(expected.short_name(), variable.short_name());
        assert_eq!(expected.short_label(), variable.short_label());
        assert_eq!(expected.short_format(), variable.short_format());
        assert_eq!(expected.format_length(), variable.format_length());
        assert_eq!(expected.format_precision(), variable.format_precision());
        assert_eq!(expected.justification(), variable.justification());
        assert_eq!(expected.short_input_format(), variable.short_input_format());
        assert_eq!(
            expected.input_format_length(),
            variable.input_format_length()
        );
        assert_eq!(
            expected.input_format_precision(),
            variable.input_format_precision()
        );
        assert_eq!(expected.position(), variable.position());
        assert_eq!(expected.medium_name(), variable.medium_name());
        assert_eq!(expected.long_name(), variable.long_name());
        assert_eq!(expected.long_label(), variable.long_label());
        assert_eq!(expected.long_format(), variable.long_format());
        assert_eq!(expected.long_input_format(), variable.long_input_format());
    }

    #[test]
    fn read_metadata() {
        let dataset = open_sync("tests/resources/v5/ae.xpt");
        let metadata = dataset.metadata();
        assert_eq!(XportFileVersion::V5, metadata.file_version());
        assert_eq!(XportMetadata::DEFAULT_SYMBOL1, metadata.symbol1());
        assert_eq!(XportMetadata::DEFAULT_SYMBOL2, metadata.symbol2());
        assert_eq!(XportMetadata::DEFAULT_LIBRARY, metadata.library());
        assert_eq!("9.0401M5", metadata.sas_version());
        assert_eq!("X64_SR12", metadata.operating_system());
        let timestamp: SasDateTime = NaiveDate::from_ymd_opt(2021, 1, 4)
            .and_then(|d| d.and_hms_opt(16, 18, 56))
            .map(|dt| Local.from_local_datetime(&dt).unwrap())
            .unwrap()
            .into();
        assert_eq!(timestamp, metadata.created());
        assert_eq!(timestamp, metadata.modified());
    }

    #[test]
    fn read_dataset_v5() {
        let dataset = open_sync("tests/resources/v5/ae.xpt");
        let schema = dataset.schema();

        assert_eq!(140, schema.variable_descriptor_length());
        assert_eq!("SAS", schema.format());
        assert_eq!("AE", schema.dataset_name());
        assert_eq!("SASDATA", schema.sas_data());
        assert_eq!("9.0401M5", schema.version());
        assert_eq!("X64_SR12", schema.operating_system());
        let timestamp: SasDateTime = NaiveDate::from_ymd_opt(2021, 1, 4)
            .and_then(|d| d.and_hms_opt(16, 18, 56))
            .map(|dt| Local.from_local_datetime(&dt).unwrap())
            .unwrap()
            .into();
        assert_eq!(timestamp, schema.created());
        assert_eq!(timestamp, schema.modified());
        assert_eq!("Adverse Events", schema.dataset_label());
        assert_eq!("", schema.dataset_type());

        assert_eq!(18, schema.variables().len());
        assert_variable(
            schema,
            0,
            "STUDYID",
            &XportVariable::builder()
                .value_type(SasVariableType::Character)
                .hash(0)
                .value_length(7)
                .number(1)
                .short_name("STUDYID")
                .short_label("Study Identifier")
                .short_format("")
                .format_length(0)
                .format_precision(0)
                .justification(SasJustification::Left)
                .short_input_format("")
                .input_format_length(0)
                .input_format_precision(0)
                .position(0)
                .build(),
        );
    }

    #[test]
    fn read_dataset_v8() {
        let dataset = open_sync("tests/resources/v9/adsl.xpt");
        let schema = dataset.schema();

        assert_eq!(140, schema.variable_descriptor_length());
        assert_eq!("SAS", schema.format());
        assert_eq!("ADSL", schema.dataset_name());
        assert_eq!("SASDATA", schema.sas_data());
        assert_eq!("9.1", schema.version());
        assert_eq!("WIN", schema.operating_system());
        let timestamp: SasDateTime = NaiveDate::from_ymd_opt(2021, 5, 21)
            .and_then(|d| d.and_hms_opt(15, 25, 46))
            .map(|dt| Local.from_local_datetime(&dt).unwrap())
            .unwrap()
            .into();
        assert_eq!(timestamp, schema.created());
        assert_eq!(timestamp, schema.modified());
        assert_eq!("", schema.dataset_label());
        assert_eq!("", schema.dataset_type());

        assert_eq!(49, schema.variables().len());
        assert_variable(
            schema,
            48,
            "varnamegreaterthan8",
            &XportVariable::builder()
                .value_type(SasVariableType::Numeric)
                .hash(0)
                .value_length(8)
                .number(49)
                .short_name("varnameg")
                .short_label("This variable has an incredibly long var")
                .short_format("TESTFMTN")
                .format_length(0)
                .format_precision(0)
                .justification(SasJustification::Left)
                .short_input_format("")
                .input_format_length(0)
                .input_format_precision(0)
                .position(160)
                .medium_name("varnamegreaterthan8")
                .long_name("varnamegreaterthan8")
                .long_label("This variable has an incredibly long variable label")
                .long_format("TESTFMTNAMETHATISLONGERTHANEIGHT.")
                .long_input_format("")
                .build(),
        );
    }
}

// ---------------------------------------------------------------------------
// Non-seekable source tests — proves the Read-only path works
// ---------------------------------------------------------------------------

#[cfg(test)]
mod non_seeking_tests {
    use sas_xport::sas::xport::{XportReader, XportValue};
    use std::fs::File;
    use std::io::{BufRead, BufReader, Read};

    /// A wrapper that implements `Read` and `BufRead` but NOT `Seek`.
    struct ReadOnly<R>(R);

    impl<R: Read> Read for ReadOnly<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.0.read(buf)
        }
    }

    impl<R: BufRead> BufRead for ReadOnly<R> {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            self.0.fill_buf()
        }

        fn consume(&mut self, amt: usize) {
            self.0.consume(amt);
        }
    }

    #[test]
    fn read_single_dataset_without_seek() {
        let file = File::open("tests/resources/v5/ae.xpt").unwrap();
        let reader = ReadOnly(BufReader::new(file));
        let xport = XportReader::from_reader(reader).unwrap();
        let mut dataset = xport.next_dataset().unwrap().unwrap();
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        while let Some(record) = dataset.next_record().unwrap() {
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(16, record_count);
    }

    #[test]
    fn read_multiple_datasets_without_seek() {
        let file = File::open("tests/resources/multi/multiple-datasets.xpt").unwrap();
        let reader = ReadOnly(BufReader::new(file));
        let xport = XportReader::from_reader(reader).unwrap();

        let mut dataset = xport.next_dataset().unwrap().unwrap();
        assert_eq!("ADSL", dataset.schema().dataset_name());
        let mut count = 0;
        while dataset.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(254, count);

        let mut dataset = dataset.next_dataset().unwrap().unwrap();
        assert_eq!("ADAE", dataset.schema().dataset_name());
        count = 0;
        while dataset.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(1_191, count);

        let mut dataset = dataset.next_dataset().unwrap().unwrap();
        assert_eq!("ADQSADAS", dataset.schema().dataset_name());
        count = 0;
        while dataset.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(12_463, count);

        assert!(dataset.next_dataset().unwrap().is_none());
    }

    #[test]
    fn read_narrow_records_without_seek() {
        let file = File::open("tests/resources/narrow/relrec.xpt").unwrap();
        let reader = ReadOnly(BufReader::new(file));
        let xport = XportReader::from_reader(reader).unwrap();
        let mut dataset = xport.next_dataset().unwrap().unwrap();

        let mut record_count = 0;
        while let Some(record) = dataset.next_record().unwrap() {
            for value in &record {
                match value {
                    XportValue::Character(c) => {
                        let _ = c.len();
                    }
                    XportValue::Number(n) => {
                        let _ = n.is_finite();
                    }
                }
            }
            record_count += 1;
        }
        assert_eq!(2, record_count);
    }

    #[test]
    fn iterator_works_without_seek() {
        let file = File::open("tests/resources/v5/ae.xpt").unwrap();
        let reader = ReadOnly(BufReader::new(file));
        let xport = XportReader::from_reader(reader).unwrap();
        let mut dataset = xport.next_dataset().unwrap().unwrap();

        let mut count = 0;
        for record in dataset.records() {
            let _ = record.unwrap();
            count += 1;
        }
        assert_eq!(16, count);
    }

    #[test]
    fn read_v9_dataset_without_seek() {
        let file = File::open("tests/resources/v9/adsl.xpt").unwrap();
        let reader = ReadOnly(BufReader::new(file));
        let xport = XportReader::from_reader(reader).unwrap();
        let mut dataset = xport.next_dataset().unwrap().unwrap();

        let mut record_count = 0;
        while dataset.next_record().unwrap().is_some() {
            record_count += 1;
        }
        assert_eq!(254, record_count);
    }
}
