use sas_xport::sas::xport::{AsyncXportReader, XportValue};
use tokio::fs::File as AsyncFile;

async fn open_async(
    path: &str,
) -> sas_xport::sas::xport::AsyncXportDataset<tokio::io::BufReader<AsyncFile>> {
    let file = AsyncFile::open(path).await.unwrap();
    let reader = AsyncXportReader::from_file(file).await.unwrap();
    reader
        .next_dataset()
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("Expected a dataset in {path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_records_single_dataset() {
        let mut dataset = open_async("tests/resources/v5/ae.xpt").await;
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        while let Some(record) = dataset.next_record().await.unwrap() {
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(16, record_count);
    }

    #[tokio::test]
    async fn read_records_narrow_dataset() {
        let mut dataset = open_async("tests/resources/narrow/relrec.xpt").await;
        let variable_count = dataset.schema().variables().len();

        let mut record_count = 0;
        while let Some(record) = dataset.next_record().await.unwrap() {
            assert_eq!(variable_count, record.len());
            record_count += 1;
        }
        assert_eq!(2, record_count);
    }

    #[tokio::test]
    async fn read_records_multiple_datasets() {
        let mut adsl = open_async("tests/resources/multi/multiple-datasets.xpt").await;
        assert_eq!("ADSL", adsl.schema().dataset_name());
        let adsl_variable_count = adsl.schema().variables().len();
        let mut adsl_count = 0;
        while let Some(record) = adsl.next_record().await.unwrap() {
            assert_eq!(adsl_variable_count, record.len());
            adsl_count += 1;
        }
        assert_eq!(254, adsl_count);

        let mut adae = adsl.next_dataset().await.unwrap().unwrap();
        assert_eq!("ADAE", adae.schema().dataset_name());
        let adae_variable_count = adae.schema().variables().len();
        let mut adae_count = 0;
        while let Some(record) = adae.next_record().await.unwrap() {
            assert_eq!(adae_variable_count, record.len());
            adae_count += 1;
        }
        assert_eq!(1_191, adae_count);

        let mut qs = adae.next_dataset().await.unwrap().unwrap();
        assert_eq!("ADQSADAS", qs.schema().dataset_name());
        let qs_variable_count = qs.schema().variables().len();
        let mut qs_count = 0;
        while let Some(record) = qs.next_record().await.unwrap() {
            assert_eq!(qs_variable_count, record.len());
            qs_count += 1;
        }
        assert_eq!(12_463, qs_count);

        let no_more = qs.next_dataset().await.unwrap();
        assert!(no_more.is_none());
    }

    #[tokio::test]
    async fn seek_record_multiple_datasets_can_seek_first_record() {
        let mut adsl = open_async("tests/resources/multi/multiple-datasets.xpt").await;
        adsl.skip_to_end().await.unwrap();

        let mut adae = adsl.next_dataset().await.unwrap().unwrap();
        assert_eq!("ADAE", adae.schema().dataset_name());

        let initial_values: Vec<String> = adae
            .next_record()
            .await
            .unwrap()
            .unwrap()
            .iter()
            .map(|v| match v {
                XportValue::Character(c) => c.to_string(),
                XportValue::Number(n) => n.to_string(),
            })
            .collect();

        adae.seek(0).await.unwrap();

        let seek_values: Vec<String> = adae
            .next_record()
            .await
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

    #[tokio::test]
    async fn seek_to_skip_dataset() {
        let mut adsl = open_async("tests/resources/v9/adsl.xpt").await;
        let adsl_record_count = adsl.schema().record_count().unwrap();
        adsl.seek(adsl_record_count).await.unwrap();

        assert!(adsl.next_record().await.unwrap().is_none());
    }
}

#[cfg(test)]
#[cfg(feature = "chrono")]
mod chrono_tests {
    use chrono::{Local, NaiveDate, TimeZone};
    use sas_xport::sas::xport::{
        AsyncXportReader, XportFileVersion, XportMetadata, XportSchema, XportVariable,
    };
    use sas_xport::sas::{SasDateTime, SasJustification, SasVariableType};
    use tokio::fs::File as AsyncFile;

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

    #[tokio::test]
    async fn read_metadata() {
        let file = AsyncFile::open("tests/resources/v5/ae.xpt").await.unwrap();
        let reader = AsyncXportReader::from_file(file).await.unwrap();
        let metadata = reader.metadata();
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

    #[tokio::test]
    async fn read_dataset_v5() {
        let file = AsyncFile::open("tests/resources/v5/ae.xpt").await.unwrap();
        let reader = AsyncXportReader::from_file(file).await.unwrap();
        let dataset = reader.next_dataset().await.unwrap().unwrap();
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
                .set_value_type(SasVariableType::Character)
                .set_hash(0)
                .set_value_length(7)
                .set_number(1)
                .set_short_name("STUDYID")
                .set_short_label("Study Identifier")
                .set_short_format("")
                .set_format_length(0)
                .set_format_precision(0)
                .set_justification(SasJustification::Left)
                .set_short_input_format("")
                .set_input_format_length(0)
                .set_input_format_precision(0)
                .set_position(0)
                .build(),
        );
    }

    #[tokio::test]
    async fn read_dataset_v8() {
        let file = AsyncFile::open("tests/resources/v9/adsl.xpt")
            .await
            .unwrap();
        let reader = AsyncXportReader::from_file(file).await.unwrap();
        let dataset = reader.next_dataset().await.unwrap().unwrap();
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
                .set_value_type(SasVariableType::Numeric)
                .set_hash(0)
                .set_value_length(8)
                .set_number(49)
                .set_short_name("varnameg")
                .set_short_label("This variable has an incredibly long var")
                .set_short_format("TESTFMTN")
                .set_format_length(0)
                .set_format_precision(0)
                .set_justification(SasJustification::Left)
                .set_short_input_format("")
                .set_input_format_length(0)
                .set_input_format_precision(0)
                .set_position(160)
                .set_medium_name("varnamegreaterthan8")
                .set_long_name("varnamegreaterthan8")
                .set_long_label("This variable has an incredibly long variable label")
                .set_long_format("TESTFMTNAMETHATISLONGERTHANEIGHT.")
                .set_long_input_format("")
                .build(),
        );
    }
}
