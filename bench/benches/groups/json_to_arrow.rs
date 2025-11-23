use crate::groups::complex_common::Item;

use {
    serde_arrow::schema::{SchemaLike as _, SerdeArrowSchema},
    std::sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::{FieldRef, Schema as ArrowSchema};
use serde_json::Value;

fn benchmark_json_to_arrow(c: &mut criterion::Criterion) {
    let rng = &mut rand::thread_rng();
    let items = (0..10_000)
        .map(|_| Item::random(rng))
        .collect::<Vec<Item>>();
    let jsons_to_deserialize = items
        .iter()
        .map(|item| serde_json::to_string(item).expect("Failed to serialize JSON"))
        .collect::<Vec<_>>();
    let jsons_to_deserialize_concatenated = jsons_to_deserialize.join("\n");
    let jsons_to_deserialize: Vec<&str> = {
        let mut prev = 0;
        jsons_to_deserialize
            .iter()
            .map(|s| {
                let ret = &jsons_to_deserialize_concatenated[prev..(prev + s.len())];
                prev += s.len() + 1;
                ret
            })
            .collect::<Vec<_>>()
    };

    let schema = SerdeArrowSchema::from_type::<Item>(Default::default()).unwrap();
    let arrow_fields = Vec::<FieldRef>::try_from(&schema).unwrap();
    let mut group = c.benchmark_group("json_to_arrow");

    // arrow-json direct
    group.bench_function("arrow_json", |b| {
        b.iter(|| {
            let schema = Arc::new(ArrowSchema::new(arrow_fields.to_owned()));
            let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder().unwrap();
            decoder
                .decode(jsons_to_deserialize_concatenated.as_bytes())
                .unwrap();
            let arrays = decoder.flush().unwrap().unwrap().columns().to_vec();
            let record_batch = RecordBatch::try_new(schema, arrays).unwrap();
            criterion::black_box(record_batch)
        })
    });

    // arrow-json via serde
    group.bench_function("arrow_json (serde_json,transcode)", |b| {
        b.iter(|| {
            let schema = Arc::new(ArrowSchema::new(arrow_fields.to_owned()));
            let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder().unwrap();
            let mut deserializers = jsons_to_deserialize
                .iter()
                .map(|json_to_deserialize| {
                    serde_json::Deserializer::from_slice(json_to_deserialize.as_bytes())
                })
                .collect::<Vec<_>>();
            let transcoders = deserializers
                .iter_mut()
                .map(serde_transcode::Transcoder::new)
                .collect::<Vec<_>>();
            decoder.serialize(&transcoders).unwrap();
            let arrays = decoder.flush().unwrap().unwrap().columns().to_vec();
            let record_batch = RecordBatch::try_new(schema, arrays).unwrap();
            criterion::black_box(record_batch)
        })
    });

    // serde_arrow via serde
    group.bench_function("serde_arrow (serde_json,transcode)", |b| {
        b.iter(|| {
            let mut arrow_builder = serde_arrow::ArrayBuilder::from_arrow(&arrow_fields).unwrap();
            for json_to_deserialize in jsons_to_deserialize_concatenated
                .as_bytes()
                .split(|c| *c == b'\n')
                .filter(|s| !s.is_empty())
            {
                let mut deserializer = serde_json::Deserializer::from_slice(json_to_deserialize);
                let transcoder = serde_transcode::Transcoder::new(&mut deserializer);
                arrow_builder.push(&transcoder).unwrap();
            }

            let record_batch = arrow_builder.to_record_batch().unwrap();
            criterion::black_box(record_batch)
        })
    });

    group.bench_function("serde_arrow (serde_json,value)", |b| {
        b.iter(|| {
            let mut arrow_builder = serde_arrow::ArrayBuilder::from_arrow(&arrow_fields).unwrap();
            for json_to_deserialize in jsons_to_deserialize_concatenated
                .as_bytes()
                .split(|c| *c == b'\n')
                .filter(|s| !s.is_empty())
            {
                let item: Value = serde_json::from_slice(json_to_deserialize).unwrap();
                arrow_builder.push(&item).unwrap();
            }

            let record_batch = arrow_builder.to_record_batch().unwrap();
            criterion::black_box(record_batch)
        });
    });

    group.bench_function("serde_arrow (simd_json,transcode)", |b| {
        b.iter(|| {
            let mut arrow_builder = serde_arrow::ArrayBuilder::from_arrow(&arrow_fields).unwrap();
            let mut jsons_to_deserialize_concatenated =
                jsons_to_deserialize_concatenated.as_bytes().to_vec();

            for json_to_deserialize in jsons_to_deserialize_concatenated
                .split_mut(|c| *c == b'\n')
                .filter(|s| !s.is_empty())
            {
                let mut deserializer =
                    simd_json::Deserializer::from_slice(json_to_deserialize).unwrap();
                let transcoder = serde_transcode::Transcoder::new(&mut deserializer);
                arrow_builder.push(&transcoder).unwrap();
            }

            let record_batch = arrow_builder.to_record_batch().unwrap();
            criterion::black_box(record_batch)
        })
    });
}

criterion::criterion_group!(benchmark, benchmark_json_to_arrow);
