use chrono::{DateTime, TimeZone, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use rand::{distributions::{Standard, Uniform}, prelude::Distribution, thread_rng};
use serde::Serialize;
use serde_arrow::{DataType, Schema};

#[derive(Serialize)]
struct Example {
    a: i64,
    b: f32,
    c: DateTime<Utc>, 
}


impl Distribution<Example> for Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Example {
        Example {
            a: Standard.sample(rng),
            b: Standard.sample(rng),
            c: Utc.timestamp(Uniform::new(0, 10_000).sample(rng), 0),
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let mut examples = Vec::<Example>::new();
    for _ in 0..10_000 {
        examples.push(Standard.sample(&mut rng));
    }
    
    let mut schema = Schema::new();
    schema.add_field("a", Some(DataType::I64), false);
    schema.add_field("b", Some(DataType::F32), false);
    schema.add_field("c", Some(DataType::DateTimeStr), false);

    c.bench_function("trace_schema", |b| {    
        b.iter(|| serde_arrow::trace_schema(black_box(&examples)).unwrap())
    });

    c.bench_function("to_record_batch", |b| {    
        b.iter(|| serde_arrow::to_record_batch(black_box(&examples), &schema).unwrap())
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
