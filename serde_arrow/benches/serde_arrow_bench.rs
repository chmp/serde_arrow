mod groups;

criterion::criterion_main!(
    groups::complex_common::benchmark,
    groups::primitives::benchmark,
    groups::json_to_arrow::benchmark,
);
