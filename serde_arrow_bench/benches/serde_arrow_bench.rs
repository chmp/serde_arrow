pub mod groups;
pub mod impls;

criterion::criterion_main!(
    groups::complex::benchmark,
    groups::primitives::benchmark,
    groups::json_to_arrow::benchmark,
);
