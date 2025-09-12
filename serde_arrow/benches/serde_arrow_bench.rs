pub mod groups;
pub mod impls;
pub mod mini_serde_arrow;

criterion::criterion_main!(
    groups::complex::benchmark,
    groups::primitives::benchmark,
    groups::primitives_subset::benchmark,
    groups::json_to_arrow::benchmark,
);
