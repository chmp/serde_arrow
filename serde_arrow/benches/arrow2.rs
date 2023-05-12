mod groups;

criterion::criterion_main!(
    groups::complex_common::benchmark,
    groups::primitives::benchmark,
);
