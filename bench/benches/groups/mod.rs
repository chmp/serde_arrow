use criterion::measurement::Measurement;

pub mod complex;
pub mod json_to_arrow;
pub mod primitives;
pub mod primitives_subset;

pub fn new_group<'a, M: Measurement>(
    c: &'a mut criterion::Criterion<M>,
    name: impl Into<String>,
) -> criterion::BenchmarkGroup<'a, M> {
    let mut group = c.benchmark_group(name);

    group.sampling_mode(criterion::SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    group
}

macro_rules! bench_impl {
    ($group:expr, $impl:ident, $items:expr) => {
        let fields = $impl::trace(&$items);
        $group.bench_function(stringify!($impl), |b| {
            b.iter(|| criterion::black_box($impl::serialize(&fields, &$items)))
        });
    };
}

pub(crate) use bench_impl;
