macro_rules! define_benchmark {
    (
        $name:ident,
        ty = $ty:ty,
        n = [$($n:expr),*],
        $(
            serialization {
                $(serde_arrow = $bench_serde_arrow:expr,)?
                $(arrow = $bench_arrow:expr,)?
                $(arrow2_convert = $bench_arrow2_convert:expr,)?
            },
        )?
    ) => {
        pub fn benchmark_serialize(c: &mut criterion::Criterion) {
            use serde_arrow::schema::{SerdeArrowSchema, SchemaLike};
            use serde_arrow::_impl::{arrow::datatypes::FieldRef, arrow2::datatypes::Field as Arrow2Field};

            for n in [$($n),*] {
                let mut group = c.benchmark_group(format!("{}_serialize({})", stringify!($name), n));

                group.sampling_mode(criterion::SamplingMode::Flat);
                if !crate::groups::impls::is_quick() {
                    group.sample_size(20);
                    group.measurement_time(std::time::Duration::from_secs(120));
                } else {
                    group.sample_size(10);
                    group.measurement_time(std::time::Duration::from_secs(5));
                }

                let n_items = if !crate::groups::impls::is_quick() { n } else { n / 1000 };

                let mut rng = rand::thread_rng();
                let items = (0..n_items)
                    .map(|_| <$ty>::random(&mut rng))
                    .collect::<Vec<_>>();
                let schema = SerdeArrowSchema::from_samples(&items, Default::default()).unwrap();
                let arrow_fields = Vec::<FieldRef>::try_from(&schema).unwrap();
                let arrow2_fields = Vec::<Arrow2Field>::try_from(&schema).unwrap();

                #[allow(unused)]
                let bench_serde_arrow = true;
                $($(let bench_serde_arrow = $bench_serde_arrow; )?)?

                if bench_serde_arrow {
                    group.bench_function("serde_arrow_arrow", |b| {
                        b.iter(|| criterion::black_box(crate::groups::impls::serde_arrow_arrow::serialize(&arrow_fields, &items).unwrap()));
                    });

                    group.bench_function("serde_arrow_arrow2", |b| {
                        b.iter(|| criterion::black_box(crate::groups::impls::serde_arrow_arrow2::serialize(&arrow2_fields, &items).unwrap()));
                    });
                }

                #[allow(unused)]
                let bench_arrow = true;
                $($(let bench_arrow = $bench_arrow; )?)?

                if bench_arrow {
                    group.bench_function("arrow", |b| {
                        b.iter(|| criterion::black_box(crate::groups::impls::arrow::serialize(&arrow_fields, &items).unwrap()));
                    });
                }

                #[allow(unused)]
                let bench_arrow2_convert = true;
                $($(let bench_arrow2_convert = $bench_arrow2_convert; )?)?

                if bench_arrow2_convert {
                    group.bench_function("arrow2_convert", |b| {
                        b.iter(|| criterion::black_box(crate::groups::impls::arrow2_convert::serialize(&arrow_fields, &items).unwrap()));
                    });
                }

                group.finish();
            }
        }
        criterion::criterion_group!(
            benchmark,
            benchmark_serialize,
        );
    };
}

use std::ops::Range;

pub(crate) use define_benchmark;
use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};

pub mod serde_arrow_arrow {
    use serde::Serialize;
    use serde_arrow::{
        Result,
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
    };

    pub fn serialize<T>(fields: &[FieldRef], items: &T) -> Result<Vec<ArrayRef>>
    where
        T: Serialize + ?Sized,
    {
        serde_arrow::to_arrow(&fields, &items)
    }
}

pub mod serde_arrow_arrow2 {
    use serde::Serialize;
    use serde_arrow::{
        Result,
        _impl::arrow2::{array::Array, datatypes::Field},
    };

    pub fn serialize<T>(fields: &[Field], items: &T) -> Result<Vec<Box<dyn Array>>>
    where
        T: Serialize + ?Sized,
    {
        serde_arrow::to_arrow2(&fields, &items)
    }
}

pub mod arrow {

    use std::sync::Arc;

    // arrow-version:replace: use arrow_json_{version}::ReaderBuilder;
    use arrow_json_54::ReaderBuilder;
    // arrow-version:replace: use arrow_schema_{version}::Schema;
    use arrow_schema_54::Schema;

    use serde::Serialize;

    use serde_arrow::{
        Error, Result,
        _impl::arrow::{array::ArrayRef, datatypes::FieldRef},
    };

    pub fn serialize<T>(fields: &[FieldRef], items: &[T]) -> Result<Vec<ArrayRef>>
    where
        T: Serialize,
    {
        let schema = Schema::new(fields.to_vec());
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .build_decoder()
            .map_err(|err| Error::custom_from(err.to_string(), err))?;
        decoder
            .serialize(items)
            .map_err(|err| Error::custom_from(err.to_string(), err))?;
        Ok(decoder
            .flush()
            .map_err(|err| Error::custom_from(err.to_string(), err))?
            .ok_or_else(|| Error::custom("no items".into()))?
            .columns()
            .to_vec())
    }
}

pub mod arrow2_convert {
    use arrow2_convert::serialize::TryIntoArrow;
    use serde_arrow::{Error, Result, _impl::arrow2::array::Array};

    pub fn serialize<'a, T, E, F>(_fields: &[F], items: T) -> Result<Box<dyn Array>>
    where
        T: TryIntoArrow<'a, Box<dyn Array>, E>,
        E: 'static,
    {
        let array: Box<dyn Array> = items
            .try_into_arrow()
            .map_err(|err| Error::custom(err.to_string()))?;

        Ok(array)
    }
}

pub fn random_string<R: Rng + ?Sized>(rng: &mut R, length: Range<usize>) -> String {
    let n_string = Uniform::new(length.start, length.end).sample(rng);

    (0..n_string)
        .map(|_| -> char { Standard.sample(rng) })
        .collect()
}

pub fn is_quick() -> bool {
    std::env::var("SERDE_ARROW_BENCH_QUICK").is_ok()
}
