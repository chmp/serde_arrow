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

            for n in [$($n),*] {
                let mut group = c.benchmark_group(format!("{}_serialize({})", stringify!($name), n));
                group.sample_size(20);
                group.sampling_mode(criterion::SamplingMode::Flat);
                group.measurement_time(std::time::Duration::from_secs(120));

                let mut rng = rand::thread_rng();
                let items = (0..n)
                    .map(|_| <$ty>::random(&mut rng))
                    .collect::<Vec<_>>();
                let arrow_fields = SerdeArrowSchema::from_samples(&items, Default::default()).unwrap().to_arrow_fields().unwrap();

                #[allow(unused)]
                let bench_serde_arrow = true;
                $($(let bench_serde_arrow = $bench_serde_arrow; )?)?

                if bench_serde_arrow {
                    group.bench_function("serde_arrow_ng", |b| {
                        b.iter(|| criterion::black_box(crate::groups::impls::serde_arrow_ng::serialize(&arrow_fields, &items).unwrap()));
                    });
                }

                if bench_serde_arrow {
                    group.bench_function("serde_arrow", |b| {
                        b.iter(|| criterion::black_box(crate::groups::impls::serde_arrow::serialize(&arrow_fields, &items).unwrap()));
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

pub mod serde_arrow {
    use serde::Serialize;
    use serde_arrow::{
        Result,
        _impl::arrow::{array::ArrayRef, datatypes::Field},
    };

    pub fn serialize<T>(fields: &[Field], items: &T) -> Result<Vec<ArrayRef>>
    where
        T: Serialize + ?Sized,
    {
        serde_arrow::to_arrow(&fields, &items)
    }
}

pub mod serde_arrow_ng {
    use serde::Serialize;
    use serde_arrow::{
        Result,
        _impl::{arrow::datatypes::Field, ArrayBuilder},
        schema::SerdeArrowSchema,
    };

    pub fn serialize<T>(fields: &[Field], items: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let mut builder = ArrayBuilder::new(&SerdeArrowSchema::from_arrow_fields(fields)?)?;
        builder.extend(items)?;

        Ok(())
    }
}

pub mod arrow {

    use std::sync::Arc;

    // arrow-version:replace: use arrow_json_{version}::ReaderBuilder;
    use arrow_json_50::ReaderBuilder;
    // arrow-version:replace: use arrow_schema_{version}::Schema;
    use arrow_schema_50::Schema;

    use serde::Serialize;

    use serde_arrow::{
        Error, Result,
        _impl::arrow::{array::ArrayRef, datatypes::Field},
    };

    pub fn serialize<T>(fields: &[Field], items: &[T]) -> Result<Vec<ArrayRef>>
    where
        T: Serialize,
    {
        let schema = Schema::new(fields.to_vec());
        let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder()?;
        decoder.serialize(items)?;
        Ok(decoder
            .flush()?
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
