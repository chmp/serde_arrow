//! Helpers to display [arrow2] types as valid rust code
//!
use arrow2::datatypes::{
    DataType as Arrow2DataType, Field as Arrow2Field, Metadata as Arrow2Metadata,
};

use super::schema::get_optional_strategy;
use crate::schema::Strategy as SerdeArrowStrategy;

pub struct Str<'a>(pub &'a str);

impl<'a> std::fmt::Display for Str<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub struct Field<'a>(pub &'a Arrow2Field);

impl<'a> std::fmt::Display for Field<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Field::new({}, {}, {})",
            Str(&self.0.name),
            DataType(&self.0.data_type),
            self.0.is_nullable
        )?;

        if !self.0.metadata.is_empty() {
            write!(f, ".with_metadata({})", Metadata(&self.0.metadata))?;
        }

        Ok(())
    }
}

pub struct Fields<'a>(pub &'a [Arrow2Field]);

impl<'a> std::fmt::Display for Fields<'a> {
    fn fmt(&self, ff: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(ff, "vec![")?;
        for (i, f) in self.0.iter().enumerate() {
            if i != 0 {
                write!(ff, ", ")?;
            }
            write!(ff, "{}", Field(f))?;
        }
        write!(ff, "]")?;
        Ok(())
    }
}

pub struct Metadata<'a>(pub &'a Arrow2Metadata);

impl<'a> std::fmt::Display for Metadata<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match get_optional_strategy(self.0).ok().flatten() {
            Some(strategy) if self.0.len() == 1 => write!(f, "{}.into()", Strategy(&strategy)),
            _ => {
                write!(f, "Metadata::from([")?;
                for (idx, (k, v)) in self.0.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "(String::from({}), String::from({}))", Str(k), Str(v))?;
                }
                write!(f, "])")?;
                Ok(())
            }
        }
    }
}

pub struct Strategy<'a>(pub &'a SerdeArrowStrategy);

impl<'a> std::fmt::Display for Strategy<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Strategy::{}", self.0)
    }
}

pub struct DataType<'a>(pub &'a Arrow2DataType);

impl<'a> std::fmt::Display for DataType<'a> {
    fn fmt(&self, ff: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Arrow2DataType::*;
        match self.0 {
            Null => write!(ff, "DataType::Null"),
            Boolean => write!(ff, "DataType::Boolean"),
            Int8 => write!(ff, "DataType::Int8"),
            Int16 => write!(ff, "DataType::Int16"),
            Int32 => write!(ff, "DataType::Int32"),
            Int64 => write!(ff, "DataType::Int64"),
            UInt8 => write!(ff, "DataType::UInt8"),
            UInt16 => write!(ff, "DataType::UInt16"),
            UInt32 => write!(ff, "DataType::UInt32"),
            UInt64 => write!(ff, "DataType::UInt64"),
            Date32 => write!(ff, "DataType::Date32"),
            Date64 => write!(ff, "DataType::Date64"),
            Float16 => write!(ff, "DataType::Float16"),
            Float32 => write!(ff, "DataType::Float32"),
            Float64 => write!(ff, "DataType::Float64"),
            Binary => write!(ff, "DataType::Binary"),
            FixedSizeBinary(s) => write!(ff, "DataType::FixedSizeBinary({s})"),
            LargeBinary => write!(ff, "DataType::LargeBinary"),
            Utf8 => write!(ff, "DataType::Utf8"),
            LargeUtf8 => write!(ff, "DataType::LargeUtf8"),
            List(f) => write!(ff, "DataType::List(Box::new({}))", Field(f.as_ref())),
            FixedSizeList(f, s) => write!(
                ff,
                "DataType::FixedSizeList(Box::new({})), {})",
                Field(f.as_ref()),
                s
            ),
            LargeList(f) => write!(ff, "DataType::LargeList(Box::new({}))", Field(f.as_ref())),
            Struct(f) => write!(ff, "DataType::Struct({})", Fields(f)),
            Map(f, o) => write!(ff, "DataType::Map(Box::new({}), {})", Field(f), o),
            Decimal(l, u) => write!(ff, "DataType::Decimal({l}, {u}"),
            Decimal256(l, u) => write!(ff, "DataType::Decimal256({l}, {u}"),
            Extension(n, d, Some(m)) => write!(
                ff,
                "DataType::Extension(String::from({}), {}, Some(String::from({})))",
                Str(n),
                DataType(d),
                Str(m)
            ),
            Extension(n, d, None) => write!(
                ff,
                "DataType::Extension(String::from({}), {}, None)",
                Str(n),
                DataType(d)
            ),

            // TODO: fix these data types
            Timestamp(_, _) => write!(ff, "DataType::Timestamp(?, ?)"),
            Time32(_) => write!(ff, "DataType::Time32(?)"),
            Time64(_) => write!(ff, "DataType::Time64(?)"),
            Duration(_) => write!(ff, "DataType::Duration(?)"),
            Interval(_) => write!(ff, "DataType::Interval(?)"),
            Union(_, _, _) => write!(ff, "DataType::Union(?, ?, ?)"),
            Dictionary(_, _, _) => write!(ff, "DataType::Dictionary(?, ?, ?)"),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::schema::Strategy;
    use arrow2::datatypes::{DataType, Field, Metadata};

    //# start tests
    #[test]
    fn example_0() {
        assert_eq!(super::Str(&"hello").to_string(), r###""hello""###);
    }

    #[test]
    fn example_1() {
        assert_eq!(super::Str(&"hel\"lo").to_string(), r###""hel\"lo""###);
    }

    #[test]
    fn example_2() {
        assert_eq!(
            super::DataType(&DataType::Null).to_string(),
            r###"DataType::Null"###
        );
    }

    #[test]
    fn example_3() {
        assert_eq!(
            super::DataType(&DataType::Boolean).to_string(),
            r###"DataType::Boolean"###
        );
    }

    #[test]
    fn example_4() {
        assert_eq!(
            super::DataType(&DataType::Int8).to_string(),
            r###"DataType::Int8"###
        );
    }

    #[test]
    fn example_5() {
        assert_eq!(
            super::DataType(&DataType::Int16).to_string(),
            r###"DataType::Int16"###
        );
    }

    #[test]
    fn example_6() {
        assert_eq!(
            super::DataType(&DataType::Int32).to_string(),
            r###"DataType::Int32"###
        );
    }

    #[test]
    fn example_7() {
        assert_eq!(
            super::DataType(&DataType::Int64).to_string(),
            r###"DataType::Int64"###
        );
    }

    #[test]
    fn example_8() {
        assert_eq!(
            super::DataType(&DataType::UInt8).to_string(),
            r###"DataType::UInt8"###
        );
    }

    #[test]
    fn example_9() {
        assert_eq!(
            super::DataType(&DataType::UInt16).to_string(),
            r###"DataType::UInt16"###
        );
    }

    #[test]
    fn example_10() {
        assert_eq!(
            super::DataType(&DataType::UInt32).to_string(),
            r###"DataType::UInt32"###
        );
    }

    #[test]
    fn example_11() {
        assert_eq!(
            super::DataType(&DataType::UInt64).to_string(),
            r###"DataType::UInt64"###
        );
    }

    #[test]
    fn example_12() {
        assert_eq!(
            super::DataType(&DataType::Date32).to_string(),
            r###"DataType::Date32"###
        );
    }

    #[test]
    fn example_13() {
        assert_eq!(
            super::DataType(&DataType::Date64).to_string(),
            r###"DataType::Date64"###
        );
    }

    #[test]
    fn example_14() {
        assert_eq!(
            super::DataType(&DataType::Float16).to_string(),
            r###"DataType::Float16"###
        );
    }

    #[test]
    fn example_15() {
        assert_eq!(
            super::DataType(&DataType::Float32).to_string(),
            r###"DataType::Float32"###
        );
    }

    #[test]
    fn example_16() {
        assert_eq!(
            super::DataType(&DataType::Float64).to_string(),
            r###"DataType::Float64"###
        );
    }

    #[test]
    fn example_17() {
        assert_eq!(
            super::DataType(&DataType::Binary).to_string(),
            r###"DataType::Binary"###
        );
    }

    #[test]
    fn example_18() {
        assert_eq!(
            super::DataType(&DataType::FixedSizeBinary(2)).to_string(),
            r###"DataType::FixedSizeBinary(2)"###
        );
    }

    #[test]
    fn example_19() {
        assert_eq!(
            super::DataType(&DataType::FixedSizeBinary(32)).to_string(),
            r###"DataType::FixedSizeBinary(32)"###
        );
    }

    #[test]
    fn example_20() {
        assert_eq!(
            super::DataType(&DataType::LargeBinary).to_string(),
            r###"DataType::LargeBinary"###
        );
    }

    #[test]
    fn example_21() {
        assert_eq!(
            super::DataType(&DataType::Utf8).to_string(),
            r###"DataType::Utf8"###
        );
    }

    #[test]
    fn example_22() {
        assert_eq!(
            super::DataType(&DataType::LargeUtf8).to_string(),
            r###"DataType::LargeUtf8"###
        );
    }

    #[test]
    fn example_23() {
        assert_eq!(
            super::DataType(&DataType::List(Box::new(Field::new(
                "element",
                DataType::Int8,
                false
            ))))
            .to_string(),
            r###"DataType::List(Box::new(Field::new("element", DataType::Int8, false)))"###
        );
    }

    #[test]
    fn example_24() {
        assert_eq!(
            super::DataType(&DataType::Struct(vec![
                Field::new("a", DataType::Int8, true),
                Field::new("b", DataType::Boolean, false)
            ]))
            .to_string(),
            r###"DataType::Struct(vec![Field::new("a", DataType::Int8, true), Field::new("b", DataType::Boolean, false)])"###
        );
    }

    #[test]
    fn example_25() {
        assert_eq!(
            super::Field(
                &Field::new("with_meta", DataType::Date64, false)
                    .with_metadata(Strategy::UtcStrAsDate64.into())
            )
            .to_string(),
            r###"Field::new("with_meta", DataType::Date64, false).with_metadata(Strategy::UtcStrAsDate64.into())"###
        );
    }

    #[test]
    fn example_26() {
        assert_eq!(
            super::Field(
                &Field::new("with_meta", DataType::Int64, false)
                    .with_metadata(Metadata::from([(String::from("foo"), String::from("bar"))]))
            )
            .to_string(),
            r###"Field::new("with_meta", DataType::Int64, false).with_metadata(Metadata::from([(String::from("foo"), String::from("bar"))]))"###
        );
    }
    //# end tests
}
