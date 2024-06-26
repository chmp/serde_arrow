use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::internal::error::{fail, Error, Result};

/// The metadata key under which to store the strategy
///
/// See the [module][crate::schema] for details.
///
pub const STRATEGY_KEY: &str = "SERDE_ARROW:strategy";

/// Strategies for handling types without direct match between arrow and serde
///
/// For the correct strategy both the field type and the field metadata must be
/// correctly configured. In particular, when determining the schema from the
/// Rust objects themselves, some field types are incorrectly recognized (e.g.,
/// datetimes).
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
#[non_exhaustive]
pub enum Strategy {
    /// Marker that the type of the field could not be determined during tracing
    ///
    InconsistentTypes,
    /// Serialize Rust strings containing UTC datetimes with timezone as Arrows
    /// Date64
    ///
    /// This strategy makes sense for chrono's `DateTime<Utc>` types without
    /// additional configuration. As they are serialized as strings.
    UtcStrAsDate64,
    /// Serialize Rust strings containing datetimes without timezone as Arrow
    /// Date64
    ///
    /// This strategy makes sense for chrono's `NaiveDateTime` types without
    /// additional configuration. As they are serialized as strings.
    ///
    NaiveStrAsDate64,
    /// Serialize Rust tuples as Arrow structs with numeric field names starting
    /// at `"0"`
    ///
    /// This strategy is most-likely the most optimal one, as Rust tuples can
    /// contain different types, whereas Arrow sequences must be of uniform type
    ///
    TupleAsStruct,
    /// Serialize Rust maps as Arrow structs
    ///
    /// The field names are sorted by name to ensure unordered map (e.g.,
    /// HashMap) have a defined order.
    ///
    /// Fields that are not present in all instances of the map are marked as
    /// nullable in schema tracing. In serialization these fields are written as
    /// null value if not present.
    ///
    /// This strategy is most-likely the most optimal one:
    ///
    /// - using the `#[serde(flatten)]` attribute converts a struct into a map
    /// - the support for arrow maps in the data ecosystem is limited (e.g.,
    ///   polars does not support them)
    ///
    MapAsStruct,
    /// Mark a variant as unknown
    ///
    /// This strategy applies only to fields with DataType Null. If
    /// serialization or deserialization of such a field is attempted, it will
    /// result in an error.
    UnknownVariant,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InconsistentTypes => write!(f, "InconsistentTypes"),
            Self::UtcStrAsDate64 => write!(f, "UtcStrAsDate64"),
            Self::NaiveStrAsDate64 => write!(f, "NaiveStrAsDate64"),
            Self::TupleAsStruct => write!(f, "TupleAsStruct"),
            Self::MapAsStruct => write!(f, "MapAsStruct"),
            Self::UnknownVariant => write!(f, "UnknownVariant"),
        }
    }
}

impl From<Strategy> for String {
    fn from(strategy: Strategy) -> String {
        strategy.to_string()
    }
}

impl TryFrom<String> for Strategy {
    type Error = Error;

    fn try_from(s: String) -> Result<Strategy> {
        s.parse()
    }
}

impl FromStr for Strategy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "InconsistentTypes" => Ok(Self::InconsistentTypes),
            "UtcStrAsDate64" => Ok(Self::UtcStrAsDate64),
            "NaiveStrAsDate64" => Ok(Self::NaiveStrAsDate64),
            "TupleAsStruct" => Ok(Self::TupleAsStruct),
            "MapAsStruct" => Ok(Self::MapAsStruct),
            "UnknownVariant" => Ok(Self::UnknownVariant),
            _ => fail!("Unknown strategy {s}"),
        }
    }
}

impl From<Strategy> for BTreeMap<String, String> {
    fn from(value: Strategy) -> Self {
        let mut res = BTreeMap::new();
        res.insert(STRATEGY_KEY.to_string(), value.to_string());
        res
    }
}

impl From<Strategy> for HashMap<String, String> {
    fn from(value: Strategy) -> Self {
        let mut res = HashMap::new();
        res.insert(STRATEGY_KEY.to_string(), value.to_string());
        res
    }
}

pub fn split_strategy_from_metadata(
    mut metadata: HashMap<String, String>,
) -> Result<(HashMap<String, String>, Option<Strategy>)> {
    let strategy = if let Some(strategy_str) = metadata.remove(STRATEGY_KEY) {
        Some(strategy_str.parse::<Strategy>()?)
    } else {
        None
    };

    Ok((metadata, strategy))
}

pub fn merge_strategy_with_metadata(
    mut metadata: HashMap<String, String>,
    strategy: Option<Strategy>,
) -> Result<HashMap<String, String>> {
    if metadata.contains_key(STRATEGY_KEY) && strategy.is_some() {
        fail!("Duplicate strategy: metadata map contains {STRATEGY_KEY} and strategy given");
    }
    if let Some(strategy) = strategy {
        metadata.insert(STRATEGY_KEY.to_owned(), strategy.to_string());
    }
    Ok(metadata)
}

#[test]
fn test_split_strategy_from_metadata_with_metadata() {
    use crate::internal::testing::hash_map;

    let input: HashMap<String, String> = hash_map!(
        "SERDE_ARROW:strategy" => "TupleAsStruct",
        "key1" => "value1",
        "key2" => "value2",
    );

    let expected_metadata: HashMap<String, String> = hash_map!(
        "key1" => "value1",
        "key2" => "value2",
    );
    let expected_strategy: Option<Strategy> = Some(Strategy::TupleAsStruct);

    let (actual_metadata, actual_strategy) = split_strategy_from_metadata(input.clone()).unwrap();
    let roundtripped =
        merge_strategy_with_metadata(actual_metadata.clone(), actual_strategy.clone()).unwrap();

    assert_eq!(actual_metadata, expected_metadata);
    assert_eq!(actual_strategy, expected_strategy);
    assert_eq!(roundtripped, input);
}

#[test]
fn test_split_strategy_from_metadata_without_metadata() {
    use crate::internal::testing::hash_map;

    let input: HashMap<String, String> = hash_map!(
        "key1" => "value1",
        "key2" => "value2",
    );

    let expected_metadata: HashMap<String, String> = hash_map!(
        "key1" => "value1",
        "key2" => "value2",
    );
    let expected_strategy: Option<Strategy> = None;

    let (actual_metadata, actual_strategy) = split_strategy_from_metadata(input.clone()).unwrap();
    let roundtripped =
        merge_strategy_with_metadata(actual_metadata.clone(), actual_strategy.clone()).unwrap();

    assert_eq!(actual_metadata, expected_metadata);
    assert_eq!(actual_strategy, expected_strategy);
    assert_eq!(roundtripped, input);
}
