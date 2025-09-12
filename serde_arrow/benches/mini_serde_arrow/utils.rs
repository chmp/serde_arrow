/// A wrapper around a static field name that compares using ptr and length
#[derive(Debug, Clone, Copy)]
pub struct StaticFieldName(pub &'static str);

impl std::cmp::PartialEq for StaticFieldName {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ptr(), self.0.len()) == (other.0.as_ptr(), other.0.len())
    }
}
