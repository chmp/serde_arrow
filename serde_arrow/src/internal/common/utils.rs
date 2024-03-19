/// A wrapper type to allow implementing foreign traits
pub struct Mut<'a, T>(pub &'a mut T);
