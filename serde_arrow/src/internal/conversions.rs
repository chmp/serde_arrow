pub trait ToBytes: Sized {
    type Bytes;

    fn to_bytes(self) -> Self::Bytes;
    fn from_bytes(val: Self::Bytes) -> Self;

    fn to_bytes_vec(items: Vec<Self>) -> Vec<Self::Bytes> {
        items.into_iter().map(Self::to_bytes).collect()
    }

    fn from_bytes_vec(items: Vec<Self::Bytes>) -> Vec<Self> {
        items.into_iter().map(Self::from_bytes).collect()
    }
}

impl ToBytes for u8 {
    type Bytes = u8;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for u16 {
    type Bytes = u16;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for u32 {
    type Bytes = u32;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for u64 {
    type Bytes = u64;

    fn to_bytes(self) -> Self::Bytes {
        self
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        val
    }
}

impl ToBytes for i8 {
    type Bytes = u8;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for i16 {
    type Bytes = u16;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for i32 {
    type Bytes = u32;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for i64 {
    type Bytes = u64;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for f32 {
    type Bytes = u32;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}

impl ToBytes for f64 {
    type Bytes = u64;

    fn to_bytes(self) -> Self::Bytes {
        Self::Bytes::from_ne_bytes(self.to_ne_bytes())
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_ne_bytes(val.to_ne_bytes())
    }
}
