#[derive(Debug, Clone, PartialEq, Default)]
pub struct BitSet(u128);

impl BitSet {
    pub const MAX: usize = u128::BITS as usize;

    pub fn clear(&mut self) {
        self.0 = 0;
    }

    pub fn insert(&mut self, field_idx: usize) {
        if field_idx >= Self::MAX {
            panic!("Attempt to insert an index beyond the maximum");
        }
        let flag = 1 << field_idx;
        self.0 |= flag;
    }

    pub fn contains(&self, field_idx: usize) -> bool {
        if field_idx >= Self::MAX {
            false
        } else {
            let flag = 1 << field_idx;
            (self.0 & flag) == flag
        }
    }
}

#[cfg(test)]
mod test {
    use crate::internal::serialization::bit_set::BitSet;

    #[test]
    fn examples() {
        let mut set = BitSet::default();
        assert_eq!(false, set.contains(0));
        assert_eq!(false, set.contains(1));
        assert_eq!(false, set.contains(2));
        assert_eq!(false, set.contains(3));

        set.insert(2);
        set.insert(0);

        assert_eq!(true, set.contains(0));
        assert_eq!(false, set.contains(1));
        assert_eq!(true, set.contains(2));
        assert_eq!(false, set.contains(3));
    }
}
