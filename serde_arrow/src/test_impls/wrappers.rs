use super::macros::test_serialize_into_array;

test_serialize_into_array!(test_name = outer_vec, values = vec![0_u32, 1_u32, 2_u32],);

test_serialize_into_array!(test_name = outer_slice, values = &[0_u32, 1_u32, 2_u32],);

test_serialize_into_array!(test_name = outer_array, values = [0_u32, 1_u32, 2_u32],);

test_serialize_into_array!(test_name = outer_tuple, values = (0_u32, 1_u32, 2_u32),);
