#![cfg_attr(any(), rustfmt::skip)]

macro_rules! define_test_module {
    ($feature:literal, $mod:ident, $array_mod:ident, $schema_mod:ident, $($test_mod:ident),* $(,)?) => {
        #[cfg(all(test, feature = $feature))]
        mod $mod {
            $(
                mod $test_mod {
                    #[allow(unused)]
                    use { $array_mod as arrow_array, $schema_mod as arrow_schema };
                    
                    include!(concat!("tests/", stringify!($test_mod), ".rs"));
                }
            )*
        }
    };
}

// arrow-version:insert: define_test_module!("arrow-{version}", arrow_{version}, arrow_array_{version}, arrow_schema_{version}, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
define_test_module!("arrow-59", arrow_59, arrow_array_59, arrow_schema_59, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
define_test_module!("arrow-58", arrow_58, arrow_array_58, arrow_schema_58, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
define_test_module!("arrow-56", arrow_56, arrow_array_56, arrow_schema_56, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
define_test_module!("arrow-55", arrow_55, arrow_array_55, arrow_schema_55, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
define_test_module!("arrow-54", arrow_54, arrow_array_54, arrow_schema_54, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
define_test_module!("arrow-53", arrow_53, arrow_array_53, arrow_schema_53, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays, views);
