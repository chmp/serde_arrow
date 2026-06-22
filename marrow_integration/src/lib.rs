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
define_test_module!("arrow-52", arrow_52, arrow_array_52, arrow_schema_52, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays, intervals, union_arrays);
define_test_module!("arrow-51", arrow_51, arrow_array_51, arrow_schema_51, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays);
define_test_module!("arrow-50", arrow_50, arrow_array_50, arrow_schema_50, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays);
define_test_module!("arrow-49", arrow_49, arrow_array_49, arrow_schema_49, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays);
define_test_module!("arrow-48", arrow_48, arrow_array_48, arrow_schema_48, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays);
define_test_module!("arrow-47", arrow_47, arrow_array_47, arrow_schema_47, utils, arrays, data_types,struct_arrays, fixed_size_binary_arrays);
define_test_module!("arrow-46", arrow_46, arrow_array_46, arrow_schema_46, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-45", arrow_45, arrow_array_45, arrow_schema_45, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-44", arrow_44, arrow_array_44, arrow_schema_44, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-43", arrow_43, arrow_array_43, arrow_schema_43, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-42", arrow_42, arrow_array_42, arrow_schema_42, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-41", arrow_41, arrow_array_41, arrow_schema_41, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-40", arrow_40, arrow_array_40, arrow_schema_40, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-39", arrow_39, arrow_array_39, arrow_schema_39, utils, arrays, data_types,struct_arrays);
define_test_module!("arrow-38", arrow_38, arrow_array_38, arrow_schema_38, utils, arrays, data_types);
define_test_module!("arrow-37", arrow_37, arrow_array_37, arrow_schema_37, utils, arrays, data_types);