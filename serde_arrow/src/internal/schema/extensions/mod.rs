mod bool8_field;
mod fixed_shape_tensor_field;
mod utils;
mod variable_shape_tensor_field;

pub use bool8_field::Bool8Field;
pub use fixed_shape_tensor_field::FixedShapeTensorField;
pub use variable_shape_tensor_field::VariableShapeTensorField;

#[allow(unused)]
const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for Bool8Field {}
    impl AssertSendSync for FixedShapeTensorField {}
    impl AssertSendSync for VariableShapeTensorField {}
};
