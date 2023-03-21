use arrow::array::ArrayData;
use arrow2::{
    array::{Array, Int32Array},
    datatypes::{DataType, Field},
};

#[derive(Debug, Clone, Copy)]
struct PanicOnError;

impl<E: std::fmt::Display> From<E> for PanicOnError {
    fn from(value: E) -> Self {
        panic!("{value}")
    }
}

fn main() -> Result<(), PanicOnError> {
    example_array()?;
    example_record_batch()?;
    Ok(())
}

fn example_array() -> Result<(), PanicOnError> {
    let arrow2_array = Int32Array::from(&[Some(1), None, Some(3)]);
    let arrow2_array = Box::new(arrow2_array);
    let arrow2_field = Field::new("a", arrow2_array.data_type().clone(), true);

    println!("Construct arrow Int32Array");
    let arrow_array = convert_arrow2_to_arrow(arrow2_array, &arrow2_field);
    let array_data = ArrayData::try_from(arrow_array)?;
    array_data.validate_full()?;

    // to create a generic dyn Array use arrow::array::make_array()
    let arrow_array = arrow::array::Int32Array::from(array_data);

    {
        use arrow::array::Array;

        println!("len:         {}", arrow_array.len());
        println!("nulls count: {}", arrow_array.null_count());
        println!("array[0]:    {}", arrow_array.value(0));
        println!("array[1]:    {}", arrow_array.value(1));
        println!("array[2]:    {}", arrow_array.value(2));
        println!();
    }

    Ok(())
}

fn example_record_batch() -> Result<(), PanicOnError> {
    let arrow2_arrays: Vec<Box<dyn Array>> = vec![
        Box::new(Int32Array::from(&[Some(1), None, Some(3)])),
        Box::new(Int32Array::from(&[None, Some(2), None])),
    ];
    let arrow2_fields = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ];

    println!("Construct a record batch");
    let record_batch = convert_to_record_batch(arrow2_arrays, &arrow2_fields)?;

    println!("columns: {}", record_batch.num_columns());
    println!("rows:    {}", record_batch.num_rows());
    println!();

    Ok(())
}

fn convert_arrow2_to_arrow(array: Box<dyn Array>, field: &Field) -> arrow::ffi::ArrowArray {
    let array = arrow2::ffi::export_array_to_c(array);
    let schema = arrow2::ffi::export_field_to_c(field);

    let array = unsafe { std::mem::transmute::<_, arrow::ffi::FFI_ArrowArray>(array) };
    let schema = unsafe { std::mem::transmute::<_, arrow::ffi::FFI_ArrowSchema>(schema) };

    arrow::ffi::ArrowArray::new(array, schema)
}

fn convert_to_record_batch(
    arrays: Vec<Box<dyn Array>>,
    fields: &[Field],
) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
    let mut arrow_arrays = Vec::new();
    for (array, field) in arrays.into_iter().zip(fields.iter()) {
        let array = convert_arrow2_to_arrow(array, field);
        let data = arrow::array::ArrayData::try_from(array)?;
        let array = arrow::array::make_array(data);

        arrow_arrays.push(array);
    }

    let mut arrow_fields = Vec::new();
    for (array, field) in arrow_arrays.iter().zip(fields.iter()) {
        let field =
            arrow::datatypes::Field::new(&field.name, array.data_type().clone(), field.is_nullable);
        arrow_fields.push(field);
    }

    let schema = arrow::datatypes::Schema::new(arrow_fields);
    let schema_ref = arrow::datatypes::SchemaRef::new(schema);

    arrow::record_batch::RecordBatch::try_new(schema_ref, arrow_arrays)
}
