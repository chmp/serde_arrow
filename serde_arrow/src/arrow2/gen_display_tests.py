# script to generate the tests in display.rs
from pathlib import Path


cases = r"""
Str "hello"
Str "hel\"lo"
DataType DataType::Null
DataType DataType::Boolean
DataType DataType::Int8
DataType DataType::Int16
DataType DataType::Int32
DataType DataType::Int64
DataType DataType::UInt8
DataType DataType::UInt16
DataType DataType::UInt32
DataType DataType::UInt64
DataType DataType::Date32
DataType DataType::Date64
DataType DataType::Float16
DataType DataType::Float32
DataType DataType::Float64
DataType DataType::Binary
DataType DataType::FixedSizeBinary(2)
DataType DataType::FixedSizeBinary(32)
DataType DataType::LargeBinary
DataType DataType::Utf8
DataType DataType::LargeUtf8
DataType DataType::List(Box::new(Field::new("element", DataType::Int8, false)))
DataType DataType::Struct(vec![Field::new("a", DataType::Int8, true), Field::new("b", DataType::Boolean, false)])
Field Field::new("with_meta", DataType::Date64, false).with_metadata(Strategy::UtcStrAsDate64.into())
Field Field::new("with_meta", DataType::Int64, false).with_metadata(Metadata::from([(String::from("foo"), String::from("bar"))]))
"""

def main():
    p = Path(__file__).parent / "display.rs"

    dst_mtime = p.stat().st_mtime
    src_mtime = Path(__file__).stat().st_mtime

    if src_mtime < dst_mtime:
        return

    print(f"Update {p}")
    source = p.read_text().splitlines()
    header, footer = split_existing_file(source)
    lines = [*header, *generate_cases(cases), *footer]

    p.write_text("\n".join(lines))


def split_existing_file(source):
    header = []
    footer = []

    state = "in_header"
    
    for line in source:
        if state == "in_header":
            header.append(line)
            
            if line.strip() == "//# start tests":
                state = "in_tests"

        elif state == "in_tests":
            if line.strip() == "//# end tests":
                footer.append(line)
                state = "in_footer"

        elif state == "in_footer":
            footer.append(line)
        
        else:
            raise ValueError(f"Unknown state: {state}")

    return header, footer

def generate_cases(cases):
    cases = [case.strip() for case in cases.splitlines()]
    cases = [case for case in cases if case]

    for idx, case in enumerate(cases):
        ty, _, expr = case.partition(" ")
        
        if idx != 0:
            yield f""

        yield f"    #[test]"
        yield f"    fn example_{idx}() {{"
        yield f"        assert_eq!(super::{ty}(&{expr}).to_string(), r###\"{expr}\"###);"
        yield f"    }}"


if __name__ == "__main__":
    main()
