// MIT License
//
// Copyright (c) 2018 Hans-Martin Will
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::io;
use std::marker;
use std::str;

use super::super::data_source::DataSource;
use super::super::types;
use super::super::types::Path;

// JSON Encoder and Decoder

fn write_ch(writer: &mut dyn io::Write, value: char) -> io::Result<()> {
    let mut buffer: [u8; 6] = [0; 6];

    // Special values that need to be escaped
    // encode unicode sequence based on value range
    match value {
        '\0' => writer.write("\\0".as_bytes())?,
        '\\' => writer.write("\\\\".as_bytes())?,
        '/' => writer.write("\\/".as_bytes())?,
        '\"' => writer.write("\\\"".as_bytes())?,
        '\u{8}' => writer.write("\\b".as_bytes())?,
        '\u{c}' => writer.write("\\f".as_bytes())?,
        '\n' => writer.write("\\n".as_bytes())?,
        '\r' => writer.write("\\r".as_bytes())?,
        '\t' => writer.write("\\t".as_bytes())?,
        b0 if 0x1f >= value as u32 => {
            buffer[0] = '\\' as u8;
            buffer[1] = 'u' as u8;
            buffer[2] = '0' as u8;
            buffer[3] = '0' as u8;
            buffer[4] = ('0' as u8) + (b0 as u8 >> 4);
            buffer[5] = ('0' as u8) + (b0 as u8 & 0xfu8);
            writer.write(&buffer[..6])?
        }
        b1 if 0x7f >= value as u32 => {
            buffer[0] = b1 as u8;
            writer.write(&buffer[..1])?
        }
        b2 if value as u32 >= 0x80 && value as u32 <= 0x7ff => {
            buffer[1] = (b2 as u8) & 0x3fu8 | 0x80u8;
            buffer[0] = (b2 as u32 >> 6) as u8 & 0x1fu8 | 0xc0u8;
            writer.write(&buffer[..2])?
        }
        b3 if value as u32 >= 0x800 && value as u32 <= 0x7fff => {
            buffer[2] = (b3 as u8) & 0x3fu8 | 0x80u8;
            buffer[1] = (b3 as u32 >> 6) as u8 & 0x3fu8 | 0x80u8;
            buffer[0] = (b3 as u32 >> 12) as u8 & 0x0fu8 | 0xe0u8;
            writer.write(&buffer[..3])?
        }
        b4 => {
            buffer[3] = (b4 as u8) & 0x3fu8 | 0x80u8;
            buffer[2] = (b4 as u32 >> 6) as u8 & 0x3fu8 | 0x80u8;
            buffer[1] = (b4 as u32 >> 12) as u8 & 0x3fu8 | 0x80u8;
            buffer[0] = (b4 as u32 >> 18) as u8 & 0x07u8 | 0xf0u8;
            writer.write(&buffer[..4])?
        }
    };
    Ok(())
}

fn emit_utf8_string(writer: &mut dyn io::Write, string: &str) -> io::Result<()> {
    writer.write("\"".as_bytes())?;

    for ch in string.chars() {
        write_ch(writer, ch)?;
    }

    writer.write("\"".as_bytes())?;
    Ok(())
}

trait JsonEmitter {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()>;
}

/// Really, EmitUnit should not be needed at all.
struct EmitUnit {}

impl JsonEmitter for EmitUnit {
    fn emit(&self, writer: &mut dyn io::Write, _: &dyn DataSource) -> io::Result<()> {
        writer.write("null".as_bytes())?;
        Ok(())
    }
}

struct EmitTuple {
    components: Vec<Box<dyn JsonEmitter>>,
}

impl JsonEmitter for EmitTuple {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        writer.write("[".as_bytes())?;

        let mut first = true;
        for field in &self.components {
            if first {
                first = false
            } else {
                writer.write(",".as_bytes())?;
            }

            field.emit(writer, source)?;
        }

        writer.write("]".as_bytes())?;
        Ok(())
    }
}

struct EmitStruct {
    fields: Vec<(String, Box<dyn JsonEmitter>)>,
}

impl JsonEmitter for EmitStruct {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        writer.write("{".as_bytes())?;

        let mut first = true;
        for (ref name, ref field) in &self.fields {
            if first {
                first = false
            } else {
                writer.write(",".as_bytes())?;
            }

            emit_utf8_string(writer, name.as_str())?;
            writer.write(":".as_bytes())?;
            field.emit(writer, source)?;
        }

        writer.write("}".as_bytes())?;
        Ok(())
    }
}

struct EmitEnum {
    path: Path,
    cases: Vec<(String, Box<dyn JsonEmitter>)>,
}

impl JsonEmitter for EmitEnum {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        writer.write("{\"kind\":".as_bytes())?;

        let case_tag = source.get_case_tag(&self.path);
        emit_utf8_string(writer, self.cases[case_tag].0.as_str())?;
        writer.write(",\"value\":".as_bytes())?;
        self.cases[case_tag].1.emit(writer, source)?;
        writer.write("}".as_bytes())?;
        Ok(())
    }
}

struct EmitString {
    path: Path,
}

impl JsonEmitter for EmitString {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        let string: &[u8] = source.get_bytes(&self.path);
        emit_utf8_string(writer, str::from_utf8(string).unwrap())?;
        Ok(())
    }
}

struct EmitBoolean {
    path: Path,
}

impl JsonEmitter for EmitBoolean {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        let value: bool = source.get_bool(&self.path);

        if value {
            writer.write("true".as_bytes())?;
        } else {
            writer.write("false".as_bytes())?;
        }

        Ok(())
    }
}

struct EmitChar {
    path: Path,
}

impl JsonEmitter for EmitChar {
    fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        writer.write("\"".as_bytes())?;
        write_ch(writer, source.get_char(&self.path))?;
        writer.write("\"".as_bytes())?;
        Ok(())
    }
}

struct EmitNumber<T> {
    path: Path,
    phantom: marker::PhantomData<T>,
}

macro_rules! encoder_emit_primitive {
    ($get_primitive:ident, $typ:ident) => {
        impl JsonEmitter for EmitNumber<$typ> {
            fn emit(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
                let value = source.$get_primitive(&self.path);
                write!(writer, "{}", value)?;
                Ok(())
            }
        }
    };
}

encoder_emit_primitive!(get_bool, bool);
encoder_emit_primitive!(get_char, char);
encoder_emit_primitive!(get_f32, f32);
encoder_emit_primitive!(get_f64, f64);
encoder_emit_primitive!(get_u8, u8);
encoder_emit_primitive!(get_u16, u16);
encoder_emit_primitive!(get_u32, u32);
encoder_emit_primitive!(get_u64, u64);
encoder_emit_primitive!(get_u128, u128);
encoder_emit_primitive!(get_i8, i8);
encoder_emit_primitive!(get_i16, i16);
encoder_emit_primitive!(get_i32, i32);
encoder_emit_primitive!(get_i64, i64);
encoder_emit_primitive!(get_i128, i128);

pub struct JsonEncoder {
    emitter: Box<dyn JsonEmitter>,
}

fn emitter_for_type(prefix: types::Path, typ: &types::Type) -> Box<dyn JsonEmitter> {
    match typ.kind {
        types::Kind::Primitive(types::Primitive::Bool) => Box::new(EmitBoolean { path: prefix }),
        types::Kind::Primitive(types::Primitive::Char) => Box::new(EmitChar { path: prefix }),
        types::Kind::Primitive(types::Primitive::U8) => {
            let emitter: EmitNumber<u8> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::U16) => {
            let emitter: EmitNumber<u16> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::U32) => {
            let emitter: EmitNumber<u32> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::U64) => {
            let emitter: EmitNumber<u64> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::U128) => {
            let emitter: EmitNumber<u128> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::I8) => {
            let emitter: EmitNumber<i8> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::I16) => {
            let emitter: EmitNumber<i16> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::I32) => {
            let emitter: EmitNumber<i32> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::I64) => {
            let emitter: EmitNumber<i64> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::I128) => {
            let emitter: EmitNumber<i128> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::F32) => {
            let emitter: EmitNumber<f32> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Primitive(types::Primitive::F64) => {
            let emitter: EmitNumber<f64> = EmitNumber {
                path: prefix,
                phantom: marker::PhantomData,
            };
            Box::new(emitter)
        }
        types::Kind::Tuple(ref components) => {
            let emitters = components
                .iter()
                .enumerate()
                .map(|(index, typ)| {
                    let component_path = prefix.with_component(index);
                    emitter_for_type(component_path, typ)
                })
                .collect();
            Box::new(EmitTuple {
                components: emitters,
            })
        }
        types::Kind::String => {
            let emitter = EmitString { path: prefix };
            Box::new(emitter)
        }
        types::Kind::Struct(ref fields) => {
            let emitters = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let component_path = prefix.with_component(index);
                    (
                        field.label.clone(),
                        emitter_for_type(component_path, &field.typ),
                    )
                })
                .collect();
            Box::new(EmitStruct { fields: emitters })
        }
        types::Kind::Enum(ref cases) => {
            let emitters = cases
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let component_path = prefix.with_component(index);
                    (
                        field.label.clone(),
                        emitter_for_type(component_path, &field.typ),
                    )
                })
                .collect();
            Box::new(EmitEnum {
                path: prefix,
                cases: emitters,
            })
        }
        types::Kind::Unit => Box::new(EmitUnit {}),
    }
}

impl JsonEncoder {
    pub fn new(typ: &types::Type) -> JsonEncoder {
        let prefix = types::Path::create_empty();

        JsonEncoder {
            emitter: emitter_for_type(prefix, typ),
        }
    }

    pub fn encode(&self, writer: &mut dyn io::Write, source: &dyn DataSource) -> io::Result<()> {
        self.emitter.emit(writer, source)
    }
}

#[cfg(test)]
mod tests {
    use super::types;
    use super::JsonEncoder;
    use crate::core::data_source::IntermediateDataSource;
    use crate::core::value;

    #[test]
    fn test_struct() {
        let parser = types::parser::TypeParser::new();
        let typ = parser
            .parse(
                r#"
            struct {
                first: string,
                last: string,
                address: string,
                city: struct {
                    name: string,
                    zip: u32
                }
            }
            "#,
            )
            .unwrap();

        let first = types::Path::resolve_qualified_name(&typ, "first").unwrap();
        let last = types::Path::resolve_qualified_name(&typ, "last").unwrap();
        let address = types::Path::resolve_qualified_name(&typ, "address").unwrap();
        let city_name = types::Path::resolve_qualified_name(&typ, "city.name").unwrap();
        let city_zip = types::Path::resolve_qualified_name(&typ, "city.zip").unwrap();

        let mut ds = IntermediateDataSource::new();
        ds.add_string(&first, "John");
        ds.add_string(&last, "Doe");
        ds.add_string(&address, "123 Main Street");
        ds.add_string(&city_name, "Anywhere");
        ds.add_primitive(&city_zip, value::PrimitiveValue::U32(12345u32));

        let encoder = JsonEncoder::new(&typ);
        let mut buffer: Vec<u8> = Vec::new();
        assert!(encoder.encode(&mut buffer, &ds).is_ok());

        assert!(&buffer[..] == r#"{"first":"John","last":"Doe","address":"123 Main Street","city":{"name":"Anywhere","zip":12345}}"#.as_bytes());
    }

    #[test]
    fn test_tuple() {
        let parser = types::parser::TypeParser::new();
        let typ = parser
            .parse(
                r#"
            (
                string,
                string,
                string,
                (
                    string,
                    u32
                )
            )
            "#,
            )
            .unwrap();

        let first = types::Path::resolve_qualified_name(&typ, "0").unwrap();
        let last = types::Path::resolve_qualified_name(&typ, "1").unwrap();
        let address = types::Path::resolve_qualified_name(&typ, "2").unwrap();
        let city_name = types::Path::resolve_qualified_name(&typ, "3.0").unwrap();
        let city_zip = types::Path::resolve_qualified_name(&typ, "3.1").unwrap();

        let mut ds = IntermediateDataSource::new();
        ds.add_string(&first, "John");
        ds.add_string(&last, "Doe");
        ds.add_string(&address, "123 Main Street");
        ds.add_string(&city_name, "Anywhere");
        ds.add_primitive(&city_zip, value::PrimitiveValue::U32(12345u32));

        let encoder = JsonEncoder::new(&typ);
        let mut buffer: Vec<u8> = Vec::new();
        assert!(encoder.encode(&mut buffer, &ds).is_ok());

        assert!(&buffer[..] == r#"["John","Doe","123 Main Street",["Anywhere",12345]]"#.as_bytes());
    }

    #[test]
    fn enum_test() {
        let parser = types::parser::TypeParser::new();
        let typ = parser
            .parse(
                r#"
            enum {
                Some: f64,
                None: ()
            }
            "#,
            )
            .unwrap();

        let encoder = JsonEncoder::new(&typ);
        let mut ds = IntermediateDataSource::new();

        let root = types::Path::create_empty();
        let some = types::Path::resolve_qualified_name(&typ, "Some").unwrap();
        //let none = types::Path::resolve_qualified_name(&typ, "None").unwrap();

        ds.add_case_tag(&root, 0);
        ds.add_primitive(&some, value::PrimitiveValue::F64(3.141));

        let mut buffer: Vec<u8> = Vec::new();
        assert!(encoder.encode(&mut buffer, &ds).is_ok());
        assert_eq!(&buffer[..], r#"{"kind":"Some","value":3.141}"#.as_bytes());

        ds.clear();
        ds.add_case_tag(&root, 1);
        buffer.clear();
        assert!(encoder.encode(&mut buffer, &ds).is_ok());
        assert_eq!(&buffer[..], r#"{"kind":"None","value":null}"#.as_bytes());
    }
}
