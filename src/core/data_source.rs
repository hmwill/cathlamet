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

use std::collections;
use std::str;
use std::u32;

use super::types;
use super::value;

type Path = types::Path;

/// Abstract accessor to a typed data structure
pub trait DataSource {
    /// Retrieve the a primitive value of the data structure element identified
    /// by path.
    fn get_bool(&self, path: &Path) -> bool;
    fn get_char(&self, path: &Path) -> char;
    fn get_f32(&self, path: &Path) -> f32;
    fn get_f64(&self, path: &Path) -> f64;
    fn get_u8(&self, path: &Path) -> u8;
    fn get_u16(&self, path: &Path) -> u16;
    fn get_u32(&self, path: &Path) -> u32;
    fn get_u64(&self, path: &Path) -> u64;
    fn get_u128(&self, path: &Path) -> u128;
    fn get_i8(&self, path: &Path) -> i8;
    fn get_i16(&self, path: &Path) -> i16;
    fn get_i32(&self, path: &Path) -> i32;
    fn get_i64(&self, path: &Path) -> i64;
    fn get_i128(&self, path: &Path) -> i128;

    /// Retrieve the enum case value of the data structure element identified
    /// by path
    fn get_case_tag(&self, path: &Path) -> usize;

    /// Retrieve the array length value of the data structure element
    /// identified by path
    fn get_length(&self, path: &Path) -> usize;

    /// Retrieve the data structure element identified by path as byte slice
    /// value
    fn get_bytes<'a>(&'a self, path: &Path) -> &'a [u8];
}

// TODO: Should we add type information to allow for validation of captured
// values?
pub struct IntermediateDataSource {
    values: collections::BTreeMap<Path, value::Value>,
}

macro_rules! intermediate_data_source_primitive {
    ($name:ident, $typ:ident, $tag:ident) => {
        fn $name(&self, path: &Path) -> $typ {
            match self.values.get(path).unwrap() {
                value::Value::Primitive(value::PrimitiveValue::$tag(val)) => *val,
                _ => panic!("Invalid data source entry"),
            }
        }
    };
}

impl DataSource for IntermediateDataSource {
    intermediate_data_source_primitive!(get_bool, bool, Bool);
    intermediate_data_source_primitive!(get_char, char, Char);
    intermediate_data_source_primitive!(get_f32, f32, F32);
    intermediate_data_source_primitive!(get_f64, f64, F64);
    intermediate_data_source_primitive!(get_u8, u8, U8);
    intermediate_data_source_primitive!(get_u16, u16, U16);
    intermediate_data_source_primitive!(get_u32, u32, U32);
    intermediate_data_source_primitive!(get_u64, u64, U64);
    intermediate_data_source_primitive!(get_u128, u128, U128);
    intermediate_data_source_primitive!(get_i8, i8, I8);
    intermediate_data_source_primitive!(get_i16, i16, I16);
    intermediate_data_source_primitive!(get_i32, i32, I32);
    intermediate_data_source_primitive!(get_i64, i64, I64);
    intermediate_data_source_primitive!(get_i128, i128, I128);

    fn get_case_tag(&self, path: &Path) -> usize {
        match self.values.get(path).unwrap() {
            value::Value::CaseTag(tag) => *tag,
            _ => panic!("Incorrect value type"),
        }
    }

    fn get_length(&self, path: &Path) -> usize {
        match self.values.get(path).unwrap() {
            value::Value::String(val) => val.len(),
            _ => panic!("Incorrect value type"),
        }
    }

    fn get_bytes<'a>(&'a self, path: &Path) -> &'a [u8] {
        match self.values.get(path).unwrap() {
            value::Value::String(val) => val.as_bytes(),
            _ => panic!("Incorrect value type"),
        }
    }
}

impl IntermediateDataSource {
    pub fn new() -> IntermediateDataSource {
        IntermediateDataSource {
            values: collections::BTreeMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.values.clear()
    }

    pub fn add_primitive(&mut self, path: &Path, val: value::PrimitiveValue) {
        self.values
            .insert(path.clone(), value::Value::Primitive(val));
    }

    pub fn add_string(&mut self, path: &Path, val: &str) {
        self.values
            .insert(path.clone(), value::Value::String(String::from(val)));
    }

    pub fn add_case_tag(&mut self, path: &Path, val: usize) {
        self.values.insert(path.clone(), value::Value::CaseTag(val));
    }
}

#[cfg(test)]
mod tests {
    use super::types;
    use super::value;
    use super::{DataSource, IntermediateDataSource};

    #[test]
    fn smoke_test() {
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
        ds.add_primitive(&city_zip, value::PrimitiveValue::U32(12345));

        assert!(ds.get_bytes(&first) == "John".as_bytes());
        assert!(ds.get_bytes(&last) == "Doe".as_bytes());
        assert!(ds.get_bytes(&address) == "123 Main Street".as_bytes());
        assert!(ds.get_bytes(&city_name) == "Anywhere".as_bytes());
        assert!(ds.get_u32(&city_zip) == 12345u32);
    }
}
