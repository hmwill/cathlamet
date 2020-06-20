// MIT License
//
// Copyright (c) 2018-2020 Hans-Martin Will
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
use std::marker;
use std::str::FromStr;

use super::super::data_source::IntermediateDataSource;
use super::super::types;
use super::super::value;
use super::tokenizer;

type Path = types::Path;

fn must_be(
    tokenizer: &mut tokenizer::Tokenizer,
    token: tokenizer::Token,
    error: &str,
) -> Result<(), String> {
    if tokenizer.token != token {
        Err(String::from(error))
    } else {
        tokenizer.next()?;
        Ok(())
    }
}

trait JsonParser {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        data_source: &mut IntermediateDataSource,
    ) -> Result<(), String>;
}

// A small trait to represent bit set operations, which we'll use for tracking
// field assignment
trait BitSet {
    fn new_with_elements(n: usize) -> Self;
    fn clear_element(&mut self, index: usize);
    fn has_element(&self, index: usize) -> bool;
    fn is_empty(&self) -> bool;
}

impl BitSet for u64 {
    fn new_with_elements(n: usize) -> Self {
        assert!(n <= 64);
        (1u64 << n) - 1
    }

    fn clear_element(&mut self, index: usize) {
        *self = *self & !(1u64 << index)
    }

    fn has_element(&self, index: usize) -> bool {
        (*self & (1u64 << index)) != 0
    }

    fn is_empty(&self) -> bool {
        *self == 0
    }
}

struct StructParser<T: BitSet> {
    indices: collections::BTreeMap<String, usize>,
    fields: Vec<Box<dyn JsonParser>>,
    _marker: marker::PhantomData<T>,
}

impl<T: BitSet> JsonParser for StructParser<T> {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        data_source: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        must_be(
            tokenizer,
            tokenizer::Token::BeginObject,
            "Structs are represented as JSON objects",
        )?;

        let mut visited = T::new_with_elements(self.fields.len());
        let mut first = true;
        // TODO: Future, in the context of optional fields, it should be OK to leave out
        //      optional fields altogether
        while !visited.is_empty() {
            if first {
                first = false;
            } else {
                must_be(
                    tokenizer,
                    tokenizer::Token::Comma,
                    "',' expected as separator between object fields",
                )?;
            }

            let index = if tokenizer.token == tokenizer::Token::String {
                match self.indices.get(tokenizer.value()) {
                    None => return Err(format!("Invalid field named '{}'", tokenizer.value())),
                    Some(index) => *index,
                }
            } else {
                return Err(String::from("Expecting a field label"));
            };

            if visited.has_element(index) {
                tokenizer.next()?;
                visited.clear_element(index);
            } else {
                return Err(format!(
                    "Duplicate assignment for field named '{}'",
                    tokenizer.value()
                ));
            }

            must_be(
                tokenizer,
                tokenizer::Token::Colon,
                "':' expected as separator between field labels and values",
            )?;

            self.fields[index].parse(tokenizer, data_source)?;
        }

        must_be(
            tokenizer,
            tokenizer::Token::EndObject,
            "Missing terminating '}' of JSON object encoding a struct value",
        )?;
        Ok(())
    }
}

struct EnumParser {
    path: Path,
    cases: collections::BTreeMap<String, (usize, Box<dyn JsonParser>)>,
}

impl JsonParser for EnumParser {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        data_source: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        // A parser that looks for a JSON object with fields 'kind' and 'value'
        // TODO: Relax the order to have 'kind' following 'value'?
        must_be(
            tokenizer,
            tokenizer::Token::BeginObject,
            "Enums are represented as JSON object",
        )?;

        if tokenizer.token != tokenizer::Token::String || tokenizer.value() != "kind" {
            return Err(String::from("Expecting 'kind' field"));
        } else {
            tokenizer.next()?;
        }

        must_be(
            tokenizer,
            tokenizer::Token::Colon,
            "':' expected as separator between field labels and values",
        )?;

        // Extract case tag here; this is also a string

        if tokenizer.token != tokenizer::Token::String {
            return Err(String::from("Case tage value must be a string value"));
        }

        let (index, parser) = match self.cases.get(tokenizer.value()) {
            None => return Err(format!("Invalid enum case '{}'", tokenizer.value())),
            Some(parser) => parser,
        };

        data_source.add_case_tag(&self.path, *index);
        tokenizer.next()?;

        must_be(
            tokenizer,
            tokenizer::Token::Comma,
            "',' expected as separator between case label and case value",
        )?;

        if tokenizer.token != tokenizer::Token::String || tokenizer.value() != "value" {
            return Err(String::from("Expecting 'value' field"));
        } else {
            tokenizer.next()?;
        }

        must_be(
            tokenizer,
            tokenizer::Token::Colon,
            "':' expected as separator between field labels and values",
        )?;

        // invoke value perser here
        parser.parse(tokenizer, data_source)?;

        must_be(
            tokenizer,
            tokenizer::Token::EndObject,
            "Missing terminating '}' of JSON object encoding an enum value",
        )
    }
}

struct TupleParser {
    elements: Vec<Box<dyn JsonParser>>,
}

impl JsonParser for TupleParser {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        data_source: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        must_be(
            tokenizer,
            tokenizer::Token::BeginArray,
            "Tuples are represented as JSON array",
        )?;

        let mut first = true;
        for element in &self.elements {
            if first {
                first = false;
            } else {
                must_be(
                    tokenizer,
                    tokenizer::Token::Comma,
                    "',' expected as separator between tuple components",
                )?;
            }

            element.parse(tokenizer, data_source)?;
        }

        must_be(
            tokenizer,
            tokenizer::Token::EndArray,
            "Missing terminating ']' of JSON array encoding a tuple value",
        )?;
        Ok(())
    }
}

struct StringParser {
    path: Path,
}

impl JsonParser for StringParser {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        data_source: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        if tokenizer.token == tokenizer::Token::String {
            data_source.add_string(&self.path, tokenizer.value());
            tokenizer.next()
        } else {
            Err(String::from("String value expected"))
        }
    }
}

struct PrimitiveParser {
    path: Path,
    primitive: types::Primitive,
}

fn parse_primitive_number<T: FromStr, F: Fn(T) -> value::PrimitiveValue>(
    tokenizer: &mut tokenizer::Tokenizer,
    data_source: &mut IntermediateDataSource,
    path: &Path,
    embed: F,
) -> Result<(), String>
where
    T::Err: ToString,
{
    if tokenizer.token != tokenizer::Token::String && tokenizer.token != tokenizer::Token::Number {
        return Err(String::from("Expecting string or number"));
    }
    match T::from_str(tokenizer.value()) {
        Ok(val) => {
            data_source.add_primitive(path, embed(val));
            tokenizer.next()
        }
        Err(err) => Err(err.to_string()),
    }
}

fn extract_char(string: &str) -> Option<char> {
    let mut sequence = string.chars();
    if let Some(ch) = sequence.next() {
        if let None = sequence.next() {
            Some(ch)
        } else {
            None
        }
    } else {
        None
    }
}

impl JsonParser for PrimitiveParser {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        data_source: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        match self.primitive {
            types::Primitive::Bool => match tokenizer.token {
                tokenizer::Token::True => {
                    data_source.add_primitive(&self.path, value::PrimitiveValue::Bool(true));
                    tokenizer.next()
                }
                tokenizer::Token::False => {
                    data_source.add_primitive(&self.path, value::PrimitiveValue::Bool(false));
                    tokenizer.next()
                }
                _ => Err(String::from("Boolean value must be True or False")),
            },
            types::Primitive::Char => {
                if tokenizer.token == tokenizer::Token::String {
                    match extract_char(tokenizer.value()) {
                        None => {
                            tokenizer.next()?;
                            return Err(String::from(
                                "Expecting a string encoding a single character",
                            ));
                        }
                        Some(ch) => {
                            data_source.add_primitive(&self.path, value::PrimitiveValue::Char(ch));
                            tokenizer.next()
                        }
                    }
                } else {
                    Err(String::from(
                        "Expecting a string encoding a single character",
                    ))
                }
            }
            types::Primitive::F32 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::F32,
            ),
            types::Primitive::F64 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::F64,
            ),
            types::Primitive::U8 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::U8,
            ),
            types::Primitive::U16 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::U16,
            ),
            types::Primitive::U32 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::U32,
            ),
            types::Primitive::U64 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::U64,
            ),
            types::Primitive::U128 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::U128,
            ),
            types::Primitive::I8 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::I8,
            ),
            types::Primitive::I16 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::I16,
            ),
            types::Primitive::I32 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::I32,
            ),
            types::Primitive::I64 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::I64,
            ),
            types::Primitive::I128 => parse_primitive_number(
                tokenizer,
                data_source,
                &self.path,
                value::PrimitiveValue::I128,
            ),
        }
    }
}

struct NullParser {}

impl JsonParser for NullParser {
    fn parse(
        &self,
        tokenizer: &mut tokenizer::Tokenizer,
        _: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        if tokenizer.token == tokenizer::Token::Null {
            tokenizer.next()
        } else {
            Err(String::from(
                "'null' is the only valid value for a unit value",
            ))
        }
    }
}

fn parser_for_type(prefix: Path, typ: &types::Type) -> Box<dyn JsonParser> {
    match &typ.kind {
        types::Kind::Unit => Box::new(NullParser {}),
        types::Kind::Primitive(primitive) => Box::new(PrimitiveParser {
            path: prefix,
            primitive: primitive.clone(),
        }),
        types::Kind::String => Box::new(StringParser { path: prefix }),
        types::Kind::Tuple(components) => {
            let emitters = components
                .iter()
                .enumerate()
                .map(|(index, typ)| {
                    let component_path = prefix.with_component(index);
                    parser_for_type(component_path, typ)
                })
                .collect();
            Box::new(TupleParser { elements: emitters })
        }
        types::Kind::Struct(fields) => {
            let indices = fields
                .iter()
                .enumerate()
                .map(|(index, field)| (field.label.clone(), index))
                .collect();
            let fields = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let component_path = prefix.with_component(index);
                    parser_for_type(component_path, &field.typ)
                })
                .collect();
            let struct_parser: StructParser<u64> = StructParser {
                indices,
                fields,
                _marker: marker::PhantomData,
            };
            Box::new(struct_parser)
        }
        types::Kind::Enum(cases) => {
            let parsers = cases
                .iter()
                .enumerate()
                .map(|(index, case)| {
                    let component_path = prefix.with_component(index);
                    (
                        case.label.clone(),
                        (index, parser_for_type(component_path, &case.typ)),
                    )
                })
                .collect();

            Box::new(EnumParser {
                path: prefix,
                cases: parsers,
            })
        }
    }
}

pub struct JsonDecoder {
    parser: Box<dyn JsonParser>,
}

impl JsonDecoder {
    pub fn new(typ: &types::Type) -> JsonDecoder {
        JsonDecoder {
            parser: parser_for_type(Path::create_empty(), typ),
        }
    }

    pub fn parse(
        &mut self,
        input: &str,
        result: &mut IntermediateDataSource,
    ) -> Result<(), String> {
        let mut tokenizer = tokenizer::Tokenizer::new(input)?;
        result.clear();
        self.parser.parse(&mut tokenizer, result)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::data_source::{DataSource, IntermediateDataSource};

    use super::types;
    use super::JsonDecoder;

    #[test]
    fn struct_test() {
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

        let input = r#"{"first":"John","last":"Doe","address":"123 Main Street","city":{"name":"Anywhere","zip":12345}}"#;

        let mut decoder = JsonDecoder::new(&typ);
        let mut data_source = IntermediateDataSource::new();
        match decoder.parse(input, &mut data_source) {
            Err(err) => panic!(format!("Parsing error: {}", err)),
            _ => (),
        }

        assert!(data_source.get_bytes(&first) == "John".as_bytes());
        assert!(data_source.get_bytes(&last) == "Doe".as_bytes());
        assert!(data_source.get_bytes(&address) == "123 Main Street".as_bytes());
        assert!(data_source.get_bytes(&city_name) == "Anywhere".as_bytes());
        assert!(data_source.get_u32(&city_zip) == 12345u32);
    }

    #[test]
    fn tuple_test() {
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

        let input = r#"["John","Doe","123 Main Street",["Anywhere",12345]]"#;

        let mut decoder = JsonDecoder::new(&typ);
        let mut data_source = IntermediateDataSource::new();

        match decoder.parse(input, &mut data_source) {
            Err(err) => panic!(format!("Parsing error: {}", err)),
            _ => (),
        }

        assert!(data_source.get_bytes(&first) == "John".as_bytes());
        assert!(data_source.get_bytes(&last) == "Doe".as_bytes());
        assert!(data_source.get_bytes(&address) == "123 Main Street".as_bytes());
        assert!(data_source.get_bytes(&city_name) == "Anywhere".as_bytes());
        assert!(data_source.get_u32(&city_zip) == 12345u32);
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

        let mut decoder = JsonDecoder::new(&typ);
        let mut data_source = IntermediateDataSource::new();

        let root = types::Path::create_empty();
        let some = types::Path::resolve_qualified_name(&typ, "Some").unwrap();
        //let none = types::Path::resolve_qualified_name(&typ, "None").unwrap();

        match decoder.parse(r#"{"kind":"Some","value":3.141}"#, &mut data_source) {
            Err(err) => panic!(format!("Parsing error: {}", err)),
            _ => (),
        }

        assert_eq!(data_source.get_case_tag(&root), 0);
        assert_eq!(data_source.get_f64(&some), 3.141);

        match decoder.parse(r#"{"kind":"None","value":null}"#, &mut data_source) {
            Err(err) => panic!(format!("Parsing error: {}", err)),
            _ => (),
        }

        assert_eq!(data_source.get_case_tag(&root), 1);
    }
}
