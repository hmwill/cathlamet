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

use std::cmp;
use std::mem;
use std::str::FromStr;

pub mod parser;

/// Primitive types
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Primitive {
    Bool,
    Char,
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    F32,
    F64,
}

impl Primitive {
    pub fn encoding_size(&self) -> usize {
        match self {
            Primitive::Bool => mem::size_of::<bool>(),
            Primitive::Char => mem::size_of::<char>(),
            Primitive::U8 => mem::size_of::<u8>(),
            Primitive::U16 => mem::size_of::<u16>(),
            Primitive::U32 => mem::size_of::<u32>(),
            Primitive::U64 => mem::size_of::<u64>(),
            Primitive::U128 => mem::size_of::<u128>(),
            Primitive::I8 => mem::size_of::<i8>(),
            Primitive::I16 => mem::size_of::<i16>(),
            Primitive::I32 => mem::size_of::<i32>(),
            Primitive::I64 => mem::size_of::<i64>(),
            Primitive::I128 => mem::size_of::<i128>(),
            Primitive::F32 => mem::size_of::<f32>(),
            Primitive::F64 => mem::size_of::<f64>(),
        }
    }
}

pub trait IsPrimitive {
    fn tag() -> Primitive;
}

macro_rules! tag_primitive {
    ($typ:ident, $tag:ident) => {
        impl IsPrimitive for $typ {
            fn tag() -> Primitive {
                Primitive::$tag
            }
        }
    };
}

tag_primitive!(bool, Bool);
tag_primitive!(char, Char);
tag_primitive!(u8, U8);
tag_primitive!(u16, U16);
tag_primitive!(u32, U32);
tag_primitive!(u64, U64);
tag_primitive!(u128, U128);
tag_primitive!(i8, I8);
tag_primitive!(i16, I16);
tag_primitive!(i32, I32);
tag_primitive!(i64, I64);
tag_primitive!(i128, I128);
tag_primitive!(f32, F32);
tag_primitive!(f64, F64);

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Field {
    pub label: String,
    pub typ: Box<Type>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Case {
    pub label: String,
    pub typ: Box<Type>,
}

pub type Components = Vec<Box<Type>>;

pub type Fields = Vec<Field>;

fn find_field_index(fields: &Fields, label: &str) -> Option<usize> {
    for index in 0..fields.len() {
        if fields[index].label == label {
            return Some(index);
        }
    }

    None
}

pub type Cases = Vec<Case>;

fn find_case_index(cases: &Cases, label: &str) -> Option<usize> {
    for index in 0..cases.len() {
        if cases[index].label == label {
            return Some(index);
        }
    }

    None
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Kind {
    Unit,
    Primitive(Primitive),
    String,
    Tuple(Components),
    Struct(Fields),
    Enum(Cases),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Type {
    pub kind: Kind,
}

impl Type {
    pub fn extract<'a>(&'a self, path: &Path) -> &'a Type {
        let mut typ = self;

        for component in &path.0 {
            match typ.kind {
                Kind::Enum(ref cases) => typ = cases[*component].typ.as_ref(),
                Kind::Struct(ref fields) => typ = fields[*component].typ.as_ref(),
                Kind::Tuple(ref components) => typ = components[*component].as_ref(),
                _ => panic!("Type node not indexable"),
            }
        }

        typ
    }
}

/// Identifier of a specific element of a typed structure
#[derive(Clone, Debug)]
pub struct Path(Vec<usize>);

impl Path {
    pub fn create_empty() -> Path {
        Path(Vec::new())
    }

    pub fn with_component(&self, component: usize) -> Path {
        let mut components = self.0.clone();
        components.push(component);
        Path(components)
    }

    pub fn is_prefix_of(&self, other: &Path) -> bool {
        if other.0.len() < self.0.len() {
            return false;
        }

        &self.0[..] == &other.0[0..self.0.len()]
    }

    pub fn is_proper_prefix_of(&self, other: &Path) -> bool {
        if self.0.len() == self.0.len() {
            return false;
        } else {
            self.is_prefix_of(other)
        }
    }

    pub fn path<'a>(&'a self) -> &'a [usize] {
        &self.0[..]
    }

    pub fn resolve_qualified_name(mut typ: &Type, name: &str) -> Result<Path, String> {
        let mut components = Vec::new();

        for component in name.split(".") {
            match typ.kind {
                Kind::Enum(ref cases) => match find_case_index(cases, component) {
                    Some(index) => {
                        components.push(index);
                        typ = cases[index].typ.as_ref();
                    }
                    None => {
                        return Err(format!("Invalid enum case label '{}'", component));
                    }
                },
                Kind::Struct(ref fields) => match find_field_index(fields, component) {
                    Some(index) => {
                        components.push(index);
                        typ = fields[index].typ.as_ref();
                    }
                    None => {
                        return Err(format!("Invalid struct field label '{}'", component));
                    }
                },
                Kind::Tuple(ref elements) => match usize::from_str(component) {
                    Ok(index) if index < elements.len() => {
                        components.push(index);
                        typ = elements[index].as_ref();
                    }
                    _ => {
                        return Err(format!("Invalid tuple component '{}'", component));
                    }
                },
                _ => {
                    return Err(String::from(
                        "Cannot index into a leaf of a type descriptor.",
                    ))
                }
            }
        }

        Ok(Path(components))
    }
}

impl PartialEq for Path {
    fn eq(&self, rhs: &Self) -> bool {
        self.0 == rhs.0
    }
}

impl PartialOrd for Path {
    fn partial_cmp(&self, rhs: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(rhs))
    }
}

impl Eq for Path {}

impl Ord for Path {
    fn cmp(&self, rhs: &Self) -> cmp::Ordering {
        let common_length = cmp::min(self.0.len(), rhs.0.len());

        for index in 0..common_length {
            if self.0[index] < rhs.0[index] {
                return cmp::Ordering::Less;
            } else if self.0[index] > rhs.0[index] {
                return cmp::Ordering::Greater;
            }
        }

        return self.0.len().cmp(&rhs.0.len());
    }
}

// TODO: Need a function that converts a path into readable form (qualified
// name)

// This helper function is used by the generated parser
fn append<T>(list: Vec<T>, elem: T) -> Vec<T> {
    let mut result = list;
    result.push(elem);
    result
}

#[cfg(test)]
mod tests {
    use crate::core::types;

    fn run_test(input: &str, expected: &types::Type) {
        let parser = types::parser::TypeParser::new();
        match parser.parse(input) {
            Err(err) => panic!("Parsing of input '{}' failed due to: {:?}", input, err),
            Ok(ref result) if *result == *expected => (),
            Ok(ref actual) => panic!(
                "Unexpected parse result for input '{}': expected {:?}, actual {:?}",
                input, expected, actual
            ),
        }
    }

    fn run_component_test(input: &str, qualified_name: &str, expected: &types::Type) {
        let parser = types::parser::TypeParser::new();
        match parser.parse(input) {
            Err(err) => panic!("Parsing of input '{}' failed due to: {:?}", input, err),
            Ok(ref result) => {
                let path = types::Path::resolve_qualified_name(result, qualified_name).unwrap();
                let component_type = result.extract(&path);
                if *component_type != *expected {
                    panic!(
                        "Unexpected parse result for input '{}': expected {:?}, actual {:?}",
                        input, expected, component_type
                    )
                }
            }
        }
    }

    #[test]
    fn test_unit() {
        run_test(
            "()",
            &types::Type {
                kind: types::Kind::Unit,
            },
        )
    }

    #[test]
    fn test_primitives() {
        run_test(
            "bool",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::Bool),
            },
        );
        run_test(
            "char",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::Char),
            },
        );
        run_test(
            "f32",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::F32),
            },
        );
        run_test(
            "f64",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::F64),
            },
        );
        run_test(
            "u8",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::U8),
            },
        );
        run_test(
            "u16",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::U16),
            },
        );
        run_test(
            "u32",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::U32),
            },
        );
        run_test(
            "u64",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::U64),
            },
        );
        run_test(
            "u128",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::U128),
            },
        );
        run_test(
            "i8",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::I8),
            },
        );
        run_test(
            "i16",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::I16),
            },
        );
        run_test(
            "i32",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::I32),
            },
        );
        run_test(
            "i64",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::I64),
            },
        );
        run_test(
            "i128",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::I128),
            },
        );
    }

    #[test]
    fn test_string() {
        run_test(
            "string",
            &types::Type {
                kind: types::Kind::String,
            },
        );
    }

    #[test]
    fn test_tuples() {
        run_test(
            "(char, bool, i16)",
            &types::Type {
                kind: types::Kind::Tuple(vec![
                    Box::new(types::Type {
                        kind: types::Kind::Primitive(types::Primitive::Char),
                    }),
                    Box::new(types::Type {
                        kind: types::Kind::Primitive(types::Primitive::Bool),
                    }),
                    Box::new(types::Type {
                        kind: types::Kind::Primitive(types::Primitive::I16),
                    }),
                ]),
            },
        );

        run_test(
            "(char, (bool), i16)",
            &types::Type {
                kind: types::Kind::Tuple(vec![
                    Box::new(types::Type {
                        kind: types::Kind::Primitive(types::Primitive::Char),
                    }),
                    Box::new(types::Type {
                        kind: types::Kind::Tuple(vec![Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Bool),
                        })]),
                    }),
                    Box::new(types::Type {
                        kind: types::Kind::Primitive(types::Primitive::I16),
                    }),
                ]),
            },
        );
    }

    #[test]
    fn test_struct() {
        run_test(
            "struct {first: char, second: bool, third: i16}",
            &types::Type {
                kind: types::Kind::Struct(vec![
                    types::Field {
                        label: String::from("first"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Char),
                        }),
                    },
                    types::Field {
                        label: String::from("second"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Bool),
                        }),
                    },
                    types::Field {
                        label: String::from("third"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::I16),
                        }),
                    },
                ]),
            },
        );

        run_test(
            "struct {first: char, second: struct { field: bool }, third: i16}",
            &types::Type {
                kind: types::Kind::Struct(vec![
                    types::Field {
                        label: String::from("first"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Char),
                        }),
                    },
                    types::Field {
                        label: String::from("second"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Struct(vec![types::Field {
                                label: String::from("field"),
                                typ: Box::new(types::Type {
                                    kind: types::Kind::Primitive(types::Primitive::Bool),
                                }),
                            }]),
                        }),
                    },
                    types::Field {
                        label: String::from("third"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::I16),
                        }),
                    },
                ]),
            },
        );
    }

    #[test]
    fn test_enum() {
        run_test(
            "enum { first: char, second: bool, third: i16}",
            &types::Type {
                kind: types::Kind::Enum(vec![
                    types::Case {
                        label: String::from("first"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Char),
                        }),
                    },
                    types::Case {
                        label: String::from("second"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Bool),
                        }),
                    },
                    types::Case {
                        label: String::from("third"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::I16),
                        }),
                    },
                ]),
            },
        );

        run_test(
            "enum { first: char, second: enum { field: bool }, third: i16}",
            &types::Type {
                kind: types::Kind::Enum(vec![
                    types::Case {
                        label: String::from("first"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::Char),
                        }),
                    },
                    types::Case {
                        label: String::from("second"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Enum(vec![types::Case {
                                label: String::from("field"),
                                typ: Box::new(types::Type {
                                    kind: types::Kind::Primitive(types::Primitive::Bool),
                                }),
                            }]),
                        }),
                    },
                    types::Case {
                        label: String::from("third"),
                        typ: Box::new(types::Type {
                            kind: types::Kind::Primitive(types::Primitive::I16),
                        }),
                    },
                ]),
            },
        );
    }

    #[test]
    fn test_resolve_name() {
        run_component_test(
            "enum { first: char, second: enum { field: bool }, third: i16}",
            "second.field",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::Bool),
            },
        );

        run_component_test(
            "enum { first: char, second: struct { field: bool }, third: i16}",
            "second.field",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::Bool),
            },
        );

        run_component_test(
            "(char, enum { case: f32 }, i16)",
            "1.case",
            &types::Type {
                kind: types::Kind::Primitive(types::Primitive::F32),
            },
        );
    }

}
