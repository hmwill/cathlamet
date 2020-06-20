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

use super::types;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum PrimitiveValue {
    Bool(bool),
    Char(char),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    F32(f32),
    F64(f64)
}

impl PrimitiveValue {
    pub fn primitive(&self) -> types::Primitive {
        match self {
            PrimitiveValue::Bool(_) => types::Primitive::Bool,
            PrimitiveValue::Char(_) => types::Primitive::Char,
            PrimitiveValue::U8(_) => types::Primitive::U8,
            PrimitiveValue::U16(_) => types::Primitive::U16,
            PrimitiveValue::U32(_) => types::Primitive::U32,
            PrimitiveValue::U64(_) => types::Primitive::U64,
            PrimitiveValue::U128(_) => types::Primitive::U128,
            PrimitiveValue::I8(_) => types::Primitive::I8,
            PrimitiveValue::I16(_) => types::Primitive::I16,
            PrimitiveValue::I32(_) => types::Primitive::I32,
            PrimitiveValue::I64(_) => types::Primitive::I64,
            PrimitiveValue::I128(_) => types::Primitive::I128,
            PrimitiveValue::F32(_) => types::Primitive::F32,
            PrimitiveValue::F64(_) => types::Primitive::F64,
        }
    }
}

pub enum Value {
    Primitive(PrimitiveValue),
    String(String),
    CaseTag(usize),
}