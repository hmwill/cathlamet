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
use std::collections;
use std::mem;
use std::slice;
use std::sync;
use std::u32;

use super::data_source;
use super::types;
use super::util;

/// Type to use for encoding array length values
type LengthEncodingType = u32;

type Path = types::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
enum FixedSizeFieldKind {
    Primitve(types::Primitive),
    Length,
    BitfieldContainer {
        index: usize,
        primitive: types::Primitive,
    },
    Bitfield {
        primitive: types::Primitive,
        bit_offset: u8,
        num_bits: u8,
    },
}

impl FixedSizeFieldKind {
    fn encoding_size(&self) -> usize {
        match self {
            FixedSizeFieldKind::Primitve(primitive) => primitive.encoding_size(),
            FixedSizeFieldKind::Length => mem::size_of::<LengthEncodingType>(),
            FixedSizeFieldKind::BitfieldContainer { primitive, .. } => primitive.encoding_size(),
            FixedSizeFieldKind::Bitfield { .. } => panic!("Shouldn't occur at this point"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FixedSizeField {
    path: Path,

    kind: FixedSizeFieldKind,

    offset: usize,
}

struct Bitfield {
    path: Path,
    bit_offset: u8,
    num_bits: u8,
    index: usize,
}

impl FixedSizeField {
    pub fn encoding_size(&self) -> usize {
        match &self.kind {
            FixedSizeFieldKind::Length => mem::size_of::<LengthEncodingType>(),
            FixedSizeFieldKind::Primitve(p) => p.encoding_size(),
            FixedSizeFieldKind::BitfieldContainer { primitive, .. } => primitive.encoding_size(),
            FixedSizeFieldKind::Bitfield { primitive, .. } => primitive.encoding_size(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum VariableSizeFieldKind {
    String,
    Enum { cases: Vec<Layout> },
}

#[derive(Debug, Eq, PartialEq)]
struct VariableSizeField {
    path: Path,

    fixed_field_index: usize,

    kind: VariableSizeFieldKind,
}

impl VariableSizeField {
    fn alignment(&self) -> usize {
        match self.kind {
            VariableSizeFieldKind::String => 1,
            VariableSizeFieldKind::Enum { ref cases } => cases
                .iter()
                .map(|ref layout| layout.alignment)
                .max()
                .unwrap_or(1usize),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Index {
    path: Path,
    fixed_size_field_index: usize,
    var_size_field_index: usize,
}

fn unroll_type(
    typ: &types::Type,
    prefix: &Path,
    fixed_size_fields: &mut Vec<FixedSizeField>,
    bit_fields: &mut Vec<Bitfield>,
    var_size_fields: &mut Vec<VariableSizeField>,
) {
    match &typ.kind {
        &types::Kind::Unit => (),
        &types::Kind::Primitive(primitive) => fixed_size_fields.push(FixedSizeField {
            path: prefix.clone(),
            kind: FixedSizeFieldKind::Primitve(primitive),
            offset: 0,
        }),
        &types::Kind::String => {
            fixed_size_fields.push(FixedSizeField {
                path: prefix.clone(),
                kind: FixedSizeFieldKind::Length,
                offset: 0,
            });
            var_size_fields.push(VariableSizeField {
                path: prefix.clone(),
                fixed_field_index: 0,
                kind: VariableSizeFieldKind::String,
            });
        }
        &types::Kind::Tuple(ref components) => {
            assert!(components.len() > 0);
            for (index, component) in components.iter().enumerate() {
                let prefix = prefix.with_component(index);
                unroll_type(
                    component,
                    &prefix,
                    fixed_size_fields,
                    bit_fields,
                    var_size_fields,
                );
            }
        }
        &types::Kind::Struct(ref fields) => {
            assert!(fields.len() > 0);
            for (index, field) in fields.iter().enumerate() {
                let prefix = prefix.with_component(index);
                unroll_type(
                    &field.typ,
                    &prefix,
                    fixed_size_fields,
                    bit_fields,
                    var_size_fields,
                );
            }
        }
        &types::Kind::Enum(ref cases) => {
            assert!(cases.len() > 0);
            let num_bits = util::required_bits(cases.len() - 1);
            bit_fields.push(Bitfield {
                path: prefix.clone(),
                bit_offset: 0,
                num_bits: num_bits as u8,
                index: 0,
            });
            let mut layouts = Vec::new();

            for (index, case) in cases.iter().enumerate() {
                let prefix = prefix.with_component(index);
                layouts.push(calculate_layout(&case.typ, &prefix));
            }

            var_size_fields.push(VariableSizeField {
                path: prefix.clone(),
                fixed_field_index: 0,
                kind: VariableSizeFieldKind::Enum { cases: layouts },
            });
        }
    }
}

fn primitive_for_bits(num_bits: usize) -> types::Primitive {
    assert!(num_bits <= 128);

    if num_bits > 64 {
        types::Primitive::U128
    } else if num_bits > 32 {
        types::Primitive::U64
    } else if num_bits > 16 {
        types::Primitive::U32
    } else if num_bits > 8 {
        types::Primitive::U16
    } else {
        types::Primitive::U8
    }
}

fn allocate_bitfields(bit_fields: &mut Vec<Bitfield>) -> Vec<FixedSizeField> {
    let mut result = Vec::new();
    let mut capacity = Vec::new();
    let mut offsets = Vec::new();

    bit_fields.sort_by(|l, r| l.num_bits.cmp(&r.num_bits).reverse());

    'outer: for field in bit_fields {
        assert!(result.len() == capacity.len());
        assert!(offsets.len() == capacity.len());

        let num_bits = field.num_bits as usize;

        for index in 0..result.len() {
            if capacity[index] >= num_bits as usize {
                field.index = index;
                field.bit_offset = offsets[index] as u8;
                capacity[index] = capacity[index] - num_bits;
                offsets[index] = offsets[index] + num_bits;

                continue 'outer;
            }
        }

        let index = result.len();
        let primitive = primitive_for_bits(num_bits);
        result.push(FixedSizeField {
            kind: FixedSizeFieldKind::BitfieldContainer { primitive, index },
            offset: 0,
            path: Path::create_empty(),
        });

        // assign the field to the new container
        field.index = index;
        field.bit_offset = 0u8;
        capacity.push(primitive.encoding_size() * 8 - num_bits);
        offsets.push(num_bits);
    }

    result
}

fn calculate_layout(typ: &types::Type, prefix: &Path) -> Layout {
    // unroll the type into fixed fields, bit fields and variable length fields
    let mut fixed_size_fields = Vec::new();
    let mut bit_fields = Vec::new();
    let mut var_size_fields = Vec::new();

    unroll_type(
        typ,
        prefix,
        &mut fixed_size_fields,
        &mut bit_fields,
        &mut var_size_fields,
    );

    // combine bitfields and allocate additional fixed fields
    let mut additional_fields = allocate_bitfields(&mut bit_fields);
    fixed_size_fields.append(&mut additional_fields);

    // order each of them in descending order by size/alignment
    fixed_size_fields.sort_by(|l, r| l.encoding_size().cmp(&r.encoding_size()).reverse());
    var_size_fields.sort_by(|l, r| l.alignment().cmp(&r.alignment()).reverse());

    // allocate offsets for fixed sizew fields
    let mut size = 0usize;

    for field in &mut fixed_size_fields {
        field.offset = size;
        size += field.kind.encoding_size();
    }

    let fixed_field_alignment = if fixed_size_fields.len() > 0 {
        fixed_size_fields[0].kind.encoding_size()
    } else {
        1usize
    };

    let var_field_alignment = if var_size_fields.len() > 0 {
        var_size_fields[0].alignment()
    } else {
        1usize
    };

    let alignment = cmp::max(fixed_field_alignment, var_field_alignment);
    size = util::align(size, var_field_alignment);

    // create index vectors that map a type element index to a field index (for
    // each of fixed, bit, variable)

    let offset_map: collections::HashMap<usize, (usize, types::Primitive)> = fixed_size_fields
        .iter()
        .filter_map(|field| match field.kind {
            FixedSizeFieldKind::BitfieldContainer { index, primitive } => {
                Some((index, (field.offset, primitive)))
            }
            _ => None,
        })
        .collect();

    fixed_size_fields = fixed_size_fields
        .iter_mut()
        .filter_map(|field| match field.kind {
            FixedSizeFieldKind::BitfieldContainer { .. } => None,
            _ => Some(field.clone()),
        })
        .chain(bit_fields.iter().map(|bitfield| {
            let (offset, primitive) = *offset_map.get(&bitfield.index).unwrap();
            FixedSizeField {
                path: bitfield.path.clone(),
                offset,
                kind: FixedSizeFieldKind::Bitfield {
                    primitive,
                    bit_offset: bitfield.bit_offset,
                    num_bits: bitfield.num_bits,
                },
            }
        }))
        .collect();

    // We order in reverse order; in this case, binary search can be used for
    // prefix searches
    let mut field_indices: Vec<Index> = fixed_size_fields
        .iter()
        .enumerate()
        .map(|(fixed_size_field_index, field)| Index {
            fixed_size_field_index,
            path: field.path.clone(),
            var_size_field_index: 0,
        })
        .collect();
    field_indices.sort_by(|l, r| l.path.cmp(&r.path).reverse());

    // back pointers of variable size fields to the case tag/length information
    let lookup: collections::BTreeMap<Path, usize> = field_indices
        .iter()
        .map(|index| (index.path.clone(), index.fixed_size_field_index))
        .collect();

    for field_index in 0..var_size_fields.len() {
        let fixed_field_index = {
            let path = &var_size_fields[field_index].path;

            match field_indices.binary_search_by(|probe| probe.path.cmp(path).reverse()) {
                Ok(idx) => field_indices[idx].var_size_field_index = field_index,
                Err(_) => panic!("Could not find guard field for variable sized filed"),
            }

            *lookup.get(path).unwrap()
        };

        var_size_fields[field_index].fixed_field_index = fixed_field_index;
    }

    Layout {
        alignment,
        var_field_offset: size,
        fixed_size_fields,
        var_size_fields,
        field_indices,
    }
}

/// Data structure to represent the memory layout for a data structure (sounds
/// meta, but isn't)
#[derive(Debug, Eq, PartialEq)]
pub struct Layout {
    /// overall alignment to use for this data structure
    alignment: usize,

    /// offset of the first variable length field
    var_field_offset: usize,

    /// Mapping of path values to fixed size fields
    field_indices: Vec<Index>,

    /// Fixed size fields
    fixed_size_fields: Vec<FixedSizeField>,

    /// Variable size fields
    var_size_fields: Vec<VariableSizeField>,
}

impl Layout {
    pub fn new(typ: &types::Type) -> Layout {
        let prefix = Path::create_empty();
        calculate_layout(typ, &prefix)
    }

    fn variable_field_offset(&self, var_size_field_index: usize, data: &[u8]) -> usize {
        let mut offset = self.var_field_offset;

        for var_size_field in &self.var_size_fields[0..var_size_field_index] {
            // determine the size of the variable length field and add it to offset
            let tag_field = &self.fixed_size_fields[var_size_field.fixed_field_index];

            match var_size_field.kind {
                VariableSizeFieldKind::String => {
                    // extract the length
                    let length = unsafe {
                        let pointer: *const LengthEncodingType =
                            mem::transmute(data[tag_field.offset..].as_ptr());
                        *pointer
                    };

                    // add to the offset
                    offset = offset + length as usize as usize;
                }
                VariableSizeFieldKind::Enum { ref cases } => {
                    // extract the case tag
                    let case_tag = {
                        match &tag_field.kind {
                            FixedSizeFieldKind::Bitfield {
                                primitive,
                                bit_offset,
                                num_bits,
                            } => {
                                let case_value = match primitive {
                                    types::Primitive::U128 => unsafe {
                                        let pointer: *const u128 = 
                                                mem::transmute(data[tag_field.offset ..].as_ptr());
                                        *pointer as usize
                                    },
                                    types::Primitive::U64 => unsafe {
                                        let pointer: *const u64 = 
                                                mem::transmute(data[tag_field.offset ..].as_ptr());
                                        *pointer as usize
                                    },
                                    types::Primitive::U32 => unsafe {
                                        let pointer: *const u32 = 
                                                mem::transmute(data[tag_field.offset ..].as_ptr());
                                        *pointer as usize
                                    },
                                    types::Primitive::U16 => unsafe {
                                        let pointer: *const u16 = 
                                                mem::transmute(data[tag_field.offset ..].as_ptr());
                                        *pointer as usize
                                    },
                                    types::Primitive::U8 => unsafe {
                                        let pointer: *const u8 = 
                                                mem::transmute(data[tag_field.offset ..].as_ptr());
                                        *pointer as usize
                                    },
                                    _ => panic!("Unexpected primitive for storing bitfields"),
                                };

                                (case_value >> bit_offset) & ((1usize << num_bits) - 1)
                            }
                            _ => panic!("Bitfiled expected as enum case tag value"),
                        }
                    };

                    // recursively calculate the size of the union case
                    // add to the offset
                    let case_data = &data[offset..];
                    let case_size = cases[case_tag].encoding_size(case_data);

                    offset = offset + case_size;
                }
            }
        }

        offset
    }

    fn encoding_size(&self, data: &[u8]) -> usize {
        util::align(
            self.variable_field_offset(self.var_size_fields.len(), data),
            self.alignment,
        )
    }

    pub fn encoding_size_with_source<D>(&self, source: &D) -> usize
    where
        D: data_source::DataSource,
    {
        let mut offset = self.var_field_offset;

        for field in &self.var_size_fields {
            match &field.kind {
                VariableSizeFieldKind::String => {
                    let length = source.get_length(&field.path);
                    offset += length;
                }
                VariableSizeFieldKind::Enum { ref cases } => {
                    let case_tag = source.get_case_tag(&field.path);
                    offset += cases[case_tag].encoding_size_with_source(source);
                }
            }
        }

        util::align(offset, self.alignment)
    }

    pub fn encode_with_source<D>(&self, buffer: &mut [u8], source: &D) -> usize
    where
        D: data_source::DataSource,
    {
        let mut offset = self.var_field_offset;
        assert!(buffer.len() >= offset);

        for field in &self.fixed_size_fields {
            match &field.kind {
                FixedSizeFieldKind::Length => {
                    let length = source.get_length(&field.path);

                    // Ideally the following line could be parameterized using
                    // ArrayLengthEncodingType; this assertion, while not
                    // ideal, will make it fail
                    // if we are changing the
                    // encoding type.
                    assert!(mem::size_of::<u32>() == mem::size_of::<LengthEncodingType>());
                    assert!(length <= u32::MAX as usize);

                    unsafe {
                        let pointer: *mut LengthEncodingType =
                            mem::transmute(buffer[field.offset..].as_ptr());
                        *pointer = length as LengthEncodingType;
                    }
                }
                FixedSizeFieldKind::Bitfield {
                    primitive,
                    bit_offset,
                    num_bits,
                } => {
                    let case_tag = source.get_case_tag(&field.path);

                    match primitive {
                        types::Primitive::U8 => unsafe {
                            let mask = ((1u8 << num_bits) - 1) << bit_offset;
                            let pointer: *mut u8 = mem::transmute(buffer[field.offset..].as_ptr());
                            *pointer =
                                (*pointer & !mask) | (((case_tag as u8) << bit_offset) & mask);
                        },
                        types::Primitive::U16 => unsafe {
                            let mask = ((1u16 << num_bits) - 1) << bit_offset;
                            let pointer: *mut u16 = mem::transmute(buffer[field.offset..].as_ptr());
                            *pointer =
                                (*pointer & !mask) | (((case_tag as u16) << bit_offset) & mask);
                        },
                        types::Primitive::U32 => unsafe {
                            let mask = ((1u32 << num_bits) - 1) << bit_offset;
                            let pointer: *mut u32 = mem::transmute(buffer[field.offset..].as_ptr());
                            *pointer =
                                (*pointer & !mask) | (((case_tag as u32) << bit_offset) & mask);
                        },
                        types::Primitive::U64 => unsafe {
                            let mask = ((1u64 << num_bits) - 1) << bit_offset;
                            let pointer: *mut u64 = mem::transmute(buffer[field.offset..].as_ptr());
                            *pointer =
                                (*pointer & !mask) | (((case_tag as u64) << bit_offset) & mask);
                        },
                        types::Primitive::U128 => unsafe {
                            let mask = ((1u128 << num_bits) - 1) << bit_offset;
                            let pointer: *mut u128 =
                                mem::transmute(buffer[field.offset..].as_ptr());
                            *pointer =
                                (*pointer & !mask) | (((case_tag as u128) << bit_offset) & mask);
                        },
                        _ => panic!("Invalid bitfield primitive type"),
                    }
                }
                FixedSizeFieldKind::Primitve(primitive) => match primitive {
                    types::Primitive::Bool => unsafe {
                        *mem::transmute::<*mut u8, *mut bool>(
                            buffer[field.offset..].as_mut_ptr(),
                        ) = source.get_bool(&field.path);
                    },
                    types::Primitive::Char => unsafe {
                        *mem::transmute::<*mut u8, *mut char>(
                            buffer[field.offset..].as_mut_ptr(),
                        ) = source.get_char(&field.path);
                    },
                    types::Primitive::F32 => unsafe {
                        *mem::transmute::<*mut u8, *mut f32>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_f32(&field.path);
                    },
                    types::Primitive::F64 => unsafe {
                        *mem::transmute::<*mut u8, *mut f64>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_f64(&field.path);
                    },
                    types::Primitive::U8 => unsafe {
                        *mem::transmute::<*mut u8, *mut u8>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_u8(&field.path);
                    },
                    types::Primitive::U16 => unsafe {
                        *mem::transmute::<*mut u8, *mut u16>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_u16(&field.path);
                    },
                    types::Primitive::U32 => unsafe {
                        *mem::transmute::<*mut u8, *mut u32>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_u32(&field.path);
                    },
                    types::Primitive::U64 => unsafe {
                        *mem::transmute::<*mut u8, *mut u64>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_u64(&field.path);
                    },
                    types::Primitive::U128 => unsafe {
                        *mem::transmute::<*mut u8, *mut u128>(
                            buffer[field.offset..].as_mut_ptr(),
                        ) = source.get_u128(&field.path);
                    },
                    types::Primitive::I8 => unsafe {
                        *mem::transmute::<*mut u8, *mut i8>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_i8(&field.path);
                    },
                    types::Primitive::I16 => unsafe {
                        *mem::transmute::<*mut u8, *mut i16>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_i16(&field.path);
                    },
                    types::Primitive::I32 => unsafe {
                        *mem::transmute::<*mut u8, *mut i32>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_i32(&field.path);
                    },
                    types::Primitive::I64 => unsafe {
                        *mem::transmute::<*mut u8, *mut i64>(buffer[field.offset..].as_mut_ptr()) =
                            source.get_i64(&field.path);
                    },
                    types::Primitive::I128 => unsafe {
                        *mem::transmute::<*mut u8, *mut i128>(
                            buffer[field.offset..].as_mut_ptr(),
                        ) = source.get_i128(&field.path);
                    },
                },
                _ => panic!("Invalid field type"),
            }
        }

        for field in &self.var_size_fields {
            match &field.kind {
                VariableSizeFieldKind::String => {
                    let length = source.get_length(&field.path);

                    copy_bytes::<D>(&mut buffer[offset..], &field.path, length, source);

                    offset += length;
                }
                VariableSizeFieldKind::Enum { ref cases } => {
                    let case_tag = source.get_case_tag(&field.path);
                    cases[case_tag].encode_with_source(&mut buffer[offset..], source);
                    offset += cases[case_tag].encoding_size_with_source(source);
                }
            }
        }

        util::align(offset, self.alignment)
    }
}

fn copy_bytes<D>(buffer: &mut [u8], path: &Path, length: usize, source: &D)
where
    D: data_source::DataSource,
{
    unsafe {
        let data: &[u8] = source.get_bytes(path);
        assert!(length == data.len());
        let base: *mut u8 = mem::transmute(buffer.as_mut_ptr());
        let target = slice::from_raw_parts_mut(base, length);
        target.copy_from_slice(data);
    }
}

pub struct Marshaller {
    layout: sync::Arc<Layout>,
}

impl Marshaller {
    pub fn new(layout: sync::Arc<Layout>) -> Marshaller {
        Marshaller { layout }
    }

    pub fn encoding_size<D>(&self, source: &D) -> usize
    where
        D: data_source::DataSource,
    {
        self.layout.encoding_size_with_source(source)
    }

    pub fn encode<D>(&self, source: &D, buffer: &mut [u8]) -> usize
    where
        D: data_source::DataSource,
    {
        self.layout.encode_with_source(buffer, source)
    }

    pub fn get_primitive<T>(&self, mut data: &[u8], path: &Path) -> T
    where
        T: Copy,
        T: types::IsPrimitive,
    {
        let primitive = T::tag();
        let mut layout: &Layout = self.layout.as_ref();

        loop {
            match layout
                .field_indices
                .binary_search_by(|probe| probe.path.cmp(path).reverse())
            {
                Ok(idx) => {
                    let field =
                        &layout.fixed_size_fields[layout.field_indices[idx].fixed_size_field_index];
                    match field.kind {
                        FixedSizeFieldKind::Primitve(prim) if prim == primitive => unsafe {
                            let pointer: *const T = mem::transmute(data[field.offset..].as_ptr());
                            return *pointer;
                        },
                        _ => panic!("Incorrect field type"),
                    }
                }
                Err(idx)
                    if idx < layout.field_indices.len()
                        && layout.field_indices[idx].path.is_prefix_of(path) =>
                {
                    let var_size_field_index = layout.field_indices[idx].var_size_field_index;
                    let case_tag = path.path()[layout.field_indices[idx].path.path().len()];

                    let offset = layout.variable_field_offset(var_size_field_index, data);

                    // recurse
                    data = &data[offset..];

                    match &layout.var_size_fields[var_size_field_index].kind {
                        VariableSizeFieldKind::Enum { ref cases } => layout = &cases[case_tag],
                        _ => panic!("Expecting an enum field"),
                    }
                }
                _ => panic!("Field path not found"),
            }
        }
    }

    pub fn get_bytes<'a>(&self, mut data: &'a [u8], path: &Path) -> &'a [u8] {
        let mut layout: &Layout = self.layout.as_ref();

        loop {
            match layout
                .field_indices
                .binary_search_by(|probe| probe.path.cmp(path).reverse())
            {
                Ok(idx) => {
                    let field =
                        &layout.fixed_size_fields[layout.field_indices[idx].fixed_size_field_index];
                    let var_size_field_index = layout.field_indices[idx].var_size_field_index;
                    let offset = layout.variable_field_offset(var_size_field_index, data);

                    let length = match field.kind {
                        FixedSizeFieldKind::Length => unsafe {
                            let pointer: *const LengthEncodingType =
                                mem::transmute(data[field.offset..].as_ptr());
                            *pointer as usize
                        },
                        _ => panic!("Incorrect field type"),
                    };

                    // need to create a slice of the appropriate type with the appropriat enumber
                    // of elements
                    match &layout.var_size_fields[var_size_field_index].kind {
                        VariableSizeFieldKind::String => unsafe {
                            let base = data[offset..].as_ptr();
                            return slice::from_raw_parts(mem::transmute(base), length);
                        },
                        _ => panic!("Incorrect field type"),
                    }
                }
                Err(idx)
                    if idx < layout.field_indices.len()
                        && layout.field_indices[idx].path.is_prefix_of(path) =>
                {
                    let var_size_field_index = layout.field_indices[idx].var_size_field_index;
                    let case_tag = path.path()[layout.field_indices[idx].path.path().len()];

                    let offset = layout.variable_field_offset(var_size_field_index, data);

                    // recurse
                    data = &data[offset..];

                    match &layout.var_size_fields[var_size_field_index].kind {
                        VariableSizeFieldKind::Enum { ref cases } => layout = &cases[case_tag],
                        _ => panic!("Expecting an enum field"),
                    }
                }
                _ => panic!("Field path not found"),
            }
        }
    }

    /// Retrieve the enum case value of the data structure element identified
    /// by path
    pub fn get_case_tag(&self, mut data: &[u8], path: &Path) -> usize {
        let mut layout: &Layout = self.layout.as_ref();

        loop {
            match layout
                .field_indices
                .binary_search_by(|probe| probe.path.cmp(path).reverse())
            {
                Ok(idx) => {
                    let tag_field =
                        &layout.fixed_size_fields[layout.field_indices[idx].fixed_size_field_index];
                    match &tag_field.kind {
                        FixedSizeFieldKind::Bitfield {
                            primitive,
                            bit_offset,
                            num_bits,
                        } => {
                            let case_value = match primitive {
                                types::Primitive::U128 => unsafe {
                                    let pointer: *const u128 =
                                        mem::transmute(data[tag_field.offset..].as_ptr());
                                    *pointer as usize
                                },
                                types::Primitive::U64 => unsafe {
                                    let pointer: *const u64 =
                                        mem::transmute(data[tag_field.offset..].as_ptr());
                                    *pointer as usize
                                },
                                types::Primitive::U32 => unsafe {
                                    let pointer: *const u32 =
                                        mem::transmute(data[tag_field.offset..].as_ptr());
                                    *pointer as usize
                                },
                                types::Primitive::U16 => unsafe {
                                    let pointer: *const u16 =
                                        mem::transmute(data[tag_field.offset..].as_ptr());
                                    *pointer as usize
                                },
                                types::Primitive::U8 => unsafe {
                                    let pointer: *const u8 =
                                        mem::transmute(data[tag_field.offset..].as_ptr());
                                    *pointer as usize
                                },
                                _ => panic!("Unexpected primitive for storing bitfields"),
                            };

                            return (case_value >> bit_offset) & ((1usize << num_bits) - 1);
                        }
                        _ => panic!("Bitfiled expected as enum case tag value"),
                    }
                }
                Err(idx)
                    if idx < layout.field_indices.len()
                        && layout.field_indices[idx].path.is_prefix_of(path) =>
                {
                    let var_size_field_index = layout.field_indices[idx].var_size_field_index;
                    let case_tag = path.path()[layout.field_indices[idx].path.path().len()];

                    let offset = layout.variable_field_offset(var_size_field_index, data);

                    // recurse
                    data = &data[offset..];

                    match &layout.var_size_fields[var_size_field_index].kind {
                        VariableSizeFieldKind::Enum { ref cases } => layout = &cases[case_tag],
                        _ => panic!("Expecting an enum field"),
                    }
                }
                _ => panic!("Field path not found"),
            }
        }
    }

    /// Retrieve the array length value of the data structure element
    /// identified by path
    pub fn get_length(&self, mut data: &[u8], path: &Path) -> usize {
        let mut layout: &Layout = self.layout.as_ref();

        loop {
            match layout
                .field_indices
                .binary_search_by(|probe| probe.path.cmp(path).reverse())
            {
                Ok(idx) => {
                    let field =
                        &layout.fixed_size_fields[layout.field_indices[idx].fixed_size_field_index];
                    match field.kind {
                        FixedSizeFieldKind::Length => unsafe {
                            let pointer: *const LengthEncodingType =
                                mem::transmute(data[field.offset..].as_ptr());
                            return *pointer as usize;
                        },
                        _ => panic!("Incorrect field type"),
                    }
                }
                Err(idx)
                    if idx < layout.field_indices.len()
                        && layout.field_indices[idx].path.is_prefix_of(path) =>
                {
                    let var_size_field_index = layout.field_indices[idx].var_size_field_index;
                    let case_tag = path.path()[layout.field_indices[idx].path.path().len()];

                    let offset = layout.variable_field_offset(var_size_field_index, data);

                    // recurse
                    data = &data[offset..];

                    match &layout.var_size_fields[var_size_field_index].kind {
                        VariableSizeFieldKind::Enum { ref cases } => layout = &cases[case_tag],
                        _ => panic!("Expecting an enum field"),
                    }
                }
                _ => panic!("Field path not found"),
            }
        }
    }
}

pub struct MarshallerDataSource<'a, 'b> {
    marshaller: &'a Marshaller,
    data: &'b [u8],
}

impl<'a, 'b> MarshallerDataSource<'a, 'b> {
    pub fn new(marshaller: &'a Marshaller, data: &'b [u8]) -> MarshallerDataSource<'a, 'b> {
        MarshallerDataSource { marshaller, data }
    }
}

macro_rules! marshaller_data_source_primitive {
    ($name:ident, $typ:ident) => {
        fn $name(&self, path: &Path) -> $typ {
            self.marshaller.get_primitive(self.data, path)
        }
    };
}

impl<'a, 'b> data_source::DataSource for MarshallerDataSource<'a, 'b> {
    marshaller_data_source_primitive!(get_bool, bool);
    marshaller_data_source_primitive!(get_char, char);
    marshaller_data_source_primitive!(get_f32, f32);
    marshaller_data_source_primitive!(get_f64, f64);
    marshaller_data_source_primitive!(get_u8, u8);
    marshaller_data_source_primitive!(get_u16, u16);
    marshaller_data_source_primitive!(get_u32, u32);
    marshaller_data_source_primitive!(get_u64, u64);
    marshaller_data_source_primitive!(get_u128, u128);
    marshaller_data_source_primitive!(get_i8, i8);
    marshaller_data_source_primitive!(get_i16, i16);
    marshaller_data_source_primitive!(get_i32, i32);
    marshaller_data_source_primitive!(get_i64, i64);
    marshaller_data_source_primitive!(get_i128, i128);

    /// Retrieve the enum case value of the data structure element identified
    /// by path
    fn get_case_tag(&self, path: &Path) -> usize {
        self.marshaller.get_case_tag(self.data, path)
    }

    /// Retrieve the array length value of the data structure element
    /// identified by path
    fn get_length(&self, path: &Path) -> usize {
        self.marshaller.get_length(self.data, path)
    }

    fn get_bytes<'c>(&'c self, path: &Path) -> &'c [u8] {
        self.marshaller.get_bytes(self.data, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::data_source::DataSource;
    use crate::core::value;

    fn value_at_offset<T: Copy>(buffer: &[u8], offset: usize) -> T {
        unsafe {
            let pointer: *const T = mem::transmute(buffer[offset..].as_ptr());
            *pointer
        }
    }

    #[test]
    fn test_struct_layout() {
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

        let layout = Layout::new(&typ);
        let alignment = layout.alignment;
        assert_eq!(layout.alignment, mem::align_of::<u32>());
        assert_eq!(layout.var_field_offset, mem::size_of::<u32>() * 5);
        assert_eq!(layout.var_size_fields.len(), 4);
        assert_eq!(layout.fixed_size_fields.len(), 5);

        assert_eq!(layout.fixed_size_fields[0].path, first);
        assert_eq!(layout.fixed_size_fields[0].offset, 0);
        assert_eq!(layout.fixed_size_fields[0].kind, FixedSizeFieldKind::Length);
        assert_eq!(layout.fixed_size_fields[1].path, last);
        assert_eq!(layout.fixed_size_fields[1].offset, mem::size_of::<u32>());
        assert_eq!(layout.fixed_size_fields[1].kind, FixedSizeFieldKind::Length);
        assert_eq!(layout.fixed_size_fields[2].path, address);
        assert_eq!(
            layout.fixed_size_fields[2].offset,
            mem::size_of::<u32>() * 2
        );
        assert_eq!(layout.fixed_size_fields[2].kind, FixedSizeFieldKind::Length);
        assert_eq!(layout.fixed_size_fields[3].path, city_name);
        assert_eq!(
            layout.fixed_size_fields[3].offset,
            mem::size_of::<u32>() * 3
        );
        assert_eq!(layout.fixed_size_fields[3].kind, FixedSizeFieldKind::Length);
        assert_eq!(layout.fixed_size_fields[4].path, city_zip);
        assert_eq!(
            layout.fixed_size_fields[4].offset,
            mem::size_of::<u32>() * 4
        );
        assert_eq!(
            layout.fixed_size_fields[4].kind,
            FixedSizeFieldKind::Primitve(types::Primitive::U32)
        );

        assert_eq!(layout.var_size_fields[0].path, first);
        assert_eq!(layout.var_size_fields[0].fixed_field_index, 0);
        assert_eq!(
            layout.var_size_fields[0].kind,
            VariableSizeFieldKind::String
        );
        assert_eq!(layout.var_size_fields[1].path, last);
        assert_eq!(layout.var_size_fields[1].fixed_field_index, 1);
        assert_eq!(
            layout.var_size_fields[1].kind,
            VariableSizeFieldKind::String
        );
        assert_eq!(layout.var_size_fields[2].path, address);
        assert_eq!(layout.var_size_fields[2].fixed_field_index, 2);
        assert_eq!(
            layout.var_size_fields[2].kind,
            VariableSizeFieldKind::String
        );
        assert_eq!(layout.var_size_fields[3].path, city_name);
        assert_eq!(layout.var_size_fields[3].fixed_field_index, 3);
        assert_eq!(
            layout.var_size_fields[3].kind,
            VariableSizeFieldKind::String
        );

        let mut ds = data_source::IntermediateDataSource::new();
        ds.add_string(&first, "John");
        ds.add_string(&last, "Doe");
        ds.add_string(&address, "123 Main Street");
        ds.add_string(&city_name, "Anywhere");
        ds.add_primitive(&city_zip, value::PrimitiveValue::U32(12345u32));

        let marshaller = Marshaller::new(sync::Arc::new(layout));
        let encoding_size = marshaller.encoding_size(&ds);

        assert_eq!(
            encoding_size,
            util::align(
                mem::size_of::<u32>() * 5
                    + "John".len()
                    + "Doe".len()
                    + "123 Main Street".len()
                    + "Anywhere".len(),
                alignment
            )
        );

        let mut buffer: Vec<u8> = Vec::with_capacity(encoding_size);
        buffer.resize(encoding_size, 0u8);

        assert_eq!(marshaller.encode(&ds, &mut buffer), encoding_size);

        assert_eq!(value_at_offset::<u32>(&buffer, 0) as usize, "John".len());
        assert_eq!(value_at_offset::<u32>(&buffer, 4) as usize, "Doe".len());
        assert_eq!(
            value_at_offset::<u32>(&buffer, 8) as usize,
            "123 Main Street".len()
        );
        assert_eq!(
            value_at_offset::<u32>(&buffer, 12) as usize,
            "Anywhere".len()
        );
        assert_eq!(value_at_offset::<u32>(&buffer, 16), 12345);

        assert_eq!(&buffer[20..24], "John".as_bytes());
        assert_eq!(&buffer[24..27], "Doe".as_bytes());
        assert_eq!(&buffer[27..42], "123 Main Street".as_bytes());
        assert_eq!(&buffer[42..50], "Anywhere".as_bytes());

        let mds = MarshallerDataSource::new(&marshaller, &buffer);

        assert_eq!(mds.get_bytes(&first), "John".as_bytes());
        assert_eq!(mds.get_bytes(&last), "Doe".as_bytes());
        assert_eq!(mds.get_bytes(&address), "123 Main Street".as_bytes());
        assert_eq!(mds.get_bytes(&city_name), "Anywhere".as_bytes());
        assert_eq!(mds.get_u32(&city_zip), 12345u32);
    }

    #[test]
    fn test_enum_layout() {
        let parser = types::parser::TypeParser::new();
        let typ = parser
            .parse(
                r#"
            enum {
                Person: struct {
                    first: string,
                    last: string,
                    age: u8
                },
                Address: struct {
                    street: string,
                    city: string,
                    zip: u32,
                    state: string
                },
                Empty: ()
            }
            "#,
            )
            .unwrap();

        let root = types::Path::create_empty();
        let first = types::Path::resolve_qualified_name(&typ, "Person.first").unwrap();
        let last = types::Path::resolve_qualified_name(&typ, "Person.last").unwrap();
        let age = types::Path::resolve_qualified_name(&typ, "Person.age").unwrap();
        let street = types::Path::resolve_qualified_name(&typ, "Address.street").unwrap();
        let city = types::Path::resolve_qualified_name(&typ, "Address.city").unwrap();
        let state = types::Path::resolve_qualified_name(&typ, "Address.state").unwrap();
        let zip = types::Path::resolve_qualified_name(&typ, "Address.zip").unwrap();

        let layout = Layout::new(&typ);
        let alignment = layout.alignment;
        let offset = layout.var_field_offset;

        assert_eq!(alignment, mem::align_of::<u32>());

        assert_eq!(layout.fixed_size_fields.len(), 1);
        assert_eq!(layout.var_size_fields.len(), 1);
        assert_eq!(layout.var_field_offset, mem::size_of::<u32>());

        assert_eq!(layout.fixed_size_fields[0].offset, 0);
        assert_eq!(layout.fixed_size_fields[0].path, root);
        assert_eq!(
            layout.fixed_size_fields[0].kind,
            FixedSizeFieldKind::Bitfield {
                primitive: types::Primitive::U8,
                bit_offset: 0,
                num_bits: 2
            }
        );

        assert_eq!(layout.var_size_fields[0].path, root);
        assert_eq!(layout.var_size_fields[0].fixed_field_index, 0);

        match &layout.var_size_fields[0].kind {
            VariableSizeFieldKind::Enum { ref cases } => {
                assert_eq!(cases.len(), 3);

                assert_eq!(cases[0].alignment, mem::align_of::<u32>());
                assert_eq!(cases[0].fixed_size_fields.len(), 3);
                assert_eq!(cases[0].var_size_fields.len(), 2);
                assert_eq!(
                    cases[0].var_field_offset,
                    mem::size_of::<u32>() * 2 + mem::size_of::<u8>()
                );

                assert_eq!(cases[0].fixed_size_fields[0].path, first);
                assert_eq!(
                    cases[0].fixed_size_fields[0].kind,
                    FixedSizeFieldKind::Length
                );
                assert_eq!(cases[0].fixed_size_fields[1].path, last);
                assert_eq!(
                    cases[0].fixed_size_fields[1].kind,
                    FixedSizeFieldKind::Length
                );
                assert_eq!(cases[0].fixed_size_fields[2].path, age);

                assert_eq!(cases[0].var_size_fields[0].path, first);
                assert_eq!(cases[0].var_size_fields[0].fixed_field_index, 0);
                assert_eq!(cases[0].var_size_fields[1].path, last);
                assert_eq!(cases[0].var_size_fields[1].fixed_field_index, 1);

                assert_eq!(cases[1].alignment, mem::align_of::<u32>());
                assert_eq!(cases[1].fixed_size_fields.len(), 4);
                assert_eq!(cases[1].var_size_fields.len(), 3);
                assert_eq!(
                    cases[1].var_field_offset,
                    mem::size_of::<u32>() * 3 + mem::size_of::<u32>()
                );

                assert_eq!(cases[1].fixed_size_fields[0].path, street);
                assert_eq!(cases[1].fixed_size_fields[1].path, city);
                assert_eq!(cases[1].fixed_size_fields[2].path, zip);
                assert_eq!(cases[1].fixed_size_fields[3].path, state);

                assert_eq!(cases[1].var_size_fields[0].path, street);
                assert_eq!(cases[1].var_size_fields[0].fixed_field_index, 0);
                assert_eq!(cases[1].var_size_fields[1].path, city);
                assert_eq!(cases[1].var_size_fields[1].fixed_field_index, 1);
                assert_eq!(cases[1].var_size_fields[2].path, state);
                assert_eq!(cases[1].var_size_fields[2].fixed_field_index, 3);

                assert_eq!(cases[2].alignment, mem::align_of::<u8>());
                assert_eq!(cases[2].fixed_size_fields.len(), 0);
                assert_eq!(cases[2].var_size_fields.len(), 0);
                assert_eq!(cases[2].var_field_offset, 0);
            }
            _ => assert!(false),
        }

        let mut ds = data_source::IntermediateDataSource::new();
        let mut buffer: Vec<u8> = Vec::with_capacity(2048);
        let marshaller = Marshaller::new(sync::Arc::new(layout));

        // Empty
        {
            ds.add_case_tag(&root, 2);
            let encoding_size = marshaller.encoding_size(&ds);

            assert_eq!(encoding_size, alignment);

            buffer.resize(encoding_size, 0u8);
            assert_eq!(marshaller.encode(&ds, &mut buffer), encoding_size);

            let mds = MarshallerDataSource::new(&marshaller, &buffer);
            assert_eq!(mds.get_case_tag(&root), 2usize);
        }

        // Person
        {
            ds.clear();
            ds.add_case_tag(&root, 0);
            ds.add_string(&first, "Hubert");
            ds.add_string(&last, "Hantel");
            ds.add_primitive(&age, value::PrimitiveValue::U8(46));

            let encoding_size = marshaller.encoding_size(&ds);

            assert_eq!(
                encoding_size,
                util::align(
                    offset
                        + mem::size_of::<u32>() * 2
                        + "Hubert".len()
                        + "Hantel".len()
                        + mem::size_of::<u8>(),
                    alignment
                )
            );

            buffer.resize(encoding_size, 0u8);
            assert_eq!(marshaller.encode(&ds, &mut buffer), encoding_size);

            let mds = MarshallerDataSource::new(&marshaller, &buffer);
            assert_eq!(mds.get_case_tag(&root), 0usize);
            assert_eq!(mds.get_bytes(&first), "Hubert".as_bytes());
            assert_eq!(mds.get_bytes(&last), "Hantel".as_bytes());
            assert_eq!(mds.get_u8(&age), 46u8);
        }

        // Address
        {
            ds.clear();
            ds.add_case_tag(&root, 1);
            ds.add_string(&street, "123 Main Street");
            ds.add_string(&city, "Anywhere");
            ds.add_primitive(&zip, value::PrimitiveValue::U32(12345));
            ds.add_string(&state, "ID");

            let encoding_size = marshaller.encoding_size(&ds);

            assert_eq!(
                encoding_size,
                util::align(
                    offset
                        + mem::size_of::<u32>() * 3
                        + "123 Main Street".len()
                        + "Anywhere".len()
                        + "ID".len()
                        + mem::size_of::<u32>(),
                    alignment
                )
            );

            buffer.resize(encoding_size, 0u8);
            assert_eq!(marshaller.encode(&ds, &mut buffer), encoding_size);

            let mds = MarshallerDataSource::new(&marshaller, &buffer);
            assert_eq!(mds.get_case_tag(&root), 1usize);
            assert_eq!(mds.get_bytes(&street), "123 Main Street".as_bytes());
            assert_eq!(mds.get_bytes(&city), "Anywhere".as_bytes());
            assert_eq!(mds.get_u32(&zip), 12345u32);
            assert_eq!(mds.get_bytes(&state), "ID".as_bytes());
        }
    }

    #[test]
    fn test_multiple_enums() {
        let parser = types::parser::TypeParser::new();
        let typ = parser
            .parse(
                r#"
                struct {
                    fill1: u16,
                    e1: enum {
                        A: (),
                        B: (),
                        C: (),
                        D: (),
                        E: (),
                        F: (),
                        G: (),
                        H: (),
                        I: (),
                        J: (),
                        K: ()
                    },
                    e2: enum {
                        None: (),
                        Some: u64
                    },
                    fill2: f64,
                    fill3: u8,
                    e3: enum {
                        A: (),
                        B: (),
                        C: (),
                        D: (),
                        E: (),
                    },
                    fill4: i128,
                    fill5: i32,
                    e4: enum {
                        A: (),
                        B: (),
                        C: (),
                        D: (),
                        E: (),
                        F: (),
                        G: (),
                        H: (),
                        I: (),
                        J: (),
                    }
                }
                "#,
            )
            .unwrap();

        let fill1 = types::Path::resolve_qualified_name(&typ, "fill1").unwrap();
        let fill2 = types::Path::resolve_qualified_name(&typ, "fill2").unwrap();
        let fill3 = types::Path::resolve_qualified_name(&typ, "fill3").unwrap();
        let fill4 = types::Path::resolve_qualified_name(&typ, "fill4").unwrap();
        let fill5 = types::Path::resolve_qualified_name(&typ, "fill5").unwrap();

        let e1 = types::Path::resolve_qualified_name(&typ, "e1").unwrap();
        let e2 = types::Path::resolve_qualified_name(&typ, "e2").unwrap();
        let e3 = types::Path::resolve_qualified_name(&typ, "e3").unwrap();
        let e4 = types::Path::resolve_qualified_name(&typ, "e4").unwrap();

        let mut ds = data_source::IntermediateDataSource::new();
        let mut buffer: Vec<u8> = Vec::with_capacity(2048);

        let layout = Layout::new(&typ);
        let marshaller = Marshaller::new(sync::Arc::new(layout));

        ds.add_primitive(&fill1, value::PrimitiveValue::U16(64123u16));
        ds.add_primitive(&fill2, value::PrimitiveValue::F64(3.1415926));
        ds.add_primitive(&fill3, value::PrimitiveValue::U8(195u8));
        ds.add_primitive(&fill4, value::PrimitiveValue::I128(-98765234567993635i128));
        ds.add_primitive(&fill5, value::PrimitiveValue::I32(-12345678i32));

        ds.add_case_tag(&e1, 4);
        ds.add_case_tag(&e2, 0);
        ds.add_case_tag(&e3, 3);
        ds.add_case_tag(&e4, 7);

        let encoding_size = marshaller.encoding_size(&ds);

        buffer.resize(encoding_size, 0u8);
        assert_eq!(marshaller.encode(&ds, &mut buffer), encoding_size);

        let mds = MarshallerDataSource::new(&marshaller, &buffer);
        assert_eq!(mds.get_u16(&fill1), 64123u16);
        assert_eq!(mds.get_f64(&fill2), 3.1415926);
        assert_eq!(mds.get_u8(&fill3), 195u8);
        assert_eq!(mds.get_i128(&fill4), -98765234567993635i128);
        assert_eq!(mds.get_i32(&fill5), -12345678i32);

        assert_eq!(mds.get_case_tag(&e1), 4);
        assert_eq!(mds.get_case_tag(&e2), 0);
        assert_eq!(mds.get_case_tag(&e3), 3);
        assert_eq!(mds.get_case_tag(&e4), 7);
    }
}
