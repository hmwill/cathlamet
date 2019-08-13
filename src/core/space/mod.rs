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

use super::data_source::DataSource;
use super::types::{Path, Primitive};

type CoordinateType = u128;

/// type used to represent coordinates
pub struct Coordinate(CoordinateType);

/// Type to represent a depth/prefix length of coordinate values
pub struct Depth(u8);

/// A bounding box in indexing space
pub struct Bound {
    upper: Coordinate,
    lower: Coordinate,
}

/// A cell prefix expressed in indexing space
pub struct Prefix {
    prefix: Coordinate,
    depth: Depth,
}

pub enum Dimension {
    Primitives(Vec<(Primitive, Path)>),
    String(Path)
}

/// Definition of a space, which is a product of tuples of types
pub struct Space {
    dimensions: Vec<Dimension>
}

pub struct Encoder {
    space: Space,
    upper: Vec<Coordinate>,
    lower: Vec<Coordinate>
}

const fn div_round_up(dividend: CoordinateType, divisor: CoordinateType) -> CoordinateType {
    (dividend + divisor - 1) / divisor
}

impl Encoder {
    pub fn calculate_bounds<D: DataSource>(&mut self, data: &D) -> Bound {
        let total_bits = std::mem::size_of::<CoordinateType>();
        let bits_per_dimension = div_round_up(total_bits as CoordinateType, 
                                              self.space.dimensions.len() as CoordinateType);

        // populate temp_values based on space specification
        self.populate_values(data);

        // determine morton encoding per dimaension
        unimplemented!("Not implemented yet")
    }

    fn populate_values<D: DataSource>(&mut self, data: &D) {
        unimplemented!("Not implemented yet")
    }
}