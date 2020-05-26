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

use std::mem;
use std::usize;
use std::convert;
use std::error;
use std::fmt;
use std::io;
use std::result;

/// Error result value used in this project
#[derive(Debug)]
pub struct Error {
    message: String,
    cause: Option<Box<dyn error::Error>>
}

impl Error {
    pub fn new(message: &str) -> Error {
        Error {
            message: String::from(message),
            cause: None
        }
    }

    pub fn from_error(err: &dyn error::Error) -> Error {
        Error {
            message: String::from(err.to_string()),
            cause: None
        }
    }
}

impl convert::From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Self {
            message: String::from(err.to_string()),
            cause: Some(Box::new(err))
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        unimplemented!()
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        unimplemented!()
    }
}

pub type Result<T> = result::Result<T, Error>;

/// Helper function to calculate alignments
pub fn align(value: usize, alignment: usize) -> usize {
    assert!(alignment.is_power_of_two());
    (value + alignment - 1) & !(alignment - 1)
}

#[test]
fn test_align() {
    assert_eq!(align(0, 1usize), 0);
    assert_eq!(align(1, 1usize), 1);
    assert_eq!(align(2, 1usize), 2);
    assert_eq!(align(3, 1usize), 3);
    assert_eq!(align(4, 1usize), 4);
    assert_eq!(align(5, 1usize), 5);
    assert_eq!(align(158273, 1usize), 158273);

    assert_eq!(align(1, 2usize), 2);
    assert_eq!(align(2, 2usize), 2);
    assert_eq!(align(3, 2usize), 4);
    assert_eq!(align(4, 2usize), 4);
    assert_eq!(align(5, 2usize), 6);
    assert_eq!(align(158273, 2usize), 158274);

    assert_eq!(align(1, 4usize), 4);
    assert_eq!(align(2, 4usize), 4);
    assert_eq!(align(3, 4usize), 4);
    assert_eq!(align(4, 4usize), 4);
    assert_eq!(align(5, 4usize), 8);
    assert_eq!(align(158273, 4usize), 158276);

    assert_eq!(align(1, 16usize), 16);
    assert_eq!(align(2, 16usize), 16);
    assert_eq!(align(3, 16usize), 16);
    assert_eq!(align(4, 16usize), 16);
    assert_eq!(align(5, 16usize), 16);
    assert_eq!(align(15, 16usize), 16);
    assert_eq!(align(16, 16usize), 16);
    assert_eq!(align(17, 16usize), 32);
    assert_eq!(align(158273, 16usize), 158288);
}

/// Determine the minimum number of bits to represent a given integer
pub fn required_bits(value: usize) -> usize {
    if value == 0 {
        0
    } else {
        let leading_zeros = value.leading_zeros() as usize;
        let total_bits = mem::size_of_val(&value) * 8;
        total_bits - leading_zeros
    }
}

#[test]
fn test_required_bits() {
    use std::usize;

    assert_eq!(required_bits(0), 0);
    assert_eq!(required_bits(1), 1);
    assert_eq!(required_bits(2), 2);
    assert_eq!(required_bits(3), 2);
    assert_eq!(required_bits(4), 3);
    assert_eq!(required_bits(5), 3);
    assert_eq!(required_bits(32767), 15);
    assert_eq!(required_bits(32768), 16);
    assert_eq!(required_bits(32769), 16);
    assert_eq!(required_bits(usize::max_value()), mem::size_of::<usize>() * 8);
}

pub fn log2(value: usize) -> u32 {
    assert!(value != 0);
    assert!(value.is_power_of_two());

    value.trailing_zeros()
}

pub fn set_value<T: Copy>(buffer: &mut [u8], offset: usize, value: T) {
    assert!(offset + mem::size_of::<T>() <= buffer.len());

    unsafe {
        let pointer: *mut T =
            mem::transmute(buffer[offset..].as_ptr());
        *pointer = value
    }
}

pub fn get_value<T: Copy>(buffer: &[u8], offset: usize) -> T {
    assert!(offset + mem::size_of::<T>() <= buffer.len());

    unsafe {
        let pointer: *const T =
            mem::transmute(buffer[offset..].as_ptr());
        *pointer
    }
}