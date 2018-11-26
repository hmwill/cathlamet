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

/// Interface specification for a parser handler.
pub trait Handler {
    /// Called when a JSON object begins
    fn begin_object(&mut self);

    /// Called when a JSON object ends
    fn end_object(&mut self);

    /// Called when an object key has been parsed
    fn key(&mut self, key: &str);

    /// Called when a JSON array begins
    fn begin_array(&mut self);

    /// Called when a JSON array ends
    fn end_array(&mut self);

    /// Called for a Boolean value ('true' or 'false')
    fn bool_value(&mut self, value: bool);

    /// Called for a string value
    fn string_value(&mut self, value: String);

    /// Called for a number value
    fn number_value(&mut self, value: f64);

    /// Called for a 'null' literal
    fn null_value(&mut self);
}

pub struct Parser<H> {
    handler: H
}

impl <H> Parser<H> {
    pub fn new(handler: H) -> Parser<H> {
        Parser {
            handler
        }
    }
}