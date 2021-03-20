// MIT License
//
// Copyright (c) 2018-2021 Hans-Martin Will
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
use std::ptr;

use crate::core::util;

type Offset = u32;

pub struct PageFormat {
    row_alignment: usize,
}

impl PageFormat {
    pub fn new(row_alignment: usize) -> Self {
        PageFormat { row_alignment }
    }

    pub fn initialize_page(&self, page: &mut [u8]) {
        assert!(mem::size_of::<Offset>() <= page.len());
        assert!(page.len() <= (Offset::max_value()) as usize);
        assert_eq!(util::align(page.len(), self.row_alignment), page.len());

        // 0 rows
        util::set_value::<Offset>(page, 0, 0);
    }

    pub fn num_rows(&self, page: &[u8]) -> usize {
        util::get_value::<Offset>(page, 0) as usize
    }

    fn set_num_rows(&self, page: &mut [u8], num_rows: usize) {
        util::set_value::<Offset>(page, 0, num_rows as Offset)
    }

    fn set_row_offset(&self, page: &mut [u8], index: usize, offset: Offset) {
        util::set_value(page, mem::size_of::<Offset>() * (index + 1), offset)
    }

    fn get_row_offset(&self, page: &[u8], index: usize) -> Offset {
        util::get_value(page, mem::size_of::<Offset>() * (index + 1))
    }

    pub fn get_row<'a>(&self, page: &'a [u8], index: usize) -> &'a [u8] {
        let (offset, padding) = self.row_offset_padding(page, index);
        let next_offset = if index > 0 {
            self.aligned_row_offset(page, index - 1)
        } else {
            page.len()
        };

        &page[offset..next_offset - padding]
    }

    pub fn append_row(&self, page: &mut [u8], row: &[u8]) {
        assert!(row.len() <= self.max_insert_row_size(page));
        let num_rows = self.num_rows(page);
        let aligned_len = util::align(row.len(), self.row_alignment);
        let upper_bound = if num_rows > 0 {
            self.aligned_row_offset(page, num_rows - 1)
        } else {
            page.len()
        };

        let row_offset = upper_bound - aligned_len;
        let padding = aligned_len - row.len();
        let index_value = (row_offset | padding) as Offset;

        {
            let row_buffer = &mut page[row_offset..row_offset + row.len()];
            row_buffer.copy_from_slice(row);
        }

        self.set_row_offset(page, num_rows, index_value);
        self.set_num_rows(page, num_rows + 1);
    }

    pub fn insert_row(&self, page: &mut [u8], index: usize, row: &[u8]) {
        assert!(row.len() <= self.max_insert_row_size(page));
        let num_rows = self.num_rows(page);

        if index == num_rows {
            self.append_row(page, row);
        } else {
            let delta = util::align(row.len(), self.row_alignment);

            let last_offset = self.aligned_row_offset(page, num_rows - 1);
            let mut row_offset = if index > 0 {
                self.aligned_row_offset(page, index - 1)
            } else {
                page.len()
            };

            unsafe {
                let src: *const u8 = mem::transmute(page[last_offset..].as_ptr());
                let dst: *mut u8 = mem::transmute(page[last_offset - delta..].as_ptr());
                ptr::copy::<u8>(src, dst, row_offset - last_offset);
            }

            for row_index in (index..num_rows).rev() {
                let old_offset = self.get_row_offset(page, row_index) as usize;
                self.set_row_offset(page, row_index + 1, (old_offset - delta) as Offset);
            }

            let padding = delta - row.len();
            row_offset -= delta;
            let index_value = (row_offset | padding) as Offset;

            {
                let row_buffer = &mut page[row_offset..row_offset + row.len()];
                row_buffer.copy_from_slice(row);
            }

            self.set_row_offset(page, index, index_value);
            self.set_num_rows(page, num_rows + 1);
        }
    }

    pub fn remove_row(&self, page: &mut [u8], index: usize) {
        let num_rows = self.num_rows(page);
        assert!(index < num_rows);

        if index < num_rows - 1 {
            let row_offset = self.aligned_row_offset(page, index);
            let prev_offset = if index > 0 {
                self.aligned_row_offset(page, index - 1)
            } else {
                page.len()
            };

            let last_offset = self.aligned_row_offset(page, num_rows - 1);

            let delta = prev_offset - row_offset;

            for row_index in index + 1..num_rows {
                let old_offset = self.get_row_offset(page, row_index) as usize;
                self.set_row_offset(page, row_index - 1, (old_offset + delta) as Offset);
            }

            unsafe {
                let src: *const u8 = mem::transmute(page[last_offset..].as_ptr());
                let dst: *mut u8 = mem::transmute(page[last_offset + delta..].as_ptr());
                ptr::copy::<u8>(src, dst, row_offset - last_offset);
            }
        }

        self.set_num_rows(page, num_rows - 1);
    }

    pub fn max_insert_row_size(&self, page: &[u8]) -> usize {
        let num_rows = self.num_rows(page);
        let lower_bound = util::align(
            mem::size_of::<Offset>() * (num_rows + 2),
            self.row_alignment as usize,
        );
        let upper_bound = if num_rows > 0 {
            self.aligned_row_offset(page, num_rows - 1)
        } else {
            page.len()
        };

        if upper_bound >= lower_bound {
            upper_bound - lower_bound
        } else {
            0
        }
    }

    fn aligned_row_offset(&self, page: &[u8], index: usize) -> usize {
        assert!(index < self.num_rows(page));
        let offset_value = self.get_row_offset(page, index);
        (offset_value & self.alignment_mask()) as usize
    }

    fn row_offset_padding(&self, page: &[u8], index: usize) -> (usize, usize) {
        assert!(index < self.num_rows(page));
        let offset_value = self.get_row_offset(page, index);
        let alignment_mask = self.alignment_mask();
        (
            (offset_value & alignment_mask) as usize,
            (offset_value & !alignment_mask) as usize,
        )
    }

    fn alignment_mask(&self) -> Offset {
        !(self.row_alignment - 1) as Offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_test() {
        const PAGE_SIZE: usize = 512;
        let mut page = [0u8; PAGE_SIZE];
        let format = PageFormat::new(8);

        format.initialize_page(&mut page);

        // this equality holds because we need a row count and and offset value
        assert_eq!(format.num_rows(&page), 0);
        assert_eq!(format.max_insert_row_size(&page), PAGE_SIZE - 8);

        format.append_row(&mut page, "This is the first row to add".as_bytes());
        format.append_row(
            &mut page,
            "Here is a second row to add to the page".as_bytes(),
        );
        format.append_row(&mut page, "And also a third row".as_bytes());
        format.append_row(&mut page, "".as_bytes());
        format.append_row(
            &mut page,
            "And one more row after we added the empty row".as_bytes(),
        );
        assert_eq!(format.num_rows(&page), 5);

        assert_eq!(
            format.get_row(&page, 0),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(
            format.get_row(&page, 1),
            "Here is a second row to add to the page".as_bytes()
        );
        assert_eq!(format.get_row(&page, 2), "And also a third row".as_bytes());
        assert_eq!(format.get_row(&page, 3), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 4),
            "And one more row after we added the empty row".as_bytes()
        );

        while format.max_insert_row_size(&page) >= "More row data to add to the page".len() {
            let row_index = format.num_rows(&page);
            format.append_row(&mut page, "More row data to add to the page".as_bytes());
            assert_eq!(
                format.get_row(&page, row_index),
                "More row data to add to the page".as_bytes()
            );
        }

        let remaining_space = format.max_insert_row_size(&page);

        if remaining_space > 0 {
            let last_row_data = &"More row data to add to the page".as_bytes()[..remaining_space];
            let row_index = format.num_rows(&page);
            format.append_row(&mut page, last_row_data);
            assert_eq!(format.get_row(&page, row_index), last_row_data);
        }

        assert_eq!(format.max_insert_row_size(&page), 0);
    }

    #[test]
    fn test_insert_remove() {
        const PAGE_SIZE: usize = 512;
        let mut page = [0u8; PAGE_SIZE];
        let format = PageFormat::new(8);

        format.initialize_page(&mut page);

        // this equality holds because we need a row count and and offset value
        assert_eq!(format.num_rows(&page), 0);
        assert_eq!(format.max_insert_row_size(&page), PAGE_SIZE - 8);

        format.append_row(&mut page, "This is the first row to add".as_bytes());
        format.append_row(
            &mut page,
            "Here is a second row to add to the page".as_bytes(),
        );
        format.append_row(&mut page, "And also a third row".as_bytes());
        format.append_row(&mut page, "".as_bytes());
        format.append_row(
            &mut page,
            "And one more row after we added the empty row".as_bytes(),
        );
        assert_eq!(format.num_rows(&page), 5);

        assert_eq!(
            format.get_row(&page, 0),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(
            format.get_row(&page, 1),
            "Here is a second row to add to the page".as_bytes()
        );
        assert_eq!(format.get_row(&page, 2), "And also a third row".as_bytes());
        assert_eq!(format.get_row(&page, 3), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 4),
            "And one more row after we added the empty row".as_bytes()
        );

        format.remove_row(&mut page, 1);
        assert_eq!(format.num_rows(&page), 4);
        assert_eq!(
            format.get_row(&page, 0),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(format.get_row(&page, 1), "And also a third row".as_bytes());
        assert_eq!(format.get_row(&page, 2), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 3),
            "And one more row after we added the empty row".as_bytes()
        );

        format.insert_row(
            &mut page,
            0,
            "Inserting a new first row in front of everything".as_bytes(),
        );
        assert_eq!(format.num_rows(&page), 5);
        assert_eq!(
            format.get_row(&page, 0),
            "Inserting a new first row in front of everything".as_bytes()
        );
        assert_eq!(
            format.get_row(&page, 1),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(format.get_row(&page, 2), "And also a third row".as_bytes());
        assert_eq!(format.get_row(&page, 3), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 4),
            "And one more row after we added the empty row".as_bytes()
        );

        format.insert_row(
            &mut page,
            3,
            "Inserting another row in the middle".as_bytes(),
        );
        assert_eq!(format.num_rows(&page), 6);
        assert_eq!(
            format.get_row(&page, 0),
            "Inserting a new first row in front of everything".as_bytes()
        );
        assert_eq!(
            format.get_row(&page, 1),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(format.get_row(&page, 2), "And also a third row".as_bytes());
        assert_eq!(
            format.get_row(&page, 3),
            "Inserting another row in the middle".as_bytes()
        );
        assert_eq!(format.get_row(&page, 4), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 5),
            "And one more row after we added the empty row".as_bytes()
        );

        format.insert_row(&mut page, 6, "And a new last row".as_bytes());
        assert_eq!(format.num_rows(&page), 7);
        assert_eq!(
            format.get_row(&page, 5),
            "And one more row after we added the empty row".as_bytes()
        );
        assert_eq!(format.get_row(&page, 6), "And a new last row".as_bytes());

        format.remove_row(&mut page, 0);
        assert_eq!(format.num_rows(&page), 6);
        assert_eq!(
            format.get_row(&page, 0),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(format.get_row(&page, 1), "And also a third row".as_bytes());
        assert_eq!(
            format.get_row(&page, 2),
            "Inserting another row in the middle".as_bytes()
        );
        assert_eq!(format.get_row(&page, 3), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 4),
            "And one more row after we added the empty row".as_bytes()
        );
        assert_eq!(format.get_row(&page, 5), "And a new last row".as_bytes());

        format.remove_row(&mut page, 5);
        assert_eq!(format.num_rows(&page), 5);
        assert_eq!(
            format.get_row(&page, 0),
            "This is the first row to add".as_bytes()
        );
        assert_eq!(format.get_row(&page, 1), "And also a third row".as_bytes());
        assert_eq!(
            format.get_row(&page, 2),
            "Inserting another row in the middle".as_bytes()
        );
        assert_eq!(format.get_row(&page, 3), "".as_bytes());
        assert_eq!(
            format.get_row(&page, 4),
            "And one more row after we added the empty row".as_bytes()
        );

        format.remove_row(&mut page, 2);
        format.remove_row(&mut page, 1);
        format.remove_row(&mut page, 0);
        format.remove_row(&mut page, 0);
        assert_eq!(format.num_rows(&page), 1);
        assert_eq!(
            format.get_row(&page, 0),
            "And one more row after we added the empty row".as_bytes()
        );

        format.remove_row(&mut page, 0);
        assert_eq!(format.num_rows(&page), 0);
    }
}
