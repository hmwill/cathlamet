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

use std::path::{Path, PathBuf};
use std::primitive::u32;

use super::errors;

// Magic number used to identify a valid volume header block
const VOLUME_HEADER_FORMAT_MAGIC: u32 = 0xCA78;

// Format revision number currently in use
const VOLUME_HEADER_FORMAT_VERSION: u32 = 0x0100;

// We won't allow going below 512 bytes per block
const MIN_VOLUME_BLOCK_SIZE: usize = 512;

// Specify a default block size
const DEFAULT_VOLUME_BLOCK_SIZE: usize = 4096;

// We won't allow going above 256 KiB per block
const MAX_VOLUME_BLOCK_SIZE: usize = 256 * 1024;

// The data structure located in the first block of a storage volume that describes
// the information needed to access the file system
#[derive(Serialize, Deserialize, Debug)]
struct VolumeHeader {
    format_magic: u32,          // file format identifier (VOLUME_HEADER_FORMAT_MAGIC)
    format_version: u32,        // file format revision (VOLUME_HEADER_FORMAT_VERSION)

    uuid: uuid::Bytes,          // identifier for this volume file

    volume_size: u64,           // total size of storage volume
    file_table_offset: u64,     // offset of the file table, from which all other information can
                                // be found
    log_block_size: u16,        // log_2(volume block size)
    log_file_header_size: u16,  // log_2(file header size); less or equal to log_block_size
}

// Index of the file table file header within the file table itself
const FILE_TABLE_INDEX_FILE_TABLE: usize = 0;

// Index of the file table mirror file header within the file table
const FILE_TABLE_INDEX_FILE_TABLE_MIRROR: usize = 1;

// Index of the block allocation bitmap file header in the file table
const FILE_TABLE_INDEX_ALLOCATION_BITMAP: usize = 2;

// Index of the journal file header in the file table
const FILE_TABLE_INDEX_JOURNAL: usize = 3;

// Index of the volume header block seen as entry in the file table
const FILE_TABLE_INDEX_VOLUME_HEADER: usize = 4;

// Number of reserved entries in the file header table; this is the index of the first user file
const FILE_TABLE_NUM_RESERVED: usize = 16;

// The file header data structure describes an entry in the file table
#[derive(Serialize, Deserialize, Debug)]
struct FileHeader {
    file_header_type: u32,      // discriminator value for this file table entry
    version: u32,               // a version number that is incremented each time the entry is
                                // updated. Used for recovery.
    timestamp_created: u64,     // time of creation, 48 bits for seconds, 16 bits for subsecond
    timestamp_modified: u64,    // time of last modofication, 48 bits for seconds, 16 bit subsecond
    blocks_allocated: u32,      // number of number of allocated disk blocks; this will also 
                                // determine the level of indirection
    blocks_used: u32,           // number of blocks actually in use, always less than or equal
                                // to the amount provided by allocation
    last_block_use: u32,        // number of bytes used in the last block
    filename_length: u8,        // length of the file name, in bytes, followed by the UTF-8 
                                // encoded file name byte sequence
}

// Size of a file header record
const DEFAULT_FILE_HEADER_RECORD_SIZE: usize = 1024;

// Minimum size of a file header record; because we allow up to 255 bytes for the file name
const MIN_FILE_HEADER_RECORD_SIZE: usize = 512;

// Maximum length of a valid file name
const MAX_FILE_NAME_LENGTH: usize = 0xff;

// File header type associated with an unused entry
const FILE_HEADER_TYPE_UNUSED: u16 = 0;

// File header type associated with a regular file
const FILE_HEADER_TYPE_FILE: u16 = 1;

/// Representation of a mounted disk volume
pub struct Volume {
    // location of the volume file on disk
    path: PathBuf,

    // the file handle used to access the mounted volume file
    // It is set to None after unmounting the volume
    file: Option<async_std::fs::File>,

    // a copy of the deserialized volume header
    header: VolumeHeader
}

pub struct VolumeParameters {
    volume_size: u64,           // total size of storage volume
    log_block_size: u16,        // log_2(volume block size)
    log_file_header_size: u16,  // log_2(file header size); less or equal to log_block_size
}

impl VolumeParameters {
    pub fn new_with_defaults(volume_size: u64) -> VolumeParameters {
        VolumeParameters {
            volume_size,
            log_block_size: 
                (32 - (DEFAULT_VOLUME_BLOCK_SIZE as u32).leading_zeros()) as u16,
            log_file_header_size: 
                (32 - (DEFAULT_FILE_HEADER_RECORD_SIZE as u32).leading_zeros()) as u16,
        }
    }
}

// How we represent volume references internally within location descriptors used by buffer pool
// pages
type VolumeIndexRepresentation = u8;

// Wrapped type 
#[derive(Debug, Copy, Clone)]
struct VolumeIndex(VolumeIndexRepresentation);

// Type to represent locations in storage; internally, this is a concatenation of a volume index
// and a storage address
type LocationRepresentation = u64;

// Logical representation of locations
#[derive(Debug, Copy, Clone, Ord, Eq, PartialEq, PartialOrd)]
struct Location(LocationRepresentation);

const LOCATION_REPRESENTATION_BITS: u32 = (0 as LocationRepresentation).count_zeros();
const LOCATION_INDEX_BITS: u32 = (0 as VolumeIndexRepresentation).count_zeros();
const LOCATION_SHIFT_BITS: u32 = LOCATION_REPRESENTATION_BITS - LOCATION_INDEX_BITS;
const LOCATION_OFFSET_MASK: u64 = (1u64 << LOCATION_SHIFT_BITS) - 1;

impl Location {
    fn new(VolumeIndex(index): VolumeIndex, offset: u64) -> Location {
        let maskedOffset = offset & LOCATION_OFFSET_MASK;
        assert!(maskedOffset == offset);
        let representation = ((index as u64) << LOCATION_SHIFT_BITS) | maskedOffset;
        Location(representation)
    }

    fn index(&self) -> VolumeIndex {
        let index = (self.0 >> LOCATION_SHIFT_BITS) as VolumeIndexRepresentation;
        VolumeIndex(index)
    }

    fn offset(&self) -> u64 {
        (self.0 & LOCATION_OFFSET_MASK) as u64
    }
} 

/// The volume manager is a mapping of all currently mounted storage volumes
pub struct VolumeManager {
    // references to all the currently mounted volumes; entries may be None if we have
    // unmounted a volume before but have not re-used the index yet.
    volumes: Vec<Option<Volume>>,

    // the index/short identifier for the next volume to mount
    next_volume: VolumeIndex,

    // number of currently mounted volumes
    num_mounted_volumes: usize
}

impl VolumeManager {
    fn new(buffer_pool: &BufferPool) -> VolumeManager {
        VolumeManager {
            volumes: Vec::with_capacity(10),
            next_volume: VolumeIndex(0),
            num_mounted_volumes: 0
        }
    }

    // generate an internal identifier for the volume to create or attach
    fn next_volume(&mut self) -> errors::Result<VolumeIndex> {
        let current = self.next_volume;
        let next = current.0 + 1;

        if next < current.0 {
            Err("Cannot allocate more internal volume identifiers".into())
        } else {
            self.next_volume = VolumeIndex(next);
            Ok(current)
        }
    }

    // Create a new volume file using the provided volume parameters
    async fn create(&mut self, path: &Path, parameters: &VolumeParameters) -> std::io::Result<VolumeIndex> {
        unimplemented!()
    }

    // Mount a previously created volume file
    async fn mount(&mut self, path: &Path) -> std::io::Result<VolumeIndex> {
        unimplemented!()
    }

    // Unmount the volume and close the associated file handle
    async fn unmount(&mut self, index: VolumeIndex) -> std::io::Result<()> {
        unimplemented!()
    }

    // Unmount and close all mounted volumes
    async fn unmount_all(&mut self) -> std::io::Result<()> {
        unimplemented!()
    }
}

impl Drop for VolumeManager {
    fn drop(&mut self) {
        if self.num_mounted_volumes != 0 {
            panic!("Improper shutdown of volume manager; still have mounted volumes")
        }
    }
}

type BufferIndex = u32;

const NIL_BUFFER_INDEX: BufferIndex = BufferIndex::MAX;

struct BufferListLink {
    // pointers to previous and next elements in a double-linked list
    prev: BufferIndex, 
    next: BufferIndex,
}

impl Default for BufferListLink {
    fn default() -> BufferListLink {
        BufferListLink {
            prev: NIL_BUFFER_INDEX,
            next: NIL_BUFFER_INDEX,
        }
    }
}

struct BufferListHead {
    // pointers to previous and next elements in a double-linked list
    head: BufferIndex, 
    tail: BufferIndex,
}

impl Default for BufferListHead {
    fn default() -> BufferListHead {
        BufferListHead {
            head: NIL_BUFFER_INDEX,
            tail: NIL_BUFFER_INDEX,
        }
    }
}

type BufferLock = Box<async_std::sync::RwLock<()>>;

// State associated with a buffer when there are access handles referring to it
struct BufferAccess {
    // associated lock when the buffer is mapped
    lock: BufferLock,

    // number of handle references
    num_handles: u32,
}

// State associated with a single buffer
struct Buffer {
    link: BufferListLink,

    // external storage location associated with this buffer
    location: Location,

    // information and state if there are access handles
    access: Option<BufferAccess>,

    // is the buffer currently mapped to a location
    mapped: bool,

    // is the buffer pinned, that is prevented from being written back to external storage
    pinned: bool,

    // is the buffer dirty and needs to be written back?
    dirty: bool,

    // has the buffer been submitted to the write-back process
    submitted: bool,
}

impl Buffer {
    fn new() -> Buffer {
        Buffer {
            link: BufferListLink::default(),
            location: Location::new(VolumeIndex(0), 0),
            access: None,
            mapped: false,
            pinned: false,
            dirty: false,
            submitted: false
        }
    }
}

impl Default for Buffer {
    fn default() -> Buffer {
        Buffer::new()
    }
}

// Internal mapping data structures of tbhe buffer pool
struct BufferPoolMapping {
    // buffer descriptors
    buffers: Vec<Buffer>,

    // find buffer based on location
    mapped_locations: std::collections::BTreeMap<Location, usize>,

    // locks that can be used to protect access to individual blocks
    read_write_locks: Vec<BufferLock>,

    // mapped buffers that are currently accessed for reading and/or writing
    in_use_buffers: BufferListHead,

    // dirty buffers; they will need at least one more access to be written back
    // potentially they are currently locked for exclusive use
    dirty_buffers: BufferListHead,

    // available buffers; these are clean buffers that may or may not be mapped to an external
    // location
    available_buffers: BufferListHead,
}

impl BufferPoolMapping {
    fn alloc_lock(locks: &mut Vec<BufferLock>) -> BufferLock {
        match locks.pop() {
            Some(lock) => lock,
            None => Box::new(async_std::sync::RwLock::new(()))
        }
    }

    fn dealloc_lock(locks: &mut Vec<BufferLock>, lock: BufferLock) {
        // TODO: Could come up with a scheme to release locks if they are no longer needed
        // after a peak usage
        locks.push(lock);
    }
}

/// Page cache to map block storage data into main memory.
struct BufferPool {
    // we are using an anonymous mmap to obtain VM page aligned memory
    memory: memmap::MmapMut,

    // the size of a single page in the pool
    buffer_size: usize,

    // mapping data structures, protected by a mutex
    mapping: async_std::sync::Mutex<BufferPoolMapping>,

    // condition variable when buffers become available
    buffer_available: async_std::sync::Condvar,

    // conditin variable when a new buffer has been marked as dirty
    buffer_dirty: async_std::sync::Condvar
}

impl BufferPool {
    fn new(buffer_size: usize, num_buffers: usize) -> std::io::Result<BufferPool> {
        assert!(buffer_size != 0);
        assert!(num_buffers != 0);
        assert!(buffer_size.is_power_of_two());
        assert!(num_buffers < BufferIndex::MAX as usize);

        let memory = memmap::MmapMut::map_anon(buffer_size * num_buffers)?;
        
        let mut buffers = Vec::new();
        buffers.resize_with(num_buffers, Buffer::default);

        // chain all buffers into a single list
        for index in 0 .. num_buffers {
            buffers[index].link.prev = (index - 1) as BufferIndex;
            buffers[index].link.next = (index + 1) as BufferIndex;
        }

        // ensure we fix up first and last elements
        buffers[0].link.prev = NIL_BUFFER_INDEX;
        buffers[num_buffers - 1].link.next = NIL_BUFFER_INDEX;

        // create list header for this chain of buffers
        let unused_buffers = BufferListHead { 
            head: 0,
            tail: (num_buffers - 1) as BufferIndex
        };

        let mut mapped_locations = std::collections::BTreeMap::new();

        let mapping_inner = BufferPoolMapping {
            buffers,
            mapped_locations,
            dirty_buffers: BufferListHead::default(),
            in_use_buffers: BufferListHead::default(),
            available_buffers: BufferListHead::default(),
            read_write_locks: Vec::new(),
        };

        Ok(BufferPool {
            memory,
            buffer_size,
            mapping: async_std::sync::Mutex::new(mapping_inner),
            buffer_available: async_std::sync::Condvar::new(),
            buffer_dirty: async_std::sync::Condvar::new(),
        })
    }

    // return the total amount of allocated memory
    fn total_size(&self) -> usize {
        self.memory.len()
    }

    // Provide read-only access to a buffer 
    fn access_read_only<'a>(&'a self, _location: Location) -> ReadOnlyBufferHandle<'a> {

        ReadOnlyBufferHandle {
            pool: self,
            buffer_index: 0
        }
    }

    // Provide read-write access to a buffer 
    fn access_read_write<'a>(&'a self, _location: Location) -> ReadWriteBufferHandle<'a> {
        unimplemented!()
        // ReadWriteBufferHandle {
        //     pool: self,
        //     buffer_index: 0
        //}
    }

    // Provide read-write access to a buffer without necessarily restoring previous content
    async fn access_initialize<'a>(&'a self, location: Location) -> ReadWriteBufferHandle<'a> {
        let (index, lock_ptr) = {
            let mut guard = self.mapping.lock().await;

            loop {
                let internal = &mut *guard;

                // determine if the location has already been mapped
                match internal.mapped_locations.get(&location) {
                    None => {
                        if internal.available_buffers.tail != NIL_BUFFER_INDEX {
                            let index = internal.available_buffers.tail;
                            let available_buffers = &mut internal.available_buffers;
                            let buffers = &mut internal.buffers;
                            available_buffers.list_remove(buffers, index);

                            let in_use_buffers = &mut internal.available_buffers;
                            in_use_buffers.list_push_front(buffers, index);

                            internal.mapped_locations.insert(location, index as usize);

                            assert!(buffers[index as usize].access.is_none());

                            let read_write_locks = &mut internal.read_write_locks;

                            let lock = BufferPoolMapping::alloc_lock(read_write_locks);
                            let lock_ptr = (&*lock) as *const async_std::sync::RwLock<()>;

                            buffers[index as usize].access = Some(BufferAccess {
                                lock,
                                num_handles: 1
                            });

                            break (index as usize, lock_ptr)
                        } else {
                            guard = self.buffer_available.wait(guard).await;
                            continue;
                        }
                    },
                    Some(&index) => {
                        match &mut internal.buffers[index as usize].access {
                            &mut None => {
                                let read_write_locks = &mut internal.read_write_locks;
                                let lock = BufferPoolMapping::alloc_lock(read_write_locks);
                                let lock_ptr = (&*lock) as *const async_std::sync::RwLock<()>;

                                internal.buffers[index as usize].access = 
                                    Some(BufferAccess {
                                        lock,
                                        num_handles: 1
                                    });
                                
                                break (index, lock_ptr);
                            },
                            &mut Some(ref mut access) => {
                                let lock_ptr = (&*access.lock) as *const async_std::sync::RwLock<()>;
                                let num_handles = access.num_handles;
                                access.num_handles = num_handles + 1;
                                break (index, lock_ptr);
                            }
                        };
                    }
                }
            }
        };

        let guard = unsafe {
            (*lock_ptr).write().await
        };

        ReadWriteBufferHandle {
            pool: self,
            guard,
            buffer_index: index
        }
    }

    fn downgrade<'b, 'a: 'b>(&'a self, handle: ReadOnlyBufferHandle<'b>) ->
        ReadOnlyBufferHandle<'a> 
    {
        // mark as dirty

        ReadOnlyBufferHandle {
            pool: self,
            buffer_index: 0
        }
    }

    // Return a read-only handle
    async fn return_read_only<'b, 'a: 'b>(&'a self, handle: ReadOnlyBufferHandle<'b>) {

        // if this is the last read access, remove from in_use list

        // if the buffer is dirty:
        // move to dirty list

        // if it has not been submitted:
        // signal buffer_dirty

        unimplemented!();

    }

    // Return a read-write handle
    fn return_read_write<'b, 'a: 'b>(&'a self, handle: ReadWriteBufferHandle<'b>) {
        unimplemented!()

        // mark as dirty

        // move from in-use to dirty buffer list

        // signal dirty_buffer

        // Question: how are we working with other tasks that have been waiting to access this 
        // buffer?
    }

    // Unmap a specific location, that is contents are no longer needed, and they should also
    // no longer be maintained in the buffer pool
    fn unmap(&self, location: Location) {
        unimplemented!()
    }

    // need a background task to write dirty buffers back to their volumes; what's the policy

    // need a way to ensure all buffers a written back to their volumes before destructing the
    // object. For example, we could block the destructor until the write-back queue is empty.

}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // this needs to ensure that all pending writes have been completed and all
        // pending handles have been dosposed before allowing to de-allocate the underlying memory 
        // areas.
    }
}

// Functions to manage list membership
impl BufferListHead {
    fn list_push_front(&mut self, buffers: &mut [Buffer], index: BufferIndex) {
        assert!(buffers[index as usize].link.next == NIL_BUFFER_INDEX);
        assert!(buffers[index as usize].link.prev == NIL_BUFFER_INDEX);

        if self.head != NIL_BUFFER_INDEX {
            assert!(self.tail != NIL_BUFFER_INDEX);
            buffers[index as usize].link.next = self.head;
            buffers[self.head as usize].link.prev = index;
            self.head = index;
        } else {
            assert!(self.tail == NIL_BUFFER_INDEX);
            self.head = index;
            self.tail = index;
        }
    }

    fn list_push_back(&mut self, buffers: &mut [Buffer], index: BufferIndex) {
        assert!(buffers[index as usize].link.next == NIL_BUFFER_INDEX);
        assert!(buffers[index as usize].link.prev == NIL_BUFFER_INDEX);

        if self.tail != NIL_BUFFER_INDEX {
            assert!(self.head != NIL_BUFFER_INDEX);
            buffers[index as usize].link.prev = self.tail;
            buffers[self.tail as usize].link.next = index;
            self.tail = index;
        } else {
            assert!(self.tail == NIL_BUFFER_INDEX);
            self.head = index;
            self.tail = index;
        }
    }

    fn list_remove(&mut self, buffers: &mut [Buffer], index: BufferIndex) {
        let next = buffers[index as usize].link.next;
        let prev = buffers[index as usize].link.prev;

        if next != NIL_BUFFER_INDEX {
            buffers[next as usize].link.prev = prev;
        } else {
            self.tail = prev;
        }

        if prev != NIL_BUFFER_INDEX {
            buffers[prev as usize].link.next = next;
        } else {
            self.head = next;
        }

        buffers[index as usize].link.next = NIL_BUFFER_INDEX;
        buffers[index as usize].link.prev = NIL_BUFFER_INDEX;
    }
}

// Handle for accessing buffers in the buffer pool
struct ReadOnlyBufferHandle<'a> {
    // pointer to the pool to which this handle refers to
    pool: &'a BufferPool,

    // index of the buffer within the pool
    buffer_index: usize
}

impl <'a> ReadOnlyBufferHandle<'a> {
    // get a read-only slice to the buffer content
}

// Handle for accessing buffers in the buffer pool
struct ReadWriteBufferHandle<'a> {
    // pointer to the pool to which this handle refers to
    pool: &'a BufferPool,

    // access guard
    guard: async_std::sync::RwLockWriteGuard<'a, ()>,

    // index of the buffer within the pool
    buffer_index: usize
}

impl <'a> ReadWriteBufferHandle<'a> {
    // get a read-only slice to the buffer content

    // get a read-write slice to the buffer content
}

