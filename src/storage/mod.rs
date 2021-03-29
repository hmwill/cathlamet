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

mod utils;

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

// The minimum number of blocks in order to create a valid volume
// We need a header block and two file tables and the initial allocation bitmap
// The file tables require each 
// (FILE_TABLE_NUM_RESERVED * FILE_HEADER_SIZE) / BLOCK_SIZE many blocks
// The allocation bitmap can be stored inline for small volumes
//
// TODO: This needs to be revised once we add journaling
const MIN_VOLUME_BLOCK_COUNT: usize = 16;

// The data structure located in the first block of a storage volume that describes
// the information needed to access the file system
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[repr(C)]
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
const FILE_TABLE_NAME_FILE_TABLE: &str = "$file_table_0";

// Index of the file table mirror file header within the file table
const FILE_TABLE_INDEX_FILE_TABLE_MIRROR: usize = 1;
const FILE_TABLE_NAME_FILE_TABLE_MIRROR: &str = "$file_table_1";

// Index of the block allocation bitmap file header in the file table
const FILE_TABLE_INDEX_ALLOCATION_BITMAP: usize = 2;
const FILE_TABLE_NAME_ALLOCATION_BITMAP: &str = "$allocation_bitmap";

// Index of the journal file header in the file table
const FILE_TABLE_INDEX_JOURNAL: usize = 3;
const FILE_TABLE_NAME_JOURNAL: &str = "$journal";

// Index of the volume header block seen as entry in the file table
const FILE_TABLE_INDEX_VOLUME_HEADER: usize = 4;
const FILE_TABLE_NAME_VOLUME_HEADER: &str = "$volume_header";

// Number of reserved entries in the file header table; this is the index of the first user file
const FILE_TABLE_NUM_RESERVED: usize = 16;

// The file header data structure describes an entry in the file table
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[repr(C)]
struct FileHeader {
    // discriminator value for this file table entry
    file_header_type: u32,      

    // a version number that is incremented each time the entry is updated. Used for recovery.
    version: u32,               

    // time of creation, 48 bits for seconds, 16 bits for subsecond
    timestamp_created: u64,     

    // time of last modofication, 48 bits for seconds, 16 bit subsecond
    timestamp_modified: u64,    

    // number of number of allocated disk blocks; this will also determine the level of indirection
    blocks_allocated: u32,      
                                
    // number of blocks actually in use, always less than or equal to the amount provided by allocation
    blocks_used: u32,           
    
    // number of bytes used in the last block
    last_block_use: u32,        
}

// Size of a file header record
const DEFAULT_LOG_FILE_HEADER_RECORD_SIZE: usize = 10;

// Minimum size of a file header record; because we allow up to 255 bytes for the file name
const MIN_LOG_FILE_HEADER_RECORD_SIZE: usize = 9;

// Maximum length of a valid file name
const MAX_FILE_NAME_LENGTH: usize = 0xff;

// File header type associated with an unused entry
const FILE_HEADER_TYPE_UNUSED: u32 = 0;

// File header type associated with a regular file
const FILE_HEADER_TYPE_FILE: u32 = 1;

// File header type associated with a reserved file header entry
const FILE_HEADER_TYPE_RESERVED: u32 = 0xffff;

//
// File attributes are identified by a 16-bit value identifying the kind of attribute
// Attributes are aligned to 16-bits (2 bytes)
//

// End of attribute list marker
const FILE_ATTRIBUTE_LIST_END: u16 = 0u16;

// Field code for file name encoded as UTF-8 byte string. The field is followed by an u8 value providing the length
// of the string, followed by the UTF-8 byte sequence
const FILE_ATTRIBUTE_NAME: u16 = 1u16;

// Field code for a list of extents is 0x1X. The low nibble encodes the levels of indirection.
// If it is 0, then the attribute includes a list of extents.
// If it is 1, then the attribute includes a list of (block ptr, num_blocks) entries, where each block contains
//  a list of extents. num_blocks aggregates the total number of blocks up to the end of the block pointed to in order
//  to allow for binary search
// And so forth for higher levels of indirection.
// This attribute code is followed by the number of entries in the extent list following as u16
// If necessary, padding to align to 32 bit, which is the alignment for block pointers and block counts
const FILE_ATTRIBUTE_EXTENT_LIST: u16 = 0x10u16;

// Representation of an extent
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[repr(C)]
struct Extent {
    // block index of the first block of this extent
    block_pointer: u32,

    // number of blocks that are part of the extent
    num_blocks: u32
}

/// Representation of a mounted disk volume
pub struct Volume {
    // location of the volume file on disk
    path: PathBuf,

    // the file handle used to access the mounted volume file
    // It is set to None after unmounting the volume
    file: Option<async_std::fs::File>,

    // a copy of the deserialized volume header
    header: VolumeHeader,

    // the index used to identify the volume internally,
    index: VolumeIndex,
}

/// VolumeParameters are used to describe the different size parameters needed to create a data volume
#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
pub struct VolumeParameters {
    // total size of storage volume
    pub volume_size: u64,           

    // log_2(volume block size)
    pub log_block_size: u16,       

    // log_2(file header size); less or equal to log_block_size
    pub log_file_header_size: u16,  
}

impl VolumeParameters {
    pub fn new_with_defaults(volume_size: u64) -> VolumeParameters {
        const LOG_BLOCK_SIZE: u64 = (32 - (DEFAULT_VOLUME_BLOCK_SIZE as u32).leading_zeros()) as u64;

        let block_size = 1u64 << LOG_BLOCK_SIZE;

        assert!(volume_size % block_size == 0 && volume_size / block_size >= 1);
        assert!(volume_size / block_size <= (u32::MAX as u64) + 1);

        VolumeParameters {
            volume_size,
            log_block_size: LOG_BLOCK_SIZE as u16,
            log_file_header_size: DEFAULT_LOG_FILE_HEADER_RECORD_SIZE as u16,
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

fn init_file_header(buffer: &mut [u8], filename: &str, initial_size: u64, initial_extent: Extent, block_size: u64) {
    let blocks_used = ((initial_size + block_size - 1) / block_size) as u32;
    let last_block_use = (initial_size % block_size) as u32;
    let now = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
    let seconds = now.as_secs();
    let nanos = now.subsec_nanos(); 
    // subsec_nanos should return a 30 bit value
    assert!(nanos & 0xC000 == 0);
    let timestamp = (seconds << 16) | (nanos as u64 >> 14);

    let file_header = FileHeader {
        file_header_type: FILE_HEADER_TYPE_FILE,      
        version: 1u32,               
        timestamp_created: timestamp,     
        timestamp_modified: timestamp,    
        blocks_allocated: initial_extent.num_blocks,      
        blocks_used,           
        last_block_use,       
    };

    let file_header_size = std::mem::size_of::<FileHeader>();

    unsafe {
        let file_header_slice = crate::common::any_as_u8_slice(&file_header);
        buffer.copy_from_slice(&file_header_slice);
    }

    // append file name
    let mut offset = utils::align_usize(file_header_size, std::mem::size_of::<u16>());
    assert!(offset < buffer.len());

    unsafe {
        let attribute_slice = &mut buffer[offset..];
        let attribute_pointer = attribute_slice.as_mut_ptr() as *mut u16;
        *attribute_pointer = FILE_ATTRIBUTE_NAME;
        let name_length_pointer = attribute_slice.as_mut_ptr().add(std::mem::size_of::<u16>());
        assert!(filename.len() <= u8::MAX as usize);
        *name_length_pointer = filename.len() as u8;
        let name_slice = &mut buffer[offset + 3..];
        name_slice.copy_from_slice(filename.as_bytes());
        offset = offset + 3 + filename.len();
    }
    
    // append extent list
    offset = utils::align_usize(offset, std::mem::size_of::<u16>());
    assert!(offset < buffer.len());

    unsafe {
        let attribute_slice = &mut buffer[offset..];
        let attribute_pointer = attribute_slice.as_mut_ptr() as *mut u16;
        *attribute_pointer = FILE_ATTRIBUTE_EXTENT_LIST;
        let list_length_pointer = attribute_slice.as_mut_ptr().add(std::mem::size_of::<u16>()) as *mut u16;
        *list_length_pointer = 1u16;
        let extent_offset = utils::align_usize(offset + std::mem::size_of::<u16>() * 2, std::mem::size_of::<u32>());
        let extent_pointer = attribute_slice.as_mut_ptr().add(extent_offset) as *mut Extent;
        *extent_pointer = initial_extent;
        offset = extent_offset + std::mem::size_of::<Extent>();
    }

    // append end of attributes
    offset = utils::align_usize(offset, std::mem::size_of::<u16>());
    assert!(offset < buffer.len());

    unsafe {
        let attribute_slice = &mut buffer[offset..];
        let attribute_pointer = attribute_slice.as_mut_ptr() as *mut u16;
        *attribute_pointer = FILE_ATTRIBUTE_LIST_END;
    }
}

/// The volume manager is a mapping of all currently mounted storage volumes
pub struct VolumeManager {
    // buffer pool used to map volume data
    buffer_pool: std::sync::Weak<BufferPool>,

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
            buffer_pool: std::sync::Weak::new(),
            volumes: Vec::with_capacity(10),
            next_volume: VolumeIndex(0),
            num_mounted_volumes: 0
        }
    }

    // set the buffer pool used to hold data to read from and write to the attached volumes
    fn set_buffer_pool(&mut self, buffer_pool: &std::sync::Arc<BufferPool>) {
        self.buffer_pool = std::sync::Arc::downgrade(buffer_pool);
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
        use async_std::os::unix::fs::OpenOptionsExt;

        let block_size = 1u64 << parameters.log_block_size;
        let file_header_size = 1u64 << parameters.log_file_header_size;
        let file_table_offset = 1u64 << parameters.log_block_size;
        let file_table_size = file_header_size * FILE_TABLE_NUM_RESERVED as u64; 
        let file_table_blocks = (file_table_size + block_size - 1) / block_size;
        let file_table_mirror_offset = file_table_offset + (file_table_blocks * block_size) as u64;
        // the file table mirror has the same size as the file table offset
        let allocation_bitmap_offset = file_table_mirror_offset + file_table_blocks as u64;

        // should hold, because we require the volume size to be an integer multiple of the block size
        assert!(parameters.volume_size % 8 == 0);
        let allocation_bitmap_size = parameters.volume_size / 8;
        let allocation_bitmap_blocks = utils::calc_file_blocks(allocation_bitmap_size, block_size);

        // create the file; we are using create_new to avoid accidentally overwriting a previous file
        let file = async_std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .await?;

        // set the file to size specified
        file.set_len(parameters.volume_size).await?;

        // create the UUID and populate the volume header structure
        let volume_header = VolumeHeader {
            format_magic: VOLUME_HEADER_FORMAT_MAGIC,
            format_version: VOLUME_HEADER_FORMAT_VERSION,
        
            uuid: uuid::Uuid::new_v4().as_bytes().clone(),
        
            volume_size: parameters.volume_size,
            log_block_size: parameters.log_block_size,
            log_file_header_size: parameters.log_file_header_size,

            file_table_offset,
        };        

        // Keep a copy for building the header block
        let volume_header_copy = volume_header.clone();

        // TODO: Adjust errors; using NotFound as proxy for out of identifiers
        let index = self.next_volume().map_err(|_| std::io::Error::from(std::io::ErrorKind::NotFound))?;

        // register the volume file with a new index
        let volume = Volume {
            path: PathBuf::from(path),
            file: Some(file),
            header: volume_header,
            index
        };

        assert!(self.volumes.len() == index.0 as usize);
        self.volumes.push(Some(volume));
        let buffer_pool = self.buffer_pool.upgrade().unwrap();

        // create the header block
        {
            let volume_header_block_location = Location::new(index, 0u64);
            let mut handle = buffer_pool.access_read_write(volume_header_block_location).await;
            let buffer = handle.buffer_mut();
            assert!(buffer.len() >= std::mem::size_of::<VolumeHeader>());

            unsafe {
                let header_data = crate::common::any_as_u8_slice(&volume_header_copy);
                buffer.copy_from_slice(header_data);
            }

            handle.release().await;
        }

        // create file table 
        {
            let volume_header_block_location = Location::new(index, block_size as u64);
            let mut handle = buffer_pool.access_read_write(volume_header_block_location).await;

            // offset within the block
            let mut offset = 0usize;
            let buffer_mut = handle.buffer_mut();
            let entry = &mut buffer_mut[offset .. offset + (file_header_size as usize)];
            let extent = Extent {
                block_pointer: (file_table_offset / block_size) as u32,
                num_blocks: file_table_blocks as u32
            };

            init_file_header(entry, FILE_TABLE_NAME_FILE_TABLE, file_table_size, extent, block_size);

            handle.release().await;
        }

        // create file table mirror
        {

        }
        
        // create the initial allocation bitmap
        {
            
        }

        Ok(index)
    }

    // Mount a previously created volume file
    async fn mount(&mut self, path: &Path) -> std::io::Result<VolumeIndex> {
        use async_std::os::unix::fs::OpenOptionsExt;

        // open the file; ensure we fail if it does not exist yet
        let file = async_std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)
            .await?;

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

    // Write buffer contents to external storage
    async fn fetch(&self, location: Location, buffer: &mut[u8]) -> std::io::Result<()> {
        unimplemented!()
    }

    // Read buffer contents from external storage
    async fn store(&self, location: Location, buffer: &[u8]) -> std::io::Result<()> {
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
    // Backingstore used to fetch and store buffers
    backing_store: std::sync::Weak<VolumeManager>,

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
        let available_buffers = BufferListHead { 
            head: 0,
            tail: (num_buffers - 1) as BufferIndex
        };

        let mapping_inner = BufferPoolMapping {
            buffers,
            mapped_locations: std::collections::BTreeMap::new(),
            dirty_buffers: BufferListHead::default(),
            in_use_buffers: BufferListHead::default(),
            available_buffers,
            read_write_locks: Vec::new(),
        };

        Ok(BufferPool {
            backing_store: std::sync::Weak::new(),
            memory,
            buffer_size,
            mapping: async_std::sync::Mutex::new(mapping_inner),
            buffer_available: async_std::sync::Condvar::new(),
            buffer_dirty: async_std::sync::Condvar::new(),
        })
    }

    // attach the backing store
    fn set_backing_store(&mut self, backing_store: &std::sync::Arc<VolumeManager>) {
        self.backing_store = std::sync::Arc::downgrade(backing_store);
    }

    // return the total amount of allocated memory
    fn total_size(&self) -> usize {
        self.memory.len()
    }

    // Provide read-only access to a buffer 
    // TODO: Update return type to a form of Result<T, E>
    async fn access_read_only<'a>(&'a self, location: Location) -> ReadOnlyBufferHandle<'a> {
        let (index, lock_ptr, init_lock) = self.buffer_access(location).await;

        let guard = match init_lock {
            None => unsafe {
                (*lock_ptr).read().await
            },
            Some (guard) => unsafe {
                let page_data = self.writable_slice(index);
                let backing_store = self.backing_store.upgrade().unwrap();
                backing_store.fetch(location, page_data).await.unwrap();
                async_std::sync::RwLockWriteGuard::downgrade(guard)
            }
        };

        ReadOnlyBufferHandle {
            pool: self,
            guard: Some(guard),
            buffer_index: index
        }
    }

    // Provide read-write access to a buffer 
    // TODO: Update return type to a form of Result<T, E>
    async fn access_read_write<'a>(&'a self, location: Location) -> ReadWriteBufferHandle<'a> {
        let (index, lock_ptr, init_lock) = self.buffer_access(location).await;

        let guard = match init_lock {
            None => unsafe {
                (*lock_ptr).write().await
            },
            Some (guard) => unsafe {
                let page_data = self.writable_slice(index);
                let backing_store = self.backing_store.upgrade().unwrap();
                backing_store.fetch(location, page_data).await.unwrap();
                guard
            }
        };

        ReadWriteBufferHandle {
            pool: self,
            guard: Some(guard),
            buffer_index: index
        }
    }

    // Provide read-write access to a buffer without necessarily restoring previous content
    // TODO: Update return type to a form of Result<T, E>
    async fn access_initialize<'a>(&'a self, location: Location) -> ReadWriteBufferHandle<'a> {
        let (index, lock_ptr, init_lock) = self.buffer_access(location).await;

        let guard = match init_lock {
            None => unsafe {
                (*lock_ptr).write().await
            },
            Some (guard) => unsafe {
                let page_data = self.writable_slice(index);
                page_data.fill(0);
                guard
            }
        };

        ReadWriteBufferHandle {
            pool: self,
            guard: Some(guard),
            buffer_index: index
        }
    }

    async fn buffer_access<'a>(&'a self, location: Location) -> (usize, *const async_std::sync::RwLock<()>,
        Option<async_std::sync::RwLockWriteGuard<'a, ()>>) {
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

                        let init_lock = unsafe {
                            (*lock_ptr).try_write().unwrap()
                        };

                    break (index as usize, lock_ptr, Some(init_lock))

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
                            
                            let init_lock = unsafe {
                                (*lock_ptr).try_write().unwrap()
                            };

                            break (index, lock_ptr, Some(init_lock));
                        },
                        &mut Some(ref mut access) => {
                            let lock_ptr = (&*access.lock) as *const async_std::sync::RwLock<()>;
                            let num_handles = access.num_handles;
                            access.num_handles = num_handles + 1;
                            break (index, lock_ptr, None);
                        }
                    };
                }
            }
        }
    }

    // Return a read-only handle
    async fn release_read_only<'a>(&'a self, buffer_index: usize) {
        // because the handle still holds the lock, we are still preventing exclusive
        // write access to this buffer

        let internal = &mut *self.mapping.lock().await;

        let (BufferAccess { lock, num_handles: _ }, dirty) = {
            let buffer = &mut internal.buffers[buffer_index];

            // if nobody picked up an access handle while we have been holding the lock, move
            // the buffer to the dirty queue and trigger the buffer_dirty condition variable if
            // the buffer is not marked for submission already.

            match &mut buffer.access {
                None => panic!("Should have access with num_handles >= 1"),
                Some(ref mut access) => {
                    assert!(access.num_handles > 0);

                    if access.num_handles > 1 {
                        access.num_handles = access.num_handles - 1;
                        return;
                    }
                }
            };

            // at this point we know that we are the only acess handle and buffer needs to be moved 
            // to the dirty queue or can become available because it is clean

            if !buffer.dirty {
                self.buffer_available.notify_one();
            } else if !buffer.submitted {
                self.buffer_dirty.notify_one();
            }

            (buffer.access.take().unwrap(), buffer.dirty)
        };

        {
            let read_write_locks = &mut internal.read_write_locks;
            BufferPoolMapping::dealloc_lock(read_write_locks, lock);
        }

        {
            let in_use_buffers = &mut internal.in_use_buffers;
            let buffers = &mut internal.buffers;
            in_use_buffers.list_remove(buffers, buffer_index as BufferIndex);
        }

        if dirty {
            let dirty_buffers = &mut internal.dirty_buffers;
            let buffers = &mut internal.buffers;
            dirty_buffers.list_push_front(buffers, buffer_index as BufferIndex);
        } else {
            let available_buffers = &mut internal.available_buffers;
            let buffers = &mut internal.buffers;
            available_buffers.list_push_front(buffers, buffer_index as BufferIndex);
        }
    }

    // Return a read-write handle
    async fn release_read_write<'a>(&'a self, buffer_index: usize) {
        // because the handle still holds the lock, we still have exclusive access to
        // this buffer
        let internal = &mut *self.mapping.lock().await;

        let BufferAccess { lock, num_handles: _ } = {
            let buffer = &mut internal.buffers[buffer_index];
            buffer.dirty = true;

            // if nobody picked up an access handle while we have been holding the lock, move
            // the buffer to the dirty queue and trigger the buffer_dirty condition variable if
            // the buffer is not marked for submission already.

            match &mut buffer.access {
                None => panic!("Should have access with num_handles >= 1"),
                Some(ref mut access) => {
                    assert!(access.num_handles > 0);

                    if access.num_handles > 1 {
                        access.num_handles = access.num_handles - 1;
                        return;
                    }
                }
            };

            // at this point we know that we are the only acess handle and buffer needs to be moved 
            // to the dirty queue

            if !buffer.submitted {
                // while we signal here, any access to the condition variable is still protected
                // by the lock we are holding
                self.buffer_dirty.notify_one();
            }

            buffer.access.take().unwrap()
        };

        {
            let read_write_locks = &mut internal.read_write_locks;
            BufferPoolMapping::dealloc_lock(read_write_locks, lock);
        }

        {
            let in_use_buffers = &mut internal.in_use_buffers;
            let buffers = &mut internal.buffers;
            in_use_buffers.list_remove(buffers, buffer_index as BufferIndex);
        }

        {
            let dirty_buffers = &mut internal.dirty_buffers;
            let buffers = &mut internal.buffers;
            dirty_buffers.list_push_front(buffers, buffer_index as BufferIndex);
        }
    }

    // Unmap a specific location, that is contents are no longer needed, and they should also
    // no longer be maintained in the buffer pool
    fn unmap(&self, location: Location) {
        unimplemented!()
    }

    // need a background task to write dirty buffers back to their volumes; what's the policy

    // need a way to ensure all buffers a written back to their volumes before destructing the
    // object. For example, we could block the destructor until the write-back queue is empty.


    unsafe fn writable_slice<'a>(&'a self, buffer_index: usize) -> &'a mut [u8] {
        let base: *const u8 = self.memory.as_ptr();
        let page_base = base.add(self.buffer_size * buffer_index);
        unsafe { std::slice::from_raw_parts_mut(page_base as *mut u8, self.buffer_size) }
    } 

    unsafe fn readable_slice<'a>(&'a self, buffer_index: usize) -> &'a [u8] {
        let base: *const u8 = self.memory.as_ptr();
        let page_base = base.add(self.buffer_size * buffer_index);
        unsafe { std::slice::from_raw_parts(page_base, self.buffer_size) }
    } 
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // TODO: this needs to ensure that all pending writes have been completed and all
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

    // access guard
    guard: Option<async_std::sync::RwLockReadGuard<'a, ()>>,

    // index of the buffer within the pool
    buffer_index: usize
}

impl <'a> ReadOnlyBufferHandle<'a> {
    // get a read-only slice to the buffer content
    fn buffer_ref(&self) -> &'a [u8] {
        unsafe { self.pool.readable_slice(self.buffer_index) }
    }

    async fn release(&mut self) {
        if self.guard.is_none() {
            panic!("Buffer handle has been released already")
        }

        self.pool.release_read_only(self.buffer_index).await;
        self.guard.take();
    }
}

impl <'a> Drop for ReadOnlyBufferHandle<'a> {
    // get a read-only slice to the buffer content
    fn drop(&mut self) {
        if self.guard.is_some() {
            panic!("Did not release handle")
        }
    }
}

// Handle for accessing buffers in the buffer pool
struct ReadWriteBufferHandle<'a> {
    // pointer to the pool to which this handle refers to
    pool: &'a BufferPool,

    // access guard
    guard: Option<async_std::sync::RwLockWriteGuard<'a, ()>>,

    // index of the buffer within the pool
    buffer_index: usize
}

impl <'a> ReadWriteBufferHandle<'a> {
    // get a read-only slice to the buffer content
    fn buffer_ref(&self) -> &'a [u8] {
        unsafe { self.pool.readable_slice(self.buffer_index) }
    }

    // get a read-write slice to the buffer content
    fn buffer_mut(&mut self) -> &'a mut [u8] {
        unsafe { self.pool.writable_slice(self.buffer_index) }
    }

    async fn release(&mut self) {
        if self.guard.is_none() {
            panic!("Buffer handle has been released already")
        }

        self.pool.release_read_write(self.buffer_index).await;
        self.guard.take();
    }
}

impl <'a> Drop for ReadWriteBufferHandle<'a> {
    // get a read-only slice to the buffer content
    fn drop(&mut self) {
        if self.guard.is_some() {
            panic!("Did not release handle")
        }
    }
}

