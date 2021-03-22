# Storage System

We assume that not all data fits into main memory and thus rely on secondary block storage to
hold the majority of data in the system. Longer term, we can conceive moving to a multi-tiered 
design that organizes data from hot to increasingly colder forms into increasingly more
affordable storage technologies with higher latency.

## Key Concepts and Components

### Volume

A data volume represents a contiguous allocation of storage space on a block device. Internally,
volumes are organized as a collection of same-sized blocks. The block size is generally a power
of two and can range from 256B (2^8) to up to about 4 MiB (2^22). A block corresponds most closely 
(but also isn't necessarily identical) to a "sector" on a spinning disk device and a "page" on
an SSD device.

Initially, volumes will have a fixed size determined upon creation. As future development, we may 
consider adding support for increaing and decreasing the size of volumes. This is primarily 
relevant for data volumes mapped to files in an underlying files system rather than being mapped
directly to raw storage devices. Decreasing the size of data volumes will require the implementation
of a compaction process in order to be of practical use.

### VolumeGroup

A volume group is a collection of volumes that are used to create a logical storage space. In
a multi-tiered storage system, each volume group will represent the storage associated with a 
given storage tier.

It is possible to attach further volumes to a volume group and to detach unused volumes during 
operation of the system. Storage compaction will be needed in order to make detaching practical. 

### Tier

Within a multi-tiered storage configuration, each tier corresponds to the storage of a given
"temperature", where "hot" is storage most in use and "cold" is storage for data with most
infrequent access needs.

### File

A logical region of storage that is organized as a contiguous sequences of pages. Files can grow by
appending additional pages and they can be truncated from the end. Access to files happens at the
granularity of pages.

Question: Are files confined to a single volume? Or: Is there something like a master volume in each 
volume group that provides an anchor for block allocation and file management?

### Page

An allocation unit in a file. It is a power of two, and it does not have to match the block size of 
the underlying storage volume. Each file can determine its own page size. When mapping files to
storage volumes, allocation will happen in the granularity of volume blocks(?).

### Buffer

A buffer is an area of main memory that holds a copy of the data associated with a specific page
of a specific file. Data is synchronized between main memory and block storage through processes
of fetching and writing back page content.

Buffers are aligned with pages in the underlying virtual memory system provided by the MMU and
operating system.

### Bufferpool

