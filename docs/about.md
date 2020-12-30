# About unitdb 

## Key characteristics
- 100% Go
- Optimized for fast lookups and writes
- Can store larger-than-memory data sets
- Data is safely written to disk with accuracy and high performant block sync technique
- Supports opening database with immutable flag
- Supports data encryption
- Supports time-to-live on message entry
- Supports writing to wildcard topics
- Queried data is returned complete and correct

The unitdb engine includes the following components:

- Buffer Pool
- Memdb
- Write Ahead Log (WAL)
- Lookup Trie
- Writing to time window file
- Writing to block index file
- Writing to data file

### Writing data to disk 
The unitdb engine handles data from the point put request is received through writing data to the physical disk. Data is written to unitdb using low latency binary messaging entry. Data is compressed and encrypted (if encryption is set) then written to a WAL for immediate durability. Entries are written to memdb and become immediately queryable. The memdb entries are periodically written to log files in the form of blocks.

### Write Ahead Log (WAL)
The Write Ahead Log (WAL) retains unitdb data when the db restarts. The WAL ensures data is durable in case of an unexpected failure.

When the unitdb engine receives a put request, the following steps occur:

- The put request is parsed, packed and appended to a memdb tinyLog buffer.
- Topic is parsed into parts and added to the lookup Trie. Contract is added to the first part of the parts in the lookup Trie.
- The data is added to the memdb.
- The tinyLog is appended to the WAL.
- The topic offset from timeWindow block is added to the Trie.
- Data is written to disk using block sync.
- The time block is free from memdb and log is released after DB sync is complete.
- When data is successfully written to WAL, a response confirms the write request was successful.

Blocks sync writes the timeWindow blocks, index blocks, and data blocks to the disk.

When the unitdb restarts, topic offsets are loaded into Trie, the WAL file is read back and pending writes are applied to the unitdb.

### Memdb
The memdb is an in-memory copy of entries that are currently stored into the WAL. The memdb:

- Organizes entries into tinyLog.
- Stores keys and offsets into time blocks.
- Writes compressed data into the WAL.

Queries to the unitdb merge data from the memdb with data from the files. Query first lookup topic offset in lookup Trie then uses Topic offset to traverse to timeWindow blocks and read window entries. The sequence from window entry is used to find block offset of index file. The index block is read from the index file, that has entry information and using these information it reads data from data file and un-compresses the data. If encryption flag was set and data was encrypted then it un-encrypts data on read.

### Block Sync

#### Time Window
To efficiently compact and store data, the unitdb engine groups entries sequence by topic key, and then orders those sequences by time and each block keep offset to next field of previous block in reverse time order.

#### Block Index
Block index stores entry sequence, offset of data block, message size and expiry details. Entry sequence is used to find block offset of index block. 

#### Data Block
The unitdb compress data and store it into data blocks. If an entry expires or deleted then the offset and size of data is marked as free and added to the leasing blocks that get allocated by new request.

After data is stored safely in files, the blocks are free from memdb and releases the blocks from the WAL.
