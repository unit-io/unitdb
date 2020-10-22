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
- Block Cache
- Write Ahead Log (WAL)
- Lookup Trie
- Writing to timeWindow file
- Writing to Block Index file
- Writing to Data file

### Writing data to disk 
The unitdb engine handles data from the point put request is received through writing data to the physical disk. Data is written to unitdb using low latency binary messaging entry. Data is compressed and encrypted (if encryption is set) then written to a WAL for immediate durability. Entries are written to block cache and become immediately queryable. The block cache is periodically written to disk in the form of blocks.

### Write Ahead Log (WAL)
The Write Ahead Log (WAL) retains unitdb data when the db restarts. The WAL ensures data is durable in case of an unexpected failure.

When the unitdb engine receives a put request, the following steps occur:

- The put request is parsed, packed and appended to a tinyBatch buffer.
- Topic is parsed into parts and added to the lookup Trie. Contract is added to the first part of the parts in the lookup Trie.
- The data is added to the block cache.
- The tinyBatch is appended to the WAL in cyclic order.
- The last offset of topic from timeWindow block is added to the Trie.
- Data is written to disk using block sync.
- The block cache is updated with free offset. The block cache shrinks if it reaches target size.
- When data is successfully written to WAL, a response confirms the write request was successful.

Blocks sync writes the timeWindow blocks, index blocks, and data blocks to disk.

When the unitdb restarts, last offset of all topics is loaded into Trie, the WAL file is read back and pending writes are applied to the unitdb.

### Block Cache
The block cache is an in-memory copy of entries that currently stored in the WAL. The block cache:

- Organizes entries as per topic hash into shards.
- Stores keys and offsets into map
- Stores compressed data into data blocks.

Queries to the unitdb merge data from the block cache with data from the files. Query first lookup topic offset in lookup Trie then uses Topic offset to traverse to timeWindow blocks and read window entries. The sequence from window entry is used to find block offset of index block file. The index block is read from the index file, that has entry information and using these information it read data from data block in data file and un-compresses the data. As encryption flag is set on first bit of sequence so if data is encrypted then it get un-encrypted while data is read.

### Block Sync

#### Time Window
To efficiently compact and store data, the unitdb engine groups entries sequence by topic key, and then orders those sequences by time and each block keep offset to next field of previous block in reverse time order.

#### Block Index
Block index stores entry sequence, offset of data block, message size and expiry details. Entry sequence is used to find block offset of index block. 

#### Data Block
The unitdb compress data and store it into data blocks. If an entry expires or deleted then the offset and size of data is marked as free and added to the leasing blocks that get allocated by new request.

After data is stored safely in files, the WAL is truncated and block cache is shrink.
