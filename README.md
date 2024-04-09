A persistent Key Value database backed by a [Log Structured Merge Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree), with weak durability.

### How it works

#### Reads
Reads are served from the in memory table if possible.  
If the in memory table doesn't hold the requested key, then existing Store files are checked in order of creation. If the key is still not found, it doesn't exist in the database.

Reading a Store file is made performant by keeping a (dense for now) index of the file offsets that keys reside at. If a Store file has the key, it's a single sequential file read to get the value (whilst the index is dense).  


#### Writes
Writes initially happen to an in memory table.
Once the in memory table reaches a certain size (or the table is manually flushed):
- The table will be written to disk as a Store file
- The Store file will be indexed in memory
- The in memory table will be cleared


#### Compaction
Over time, the in memory table will be flushed many times, creating a new Store file each time.  
As the number of Store files needed to check a key for increases, performance takes a hit. To minimize this, compaction can be performed on the Store files which will take all existing Store files and merge them into one.  
Keys in newer Store files take priority over older ones, ensuring keys are always up to date. Once compacted, the newly merged Store file can be indexed as normal.


#### Limitations
- Keys are unsigned 32 bit integers for now
- No check sums for data
- Anything in the 'In Memory' store will be lost on crash at the moment. A write ahead log to fix this is planned.
- No timed key expiry
