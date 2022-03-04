# kvstore

This repository contains the source of kvstore, a flexible associative data store based on string-sorted tables. It is supposed to be easily embeddable into any application.

## What's working?

- [x] Basic table implementation
- [x] Index-based block access
- [x] Key filtering using bloom filters
- [x] Block level caching
- [x] Block level compression/decompression
- [x] Table-level access conflict resolution
- [x] Row-level access conflict resolution
- [x] Automatic memtable flushing on overflow
- [ ] Automatic table merging on overflow using leveled compaction