# Godat

This repository contains the source of Godat, a flexible associative data store baed on string-sorted tables.
The goal is to build an interstellar-scale key-value storage based on gossiping and a unique time system.

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
- [ ] HTTP-based access protocol
- [ ] Peer-to-peer networking
- [ ] Network-level conflict resolution
- [ ] Global timestamp clock