# Mini LSM in Kotlin
<b>⭐️⭐️ Note that this is not production ready yet ⭐️⭐</b>

LSM database written in kotlin. 
Inspired by [mini-lsm](https://github.com/skyzh/mini-lsm) which is implemented in Rust.


### Table of Contents

- [Cli](#cli)
    - [Requirements](#requirements)
    - [How to start](#how-to-start)
    - [Examples](#examples)
- [What is LSM](#what-is-lsm)
    - [Characteristics](#characteristics)
    - [Related keyword](#related-keywords)

## Cli

### Requirements

- Use java 21

### How to start

```shell
# build jar 
$ ./gradlew :cli:build

# run cli application 
$ java -jar cli/build/libs/cli.jar
```

### Examples

```shell
# put key value  
$ put <key> <value>  

# get key  
$ get <key>

# delete key 
$ delete <key> 

# force freeze memtable  
$ freeze 

# force flush memtable, note that this command only flushes the earliest memtable only 
$ flush  
```

## What is LSM?

[Reference](https://www.scylladb.com/glossary/log-structured-merge-tree/)
> LSM or LSM tree is a data structure that efficiently stores key-value pairs for retrieval in disk or flash
> based storage systems.Log structured merge trees optimize both read and write operations using a combination of
> in-memory and disk-based structures.

### Characteristics

- Organize data into multiple levels, with a sequence of increasingly larger, sorted components.
    - In-memory
        - Memtable: an in memory write buffer
    - Disk based storage
        - Level 0 sorted runs(SSTables): Memtables are periodically flushed to disk as sorted string tables(
          SSTables) in level 0.
- Merge process periodically merges data for faster lookups and efficient write operations.
- Ideal for write operations and write heavy workloads.

### Related Keywords

- Memtable: In memory write buffer.
- SSTable(Sorted String Table): Immutable data structure created when memtables are flushed to disk(or multiple
  SSTables can be merged into a new SSTable).
- Bloom filter: A probabilistic data structure used to check whether specified data exist in the system. 
