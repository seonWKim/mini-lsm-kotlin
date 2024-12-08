# Mini LSM in Kotlin

<b>⭐️⭐️ Note that this is not production ready yet ⭐️⭐</b>

LSM database written in kotlin.
Inspired by [mini-lsm](https://github.com/skyzh/mini-lsm) which is implemented in Rust.

### Table of Contents

- [Modules](#modules)
- [Cli](#cli)
    - [Requirements](#requirements)
    - [How to start](#how-to-start)
    - [Examples](#examples)
- [What is LSM](#what-is-lsm)
    - [Characteristics](#characteristics)
    - [Related keyword](#related-keywords)

## Modules

- cli: Provides command-line interface commands to interact with the LSM database. 
- core: A production-ready implementation of the LSM database. Currently, it's a mirror of mini-lsm-mvcc module.  
- mini-lsm-starter: A Kotlin rewrite for [week1](https://skyzh.github.io/mini-lsm/week1-overview.html)
  and [week2](https://skyzh.github.io/mini-lsm/week2-overview.html) of MINI-LSM project. 
- mini-lsm-mvcc: A Kotlin rewrite of the [week3](https://skyzh.github.io/mini-lsm/week1-overview.html) of MINI-LSM project. 

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
> based storage systems.Log structured merge trees optimize both read and write operations using a combination
> of
> in-memory and disk-based structures.
