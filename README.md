# Mini LSM in Kotlin 

This project is about implementing production ready LSM in kotlin. 
Inspired by [mini-lsm](https://github.com/skyzh/mini-lsm) which is implemented in Rust. 

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
