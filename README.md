**Beecask is a fast key-value storage library based on bitcask model.**

## Features
+ Keys and values are arbitrary byte arrays.
+ The basic operations are Set(key, value), Get(key), Delete(key).
+ Support setting the record expiration time.
+ All APIs are thread-safe.

## Benchmarks
We use a database with ten million records. Each record has a 10 byte key, and 100 byte value.
```
RandomSetBench:
10000000 set operation[key: 10B, value: 100B] in 36.957142s
average 270583.690291 qps
average 27.058369 MB/s
average 3.695714 micros/op

RandomGetBench:
10000000 get operation in 58.534869s
average 170838.343461 qps
average 5.853487 micros/op

RandomGetBenchWhenMerge:
10000000 get operation in 131.980224s
average 75768.927230 qps
average 13.198022 micros/op

RandomSetBenchWhenMerge:
10000000 set operation[key: 10B, value: 100B] in 46.313264s
average 215920.864727 qps
average 21.592086 MB/s
average 4.631326 micros/op
```

## Installation
+ go get github.com/yplusplus/ylog
+ go get github.com/yplusplus/beecask

## Other
welcome all the bug feedbacks and pull requests
