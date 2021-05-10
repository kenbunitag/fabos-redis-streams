# Redis Streams examples

Collection of examples how to use [redis streams](https://redis.io/topics/streams-intro).

## Setup
```
docker run -p 6379:6379 --name some-redis -d redis

conda install -c conda-forge aioredis msgpack-python numpy
# OR
pip install -r requirements.txt
```


## Provided stream examples
- One to one
- Fanout: one to many
- Aggregation: many to one
- Load balancing: one to consumer group
- Failure recovery: one to one consumer group

## Benchmarks
- Delay benchmark
- Batch write 1GB
- Batch read 1GB

