import asyncio
import aioredis
import time

SIZE = 1_000_000
COUNT = 1000

async def write():
    redis = await aioredis.create_redis(
        f'redis://localhost:6379/0')
    data = bytes(SIZE)
    print("delete stream")
    await redis.delete("mystream")
    print("start writing ...")
    start = time.perf_counter()
    reqs = []
    for i in range(COUNT):
        reqs.append(redis.xadd("mystream", dict(i=i, d=data)))
    await asyncio.gather(*reqs)
    end = time.perf_counter()
    duration = end - start
    print(duration)
    print(f'{SIZE*COUNT/1e6/duration} MB/s')

        
asyncio.run(write())