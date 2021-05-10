import asyncio
import aioredis
import time

SIZE = 1_000_000
COUNT = 1000

async def read_all():
    redis = await aioredis.create_redis(f'redis://localhost:6379/0')
    latest_id = "0"
    c = 0
    print("start reading ...")
    start = time.perf_counter()
    while c < COUNT:
        result = await redis.xread(["mystream"], timeout=50, latest_ids=[latest_id])
        for e in result:
            stream, id, msg = e
            latest_id = id
            c += 1

    end = time.perf_counter()
    duration = end - start
    print(duration)
    print(f'{SIZE*COUNT/1e6/duration} MB/s')

asyncio.run(read_all())