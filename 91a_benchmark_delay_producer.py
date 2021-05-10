import asyncio
import aioredis
import time
async def produce():
    redis = await aioredis.create_redis(
        f'redis://localhost:6379/0')
    await redis.delete("mystream")
    for i in range(10000):
        result = await redis.xadd("mystream", 
            dict(i=i, t=time.time_ns()))
        print(i, result)
        await asyncio.sleep(0.5)

asyncio.run(produce())