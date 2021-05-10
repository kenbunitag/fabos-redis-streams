import asyncio
import aioredis
async def consume_all():
    redis = await aioredis.create_redis(
        f'redis://localhost:6379/0')
    latest_id = "0"
    while True:
        result = await redis.xread(["mystream"], 
            timeout=50, latest_ids=[latest_id])
        for e in result:
            stream, id, msg = e
            print(id, msg)
            latest_id = id

asyncio.run(consume_all())