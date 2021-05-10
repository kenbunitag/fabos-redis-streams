import asyncio
import os
import time

import aioredis
import aioredis.commands.transaction

import utils
from utils import RedisClient

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

overall_start = time.time()

async def stream_one_to_many():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
    await r.flushdb()
    utils.overall_start = time.time()

    p = await RedisClient.create("sensor1")
    c1 = await RedisClient.create("c_AAA")
    c2 = await RedisClient.create("c_BBB")
    c3 = await RedisClient.create("c_CCC")

    await asyncio.gather(
        p.produce(range(3)),
        c1.consume(["sensor1"], 3),
        c2.consume(["sensor1"], 3),
        c3.consume(["sensor1"], 3),
        )

asyncio.run(stream_one_to_many())
