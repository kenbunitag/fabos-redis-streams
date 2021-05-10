import aioredis
import os
import asyncio
from io import BytesIO
import msgpack
import numpy as np
import time
from typing import List
from utils import RedisClient
import utils
import aioredis.commands.transaction

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

overall_start = time.time()


async def stream_many_to_one():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
    await r.flushdb()
    utils.overall_start = time.time()

    p1 = await RedisClient.create("AAA")
    p2 = await RedisClient.create("BBB")
    p3 = await RedisClient.create("CCC")
    c = await RedisClient.create("consumer1")

    await asyncio.gather(
        p1.produce(range(3), delay=0.3),
        p2.produce(range(3), delay=0.4),
        p3.produce(range(3), delay=0.5),
        c.consume(["AAA", "BBB", "CCC"], 9)
        )
    

asyncio.run(stream_many_to_one())