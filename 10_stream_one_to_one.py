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


async def stream_one_to_one():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
    await r.flushdb()
    utils.overall_start = time.time()

    p : RedisClient = await RedisClient.create("sensor1")
    c1 : RedisClient = await RedisClient.create("consumer1")

    await asyncio.gather(
        p.produce(range(3)), 
        c1.consume(["sensor1"], 3)
        )

    c2 = await RedisClient.create("consumer2")
    await asyncio.gather(
        p.produce(range(3, 6)), 
        c2.consume(["sensor1"], 6)
        )


asyncio.run(stream_one_to_one())