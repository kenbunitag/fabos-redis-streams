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


async def stream_one_to_may_group():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
    await r.flushdb()
    utils.overall_start = time.time()

    p = await RedisClient.create("requests")
    c1 : RedisClient = await RedisClient.create("c_AAA")
    c2 : RedisClient = await RedisClient.create("c_BBB")
    c3 : RedisClient = await RedisClient.create("c_CCC")

    await p.produce(range(9), delay=0)
    await r.xgroup_create("requests", "my_group", latest_id="0", mkstream=True)
    gmc = utils.GroupMessageCounter(9)

    await asyncio.gather(
        c1.consume_group("my_group", ["requests"], None),
        c2.consume_group("my_group", ["requests"], None),
        c3.consume_group("my_group", ["requests"], None, do_ack=0),
        )

    gmc = utils.GroupMessageCounter(3)
    await c3.consume_group("my_group", ["requests"], None, do_ack=1, first_id="0")


asyncio.run(stream_one_to_may_group())
