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


async def stream_one_to_one_group():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
    await r.flushdb()
    utils.overall_start = time.time()

    p = await RedisClient.create("requests")
    c1 : RedisClient = await RedisClient.create("client1")

    await p.produce(range(3), delay=0)
    await r.xgroup_create("requests", "my_group", latest_id="0", mkstream=True)

    #pending = await r.xpending("requests", "my_group")
    #print("pending:", pending)
    #info = await r.xinfo_groups("requests")
    #print("xinfo_groups:", info)

    await c1.consume_group("my_group", ["requests"], None, do_ack=0, delay=0)
    c1.log("simulating failure")
    await c1.consume_group("my_group", ["requests"], None, do_ack=0, delay=0, first_id="0")
    c1.log("simulating permanent failure")


    #pending = await r.xpending("requests", "my_group")
    #print("pending:", pending)
    #info = await r.xinfo_groups("requests")
    #print("xinfo_groups:", info)


    c2 : RedisClient = await RedisClient.create("client2")
    await c2.claim_all("requests", "my_group", "client1")

    await c2.consume_group("my_group", ["requests"], None, do_ack=1, delay=0, first_id="0")


asyncio.run(stream_one_to_one_group())