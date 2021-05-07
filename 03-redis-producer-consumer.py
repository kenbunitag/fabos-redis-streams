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

    p = await RedisClient.create("sensor1")
    c = await RedisClient.create("consumer1")

    await asyncio.gather(
        p.produce(range(3)), 
        c.consume(["sensor1"], 3)
        )

    c = await RedisClient.create("consumer2")
    await asyncio.gather(
        p.produce(range(3, 6)), 
        c.consume(["sensor1"], 6)
        )

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

async def stream_transactions():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
    await r.flushdb()

    N = 1_000

    start = time.time()
    for i in range(N):
        await r.xadd("test", dict(i=i))
    end = time.time()
    print(f"one by one duration: {end - start}")

    result = await r.xread(["test"], timeout=None, latest_ids=["0"])
    print(f"entries in stream: {len(result)}")
    await r.delete("test")

    
    start = time.time()
    futs = []
    for i in range(N):
        futs.append(r.xadd("test", dict(i=i)))
    await asyncio.gather(*futs)
    end = time.time()
    print(f"iterative duration: {end - start}")

    result = await r.xread(["test"], timeout=None, latest_ids=["0"])
    print(f"entries in stream: {len(result)}")
    await r.delete("test")


    start = time.time()
    m : aioredis.commands.transaction.MultiExec = r.multi_exec()
    futs = []
    for i in range(N):
        futs.append(m.xadd("test", dict(i=i)))
    await m.execute()
    end = time.time()
    print(f"transaction duration: {end - start}")

    result = await r.xread(["test"], timeout=None, latest_ids=["0"])
    print(f"entries in stream: {len(result)}")
    #await r.delete("test")
    

async def main():
    #await stream_one_to_one()
    #await stream_one_to_many()
    #await stream_many_to_one()

    #await stream_one_to_may_group()

    #await stream_one_to_one_group()

    await stream_transactions()

    #await scan(rp)

asyncio.run(main())