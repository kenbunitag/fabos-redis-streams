import aioredis
import os
import asyncio
import time
from typing import List
import random

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
overall_start = time.time()

class GroupMessageCounter:
    def __init__(self, expected_count):
        self.expected_count = expected_count
        self.remainig = expected_count

class RedisClient:


    def __init__(self, name : str, r : aioredis.Redis):
        self.name = name
        self.r = r

    @staticmethod
    async def create(name : str, r : aioredis.Redis = None) -> 'RedisClient':
        if r is None:
            r = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')
        return RedisClient(name, r)

    def log(self, msg):
        print(f"[{self.name:<9} {(time.time() - overall_start)*1000:5.1f}] {msg}")

    async def produce(self, produce_range, delay = 0.5):
        for i in produce_range:
            self.log(f"produce <{self.name}> i={i}")
            await self.r.xadd(self.name, dict(i=i, time=time.time() - overall_start))
            await asyncio.sleep(delay)

    async def consume(self, streams : List[str], expected_count : int):
        latest_ids = ["0" for s in streams]
        while True:
            result = await self.r.xread(streams, timeout=50, latest_ids=latest_ids)
            if expected_count == 0: 
                self.log("exit")
                break
            for e in result:
                stream, id, msg = e
                stream, id = stream.decode("UTF-8"), id.decode("UTF-8")
                #msg = ", ".join([f"{k.decode('UTF-8')}={v}" for k,v in msg.items()])
                self.log(f"consume <{stream}> id={id}, i={msg[b'i']}")
                latest_ids[streams.index(stream)] = id
                expected_count -= 1
            await asyncio.sleep(0)

    async def consume_group(self, group_name, streams : List[str], gmc : GroupMessageCounter, delay = 0.5, do_ack = 1, first_id=">"):
        latest_ids = [first_id for s in streams]
        while True:
            #print(result)
            if gmc is not None and gmc.remainig == 0: 
                self.log("exit")
                break
            result = await self.r.xread_group(group_name, self.name, streams, timeout=50, latest_ids=latest_ids, count=1)
            if gmc is None and len(result) == 0:
                self.log("exit")
                break
            for e in result:
                stream, id, msg = e
                stream, id = stream.decode("UTF-8"), id.decode("UTF-8")
                self.log(f"consume <{group_name}@{stream}> id={id}, i={msg[b'i']}")
                if first_id != ">":
                    latest_ids[streams.index(stream)] = id
                if gmc is not None:
                    gmc.remainig -= 1
                if random.random() < do_ack:
                    self.log(f"ack {id}")
                    await self.r.xack(stream, group_name, id)
                else:
                    self.log(f"NACK {id}")
            await asyncio.sleep(delay)

    async def claim_all(self, stream : str, group_name, from_name):
        pending = await self.r.xpending(stream, group_name, start="-", stop="+", count=999, consumer=from_name)
        for p in pending:
            id, client, delay, deliver_count = p
            self.log(f"claiming pending {id}")
            await self.r.xclaim(stream, group_name, self.name, 0, id)

async def dump_stream(r : aioredis.Redis, stream : str):
    result = await r.xread([stream], timeout=1, latest_ids=["0"])
    print(f"messages in stream '{stream}'")
    for e in result:
        stream, id, msg = e
        stream, id = stream.decode("UTF-8"), id.decode("UTF-8")
        msg = ", ".join([f"{k.decode('UTF-8')}={v}" for k,v in msg.items()])
        print(f"  {id} {msg}")

async def scan(r : aioredis.Redis):
    match = '*'
    cur = b'0'
    print("scan of all keys:")
    while cur:
        cur, keys = await r.scan(cur, match=match)
        for key in keys:
            t = await r.type(key)
            print(f"  {key.decode('UTF-8')} ({t})")