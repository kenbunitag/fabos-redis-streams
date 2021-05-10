import asyncio
import os

import aioredis
import msgpack

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

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

async def main():
    r : aioredis.Redis = await aioredis.create_redis(f'redis://{REDIS_HOST}:{REDIS_PORT}/0')

    await r.flushdb()

    await r.set("mykey", "someval")
    print(await r.get("mykey"))

    await r.set("mydata", msgpack.packb(dict(a=1, b=2)))
    print(msgpack.unpackb(await r.get("mydata")))

    await r.xadd("mystream", dict(msg="hello!"))
    await dump_stream(r, "mystream")

    await r.xadd("mystream", dict(msg="second msg", key="another value"))
    await dump_stream(r, "mystream")

    await scan(r)

asyncio.run(main())
