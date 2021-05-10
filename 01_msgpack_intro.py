import msgpack
import json
import numpy as np
import math
import time

def msgpack_decoder(obj):
    if '__np' in obj:
        shape = obj["s"]
        dt = np.dtype(obj["t"])
        arr = np.frombuffer(obj["d"], dtype=dt)
        arr = arr.reshape(shape)
        return arr
    return obj

def msgpack_encoder(obj):
    if isinstance(obj, np.ndarray):
        arr : np.ndarray = obj
        return dict(__np=True, t=arr.dtype.char, s=arr.shape, d=arr.tobytes())
    return obj

def json_encoder(obj):
    if isinstance(obj, np.ndarray):
        arr : np.ndarray = obj
        return dict(__np=True, t=arr.dtype.char, s=arr.shape, d=arr.tolist())
    return obj

def json_decoder(obj):
    if '__np' in obj:
        shape = obj["s"]
        dt = np.dtype(obj["t"])
        arr = np.array(obj["d"], dtype=dt)
        return arr
    return obj

def encode_decode(data):
    N = 1_000

    start = time.perf_counter()
    for _ in range(N):
        json_encoded = json.dumps(data, default=json_encoder)
        json_decoded = json.loads(json_encoded, object_hook=json_decoder)
    end = time.perf_counter()
    json_duration = end - start

    start = time.perf_counter()
    for _ in range(N):
        msgpack_encoded = msgpack.packb(data, default=msgpack_encoder, use_bin_type=True)
        msgpack_decoded = msgpack.unpackb(msgpack_encoded, object_hook=msgpack_decoder, raw=False)
    end = time.perf_counter()
    msgpack_duration = end - start

    print(f"data: {data}")
    print(f"  json:    {len(json_encoded.encode('UTF-8'))} bytes, {json_duration:.3f} s")
    print(f"  msgpack: {len(msgpack_encoded)} bytes, {msgpack_duration:.3f} s")
    #print(f"  json: {json_encoded} ({len(json_encoded.encode('UTF-8'))} bytes)")
    #print(f"  msgpack: {msgpack_encoded} ({len(msgpack_encoded)} bytes)")

    return (json_decoded, msgpack_decoded)

data = {"key1": 5, "key2": [0,1,2,3,4]}
encode_decode(data)

data = dict(arr=np.linspace(0, math.pi, 1800, dtype=np.float64))
json_decoded, msgpack_decoded = encode_decode(data)
