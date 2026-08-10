"""Subscribe to live measurement streams from sqtseries.

Two approaches demonstrated: raw ZMQ SUB socket and the Python Client.
Every measurement accepted by sqtseries is immediately republished to
all SUB sockets that match the topic prefix.

Usage:
    python3 examples/stream_examples.py          # subscribe to all topics
    python3 examples/stream_examples.py cpu.     # subscribe to cpu.* only
    python3 examples/stream_examples.py --ws     # use WebSocket
"""

import argparse

import orjson
import zmq

STREAM_PORT = 12503
HTTP_PORT = 12505


def subscribe_zmq(host: str, port: int, topic: str):
    """Raw ZMQ SUB socket — works from any language with a ZMQ binding."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{host}:{port}")
    sock.setsockopt(zmq.SUBSCRIBE, topic.encode() if topic != "*" else b"")

    print(f"Subscribed via ZMQ SUB to {host}:{port} (prefix: '{topic}')")
    print("Waiting for measurements... (Ctrl+C to stop)")
    try:
        while True:
            topic_bytes, payload = sock.recv_multipart()
            data = orjson.loads(payload)
            metric = data.get("metric", "?")
            value = data.get("value", 0)
            tags = data.get("tags", {})
            print(f"[zmq] {topic_bytes.decode():20s} {metric}={value}  {tags}")
    except KeyboardInterrupt:
        print("stopped")
    finally:
        sock.close(linger=0)
        ctx.term()


def subscribe_client(host: str, topic: str, port: int = STREAM_PORT):
    """Python Client's subscribe() — wraps ZMQ SUB with a generator API."""
    from sqtseries.client import Client

    ports = {"subscribe": port} if host == "127.0.0.1" else {}
    client = Client(host=host, ports=ports or None)

    print(f"Subscribed via Client to {host} (prefix: '{topic}')")
    print("Waiting for measurements... (Ctrl+C to stop)")
    try:
        for payload in client.subscribe(topic):
            if payload is None:
                continue
            metric = payload.get("metric", "?")
            value = payload.get("value", 0)
            print(f"[client] {metric}={value}")
    except KeyboardInterrupt:
        print("stopped")
    finally:
        client.close()


def subscribe_websocket(host: str, topic: str, http_port: int = HTTP_PORT):
    """Connect via WebSocket — works from a browser or any WebSocket client."""
    import asyncio

    import websockets

    async def _listen():
        uri = f"ws://{host}:{http_port}/ws/subscribe?metric={topic}"
        print(f"Connecting to {uri}")
        async with websockets.connect(uri) as ws:
            print("Connected. Waiting for measurements... (Ctrl+C to stop)")
            while True:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = orjson.loads(msg)
                    if data.get("type") == "ping":
                        continue
                    print(f"[ws] {data.get('metric','?')}={data.get('value',0)}")
                except TimeoutError:
                    print("[ws] ping (idle)")

    try:
        asyncio.run(_listen())
    except KeyboardInterrupt:
        print("stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to live measurements from sqtseries"
    )
    parser.add_argument(
        "topic", nargs="?", default="*", help="Topic prefix to subscribe to (* = all)"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=STREAM_PORT)
    parser.add_argument(
        "--http-port",
        type=int,
        default=HTTP_PORT,
        help="HTTP/WebSocket gateway port (for --ws)",
    )
    parser.add_argument(
        "--ws", action="store_true", help="Use WebSocket instead of ZMQ SUB"
    )
    parser.add_argument(
        "--client", action="store_true", help="Use the Python Client instead of raw ZMQ"
    )
    args = parser.parse_args()

    if args.ws:
        subscribe_websocket(args.host, args.topic, args.http_port)
    elif args.client:
        subscribe_client(args.host, args.topic, args.port)
    else:
        subscribe_zmq(args.host, args.port, args.topic)


if __name__ == "__main__":
    main()
