"""Tiny WebSocket relay. Host and viewer both connect OUT to it (port 443 via wss),
so it works through firewalls that only allow web traffic.

Run:  python relay.py            (listens on $PORT or 8765)
Put it behind TLS (Render/Fly/Caddy/nginx) so clients can use wss://.
"""
import asyncio
import json
import os
from urllib.parse import urlparse, parse_qs

import websockets

rooms = {}  # code -> {"host": ws, "viewer": ws | None}


async def handler(ws):
    q = parse_qs(urlparse(ws.request.path).query)
    role = q.get("role", [""])[0]
    code = q.get("code", [""])[0].upper()
    if not code or role not in ("host", "viewer"):
        await ws.close(1008, "bad request")
        return

    if role == "host":
        if code in rooms:
            await ws.close(4009, "code in use")
            return
        rooms[code] = {"host": ws, "viewer": None}
    else:
        room = rooms.get(code)
        if not room:
            await ws.close(4004, "no such session")
            return
        if room["viewer"]:
            await ws.close(4003, "session already has a viewer")
            return
        room["viewer"] = ws
        await room["host"].send(json.dumps({"t": "joined"}))

    room = rooms[code]
    peer_key = "viewer" if role == "host" else "host"
    try:
        async for msg in ws:
            peer = room[peer_key]
            if peer:
                await peer.send(msg)
    except websockets.ConnectionClosed:
        pass
    finally:
        if role == "host":
            rooms.pop(code, None)
            if room["viewer"]:
                await room["viewer"].close(1001, "host left")
        else:
            room["viewer"] = None
            try:
                await room["host"].send(json.dumps({"t": "left"}))
            except Exception:
                pass


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port, max_size=16 * 1024 * 1024):
        print(f"relay listening on :{port}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
