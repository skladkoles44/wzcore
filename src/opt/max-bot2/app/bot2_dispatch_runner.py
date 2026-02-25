import os, asyncio, signal, logging
import httpx
from notify_dispatch import start_notify_dispatch, stop_notify_dispatch

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("max-bot2-dispatch")

API_BASE = os.getenv("API_BASE", "https://platform-api.max.ru").rstrip("/")
BOT2_TOKEN = os.getenv("BOT2_TOKEN", "").strip()
if not BOT2_TOKEN:
    raise RuntimeError("BOT2_TOKEN missing")

_timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
_client: httpx.AsyncClient | None = None

async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=_timeout)
    return _client

async def send_max_func(text: str, user_id: int):
    client = await _get_client()
    headers = {"Authorization": BOT2_TOKEN}
    params = {"user_id": int(user_id)}
    r = await client.post(f"{API_BASE}/messages", params=params, json={"text": (text or "")}, headers=headers)
    try:
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error("MAX send HTTP %s body=%s", e.response.status_code, e.response.text)
        raise

_stop = asyncio.Event()
def _sig(*_): _stop.set()

async def main():
    logger.info("bot2_dispatch_runner: starting")
    start_notify_dispatch(send_max_func)
    await _stop.wait()
    logger.info("bot2_dispatch_runner: stopping")
    await stop_notify_dispatch()
    if _client is not None:
        await _client.aclose()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _sig)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _sig())
    loop.run_until_complete(main())
