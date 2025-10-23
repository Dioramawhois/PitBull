import os
from faststream import FastStream
from faststream.redis import RedisBroker
from loguru import logger

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

logger.info(f"Connecting to Redis at {redis_url}")
broker = RedisBroker(url=redis_url)
stream_app = FastStream(broker)


@broker.subscriber("in-channel")
@broker.publisher("out-channel")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
