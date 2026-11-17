"""Redis is only a wake-up channel; PostgreSQL trace rows are the source of truth."""
import redis
from src.config.settings import REDIS_URL
QUEUE_KEY='operator-traces'
_client=None
def client():
 global _client
 if _client is None:
  _client=redis.Redis.from_url(REDIS_URL,socket_connect_timeout=2,socket_timeout=2,decode_responses=True)
 return _client
def reset():
 global _client
 _client=None
def enqueue(trace_id):
 client().rpush(QUEUE_KEY,str(trace_id))
def take(timeout=1):
 return client().blpop(QUEUE_KEY,timeout=timeout)
def ping():
 return client().ping()
