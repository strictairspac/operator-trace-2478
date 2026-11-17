import os,logging,threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from src.config.database import engine
from src.config.schema import ensure_schema
from src.config.settings import SWEEP_SECONDS
from src.services import queueing,traces
from src.routes import auth,datasets,plans,traces as trace_routes,audit
from src.middlewares.errors import install_errors
from src.middlewares.requests import RequestTrace
STOP=threading.Event()
def _sweeper():
 while not STOP.wait(SWEEP_SECONDS):
  try:traces.sweep()
  except Exception:logging.exception('trace sweep failed')
@asynccontextmanager
async def lifespan(app):
 try:ensure_schema()
 except Exception:logging.exception('schema check failed; API remains available')
 if os.getenv('SEED_DEMO','true').lower()=='true':
  try:
   from scripts.seed_demo import seed
   seed()
  except Exception:logging.exception('seed failed; API remains available')
 # Recovery does not depend on a worker being alive: the API re-queues
 # accepted-but-undelivered traces and expires lost leases on its own.
 threading.Thread(target=_sweeper,daemon=True,name='trace-sweeper').start()
 yield
 STOP.set()
app=FastAPI(title='松原 · 分析算子执行剖面仪',lifespan=lifespan)
app.add_middleware(RequestTrace);install_errors(app)
for r in (auth,datasets,plans,trace_routes,audit):app.include_router(r.router)
@app.get('/health')
def health():
 with engine.connect() as c:c.execute(text('SELECT 1'))
 queueing.ping();return {'status':'ok'}
