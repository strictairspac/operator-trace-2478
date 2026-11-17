"""Leased, concurrent trace executor.

Every execution holds a lease (owner + attempt) that a heartbeat thread keeps
alive. The work is always computed from the snapshot stored when the trace was
accepted. All result writes are fenced: if the lease was taken over, the old
executor's write is rejected. On shutdown an in-flight execution releases its
lease at once so another worker can take the *same* trace over; if the process
dies without running that release, the lease simply expires.
"""
import logging,os,signal,socket,threading,time
from pathlib import Path
from src.config.settings import HEARTBEAT_SECONDS,SWEEP_SECONDS,WORKER_CONCURRENCY
from src.services import queueing,traces
from src.services.operators import execute

STOP=threading.Event()
HEARTBEAT_FILE=Path(os.getenv('WORKER_HEARTBEAT_FILE','/tmp/worker-heartbeat'))
_claims={}
_claims_lock=threading.Lock()

def worker_id():
 return f'{socket.gethostname()}:{os.getpid()}'

def _register(key,info):
 with _claims_lock:_claims[key]=info

def _unregister(key):
 with _claims_lock:_claims.pop(key,None)

def _release_interrupted():
 """Hand in-flight leases back for immediate takeover."""
 with _claims_lock:
  items=[(k,v[0],v[1]) for k,v in _claims.items()]
 for key,owner,attempt in items:
  try:
   if traces.release(key,owner,attempt,'执行者停机，已交还租约'):
    logging.info('released interrupted trace %s attempt=%s',key,attempt)
  except Exception:logging.exception('could not release trace %s',key)
  _unregister(key)

def _heartbeat_loop(key,owner,attempt,lost,done):
 while not done.wait(HEARTBEAT_SECONDS):
  if STOP.is_set():return
  try:
   if not traces.heartbeat(key,owner,attempt):lost.set();return
  except Exception:logging.exception('heartbeat failed for trace %s',key)

def run_one(trace_id,worker=None):
 """Claim and execute one trace. Returns an outcome string for tests/logs."""
 worker=worker or worker_id()
 claim=traces.claim(trace_id,worker)
 if not claim:return 'skipped'
 key,owner,attempt=claim['id'],claim['owner'],claim['attempt']
 done=threading.Event();lost=threading.Event()
 _register(key,(owner,attempt))
 beat=threading.Thread(target=_heartbeat_loop,args=(key,owner,attempt,lost,done),daemon=True)
 beat.start()
 try:
  try:
   result=execute(claim['snapshot']['dataset'],claim['snapshot']['plan']['steps'])
  except Exception as exc:
   # Operator/limit failures are deterministic business errors: keep them
   # failed and do not retry or overwrite an earlier result.
   if lost.is_set():return 'fenced'
   try:ok=traces.fail(key,owner,attempt,exc)
   except Exception:
    logging.exception('could not record failure for trace %s; lease will expire',key)
    return 'error'
   if ok:
    traces.log('worker','trace.failed',f'{key} attempt={attempt}')
    return 'failed'
   return 'fenced'
  if lost.is_set():return 'fenced'
  if STOP.is_set():
   try:traces.release(key,owner,attempt,'执行者停机，已交还租约')
   except Exception:logging.exception('could not release trace %s on shutdown',key)
   return 'released'
  try:ok=traces.complete(key,owner,attempt,result)
  except Exception:
   logging.exception('could not persist result for trace %s; lease will expire',key)
   return 'error'
  if ok:
   traces.log('worker','trace.completed',f'{key} attempt={attempt}')
   return 'completed'
  logging.warning('discarded stale result for trace %s attempt=%s',key,attempt)
  return 'fenced'
 finally:
  done.set();_unregister(key)

def consume():
 while not STOP.is_set():
  try:item=queueing.take(timeout=1)
  except Exception:
   logging.exception('queue read failed; retrying')
   STOP.wait(2);continue
  if not item:continue
  try:trace_id=int(item[1])
  except (TypeError,ValueError):
   logging.warning('ignored malformed queue item %r',item);continue
  try:run_one(trace_id)
  except Exception:logging.exception('execution crashed for trace %s',trace_id)

def _sweeper():
 while not STOP.is_set():
  try:
   HEARTBEAT_FILE.touch()
   traces.sweep()
  except Exception:logging.exception('sweep failed')
  STOP.wait(SWEEP_SECONDS)

def _stop(signum,frame):
 logging.info('signal %s received; releasing leases',signum)
 STOP.set();_release_interrupted()

def main():
 logging.basicConfig(level=logging.INFO)
 signal.signal(signal.SIGTERM,_stop)
 signal.signal(signal.SIGINT,_stop)
 HEARTBEAT_FILE.touch()
 threading.Thread(target=_sweeper,daemon=True,name='sweeper').start()
 logging.info('worker %s starting with %d slots',worker_id(),WORKER_CONCURRENCY)
 threads=[threading.Thread(target=consume,daemon=True,name=f'slot-{i}') for i in range(WORKER_CONCURRENCY)]
 for t in threads:t.start()
 while not STOP.is_set():time.sleep(1)
 for t in threads:t.join(timeout=5)
 _release_interrupted()

if __name__=='__main__':main()
