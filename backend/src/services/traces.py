"""Trace lifecycle: durable acceptance, at-least-once dispatch, leased execution.

PostgreSQL is the source of truth. Redis only carries wake-up messages, so a
Redis outage never fails an accepted trace: it stays ``pending`` until the
dispatcher can deliver it. A worker that stops running leaves a lease that
expires; the sweeper then re-queues the *same* row (same id, same input
snapshot) for takeover. Every write from an executor is fenced by
``owner`` + ``attempt``, so a resurrected old executor cannot overwrite a
newer attempt's result.
"""
import logging
from datetime import timedelta
from sqlalchemy import or_,update
from src.config.database import Session
from src.config.settings import LEASE_SECONDS,REDELIVER_SECONDS
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services import queueing
from src.services.audit import log
from src.utils.clock import utcnow
from src.utils.serialization import record

ACTIVE=('pending','queued')
LOST_MESSAGE='执行者失联，已自动恢复原记录并等待重新接管'

def list_items():
 with Session() as s:return [record(r) for r in s.query(Trace).order_by(Trace.id.desc()).limit(100).all()]

def _missing():
 from fastapi import HTTPException
 return HTTPException(404,'未找到执行轨迹')

def detail(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise _missing()
  return record(row)

def create(data):
 from fastapi import HTTPException
 with Session() as s:
  p=s.get(Plan,data.plan_id)
  if not p:raise HTTPException(404,'未找到算子计划')
  d=s.get(Dataset,p.dataset_id)
  # Snapshot the accepted input and plan now; every attempt uses this copy.
  row=Trace(name=data.name,plan_id=p.id,dataset_id=d.id,status='pending',snapshot={'dataset':record(d),'plan':record(p)},attempt=0,recoveries=0,updated_at=utcnow())
  s.add(row);s.commit();s.refresh(row);trace_id=row.id
 # Delivery is separate from acceptance: a Redis outage leaves the accepted
 # trace pending for redelivery instead of failing it.
 try:
  dispatch(trace_id)
 except Exception:
  logging.exception('dispatch deferred for trace %s',trace_id)
 log('system','trace.accepted',str(trace_id))
 return detail(trace_id)

def dispatch(trace_id):
 """Push a wake-up message for an accepted trace. Safe to call repeatedly."""
 now=utcnow()
 with Session() as s:
  current=s.query(Trace.status).filter(Trace.id==trace_id).scalar()
 if current not in ACTIVE:return False
 try:queueing.enqueue(trace_id)
 except Exception:
  logging.warning('queue unavailable while dispatching trace %s',trace_id)
  return False
 with Session() as s:
  # Only flip to queued after the message is really in Redis. Losing this
  # race is harmless: the duplicate message is deduplicated by claim().
  s.execute(update(Trace).where(Trace.id==trace_id,Trace.status.in_(ACTIVE)).values(status='queued',dispatched_at=now,updated_at=now).execution_options(synchronize_session=False))
  s.commit()
 return True

def dispatch_pending():
 """Deliver pending traces and re-deliver queued ones whose message may be lost."""
 now=utcnow()
 with Session() as s:
  rows=s.query(Trace.id,Trace.status,Trace.dispatched_at).filter(Trace.status.in_(('pending','queued'))).order_by(Trace.id).limit(500).all()
 for trace_id,status,dispatched_at in rows:
  if status=='queued' and not _older_than(dispatched_at,now,REDELIVER_SECONDS):continue
  try:dispatch(trace_id)
  except Exception:logging.exception('dispatch sweep failed for trace %s',trace_id)

def _older_than(value,now,seconds):
 return value is None or (now-value).total_seconds()>=seconds

def claim(trace_id,worker_id):
 """Atomically take ownership. Returns claim info or None if unavailable.

 The single conditional UPDATE is the concurrency gate: only one executor can
 move pending/queued -> running, and each successful claim bumps ``attempt``.
 """
 import uuid
 from sqlalchemy import func
 now=utcnow()
 owner=f'{worker_id}:{uuid.uuid4().hex[:12]}'
 with Session() as s:
  res=s.execute(
   update(Trace)
   .where(Trace.id==trace_id,Trace.status.in_(ACTIVE))
   .values(status='running',owner=owner,attempt=Trace.attempt+1,
           started_at=func.coalesce(Trace.started_at,now),heartbeat_at=now,
           lease_expires_at=now+timedelta(seconds=LEASE_SECONDS),updated_at=now,
           error=None,dispatched_at=None)
   .execution_options(synchronize_session=False))
  s.commit()
  if res.rowcount!=1:return None
 with Session() as s:
  row=s.get(Trace,trace_id)
  if not row:return None
  return {'id':row.id,'owner':row.owner,'attempt':row.attempt,'snapshot':row.snapshot,'recoveries':row.recoveries}

def heartbeat(trace_id,owner,attempt):
 """Refresh the lease. False means ownership was fenced away."""
 now=utcnow()
 with Session() as s:
  res=s.execute(update(Trace).where(Trace.id==trace_id,Trace.status=='running',Trace.owner==owner,Trace.attempt==attempt).values(heartbeat_at=now,lease_expires_at=now+timedelta(seconds=LEASE_SECONDS),updated_at=now).execution_options(synchronize_session=False))
  s.commit()
  return res.rowcount==1

def complete(trace_id,owner,attempt,result):
 """Persist the whole result and the completed flag in one fenced statement."""
 now=utcnow()
 with Session() as s:
  res=s.execute(update(Trace).where(Trace.id==trace_id,Trace.status=='running',Trace.owner==owner,Trace.attempt==attempt).values(result=result,status='completed',error=None,owner=None,lease_expires_at=None,completed_at=now,updated_at=now).execution_options(synchronize_session=False))
  s.commit()
  return res.rowcount==1

def fail(trace_id,owner,attempt,error):
 """A business error (limits, invalid data) is terminal; never retried."""
 now=utcnow()
 with Session() as s:
  res=s.execute(update(Trace).where(Trace.id==trace_id,Trace.status=='running',Trace.owner==owner,Trace.attempt==attempt).values(status='failed',error=str(error)[:1000],result=None,owner=None,lease_expires_at=None,completed_at=now,updated_at=now).execution_options(synchronize_session=False))
  s.commit()
  return res.rowcount==1

def release(trace_id,owner,attempt,error):
 """Hand an interrupted lease back for redelivery of the same record."""
 now=utcnow()
 with Session() as s:
  res=s.execute(update(Trace).where(Trace.id==trace_id,Trace.status=='running',Trace.owner==owner,Trace.attempt==attempt).values(status='pending',owner=None,lease_expires_at=None,dispatched_at=None,recoveries=Trace.recoveries+1,error=str(error)[:1000],updated_at=now).execution_options(synchronize_session=False))
  s.commit()
  return res.rowcount==1

def reap_expired(now=None):
 """Re-queue traces whose executor stopped refreshing its lease.

 ``lease_expires_at IS NULL`` covers rows left ``running`` before leases
 existed (schema v1 upgrade) or by a writer that crashed mid-claim; they are
 abandoned by construction because every live claim sets a lease.
 """
 now=now or utcnow()
 with Session() as s:
  expired=[(r.id,r.attempt,r.owner) for r in s.query(Trace).filter(Trace.status=='running',or_(Trace.lease_expires_at==None,Trace.lease_expires_at<now)).all()]
 recovered=[]
 for trace_id,attempt,owner in expired:
  stamp=utcnow()
  with Session() as s:
   # Re-check the lease in the same statement: a heartbeat that landed after
   # our read keeps the execution alive.
   res=s.execute(update(Trace).where(Trace.id==trace_id,Trace.status=='running',Trace.owner==owner,Trace.attempt==attempt,or_(Trace.lease_expires_at==None,Trace.lease_expires_at<stamp)).values(status='pending',owner=None,lease_expires_at=None,dispatched_at=None,recoveries=Trace.recoveries+1,error=LOST_MESSAGE,updated_at=stamp).execution_options(synchronize_session=False))
   s.commit()
  if res.rowcount==1:recovered.append(trace_id)
 for trace_id in recovered:log('system','trace.recovered',str(trace_id))
 return recovered

def sweep():
 """One recovery/dispatch cycle; safe to run concurrently in several processes."""
 reap_expired()
 dispatch_pending()
