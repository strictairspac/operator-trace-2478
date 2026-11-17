import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://trace:tracepass@db:5432/operatortrace')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
# A healthy worker refreshes its lease every HEARTBEAT_SECONDS. A lease that is
# not refreshed for LEASE_SECONDS is considered lost and may be taken over.
LEASE_SECONDS=max(3,int(os.getenv('TRACE_LEASE_SECONDS','30')))
HEARTBEAT_SECONDS=max(1,int(os.getenv('TRACE_HEARTBEAT_SECONDS',str(max(1,LEASE_SECONDS//3)))))
SWEEP_SECONDS=max(1,int(os.getenv('TRACE_SWEEP_SECONDS','5')))
# How long a dispatched-but-unclaimed message may wait before it is sent again.
REDELIVER_SECONDS=max(SWEEP_SECONDS,int(os.getenv('TRACE_REDELIVER_SECONDS','30')))
WORKER_CONCURRENCY=max(1,int(os.getenv('WORKER_CONCURRENCY','4')))
