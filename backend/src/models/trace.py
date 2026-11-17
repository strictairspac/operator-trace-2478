from sqlalchemy import Column,Integer,String,JSON,DateTime,func,ForeignKey
from src.config.database import Base
class Trace(Base):
 __tablename__='traces'
 id=Column(Integer,primary_key=True)
 name=Column(String(120),unique=True,nullable=False)
 dataset_id=Column(Integer,ForeignKey('datasets.id'),nullable=False)
 plan_id=Column(Integer,ForeignKey('plans.id'),nullable=False)
 # pending -> queued -> running -> completed|failed
 status=Column(String(20),default='pending',nullable=False)
 result=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 snapshot=Column(JSON,nullable=False)
 # lease / fencing bookkeeping
 attempt=Column(Integer,nullable=False,server_default='0')
 recoveries=Column(Integer,nullable=False,server_default='0')
 owner=Column(String(160),nullable=True)
 lease_expires_at=Column(DateTime,nullable=True)
 heartbeat_at=Column(DateTime,nullable=True)
 dispatched_at=Column(DateTime,nullable=True)
 started_at=Column(DateTime,nullable=True)
 completed_at=Column(DateTime,nullable=True)
 updated_at=Column(DateTime,nullable=True)
 created_at=Column(DateTime,server_default=func.now())
