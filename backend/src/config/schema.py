"""Create missing tables/columns on startup so deployed volumes pick up lease fields."""
from sqlalchemy import inspect,text
from src.config.database import Base,engine
# Only additive changes: never rewrite an accepted trace snapshot or result.
_ADDITIVE={
 'attempt':'INTEGER NOT NULL DEFAULT 0',
 'recoveries':'INTEGER NOT NULL DEFAULT 0',
 'owner':'VARCHAR(160)',
 'lease_expires_at':'TIMESTAMP',
 'heartbeat_at':'TIMESTAMP',
 'dispatched_at':'TIMESTAMP',
 'started_at':'TIMESTAMP',
 'completed_at':'TIMESTAMP',
 'updated_at':'TIMESTAMP',
}
def ensure_schema():
 from src.models import identity,dataset,plan,trace  # noqa: F401
 Base.metadata.create_all(engine)
 inspector=inspect(engine)
 if 'traces' not in inspector.get_table_names():return
 present={c['name'] for c in inspector.get_columns('traces')}
 with engine.begin() as c:
  for name,ddl in _ADDITIVE.items():
   if name not in present:c.execute(text(f'ALTER TABLE traces ADD COLUMN {name} {ddl}'))
