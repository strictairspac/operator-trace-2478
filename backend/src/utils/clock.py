from datetime import datetime,timezone
def utcnow():
 """Naive UTC timestamps: PostgreSQL DateTime columns and SQLite both store them."""
 return datetime.now(timezone.utc).replace(tzinfo=None)
