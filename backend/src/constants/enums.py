ROLES=('admin','operator','viewer')
# pending: accepted and persisted, but not yet handed to Redis (redelivery pending)
# queued: handed to Redis, waiting for a worker to claim it
STATUSES=('pending','queued','running','completed','failed')
OPERATORS=('scan','filter','hash_join','group')
