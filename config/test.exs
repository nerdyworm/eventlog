use Mix.Config

config :eventlog, [
  table: "Eventlog.Events.Test",
  leases: "Eventlog.Leases.Test",
  stream: "arn:aws:dynamodb:us-east-1:907015576586:table/Eventlog.Events.Test/stream/2017-12-15T19:08:27.650",
  retries: 5,
  shard_syncer_start_timeout: 1000
]

# Store
#
# table name
# append retry limits

# Consumer
#
# stream name
# lease table name
# dead table name
# syncer intervals
