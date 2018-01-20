# Eventlog

A simple event log backed by dynamodb and dynamodb streams.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `eventlog` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:eventlog, "~> 0.1.0"}
  ]
end
```


## Eventlog
```elixir
config :eventlog, table: "table_name"
```

```elixir
:ok = Eventlog.append("xxxx-xxxx-xxxx-xxxx", %YourEventHere{})
```

### Setup Eventlog tables
```sh
:ok = Eventlog.setup
```

## Consumers

```elixir
defmodule Consumer do
  use Eventlog.Consumer

  def handle_records(events) do
    :ok
  end
end
```

```elixir
config :eventlog, Consumer, table: "consumer_leases_table_name"
```

### Setup consumer tables

```sh
mix run "Consumer.setup"
```


