defmodule Eventlog.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Registry.Shards},
      {Task.Supervisor, name: Eventlog.Tasks},
      {Eventlog.Store, []},
      {Eventlog.ShardSupervisor, []},
    ]

    opts = [strategy: :one_for_one, name: __MODULE__]
    Supervisor.start_link(children, opts)
  end
end
