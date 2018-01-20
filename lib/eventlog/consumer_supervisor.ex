defmodule Eventlog.ConsumerSupervisor do
  @moduledoc false

  def start_link(handler) do
    children = [
      {Eventlog.Leases, handler},
      {Eventlog.ShardSyncer, handler},
      {Eventlog.LeaseTaker, handler},
      {Eventlog.LeaseCoordinator, handler},
    ]

    opts = [strategy: :one_for_one, name: handler]
    Supervisor.start_link(children, opts)
  end
end
