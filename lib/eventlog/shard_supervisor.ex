defmodule Eventlog.ShardSupervisor do
  alias Eventlog.{ShardReader, Lease}

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def start_worker(handler, lease) do
    Supervisor.start_child(__MODULE__, [handler, lease])
  end

  def stop_worker(%Lease{shard_id: shard_id}) do
    case Registry.lookup(Registry.Shards, shard_id) do
      [{pid, _}] ->
        :ok = Supervisor.terminate_child(__MODULE__, pid)

      [] ->
        :ok
    end
  end

  def init(_) do
    children = [
      worker(ShardReader, [], restart: :temporary)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end
end
