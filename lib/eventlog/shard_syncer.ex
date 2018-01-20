defmodule Eventlog.ShardSyncer do
  @moduledoc """
  Polls for new shards and creates leases for them
  """
  use GenServer
  require Logger

  alias Eventlog.{
    Leases,
    Shards,
  }

  @start_timeout Application.get_env(:eventlog, :shard_syncer_start_timeout, 10_000)
  @sync_interval Application.get_env(:eventlog, :shard_syncer_sync_interval, 30_000)

  def start_link(consumer) do
    GenServer.start_link(__MODULE__, consumer, name: name(consumer))
  end

  def name(consumer) do
    Module.concat(consumer, ShardSyncer)
  end

  def init(consumer) do
    {:ok, consumer, @start_timeout}
  end

  def sync(syncer) do
    GenServer.call(syncer, :sync)
  end

  def handle_info(:timeout,  state) do
    :ok = do_sync(state)
    {:noreply, state, @sync_interval}
  end

  def handle_call(:sync, _, state) do
    {:reply, do_sync(state), state}
  end

  defp do_sync(consumer) do
    {:ok, shards} = Shards.list_shards(consumer.stream())
    {:ok, leases} = Leases.list_leases(consumer)

    # shards with no leases
    Enum.each(shards, fn(shard) ->
      case Enum.find(leases, &(&1.shard_id == shard.shard_id)) do
        nil ->
          Leases.create(consumer, shard)

        lease ->
          lease
      end
    end)

    # delete leases with no shards
    Enum.each(leases, fn(lease) ->
      case Enum.find(shards, &(&1.shard_id == lease.shard_id)) do
        nil ->
          Leases.delete(consumer, lease)

        shard ->
          shard
      end
    end)

    :ok = Logger.debug "[eventlog] synced shards"
  end
end

