defmodule Eventlog.ShardReader do
  require Logger

  alias Eventlog.{Lease, Leases, Shards, Dispatcher}

  defmodule State do
    defstruct [consumer: nil, lease: nil, iterator: nil, pending: nil]
  end

  def start_link(consumer, %Lease{} = lease) do
    GenServer.start_link(__MODULE__, {consumer, lease}, name: via_tuple(lease))
  end

  def via_tuple(%Lease{shard_id: shard_id}) do
    {:via, Registry, {Registry.Shards, shard_id}}
  end

  def init({consumer, %Lease{} = lease}) do
    Logger.info "[eventlog] starting reader shard_id=#{lease.shard_id} checkpoint=#{lease.checkpoint} counter=#{lease.counter}"
    send(self(), :pull)
    Process.send_after(self(), :renew, 5000)
    {:ok, %State{consumer: consumer, lease: lease}}
  end

  def handle_info(:ack, %State{consumer: consumer, lease: lease, pending: pending} = state) do
    case Leases.checkpoint(consumer, lease, pending) do
      {:ok, lease} ->
        send(self(), :pull)
        {:noreply, %State{state | lease: lease, pending: pending}}

      {:error, :stolen} ->
        IO.puts "the lease was stolen.... lease=#{ inspect lease } pending=#{ pending }"
        {:stop, :shutdown, state}
    end
  end

  def handle_info(:nack, %State{consumer: consumer, pending: pending, lease: lease} = state) do
    IO.puts "nack... just keep rolling?"

    send(self(), :pull)
    {:ok, lease} = Leases.checkpoint(consumer, lease, pending)
    {:noreply, %State{state | lease: lease, pending: pending}}
  end

  def handle_info(:pull, %State{lease: %Lease{checkpoint: "SHARD_END"}} = state) do
    {:stop, :shutdown, state}
  end

  def handle_info(:pull, %State{consumer: consumer, iterator: iterator, lease: lease} = state) when is_nil(iterator) do
    case Shards.get_iterator(consumer.stream(), lease) do
      {:error, {"ValidationException", message}} ->
        {:stop, message, state}

      {:ok, %{"ShardIterator" => iterator}} ->
        send(self(), :pull)
        {:noreply, %State{state | iterator: iterator}}
    end
  end

  def handle_info(:pull, %State{iterator: iterator} = state) do
    case Shards.get_records(iterator) do
      {:error, response} ->
        :ok = Logger.error "Error getting records: #{ inspect response }"
        {:noreply, state}

      {:ok, response} ->
        handle_records(response, state)
    end
  end

  def handle_info(:renew, %State{consumer: consumer, lease: lease} = state) do
    case Leases.renew(consumer, lease) do
      {:ok, lease} ->
        Process.send_after(self(), :renew, 5000)
        {:noreply, %State{state | lease: lease}}

      {:error, :lost} ->
        Logger.error "lease failed to renew: #{inspect lease}"
        {:stop, :shutdown, state}
    end
  end

  defp handle_records(%{"Records" => [], "NextShardIterator" => iterator}, state) do
    Process.send_after(self(), :pull, 2000)
    {:noreply,  %State{state | iterator: iterator}}
  end

  defp handle_records(%{"Records" => records, "NextShardIterator" => iterator}, state) do
    :ok = run(records, state)
    checkpoint = records |> List.last() |> Shards.checkpoint_for_record()
    {:noreply,  %State{state | iterator: iterator, pending: checkpoint}}
  end

  # No iterator, shard has ended
  defp handle_records(%{"Records" => records}, state) do
    :ok = run(records, state)
    {:noreply,  %State{state | iterator: nil, pending: "SHARD_END"}}
  end

  defp run(records, %State{consumer: consumer}) do
    events = Eventlog.Storage.parse_records(records)
    Dispatcher.dispatch_records(self(), consumer, events)
  end
end

