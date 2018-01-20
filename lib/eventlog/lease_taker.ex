defmodule Eventlog.LeaseTaker do
  @moduledoc """
  Takes leases that need to be worked
  """
  use GenServer


  alias Eventlog.{
    Lease,
    Leases,
  }

  defmodule State do
    defstruct [leases: [], times: %{}, consumer: nil]
  end

  def start_link(consumer) do
    GenServer.start_link(__MODULE__, consumer, name: name(consumer))
  end

  def name(consumer) do
    Module.concat(consumer, LeaseTaker)
  end

  def init(consumer) do
    {:ok, %State{consumer: consumer}}
  end

  def take(consumer) do
    GenServer.call(name(consumer), :take)
  end

  def handle_call(:take, _, %State{leases: old_leases, times: times, consumer: consumer} = state) do
    start_time = :os.system_time(:milli_seconds)

    {:ok, leases} = Leases.list_leases(consumer)

    # calculate the last time the leases were updated
    new_times = track_leases(leases, old_leases, times, start_time)
    expired = get_expired_leases(leases, new_times, start_time, stale_after())

    counts = compute_lease_counts(leases, expired)
    num_leases = length(leases)
    num_workers = length(Map.keys(counts))

    # TODO -  handle spill over
    target = compute_target(num_leases, num_workers, max_leases())

    my_count = Map.get(counts, name())
    leases_to_reach_target = target - my_count

    # IO.puts "target: #{target}"
    # IO.puts "my_count: #{my_count}"
    # IO.puts "expired leases: `#{Enum.map(expired, &(&1.shard_id)) |> Enum.join(", ")}`"

    taken = take_leases(expired, leases_to_reach_target, consumer)
    state = %State{state | leases: leases, times: new_times}
    {:reply, {:ok, taken}, state}
  end

  # TODO - balance cluster
  #
  # If one worker has 10 leases and everyone else only has
  # one then we should steal a lease from that worker
  def take_leases(leases, take, consumer) do
    leases
    |> Enum.filter(&Lease.data?/1)
    |> Enum.shuffle() # shuffle all the leases so that workers don't contend for the same lease
    |> Enum.take(take)
    |> Enum.map(fn(lease) ->
      case Leases.take(consumer, lease, name()) do
        {:ok, lease} ->
          lease
        :lost ->
          nil
      end
    end)
    |> Enum.filter(&(&1 != nil))
  end

  def track_leases(fresh, old, times, start_time) do
    Enum.reduce(fresh, times, fn(new_lease, acc) ->
      shard_id = new_lease.shard_id
      old_lease = Enum.find(old, &(&1.shard_id == shard_id))

      if old_lease do
        if old_lease.counter == new_lease.counter do
          acc
        else
          Map.put(acc, shard_id, start_time)
        end
      else
        if new_lease.owner == nil do
          Map.put(acc, shard_id, 0)
        else
          Map.put(acc, shard_id, start_time)
        end
      end
    end)
  end

  defp get_expired_leases(leases, new_times, start_time, lease_stale_after) do
    Enum.filter(leases, fn(%Lease{shard_id: shard_id}) ->
      last_scanned = Map.get(new_times, shard_id)
      start_time - last_scanned > lease_stale_after
    end)
  end

  defp compute_lease_counts(leases, expired) do
    Enum.reduce(leases, %{}, fn(%Lease{shard_id: shard_id} = lease, acc) ->
      if lease.owner == nil do
        acc
      else
        if Enum.find(expired, &(&1.shard_id == shard_id)) do
          acc
        else
          Map.update(acc, lease.owner, 1, &(&1 + 1))
        end
      end
    end)
    |> Map.update(name(), 0, &(&1)) # update my worker count just incase we do not have any leases
  end

  def compute_target(num_leases, num_workers, max) do
    target =
      if num_workers >= num_leases do
        1
      else
        overflow = if rem(num_leases, num_workers) == 0, do: 0, else: 1
        Integer.floor_div(num_leases, num_workers) + overflow
      end

    if target > max, do: max, else: target
  end

  def name do
    Node.self() |> Atom.to_string()
  end

  def max_leases do
    Application.get_env(:eventlog, :workers, 2)
  end

  def stale_after do
    Application.get_env(:eventlog, :stale, 10_000)
  end
end
