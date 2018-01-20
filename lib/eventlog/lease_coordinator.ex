defmodule Eventlog.LeaseCoordinator do
  @moduledoc """
  Coordinates the lease taker and starting workers for those leases
  """

  use GenServer

  alias Eventlog.{
    Leases,
    LeaseTaker
  }

  require Logger

  defmodule State do
    defstruct [consumer: nil, workers: %{}, timeout: 5000]
  end

  def start_link(consumer) do
    GenServer.start_link(__MODULE__, consumer)
  end

  def init(consumer) do
    Process.flag(:trap_exit, true)
    state = %State{consumer: consumer}
    {:ok, state, 3000}
  end

  def handle_info(:timeout, %State{consumer: consumer, workers: workers} = state) do
    {:ok, leases} = LeaseTaker.take(consumer)

    workers =
      Enum.reduce(leases, workers, fn(lease, workers) ->
        case Eventlog.ShardSupervisor.start_worker(consumer, lease) do
          {:ok, pid} ->
            Process.monitor(pid)
            Map.put(workers, pid, lease.shard_id)

          {:error, {:already_started, _}} ->
            workers

          {:error, {:shutdown, reason}} ->
            Logger.error "failed to start worker: #{inspect reason}"
            workers
        end
      end)

    {:noreply, %State{state | workers: workers}, state.timeout}
  end

  def handle_info({:DOWN, _ref, :process, pid, :shutdown}, %State{workers: workers, consumer: consumer} = state) do
    shard_id = Map.get(workers, pid)
    Logger.info "[eventlog] #{shard_id} shutdown"

    worker_id = Node.self() |> Atom.to_string()
    :ok = Leases.release(consumer, shard_id, worker_id)
    workers = Map.delete(workers, pid)
    {:noreply, %State{state | workers: workers}, 0}
  end

  def terminate(_, _) do
    :normal
  end
end
