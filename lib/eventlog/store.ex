defmodule Eventlog.Store do
  use GenServer

  alias Eventlog.Storage

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:ok, :ok}
  end

  def append(stream_uuid, stream_version, events, timeout) do
    GenServer.call(__MODULE__, {:append, stream_uuid, stream_version, events}, timeout)
  end

  def append_async(stream_uuid, stream_version, events) do
    GenServer.cast(__MODULE__, {:append, stream_uuid, stream_version, events})
  end

  def read_stream_forward(stream_uuid, limit) do
    Storage.read_stream_forward(stream_uuid, -1, limit)
  end

  def read_stream_backward(stream_uuid, limit) do
    Storage.read_stream_backward(stream_uuid, limit)
  end

  def handle_call({:append, stream_uuid, stream_version, events}, _, state) do
    result = Storage.append(stream_uuid, stream_version, events)
    {:reply, result, state}
  end

  def handle_cast({:append, stream_uuid, stream_version, events}, state) do
    :ok = Storage.append(stream_uuid, stream_version, events)
    {:noreply, state}
  end
end
