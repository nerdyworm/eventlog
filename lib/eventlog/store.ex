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

  def read_stream_forward(stream_uuid, limit) do
    GenServer.call(__MODULE__, {:forward, stream_uuid, limit})
  end

  def read_stream_backward(stream_uuid, limit) do
    GenServer.call(__MODULE__, {:backward, stream_uuid, limit})
  end

  def handle_call({:append, stream_uuid, stream_version, events}, _, state) do
    result = Storage.append(stream_uuid, stream_version, events)
    {:reply, result, state}
  end

  def handle_call({:forward, stream_uuid, limit}, _, state) do
    result = Storage.read_stream_forward(stream_uuid, -1, limit)
    {:reply, result, state}
  end

  def handle_call({:backward, stream_uuid, limit}, _, state) do
    result = Storage.read_stream_backward(stream_uuid, limit)
    {:reply, result, state}
  end
end
