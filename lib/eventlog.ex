defmodule Eventlog do
  @moduledoc """
  Documentation for Eventlog.
  """

  def append(stream_uuid, events, timeout \\ 20000) do
    Eventlog.Store.append(stream_uuid, timestamp(), events, timeout)
  end

  def append_async(stream_uuid, events) do
    Eventlog.Store.append_async(stream_uuid, timestamp(), events)
  end

  def setup do
    name = Application.get_env(:eventlog, :table)

    :ok = Eventlog.Setup.create_table(
      name,
      [stream_uuid: :hash,   stream_version: :range],
      [stream_uuid: :string, stream_version: :number])

    :ok = Eventlog.Setup.enable_stream(name)
  end

  def read_stream_forward(stream_uuid, limit \\ 100) do
    Eventlog.Store.read_stream_forward(stream_uuid, limit)
  end

  def read_stream_backward(stream_uuid, limit \\ 100) do
    Eventlog.Store.read_stream_backward(stream_uuid, limit)
  end

  def timestamp do
    :os.system_time(:milli_seconds)
  end

  def uuid do
    UUID.uuid4(:hex)
  end
end

