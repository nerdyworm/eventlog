defmodule Eventlog.Commit do
  @derive [ExAws.Dynamo.Encodable]
  @type t :: module

  alias Eventlog.{Commit, CommitRecord}

  defstruct [
    stream_type:    nil,
    stream_uuid:    nil,
    stream_version: 0,
    timestamp:      0,
    events:         [],
    count:          0
  ]

  def new(stream_uuid, expected_version) do
    %__MODULE__{
      stream_uuid:    stream_uuid,
      stream_version: expected_version,
      timestamp:      Eventlog.timestamp(),
    }
  end

  def new(stream_uuid, stream_type, expected_version) do
    %__MODULE__{
      stream_type:    Eventlog.Pack.encode(stream_type),
      stream_uuid:    stream_uuid,
      stream_version: expected_version,
      timestamp:      Eventlog.timestamp(),
    }
  end

  def build(%{uuid: stream_uuid, __struct__: stream_type}, expected_version, events) do
    stream_uuid
    |> new(stream_type, expected_version)
    |> build_events(events)
  end

  def build(stream_uuid, expected_version, events) do
    stream_uuid
    |> new(expected_version)
    |> build_events(events)
  end

  defp build_events(%Commit{} = commit, events) do
    events
    |> List.wrap()
    |> Enum.reduce(commit, &append_event/2)
  end

  defp append_event(event, %Commit{count: count, events: events} = commit) do
    next_count = count + 1
    record = CommitRecord.new(next_count, event)
    %Commit{commit | events: events ++ [record], count: next_count}
  end
end

