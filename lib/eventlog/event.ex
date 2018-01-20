defmodule Eventlog.Event do
  @type t :: module

  defstruct [
    stream_type:    nil,
    stream_uuid:    nil,
    stream_version: 0,
    event_id:       nil,
    event_type:     nil,
    event_data:     nil,
    event_sequence: 0,
    timestamp:      0
  ]

  def sort_backwards(events) do
    events
    |> Enum.sort_by(&(&1.event_sequence))
    |> Enum.sort_by(&(&1.stream_version))
    |> Enum.reverse()
  end
end

