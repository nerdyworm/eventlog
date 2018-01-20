defmodule Eventlog.CommitRecord do
  @derive [ExAws.Dynamo.Encodable]
  @type t :: module

  alias Eventlog.Pack

  defstruct [
    sequence: 0,
    type:     nil,
    data:     nil,
  ]

  def new(sequence, event) do
    %__MODULE__{sequence: sequence, type: event_type(event), data: event}
  end

  defp event_type(event) when is_atom(event) do
    Pack.encode(event)
  end

  defp event_type(%{__struct__: type}) do
    Pack.encode(type)
  end

  defp event_type(event) when is_map(event) do
    "map"
  end
end
