defmodule Eventlog.Lease do
  @derive [ExAws.Dynamo.Encodable]

  defstruct [
    name: nil,
    checkpoint: nil,
    counter: nil,
    parent_id: nil,
    shard_id: nil,
    owner: nil,
  ]

  alias Eventlog.{
    Lease,
    Shard,
  }

  def finished?(%Lease{checkpoint: checkpoint}) do
    Shard.compare(checkpoint, "SHARD_END") >= 0
  end

  def data?(%Lease{checkpoint: checkpoint}) do
    checkpoint != "SHARD_END"
  end
end

