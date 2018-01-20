defmodule Eventlog.Shards do
  alias ExAws.DynamoStreams
  alias Eventlog.{
    Lease,
    Shard,
  }

  def get_iterator(stream, %Lease{shard_id: shard_id, checkpoint: checkpoint}) do
    if checkpoint == "TRIM_HORIZON" do
      DynamoStreams.get_shard_iterator(stream, shard_id, :trim_horizon)
    else
      DynamoStreams.get_shard_iterator(stream, shard_id, :after_sequence_number, [sequence_number: checkpoint])
    end
    |> ExAws.request
  end

  def list_shards(stream) do
    case describe_stream(stream) do
      {:error, response} ->
        {:error, response}

      {:ok, %{"StreamDescription" => %{"Shards" => shards}}} ->
        {:ok, Enum.map(shards, &Shard.decode/1)}
    end
  end

  def describe_stream(stream_name) do
    stream_name
    |> DynamoStreams.describe_stream()
    |> ExAws.request()
  end

  def get_records(iterator) do
    iterator
    |> DynamoStreams.get_records()
    |> ExAws.request()
  end

  def checkpoint_for_record(%{"dynamodb" => %{"SequenceNumber" => checkpoint}}) do
    checkpoint
  end
end

