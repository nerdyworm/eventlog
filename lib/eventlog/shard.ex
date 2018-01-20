defmodule Eventlog.Shard do
  alias Eventlog.Shard

  defstruct [
    shard_id: nil,
    parent_shard_id: nil,
    adjacent_parent_shard_id: nil,
    hash_key_range: nil,
    sequence_number_range: nil,
  ]

  defmodule HashKeyRange do
    defstruct [
      starting_hash_key: nil,
      ending_hash_key: nil,
    ]

    def decode(map) do
      %HashKeyRange{
        starting_hash_key: Map.get(map, "StartingHashKey"),
        ending_hash_key:   Map.get(map, "EndingHashKey"),
      }
    end
  end

  defmodule SequenceNumberRange do
    defstruct [
      starting_sequence_number: nil,
      ending_sequence_number: nil,
    ]

    def decode(map) do
      %SequenceNumberRange{
        starting_sequence_number: Map.get(map, "StartingSequenceNumber"),
        ending_sequence_number:   Map.get(map, "EndingSequenceNumber")
      }
    end
  end


  def decode(map) do
    hash_key_range =
    case Map.get(map, "HashKeyRange") do
      nil -> nil
      range -> HashKeyRange.decode(range)
    end

    %Shard{
      shard_id: Map.get(map, "ShardId"),
      parent_shard_id: Map.get(map, "ParentShardId"),
      adjacent_parent_shard_id: Map.get(map, "AdjacentParentShardId"),
      hash_key_range: hash_key_range,
      sequence_number_range: SequenceNumberRange.decode(Map.get(map, "SequenceNumberRange")),
    }
  end

  def compare("SHARD_END", "SHARD_END"), do: 0
  def compare("SHARD_END", _), do: 1
  def compare(_, "SHARD_END"), do: -1

  def compare("TRIM_HORIZON", "TRIM_HORIZON"), do: 0
  def compare("TRIM_HORIZON", _), do: -1
  def compare(_, "TRIM_HORIZON"), do: 1

  def compare(s1, s2) when is_nil(s1) and is_nil(s2), do: 0
  def compare(_, s2) when is_nil(s2), do: -1
  def compare(s1, _) when is_nil(s1), do: 1

  def compare(s1, s2) do
    s1 = String.to_integer(s1)
    s2 = String.to_integer(s2)
    cond do
      s1 < s2  -> -1
      s1 > s2  ->  1
      s1 == s2 ->  0
    end
  end

  def unprocessed(checkpoint, shards) do
    Enum.filter(shards, fn(%Shard{sequence_number_range: %SequenceNumberRange{starting_sequence_number: start}}) ->
      compare(checkpoint, start) <= 0
    end)
  end
end

