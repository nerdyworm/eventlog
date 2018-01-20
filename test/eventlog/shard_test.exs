defmodule ShardTest do
  use ExUnit.Case
  alias Eventlog.{
    Shard
  }

  alias Eventlog.Shard.{
    HashKeyRange,
    SequenceNumberRange
  }

  test "sorting" do
    assert 0 == Shard.compare("SHARD_END", "SHARD_END")
    assert 1 == Shard.compare("SHARD_END", "TRIM_HORIZON")
    assert -1 == Shard.compare("TRIM_HORIZON", "SHARD_END")
    assert -1 == Shard.compare("TRIM_HORIZON", "49569758881787296466787764410752717282499349938480087042")
    assert 1 == Shard.compare("49569758881787296466787764410752717282499349938480087042", "TRIM_HORIZON")
    assert 1 == Shard.compare("49569758881787296466787764410752717282499349938480087042", "TRIM_HORIZON")
    assert 1 == Shard.compare("49569758881820747584585560345463811934088708195161735186", "49569758881809597211986295033894253000771998299986067474")
    assert -1 == Shard.compare("49569758881809597211986295033894253000771998299986067474", "49569758881820747584585560345463811934088708195161735186")
    assert 0 == Shard.compare("49569758881809597211986295033894253000771998299986067474", "49569758881809597211986295033894253000771998299986067474")

    # nil means no end
    assert -1 == Shard.compare("49569758881809597211986295033894253000771998299986067474", nil)
    assert 1 == Shard.compare(nil, "49569758881809597211986295033894253000771998299986067474")
    assert 0 == Shard.compare(nil, nil)
  end

  test "can convert string map to structs" do
    shard = %{
      "AdjacentParentShardId" => "shardId-000000000001",
      "HashKeyRange" => %{
        "EndingHashKey" => "340282366920938463463374607431768211455",
        "StartingHashKey" => "0"
      },
      "ParentShardId" => "shardId-000000000000",
      "SequenceNumberRange" => %{
        "EndingSequenceNumber" => "49569758883983919868843030790192776606535599948436602914",
        "StartingSequenceNumber" => "49569758883972769496243765478623217673218889778383028258"
      },
      "ShardId" => "shardId-000000000002"
    }

    decoded = Shard.decode(shard)
    assert decoded.shard_id == "shardId-000000000002"
    assert decoded.adjacent_parent_shard_id == "shardId-000000000001"
    assert decoded.parent_shard_id == "shardId-000000000000"
    assert decoded.hash_key_range == %HashKeyRange{
      ending_hash_key: "340282366920938463463374607431768211455",
      starting_hash_key: "0"
    }
    assert decoded.sequence_number_range == %SequenceNumberRange{
      ending_sequence_number: "49569758883983919868843030790192776606535599948436602914",
      starting_sequence_number: "49569758883972769496243765478623217673218889778383028258"
    }
  end

  @shards [
    %{"HashKeyRange" => %{"EndingHashKey" => "170141183460469231731687303715884105727",
       "StartingHashKey" => "0"},
     "SequenceNumberRange" => %{"EndingSequenceNumber" => "49569758881798446839387029722322276215816059833655754754",
       "StartingSequenceNumber" => "49569758881787296466787764410752717282499349938480087042"},
     "ShardId" => "shardId-000000000000"},
   %{"HashKeyRange" => %{"EndingHashKey" => "340282366920938463463374607431768211455",
       "StartingHashKey" => "170141183460469231731687303715884105728"},
     "SequenceNumberRange" => %{"EndingSequenceNumber" => "49569758881820747584585560345463811934088708195161735186",
       "StartingSequenceNumber" => "49569758881809597211986295033894253000771998299986067474"},
     "ShardId" => "shardId-000000000001"},
   %{"AdjacentParentShardId" => "shardId-000000000001",
     "HashKeyRange" => %{"EndingHashKey" => "340282366920938463463374607431768211455",
       "StartingHashKey" => "0"}, "ParentShardId" => "shardId-000000000000",
     "SequenceNumberRange" => %{"EndingSequenceNumber" => "49569758883983919868843030790192776606535599948436602914",
       "StartingSequenceNumber" => "49569758883972769496243765478623217673218889778383028258"},
     "ShardId" => "shardId-000000000002"},
   %{"HashKeyRange" => %{"EndingHashKey" => "170141183460469231731687303715884105726",
       "StartingHashKey" => "0"}, "ParentShardId" => "shardId-000000000002",
     "SequenceNumberRange" => %{"StartingSequenceNumber" => "49569758887563189473207195804410468315115276668040642610"},
     "ShardId" => "shardId-000000000003"},
   %{"HashKeyRange" => %{"EndingHashKey" => "340282366920938463463374607431768211455",
       "StartingHashKey" => "170141183460469231731687303715884105727"},
     "ParentShardId" => "shardId-000000000002",
     "SequenceNumberRange" => %{"StartingSequenceNumber" => "49569758887585490218405726427552004033387925029546623042"},
     "ShardId" => "shardId-000000000004"}]

  test "filters unprocessed shards based on checkpoint" do
    shards = Enum.map(@shards, &Shard.decode/1)
    checkpoint = "TRIM_HORIZON"
    unprocessed = Shard.unprocessed(checkpoint, shards)
    assert length(unprocessed) == 5

    checkpoint = "49569758887585490218405726427552004033387925029546623042"
    unprocessed = Shard.unprocessed(checkpoint, shards)
    assert length(unprocessed) == 1
  end
end


