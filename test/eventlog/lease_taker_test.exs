defmodule Eventlog.LeaseTakerTest do
  use ExUnit.Case

  alias Eventlog. {
    Leases,
    LeaseTaker,
    Shard,
  }

  setup do
    {:ok, _pid} = Leases.start_link(__MODULE__)
    :ok = Leases.clear(__MODULE__)
  end

  def table_name do
    Application.get_env(:eventlog, :leases)
  end

  test "no leases to take" do
    {:ok, _pid} = LeaseTaker.start_link(__MODULE__)
    assert {:ok, []} = LeaseTaker.take(__MODULE__)
  end

  test "compute_target" do
    assert 1 == LeaseTaker.compute_target(0, 1, 3)
    assert 1 == LeaseTaker.compute_target(1, 1, 3)
    assert 2 == LeaseTaker.compute_target(2, 1, 3)
    assert 3 == LeaseTaker.compute_target(5, 2, 3)
  end

  test "take a single unowned lease" do
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})

    {:ok, _pid} = LeaseTaker.start_link(__MODULE__)
    assert {:ok, [lease]} = LeaseTaker.take(__MODULE__)
    assert lease.shard_id == "shard-0001"
    assert lease.owner == Node.self() |> Atom.to_string()
  end

  test "take should return nothing if nothing to take" do
    {:ok, _pid} = LeaseTaker.start_link(__MODULE__)
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    assert {:ok, [_]} = LeaseTaker.take(__MODULE__)
    assert {:ok, []} = LeaseTaker.take(__MODULE__)
  end

  test "take serveral leases" do
    {:ok, _pid} = LeaseTaker.start_link(__MODULE__)
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0002"})
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0003"})
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0004"})

    assert {:ok, taken} = LeaseTaker.take(__MODULE__)
    assert length(taken) == 2

    # TODO - figure out how to simulate a lease going stale
    assert {:ok, taken} = LeaseTaker.take(__MODULE__)
    assert length(taken) == 0
  end
end

