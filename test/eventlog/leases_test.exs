defmodule Eventlog.LeasesTest do
  use ExUnit.Case

  alias Eventlog. {
    Lease,
    Leases,
    Shard,
  }

  setup do
    {:ok, pid} = Leases.start_link(__MODULE__)
    :ok = Leases.clear(__MODULE__)
    {:ok, %{leases: pid}}
  end

  def table_name do
    Application.get_env(:eventlog, :leases)
  end

  test "list no leases" do
    assert {:ok, []} = Leases.list_leases(__MODULE__)
  end

  test "creates one lease per shard" do
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    assert {:ok, [%Lease{} = lease]} = Leases.list_leases(__MODULE__)
    assert lease.shard_id == "shard-0001"
    assert lease.checkpoint == "TRIM_HORIZON"
  end

  test "get lease by id" do
    :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = Leases.get(__MODULE__, "shard-0001")
    assert lease.shard_id == "shard-0001"
    assert lease.checkpoint == "TRIM_HORIZON"
    assert lease.counter == 0
    assert lease.owner == nil
  end

  test "renew lease" do
    assert :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = Leases.get(__MODULE__, "shard-0001")

    assert {:ok, lease} = Leases.renew(__MODULE__, lease)
    assert lease.counter == 1
  end

  test "take unowned lease" do
    :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = Leases.get(__MODULE__, "shard-0001")
    {:ok, lease} = Leases.take(__MODULE__, lease, "owner")
    assert lease.owner == "owner"
    assert lease.counter == 1
  end

  test "take owned lease" do
    :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = Leases.get(__MODULE__, "shard-0001")
    {:ok, lease} = Leases.take(__MODULE__, lease, "owner")
    {:ok, lease} = Leases.take(__MODULE__, lease, "new_owner")
    assert lease.owner == "new_owner"
    assert lease.counter == 2
  end

  test "release" do
    :ok = Leases.create(__MODULE__, %Shard{shard_id: "shard-0001"})
    {:ok, lease} = Leases.get(__MODULE__, "shard-0001")
    {:ok, _lease} = Leases.take(__MODULE__, lease, "owner")

    :ok = Leases.release(__MODULE__, "shard-0001", "owner")
    {:ok, lease} = Leases.get(__MODULE__, "shard-0001")
    assert lease.owner == nil
  end
end

