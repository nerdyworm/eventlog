defmodule Eventlog.ShardReaderTest do
  use ExUnit.Case

  alias Eventlog.{
    Leases,
    ShardSyncer,
    ShardReader,
  }

  setup do
    true = Process.register(self(), :testing)
    {:ok, pid} = Leases.start_link(__MODULE__)
    :ok = Leases.clear(__MODULE__)

    {:ok, pid} = ShardSyncer.start_link(__MODULE__)
    :ok = ShardSyncer.sync(pid)
  end

  def stream do
    Application.get_env(:eventlog, :stream)
  end

  def table_name do
    Application.get_env(:eventlog, :leases)
  end

  def handle_events(_events) do
    :ok = send(:testing, :ok)
  end

  test "reading from a shard" do
    {:ok, leases} = Leases.list_leases(__MODULE__)
    lease = leases |> hd
    {:ok, _pid} = ShardReader.start_link(__MODULE__, lease)
    assert_receive :ok, 30000
  end
end
