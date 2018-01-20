defmodule Eventlog.Leases do
  require Logger

  @moduledoc """
  Defines the crud operations for leases using DynamoDB
  """
  use GenServer

  alias Eventlog.{
    Lease,
  }

  alias ExAws.Dynamo

  defmodule State do
    defstruct [ consumer: nil, table: nil ]
  end

  def start_link(consumer) do
    GenServer.start_link(__MODULE__, consumer, name: name(consumer))
  end

  def name(consumer) do
    Module.concat(consumer, Leases)
  end

  def init(consumer) do
    {:ok, %State{consumer: consumer, table: consumer.table_name()}}
  end

  def clear(consumer) do
    GenServer.call(name(consumer), :clear)
  end

  def list_leases(consumer) do
    GenServer.call(name(consumer), :list)
  end

  def create(consumer, shard) do
    GenServer.call(name(consumer), {:create, shard})
  end

  def delete(consumer, lease) do
    GenServer.call(name(consumer), {:delete, lease})
  end

  def renew(consumer, lease) do
    GenServer.call(name(consumer), {:renew, lease})
  end

  def release(consumer, shard_id, worker_id) do
    GenServer.call(name(consumer), {:release, shard_id, worker_id})
  end

  def get(consumer, shard_id) do
    GenServer.call(name(consumer), {:get, shard_id})
  end

  def take(consumer, lease, owner) do
    GenServer.call(name(consumer), {:take, lease, owner})
  end

  def steal(consumer, lease, owner) do
    GenServer.call(name(consumer), {:steal, lease, owner})
  end

  def checkpoint(consumer, lease, checkpoint) do
    GenServer.call(name(consumer), {:checkpoint, lease, checkpoint})
  end

  def handle_call({:renew, %Lease{counter: counter} = lease}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#counter = :last",
      expression_attribute_names: %{"#counter" => "counter"},
      expression_attribute_values: [last: counter]]

    lease = %Lease{lease | counter: counter + 1}

    case Dynamo.put_item(table, lease, opts) |> ExAws.request do
      {:ok, _} ->
        {:reply, {:ok, lease}, state}

      {:error, {"ConditionalCheckFailedException", _}} ->
        {:reply, {:error, :lost}, state}
    end
  end

  def handle_call({:release, shard_id, owner}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#owner = :owner",
      expression_attribute_names: %{"#owner" => "owner"},
      expression_attribute_values: [owner: owner],
      update_expression: "REMOVE #owner"]

    case Dynamo.update_item(table, %{shard_id: shard_id}, opts) |> ExAws.request do
      {:ok, _} ->
        {:reply, :ok, state}

      {:error, {"ConditionalCheckFailedException", _}} ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:take, %Lease{counter: counter} = lease, owner}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#counter = :last",
      expression_attribute_names: %{"#counter" => "counter"},
      expression_attribute_values: [last: counter]]

    lease = %Lease{lease | owner: owner, counter: counter + 1}
    response = ExAws.Dynamo.put_item(table, lease, opts) |> ExAws.request
    case response do
      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        {:reply, :lost, state}

      {:ok, _} ->
        {:reply, {:ok, lease}, state}
    end
  end

  def handle_call({:steal, %Lease{counter: counter} = lease, owner}, _, %State{table: table} = state) do
    lease = %Lease{lease | owner: owner, counter: counter + 1}

    response = ExAws.Dynamo.put_item(table, lease) |> ExAws.request
    case response do
      {:ok, _} ->
        {:reply, {:ok, lease}, state}
    end
  end

  def handle_call({:get, shard_id}, _, %State{table: table} = state) do
    lease =
      Dynamo.get_item(table, %{shard_id: shard_id})
      |> ExAws.request!
      |> Dynamo.decode_item(as: Lease)

    {:reply, {:ok, lease}, state}
  end

  def handle_call(:clear, _, %State{table: table} = state) do
    leases = scan(table)

    Enum.each(leases, fn(lease) ->
      Dynamo.delete_item(table, %{shard_id: lease.shard_id})
      |> ExAws.request!
    end)

    {:reply, :ok, state}
  end

  def handle_call({:delete, %Lease{shard_id: shard_id}}, _, %State{table: table} = state) do
    Dynamo.delete_item(table, %{shard_id: shard_id})
    |> ExAws.request!

    {:reply, :ok, state}
  end

  def handle_call(:list, _, %State{table: table} = state) do
    {:reply, {:ok, scan(table)}, state}
  end

  def handle_call({:create, shard}, _, %State{table: table} = state) do
    lease = %Lease{
      checkpoint: "TRIM_HORIZON",
      shard_id: shard.shard_id,
      parent_id: shard.parent_shard_id,
      counter: 0,
    }

    opts = [condition_expression: "attribute_not_exists(shard_id)"]

    result =
      ExAws.Dynamo.put_item(table, lease, opts)
      |> ExAws.request

    case result do
      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        {:reply, :ok, state}

      {:ok, _} ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:checkpoint, %Lease{counter: counter} = lease, checkpoint}, _, %State{table: table} = state) do
    opts = [
      condition_expression: "#counter = :last",
      expression_attribute_names: %{"#counter" => "counter"},
      expression_attribute_values: [last: counter]]

    lease = %Lease{lease | counter: counter + 1, checkpoint: checkpoint}

    result =
      ExAws.Dynamo.put_item(table, lease, opts)
      |> ExAws.request

    case result do
      {:error, {"ConditionalCheckFailedException", "The conditional request failed"}} ->
        IO.puts "OMG LOST LEASE"
        IO.puts "CURRENT LEASES"
        scan(table) |> IO.inspect
        IO.puts "TRIED TO CHECKPOINT"
        IO.inspect lease
        {:reply, {:error, :stolen}, state}

      {:ok, _} ->
        {:reply, {:ok, lease}, state}
    end
  end

  defp scan(table) do
    Dynamo.scan(table)
    |> ExAws.request!
    |> fn(%{"Items" => items}) -> items end.()
    |> Enum.map(fn(l) -> Dynamo.Decoder.decode(l, as: Lease) end)
  end
end
