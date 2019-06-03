defmodule Eventlog.Storage do
  require Logger

  @table Application.get_env(:eventlog, :table)
  @retries Application.get_env(:eventlog, :retries)

  alias ExAws.Dynamo
  alias Eventlog.{Commit, Event, Pack, Backoff}

  def append(stream_uuid, stream_version, events, attempts \\ 0) do
    start = :os.system_time(:milli_seconds)

    commit = Commit.build(stream_uuid, stream_version, events)
    packed = commit |> Pack.pack()

    opts = [
      condition_expression: "attribute_not_exists(stream_version)",
      return_consumed_capacity: "TOTAL"
    ]

    case Dynamo.put_item(@table, packed, opts) |> ExAws.request() do
      {:ok, results} ->
        consumed = results["ConsumedCapacity"]["CapacityUnits"]

        :ok =
          Logger.debug(
            "[write] stream_uuid=#{commit.stream_uuid} stream_version=#{commit.stream_version} events=#{
              commit.count
            } consumed=#{consumed} runtime=#{:os.system_time(:milli_seconds) - start}ms"
          )

      # :ok = Logger.info "[write] stream_uuid=#{commit.stream_uuid} stream_version=#{commit.stream_version} events=#{commit.count} consumed=#{consumed} runtime=#{:os.system_time(:milli_seconds) - start}ms"

      {:error, {"ConditionalCheckFailedException", _}} ->
        {:error, :version_conflict}

      {:error, {"ProvisionedThroughputExceededException", _}} ->
        Logger.error(
          "[write] [ProvisionedThroughputExceededException] stream_uuid=#{commit.stream_uuid} attempts=#{
            attempts
          }"
        )

        :ok = enforce_limit!(commit, attempts)
        append(stream_uuid, stream_version, events, attempts + 1)

      {:error, why} ->
        {:error, why}
    end
  end

  def read_stream_forward(stream_uuid, start_version, limit) do
    results =
      read_events(stream_uuid,
        return_consumed_capacity: "TOTAL",
        expression_attribute_values: [
          stream_uuid: stream_uuid,
          stream_version: start_version
        ],
        key_condition_expression:
          "stream_uuid = :stream_uuid and stream_version > :stream_version",
        scan_index_forward: true,
        limit: limit
      )

    case results do
      {:ok, events} -> {:ok, events}
      {:ok, events, _last} -> {:ok, events}
    end
  end

  def read_stream_backward(stream_uuid, limit) do
    results =
      read_events(stream_uuid,
        return_consumed_capacity: "TOTAL",
        expression_attribute_values: [
          stream_uuid: stream_uuid
        ],
        key_condition_expression: "stream_uuid = :stream_uuid",
        scan_index_forward: false,
        limit: limit
      )

    case results do
      {:ok, events} -> {:ok, events |> Event.sort_backwards()}
      {:ok, events, _last} -> {:ok, events |> Event.sort_backwards()}
    end
  end

  defp read_events(stream_uuid, query, attempts \\ 0) do
    start = :os.system_time(:milli_seconds)

    case Dynamo.query(@table, query) |> ExAws.request() do
      {:ok, results} ->
        handle_results(stream_uuid, results, start)

      {:error, {"ProvisionedThroughputExceededException", _}} ->
        Logger.error(
          "[read] [ProvisionedThroughputExceededException] stream_uuid=#{stream_uuid} attempts=#{
            attempts
          }"
        )

        :ok = enforce_limit!(%{stream_uuid: stream_uuid}, attempts)
        read_events(stream_uuid, query, attempts + 1)
    end
  end

  defp handle_results(stream_uuid, results, start) do
    last_key = results["LastEvaluatedKey"]
    consumed = results["ConsumedCapacity"]["CapacityUnits"]
    count = results["Count"]

    if last_key do
      last_version = last_key["stream_version"]["N"] |> String.to_integer()

      Logger.info(
        "[read] stream_uuid=#{stream_uuid} consumed=#{consumed}u count=#{count} upto=#{
          last_version
        } duration=#{:os.system_time(:milli_seconds) - start}ms"
      )
    else
      Logger.info(
        "[read] stream_uuid=#{stream_uuid} consumed=#{consumed}u count=#{count} duration=#{
          :os.system_time(:milli_seconds) - start
        }ms"
      )
    end

    events =
      Enum.map(results["Items"], fn item ->
        Dynamo.Decoder.decode(item, as: Commit)
        |> Pack.unpack()
      end)
      |> List.flatten()

    case last_key do
      nil ->
        {:ok, events}

      key ->
        {:ok, events, key["stream_version"]["N"] |> String.to_integer()}
    end
  end

  defp enforce_limit!(commit, attempts) do
    if attempts > @retries do
      raise "Retry Limit Reached stream_uuid=#{commit.stream_uuid}"
    else
      :ok = Backoff.backoff(attempts)
    end
  end

  def parse_records(records) do
    records
    |> Enum.map(&parse_record/1)
    |> List.flatten()
  end

  def parse_record(%{"dynamodb" => %{"NewImage" => record}}) do
    Dynamo.Decoder.decode(record, as: Commit)
    |> Pack.unpack()
  end
end
