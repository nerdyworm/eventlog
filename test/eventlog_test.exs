defmodule EventlogTest do
  use ExUnit.Case
  doctest Eventlog

  defmodule Clicked do
    defstruct [:href]
  end

  setup_all do
    # :ok = Eventlog.setup()
    {:ok, _} = Eventlog.Leases.start_link(__MODULE__)
    :ok = Eventlog.Leases.clear(__MODULE__)
  end

  def table_name do
    Application.get_env(:eventlog, :leases)
  end

  # test "appending a single event" do
  #   stream_uuid = Eventlog.uuid()
  #   assert :ok == Eventlog.append(stream_uuid, %Clicked{href: "http://testing.com"})
  #   assert :ok == Eventlog.append(stream_uuid, %Clicked{href: "http://testing.com"})

  #   {:ok, forward} = Eventlog.read_stream_forward(stream_uuid)
  #   assert length(forward) == 2

  #   {:ok, backward} = Eventlog.read_stream_backward(stream_uuid)
  #   assert length(backward) == 2

  #   assert forward |> Enum.reverse() == backward
  # end

  # test "waiting a bunch of time for things" do
    # {:ok, pid} = Eventlog.Supervisor.start_link(__MODULE__)
    # spawn_producer()
    # spawn_producer()
    # :observer.start()
    # :timer.sleep(60_000 * 19)
  # end

  test "parse stream record" do
    record = %{
      "awsRegion" => "us-east-1",
      "dynamodb" => %{"ApproximateCreationDateTime" => 1513368360.0, "Keys" => %{"stream_uuid" => %{"S" => "3b67318553de4312ac06f1acd653808f"}, "stream_version" => %{"N" => "1513368369668202000"}},
        "NewImage" => %{"count" => %{"N" => "1"},
          "events" => %{"L" => [%{"M" => %{"data" => %{"M" => %{"href" => %{"S" => "http://testing.com"}}},
            "sequence" => %{"N" => "1"},
            "type" => %{"S" => "Elixir.EventlogTest.Clicked"}}}]},
          "stream_type" => %{"NULL" => true},
          "stream_uuid" => %{"S" => "3b67318553de4312ac06f1acd653808f"},
          "stream_version" => %{"N" => "1513368369668202000"},
          "timestamp" => %{"N" => "1513368369668"}},
        "SequenceNumber" => "54400000000021482909858", "SizeBytes" => 257,
        "StreamViewType" => "NEW_IMAGE"},
      "eventID" => "891c4dd7b0853cd5ea037ad80de87075", "eventName" => "INSERT",
      "eventSource" => "aws:dynamodb", "eventVersion" => "1.1"}

    [event] = Eventlog.Storage.parse_record(record)
    assert event.__struct__ == Eventlog.Event
    assert event.stream_uuid == "3b67318553de4312ac06f1acd653808f"
  end

  def spawn_producer do
    spawn(fn ->
      Enum.each(1..100_000, fn(_) ->
        events = [%Clicked{href: "http://testing.com"}]
        stream_uuid = Eventlog.uuid()
        assert :ok == Eventlog.append(stream_uuid, events)
        :timer.sleep(500)
      end)
    end)
  end
end
