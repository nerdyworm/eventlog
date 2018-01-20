defmodule EventlogCommitTest do
  use ExUnit.Case
  doctest Eventlog

  defmodule Clicked do
    defstruct [:href]
  end

  test "building a commit" do
    stream_uuid = Eventlog.uuid()
    events = [%Clicked{href: "testing"}, %Clicked{href: "testing"}]
    commit = Eventlog.Commit.build(stream_uuid, -1, events)

    assert commit.stream_uuid == stream_uuid
    assert commit.stream_version == -1
    assert commit.timestamp != nil
    assert commit.count == 2
    assert length(commit.events) == 2
  end

  defmodule Ag do
    defstruct [:uuid]
  end

  test "building a commit with a struct" do
    events = [%Clicked{href: "testing"}, %Clicked{href: "testing"}]
    commit = Eventlog.Commit.build(%Ag{uuid: "xxx-xxx"}, -1, events)
    assert commit.stream_uuid == "xxx-xxx"
    assert commit.stream_type == "EventlogCommitTest.Ag"
    assert length(commit.events) == 2
    assert commit.events |> hd |> Map.get(:type) == "EventlogCommitTest.Clicked"
  end
end
