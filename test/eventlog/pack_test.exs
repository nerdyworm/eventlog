defmodule EventlogPackTest do
  use ExUnit.Case

  alias Eventlog.Pack

  defmodule Clicked do
    defstruct [:href]
  end

  test "encoding and decoding atoms" do
    assert Pack.encode(__MODULE__) == "EventlogPackTest"
    assert Pack.decode("EventlogPackTest") == __MODULE__
  end

  test "pack and unpack data will encode" do
    event = %Clicked{href: "testing"}
    commit = Eventlog.Commit.build("xxx", -1, [event, event])
    packed = Pack.pack(commit)
    assert packed.events == "H4sIAAAAAAAAA4uuViqpLEhVslJyLUvNK8nJTw9ITM4OSS0u0XPOyUzOTk1R0lEqTi0sTc1LBqoy1FFKSSxJVLKqVsooSk0DaisBKs3MS1eqrdUhzSgjPEbFAgA9zkGtlwAAAA=="

    [unpacked1, unpacked2] = Pack.unpack(packed)
    assert unpacked1.event_data == event
    assert unpacked2.event_data == event
  end
end
