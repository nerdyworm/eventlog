defmodule EventlogPackTest do
  use ExUnit.Case

  alias Eventlog.Pack

  test "encoding and decoding atoms" do
    assert Pack.encode(__MODULE__) == "EventlogPackTest"
    assert Pack.decode("EventlogPackTest") == __MODULE__
  end
end
