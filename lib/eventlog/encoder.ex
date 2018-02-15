defmodule Eventlog.Encoder do
  alias Poison.Decode

  def encode(term) do
    term
    |> Poison.encode!()
    |> :zlib.gzip()
    |> Base.encode64
  end

  def decode(binary) do
    binary
    |> Base.decode64!
    |> :zlib.gunzip()
    |> Poison.decode!()
  end
end

