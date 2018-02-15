defmodule Eventlog.Pack do
  alias Eventlog.{Encoder, Event, Commit, CommitRecord}

  def pack(%Commit{events: events} = commit) when is_list(events) do
    %Commit{commit | events: events |> pack()}
  end

  def pack(events) when is_list(events) do
    events
    |> Enum.map(&pack/1)
    |> Encoder.encode()
  end

  def pack(%CommitRecord{data: data} = event) do
    %CommitRecord{event | data: encode(data)}
  end

  def unpack(%Commit{events: events} = commit) when is_binary(events) do
    events
    |> Encoder.decode()
    |> Enum.map(&(unpack(&1, commit)))
  end

  def unpack(%Commit{events: events} = commit) when is_list(events) do
    Enum.map(events, &(unpack(&1, commit)))
  end

  def unpack(%{"sequence" => sequence, "data" => event_data, "type" => event_type}, commit) do
    unpack(%{sequence: sequence, data: event_data, type: event_type}, commit)
  end

  def unpack(%{sequence: sequence, data: event_data, type: event_type}, commit) do
    event_type = decode(event_type)
    %Event{
      event_id: "#{commit.stream_uuid}.#{commit.stream_version}.#{sequence}",
      stream_type: decode(commit.stream_type),
      stream_uuid: commit.stream_uuid,
      stream_version: commit.stream_version,
      event_data: decode(event_data, event_type),
      event_type: event_type,
      event_sequence: sequence,
      timestamp: commit.timestamp,
    }
  end

  def encode(%{__struct__: _type} = data) do
    Map.delete(data, :__struct__)
  end

  def encode(atom) when is_atom(atom) do
    atom
    |> Atom.to_string()
    |> encode()
  end

  def encode("Elixir." <> name) do
    name
  end

  def decode(event_data, event_type) do
    event_data
    |> Map.keys()
    |> Enum.reduce(struct(event_type), fn(key, acc) ->
      atom = String.to_atom(key)
      value = Map.get(event_data, key)
      Map.put(acc, atom, value)
    end)
  end

  def decode(atom) when is_nil(atom) do
    nil
  end

  def decode("Elixir." <> _ = name) do
    String.to_atom(name)
  end

  def decode(string) when is_binary(string) do
    "Elixir." <> string |> decode()
  end
end

