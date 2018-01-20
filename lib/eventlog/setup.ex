defmodule Eventlog.Setup do
  require Logger

  alias ExAws.Dynamo

  def create_table(name, a, b) do
    results =
      name
      |> Dynamo.describe_table()
      |> ExAws.request()

    case results do
      {:ok, %{"Table" => %{"TableStatus" => "ACTIVE"}}} ->
        :ok

      {:ok, %{"Table" => %{"TableStatus" => "CREATING"}}} ->
        :timer.sleep(2000)
        create_table(name, a, b)

      {:error, _} ->
        Logger.info "Creating #{name} with one read capacity and one write capacity"
        name
        |> Dynamo.create_table(a, b, 1, 1)
        |> ExAws.request!()
        create_table(name, a, b)
    end
  end

  def enable_stream(name) do
    result =
      Dynamo.update_table(name, %{
        "StreamSpecification" => %{
          "StreamEnabled" => true,
          "StreamViewType" => "NEW_IMAGE"
        }
      })
      |> ExAws.request()

    case result do
      {:ok, _} ->
        :ok

      {:error, {"ValidationException", "Table already has an enabled stream:" <> _name}} ->
        :ok

      {:error, message} ->
        raise message
    end
  end
end
