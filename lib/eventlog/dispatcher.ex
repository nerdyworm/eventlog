defmodule Eventlog.Dispatcher do
  require Logger

  @timeout Application.get_env(:eventlog, :timeout, 10_000)

  def dispatch_records(reader, handler, records) do
    task = Task.Supervisor.async_nolink(
      Eventlog.Tasks,
      handler,
      :handle_records,
      [records])

    case Task.yield(task, @timeout) || Task.shutdown(task) do
      {:ok, :ok} ->
        send(reader, :ack)
        :ok

      {:exit, reason} ->
        message = Exception.format(:exit, reason, System.stacktrace)
        Logger.error "[eventlog] #{message}"
        send(reader, :nack)
        :ok

      nil ->
        Logger.warn "[eventlog] Failed to get a result in #{@timeout}ms"
        send(reader, :nack)
        :ok
    end
  end
end
