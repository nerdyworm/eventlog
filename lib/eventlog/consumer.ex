defmodule Eventlog.Consumer do
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      def start_link do
        Eventlog.ConsumerSupervisor.start_link(__MODULE__)
      end

      def config do
        Application.get_env(:eventlog, __MODULE__)
      end

      def stream do
        config()[:stream]
      end

      def table_name do
        config()[:table]
      end

      def setup do
        Eventlog.Setup.create_table(
          table_name(),
          [shard_id: :hash],
          [shard_id: :string])
      end
    end
  end
end
