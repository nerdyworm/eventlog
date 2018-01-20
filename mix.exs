defmodule Eventlog.Mixfile do
  use Mix.Project

  def project do
    [
      app: :eventlog,
      version: "0.1.0",
      elixir: "~> 1.5",
      start_permanent: Mix.env == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ex_aws],
      mod: {Eventlog.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:uuid, "~> 1.1" },
      {:ex_aws, "~> 2.0"},
      {:ex_aws_dynamo, "~> 2.0"},
      {:ex_aws_dynamo_streams, "~> 2.0"},
      {:poison, "~> 3.0"},
      {:hackney, "~> 1.9"},
    ]
  end
end
