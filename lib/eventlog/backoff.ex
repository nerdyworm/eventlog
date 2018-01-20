defmodule Eventlog.Backoff do
  def backoff(0) do
    :ok
  end

  def backoff(attempts) do
    ms = compute(attempts + 1)
    :timer.sleep(ms)
    :ok
  end

  def compute(attempts, base \\ 100, limit \\ 5000) do
    exp = min(limit / 2, :math.pow(2, attempts) * base) |> round()
    jitter = :rand.uniform(exp)
    exp + jitter
  end
end

