defmodule Cassandra.Reconnection.Exponential do
  @moduledoc """
  Constant reconnection policy

  ## Acceptable args

  * `:initial` - how long to wait in milliseconds after the first failure before retrying (default: `500`)
  * `:multiplayer` - factor with which to multiply backoff after a failed retry (default: `1.6`)
  * `:jitter` - noise factor (default: `0.2`)
  * `:max` - max backoff time in milliseconds (default: `12000`)
  * `:max_attempts` - max number of attempts on a host before aborting (default: `3`)
  """

  defstruct [
    current: nil,
    attempts: 0,

    initial: 500,
    multiplayer: 1.6,
    jitter: 0.2,
    max: 12000,
    max_attempts: 3,
  ]

  defimpl Cassandra.Reconnection.Policy do
    def get(exp) do
      if exp.attempts < exp.max_attempts do
        exp.current
      else
        :stop
      end
    end

    def next(exp) do
      current = exp.current || exp.initial
      next = current * exp.multiplayer
      noise = (:rand.uniform - 0.5) * exp.jitter * current
      %{exp | attempts: exp.attempts + 1, current: round(min(next, exp.max) + noise)}
    end

    def reset(exp) do
      %{exp | attempts: 0, current: nil}
    end
  end
end
