defmodule Cassandra.Reconnection.Constant do
  @moduledoc """
  Constant reconnection policy

  ## Acceptable args

  * `:initial` - how long to wait in milliseconds after the first failure before retrying (default: `500`)
  * `:step` - number of milliseconds to add to backoff after a failed retry (default: `1000`)
  * `:jitter` - noise factor (default: `0.2`)
  * `:max` - max backoff time in milliseconds (default: `12000`)
  * `:max_attempts` - max number of attempts on a host before aborting (default: `3`)
  """

  defstruct [
    current: nil,
    attempts: 0,

    initial: 500,
    step: 1000,
    jitter: 0.2,
    max: 12000,
    max_attempts: 3,
  ]

  defimpl Cassandra.Reconnection.Policy do
    def get(cons) do
      if cons.attempts < cons.max_attempts do
        cons.current
      else
        :stop
      end
    end

    def next(cons) do
      current = cons.current || cons.initial
      next = current + cons.step
      noise = (:rand.uniform - 0.5) * cons.jitter * current
      %{cons | attempts: cons.attempts + 1, current: round(min(next, cons.max) + noise)}
    end

    def reset(cons) do
      %{cons | attempts: 0, current: nil}
    end
  end
end
