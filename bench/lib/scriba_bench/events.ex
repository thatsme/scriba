defmodule ScribaBench.Events.Ticked do
  @moduledoc """
  The benchmark's only event. Deliberately trivial: the measurement is of
  the delivery path — subscription, batching, commit, acknowledgement —
  not of handler work. A handler that did anything interesting would
  measure the handler.
  """
  @derive Jason.Encoder
  defstruct [:stream, :n]
end
