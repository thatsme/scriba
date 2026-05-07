defmodule Scriba.Test.Projection do
  @moduledoc false

  @doc "Stub handler that always returns a non-:skip tag so the Test target records the event."
  def handle(_event_data, _meta), do: {:test_record, :ok}
end
