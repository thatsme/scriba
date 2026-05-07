defmodule Scriba.Test.PropertyDb.Handler do
  @moduledoc """
  Handler used by the property_db property tests (PD2, PD3, PD1).

  Returns `{:insert, %Scriba.Test.ReadModel{...}}` for every event,
  building a row from the event's id, stream_id, and position. The Ecto
  target merges that insert into the per-batch Multi alongside the
  scriba_positions cursor upsert; if both commit, the read model and the
  cursor are guaranteed atomically consistent — which is what PD2
  asserts.

  Because `event_id` is the read-model's primary key, a double-apply on
  the same event would raise a uniqueness violation and roll back the
  transaction — which is what PD1's exactly-once-under-crash property
  exploits.
  """

  alias Scriba.Test.ReadModel

  def handle(_event_data, meta) do
    {:insert,
     %ReadModel{
       event_id: meta.id,
       stream_id: meta.stream_id,
       position: meta.position
     }}
  end
end
