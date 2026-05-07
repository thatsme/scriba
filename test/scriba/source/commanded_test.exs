defmodule Scriba.Source.CommandedTest do
  use ExUnit.Case, async: true

  alias Scriba.Source.Commanded

  describe "child_spec/1" do
    test "returns a worker spec without invoking Commanded" do
      spec = Commanded.child_spec(application: SomeApp, subscription_name: "test")

      assert spec.id == Commanded
      assert {Commanded, :start_link, [_opts]} = spec.start
      assert spec.type == :worker
      assert spec.restart == :permanent
    end
  end

  # Live integration with a real Commanded application is out of scope here:
  # those tests need a running event store. The compile-time invariant that
  # this module does not statically depend on :commanded is verified manually
  # per §11 (see CHECKPOINT notes — temporarily remove the dep, mix compile,
  # restore).
end
