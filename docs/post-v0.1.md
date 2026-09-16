# Post-v0.1 work

Design notes for work deliberately deferred from v0.1, preserved here so
the design research isn't lost when these become priorities.

---

## PD1 — exactly-once under crash (chaos-engineering property test)

**Status:** deferred from v0.1.

v0.1 ships PD2 + PD3 + the fast-suite dedup integration test. Together
with the read-model `event_id` PK rejecting duplicates pre-commit, those
**structurally enforce** exactly-once-under-crash by composition:

| Layer | Guarantee |
|---|---|
| Read-model PK on `event_id` | Duplicate inserts are PK violations → transaction rolls back → no duplicate row reaches the read model |
| `Scriba.Target.Ecto` Multi atomicity | Read-model write + per-stream cursor upsert commit together or roll back together |
| PD2 (real-Postgres, 200 iterations) | Cursor never lags behind a committed read-model row |
| PD3 (real-Postgres, 100 iterations) | Source `:start_from` filter blocks events ≤ committed cursor |
| Fast-suite dedup integration test (`test/scriba/projection/pipeline_test.exs`) | Pipeline-side dedup catches redelivered events whose position ≤ cache cursor |

PD1 would be a regression test for that composition under crash
injection. It exists in design but isn't load-bearing for v0.1 — the
properties it would re-verify are each verified in isolation by the
tests above. The work to build it is hours of timing-sensitive race
debugging; it's worth doing when chaos-engineering becomes a stated
priority, not before.

### PD1 design summary

For arbitrary event streams with arbitrary crash schedules, exactly one
row appears in the read model per source event after recovery.
100 iterations.

#### Generator

`events_gen_pd(min: 10, max: 200, shuffle: true)` plus a crash schedule.
Shuffle matters here: PD1 must vary stream layouts so crash injection at
"after position K" interacts with different stream boundaries across
iterations. PD2/PD3 don't need this; PD1 does.

Crash schedule generator: `(events, crashes)` tuples where `crashes` is
a list of 0–3 crash points. Each crash specifies:
- **trigger**: `at_position` ∈ 1..length(events)-1 — kill when N events
  have been committed
- **target**: `:coordinator | :pipeline | :broadway_processor` —
  uniformly random per crash point

#### Crash targets — what each exercises

- **Coordinator kill** (`Process.exit(coordinator_pid, :kill)`):
  rest_for_one cascades; Pipeline + source die; ETS cache for this
  projection wiped on the new Coordinator's `init/1` wipe-then-preload
  step. Heaviest path. Tests cache rebuild from Postgres.
- **Pipeline kill** (`Process.exit(pipeline_pid, :kill)`): Broadway's
  tree dies; Coordinator survives; cache survives. New Pipeline spawns
  a new source which restarts from `:start_from: 0` (default). **This
  case directly exercises the Pipeline-side source dedup** that PD3
  doesn't independently cover (PD3's source filter blocks everything).
- **Broadway processor kill**
  (`Process.exit(broadway_processor_pid, :kill)`): Broadway's internal
  supervisor restarts the processor. In-flight messages on that
  processor are lost from the processor's perspective; demand re-ups,
  source re-yields. Tests Broadway's restart semantics inside our
  pipeline.

Include all three with uniform random selection per crash point.

#### Cursor persistence across Pipeline kill

The Test source is deliberately scoped: the cursor lives in the source
process and dies with it. Pipeline kill = source kill. New source has
`:start_from: 0` unless we explicitly pass the cursor.

For PD1 to test exactly-once meaningfully, the post-crash source must
resume from the acked cursor, not from zero — otherwise we're testing
dedup-via-replay (the P3-theater pattern). Solution: **test-owned
external Agent persists the cursor**. The source's start spec consults
the Agent on init; the source's ack callback updates the Agent. Test
infrastructure only; doesn't violate that scoping because
the *source's own internal cursor* still dies on crash — we're just
feeding it back from a test-side Agent on restart.

(Alternative: patch Pipeline to pass cursor on restart. Architectural
change beyond PD1's scope.)

#### Trigger mechanism

Telemetry handler subscribes to
`[:broadway, :batch_processor, :stop]`; on each batch, queries
`read_model_count(name)`; when the count crosses a crash trigger,
kills the target. Adds a DB query per batch but is precise.

#### Completion signal

`wait_for_read_model(ref, name, total_events, 30_000)` — same helper
PD2/PD3 use, with timeout 3× larger to absorb crash-recovery latency.
If the property holds, all events eventually commit. If a bug drops
events, the wait times out with a precise diagnostic.

#### Exactly-once assertion

Three checks:
1. `length(read_model_rows) == length(scoped_events)` — count match
2. Every input `event_id` appears in the read model — completeness
3. No `event_id` appears twice (PK enforces this; assert anyway for
   diagnostic clarity)

#### Cleanup risk

`stop_supervised(sup_id)` after a crash-injected phase 1 may hit a
supervisor mid-restart. `Supervisor.stop` waits for children to
terminate; restart-in-progress should resolve before stop completes.
Bounded by supervisor restart timeout. **Document as the riskiest of
the three property_db tests — if iterations start hanging in cleanup,
this is the suspect.**

#### Iteration budget

100 iterations. ~3–5s per iteration (phase 1 with 0–3 crashes adds
~50ms recovery latency each + wait + cleanup + phase 2). Total
runtime: 5–8 minutes. `@moduletag timeout: 600_000`.

### Why deferred — explicit rationale

Building PD1 is hours of work:
- Crash-schedule generator extension
- Telemetry-driven crash-injection plumbing
- External-Agent cursor persistence for the Test source
- Lookup helpers for each target (Coordinator/Pipeline/Broadway processor pids)
- 100 iterations of debugging timing-sensitive races

The test exercises composition, not novel correctness — it would catch
a regression where one of PD2 / PD3 / the dedup test / the PK constraint
silently degraded such that they no longer compose to exactly-once.
That's a real risk over time, but it's a chaos-engineering risk
(gradual drift), not a correctness risk for v0.1 ship.

When PD1 becomes a priority: this design is the starting point.
