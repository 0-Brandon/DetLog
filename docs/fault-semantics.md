# Deterministic fault semantics

The simulator is a single-threaded discrete-event machine. Each event has an
integer virtual time and monotonically increasing insertion sequence; the pair
is a total ordering. No simulated decision depends on wall time, filesystem
enumeration, pointer values, thread IDs, locale, or an implementation-defined
random distribution.

## Timers and restarts

Timer effects carry a generation. Resetting a timer increments its generation,
so an older scheduled firing is consumed as stale. Each node also has an
incarnation number. Crash/restart increments it, making old timers, packets,
and storage completions unable to affect the restarted node.

Election timer adapters add seeded jitter to the core's requested base delay.
Heartbeat delays are fixed.

## Directed links

Fault state belongs to a directed `(source, destination)` link. Therefore a
partition may be symmetric or asymmetric. Reordering is represented by
different delivery delays.

- `drop_next(n)` consumes the next `n` sends on that directed link.
- `duplicate_next(n)` schedules at most the configured duplicate cap.
- partitioning drops new sends and suppresses deliveries that reach their due
  time while the link is partitioned;
- healing does not resurrect a suppressed delivery;
- endpoint crash invalidates all old-incarnation packets.

Each disposition is recorded in the trace, including saturation rather than
silently turning resource exhaustion into an unexplained network fault.

## Storage

Writes and flushes are independent scheduled operations. A successful flush
makes all preceding completed writes stable. A crash invalidates pending
completions and may retain a configured prefix of the unstable tail or active
write. Flushed bytes are never changed by an ordinary crash.

Media corruption of flushed bytes is a separate parser/recovery test. It is
detected and fails the node closed rather than being treated as a recoverable
crash tail.

## Scenario replay

The replay key is the executable/build, seed, complete configuration, and
external action list. The property harness prints the generated actions and a
deterministically minimized counterexample on failure; its fixed generator can
be rerun from the reported seed in the same build. JSONL traces materialize the
resulting event decisions for diagnosis, but they are not an executable replay
format and are not promised to survive changes to PRNG call sites.
