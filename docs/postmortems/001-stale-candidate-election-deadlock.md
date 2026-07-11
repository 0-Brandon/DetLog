# Postmortem 001: stale candidate could strand a live quorum

Status: fixed before release

Severity: liveness failure; no committed-data safety violation

## Summary

A higher-term `RequestVote` from a stale candidate could make the current
leader step down and deny the vote without arming an election timer. With the
third node unavailable, the two live nodes then had no path back to a leader:
the stale candidate could not win, and the up-to-date follower would never
campaign.

The first proposed repair—reset the timer after every higher-term denial—had a
second liveness flaw. A stale candidate with a shorter timeout could repeatedly
raise the term and reset the up-to-date follower just before its deadline,
preventing it from ever campaigning.

## Minimal history

1. Nodes A, B, and C form a three-node cluster. A is the leader and has a log
   entry that B lacks.
2. C is unavailable. A and B are still a live majority.
3. B times out, increments its term, persists its self-vote, and requests A's
   vote.
4. A observes the higher term, persists its follower transition, and correctly
   denies B because B's log is stale.
5. Before the fix, A had invalidated its old election timer when it became
   leader and did not create a new one after the denial. B cannot obtain a
   majority; A never times out. Progress stops permanently.

## Root cause

Timer ownership was implicit across two paths. `step_down()` was deliberately
called without emitting a timer because the caller needed to persist the new
term before any dependent action. The post-persistence vote-reply continuation
only scheduled a timer when it granted the vote. That was correct for an
ordinary follower with an existing timer, but not for a leader, whose election
timer had been invalidated.

The implementation had tests for durable vote gating and stale-log rejection,
but not for their composition with a leader-to-follower transition and a
failed election.

## Resolution

The vote handler captures whether the node was leader before stepping down.
After the hard-state write completes, it arms an election timer when either:

- the vote is granted, as required for the normal reset rule; or
- the node was previously leader and therefore had no live election timer.

A follower or candidate that denies a higher-term stale request keeps its
existing timer. Term persistence is still completed before the response.

Two regression histories protect the fix:

1. a leader denies one higher-term stale candidate and receives a new election
   timer after the storage completion;
2. the new follower denies another higher-term stale request without resetting
   that timer, then successfully starts its own election when the original
   deadline fires.

## Why safety was preserved

The faulty behavior did not grant an incorrect vote, apply an uncommitted
entry, or change a committed prefix. Term and vote changes were still durable
before messages. The failure was an absence of future events, so it affected
availability only.

## Follow-up actions

- Treat timer generation and ownership as part of every role-transition test.
- Include at least one unavailable member in election-history tests.
- Test attempted fixes against repeated stale-candidate traffic, not only a
  single request.
- Keep simulator timer-rearm saturation tests so adapter-level generation
  mapping cannot recreate the same stranded-node state.

