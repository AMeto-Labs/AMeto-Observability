# PR #39 — text for the PR description

Paste the section below into the PR description. It replaces the current wording on the
mixed-level question, which says only that the stand is not yet deployed — a reader takes that
to mean deployment closes the problem, and it does not.

---

## Accepted: compaction carries legacy mixed-level segments forward

Before the level split, a flush wrote its tier as one segment holding every level that tier had
seen. Those files are still on disk on any install that was upgraded rather than rebuilt, and
compaction does not undo them. The merge planner buckets candidates by `SegmentInfo.MinLevel`,
and MinLevel is the lowest severity *value* in a file — not a claim that the file holds one
level. A legacy mixed segment is therefore bucketed as though it were level-pure and merged with
segments that genuinely are. The output is a **new** file, mixed again, carrying the lowest
level's TTL; and because a merged file inherits the newest source's `MaxTimestamp`, every pass
pushes its deadline further out. This was confirmed on a live server, not inferred: the deployed
build wrote fresh v7 files holding levels [2,3,4], [2,3] and [1,2,3,4] — the last 26,386 events
and 11.2 MB, whose Debug-derived three-day deadline had already passed.

**Deploying this branch does not close this.** The level split fixes the flush path, so every
segment this build writes is level-pure. It does nothing to the files an upgrade inherits, and
compaction goes on rewriting those into fresh mixed files for as long as they exist. Anyone
reading "not yet deployed to the stand" as the remaining work on this item is reading it wrong.

We are accepting it rather than splitting on the merge side. Deciding that a segment is
level-homogeneous needs a **read** of every row — MinLevel cannot answer it, and neither can
anything else in the catalog — which would put a full decode of every candidate onto a planning
path that today opens no file at all. The condition also self-heals, and fast: a mixed segment
carries the shortest TTL of the levels inside it, so it expires within days of the upgrade. Once
the last one is gone every segment on disk is level-pure, and from then on same-MinLevel inputs
give level-pure output by construction.

The cost while it lasts is bounded and worth stating plainly: inside an inherited mixed file,
rows above Debug are deleted on Debug's three-day deadline instead of their own ninety — 87 days
early. That is the same window in which those files are expiring anyway, which is exactly why
the condition ends.

What the branch adds is the coverage this had none of. `SegmentMergeTests` now stages an upgraded
install — two mixed-level files written straight to the segment directory, beside a genuinely
level-pure segment produced by the current flush path, all in one sealed bucket — and pins three
things: the merge collapses all three into one file whose MinLevel is the lowest level present
and whose contents are mixed again; the Error rows in that output sit on a three-day deadline,
87 days short of what a level-pure file would give them; and every row's level survives the merge
unchanged. Each test says in its own prose that this is accepted behaviour and why, and names the
assertions to change if merges are ever made level-pure.

---

## Notes for reviewers, not for the description

**The claim about the tests was accurate.** `SegmentMergeTests` did lose its mixed-level input,
and the loss is visible in the level-split commit itself (`1879538`), which changed the helper's
`Level` from `fixedLevel ?? (LogLevel)(n % 6)` to `fixedLevel ?? LogLevel.Information`. That was
necessary at the time — after the split a mixed-level round flushes to six segments, so every
"N rounds ⇒ N segments" assertion in the file would have started counting something else — but it
left `Merge_CollapsesSmallSegments_Losslessly` comparing a level column that no longer varies. A
merge that wrote a constant level, or dropped the column and let the reader default it, would have
passed the whole suite. The new tests restore that check without disturbing the segment
arithmetic, because their sources are staged on disk rather than flushed.

**No other suite lost the same thing.** `MergeAbortTests`, `StreamingMergeCrashSafetyTests`,
`MergeRunPlannerTests` and `SegmentBucketCompactionTests` were all authored *after* the level
split and were single-level from their first commit — there is nothing there to restore.
`LegacySegmentFormatTests` is the one place mixed-level input survived: its rows cycle
Debug..Error, and `AMixedLevelV4AndV5MergeIntoAV7FileWithoutLosingARow` already drove a mixed
merge through `SegmentWriter` directly. What it does not cover, and what the new tests add, is the
**planner** — that a mixed file is bucketed alongside pure ones by metadata alone, and the
retention consequence downstream of that.

**One doc comment was overclaiming.** `SegmentBucketCompactionTests.RetentionDeletesExactlyThe`
`ExpiredBuckets` opened with "Compaction must not move any event's deadline. Merged files stay
level-pure" — true of its own inputs and of anything this build writes, but not true in general,
and it is the comment a reader would land on when asking whether compaction can move a deadline.
It now states the premise it depends on and points at the accepted exception.

**Still open, and deliberately not done here.** The merge planner cannot distinguish a mixed
segment from a pure one, because `SegmentInfo` carries only `MinLevel`. If merges are ever made
level-pure, that is the gap to close first — a planner-level test would need a `SegmentInfo`
fixture that can express "mixed", which does not exist today.
