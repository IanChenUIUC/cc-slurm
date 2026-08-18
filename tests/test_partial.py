"""spec.md §11: `submit` runs only nodes whose latest state is not COMPLETED.

Arrays used to resubmit whole, so a completed array could never take on new work.
These cover the per-node fold and the sparse `--array` submission that acts on it.

`GRID` mirrors the shape that motivated it: an inner axis swept under an outer one,
so widening the inner list both adds tasks *and* renumbers the ones already run --
`_sortkey` stringifies, so batches sort "1","10","100","5".
"""
import json

GRID = """
[recipe.wide]
array      = true
array_axes = ["size", "batch"]
params     = { dataset = ["a"], size = ["1", "10"], batch = ["1", "10", "100"] }
command    = "echo wide ${dataset} ${size} ${batch}"
"""

WIDENED = GRID.replace('batch = ["1", "10", "100"]', 'batch = ["1", "10", "100", "5"]')

# Task order under the narrow spec, which is what the prior run's record carries.
NARROW_NODES = ["wide-a-1-1", "wide-a-1-10", "wide-a-1-100",
                "wide-a-10-1", "wide-a-10-10", "wide-a-10-100"]

# Under WIDENED the same six sit at 0,1,2,4,5,6 -- "5" lands last within each size
# group, so the b5 cells are indices 3 and 7 and everything after the first group
# shifts by one. That shift is exactly what keying the fold on ident survives.
NEW_INDICES = [3, 7]


def done(nodes, job_id="800", state="COMPLETED"):
    """A prior submit record: what the log holds after that run reconciled."""
    return {"unit": "wide:a", "kind": "array", "job_id": job_id, "state": state,
            "event": "submit", "nodes": nodes}


def test_widened_axis_submits_only_the_new_tasks(mock_run):
    """The case this exists for: an array that COMPLETED at the old width takes on
    the tasks a widened axis added, without redoing the ones already paid for."""
    r = mock_run(WIDENED, {}, "submit", extra_log=[done(NARROW_NODES)])
    assert r.ok, r.stderr

    s = r.submitted["wide:a"]
    assert s.indices == NEW_INDICES
    assert not r.skipped


def test_the_resubmission_records_which_tasks_it_covered(mock_run):
    """Without `indices` the next fold would read the new record as speaking for the
    whole array, and the six completed tasks would lose their verdict."""
    r = mock_run(WIDENED, {}, "submit", extra_log=[done(NARROW_NODES)])
    rec = [x for x in r.log if x.get("event") == "submit"][-1]

    assert rec["indices"] == NEW_INDICES
    assert rec["nodes"] == ["wide-a-1-1", "wide-a-1-10", "wide-a-1-100", "wide-a-1-5",
                            "wide-a-10-1", "wide-a-10-10", "wide-a-10-100", "wide-a-10-5"]
    # The renumbering the ident key protects against: index 3 was wide-a-10-1 before.
    assert rec["nodes"][3] != NARROW_NODES[3]


def test_an_unchanged_completed_array_is_still_skipped_whole(mock_run):
    """No widening, nothing failed: the array must stay untouched, not resubmit zero
    tasks or all of them."""
    r = mock_run(GRID, {}, "submit", extra_log=[done(NARROW_NODES)])
    assert r.ok, r.stderr
    assert not r.submitted
    assert [u for u, _ in r.skipped] == ["wide:a"]


def test_a_full_array_names_no_index_subset(mock_run):
    """A first run submits the plain form -- the runners' own 0..N-1 default stays
    the common path, so nothing changes for a pipeline that never resumes."""
    r = mock_run(GRID, {}, "submit")
    assert r.ok, r.stderr
    assert r.submitted["wide:a"].array_indices is None
    assert not [x for x in r.log if "indices" in x]


def test_only_the_failed_tasks_are_retried(mock_run):
    """Per-task state was already recorded; now it is acted on. A 6-task array with
    two failures resubmits those two, not all six."""
    r = mock_run(GRID, {"wide:a": {"tasks": {0: "COMPLETED", 1: "FAILED",
                                             2: "COMPLETED", 3: "COMPLETED",
                                             4: "OUT_OF_MEMORY", 5: "COMPLETED"},
                                   "id": "900"}},
                 "submit")
    assert r.ok, r.stderr
    assert r.submitted["wide:a"].indices == [1, 4]


def test_no_retry_takes_the_new_tasks_and_leaves_the_failures(mock_run):
    """`retry=0` moves the frontier: never-attempted tasks run, tasks that already
    failed do not -- the two now coexisting inside one array."""
    prior = done(NARROW_NODES)
    prior["tasks"] = {str(i): {"state": "COMPLETED"} for i in range(6)}
    prior["tasks"]["4"] = {"state": "OUT_OF_MEMORY"}

    both = mock_run(WIDENED, {}, "submit", extra_log=[prior])
    assert both.ok, both.stderr
    assert both.submitted["wide:a"].indices == [3, 5, 7]   # 5 == the old index 4

    only_new = mock_run(WIDENED, {}, "submit", "--no-retry", extra_log=[prior])
    assert only_new.ok, only_new.stderr
    assert only_new.submitted["wide:a"].indices == NEW_INDICES


def test_a_live_array_is_left_alone_even_when_the_axis_widened(mock_run):
    """One live job per unit is all the log's single `job_id` can describe, so new
    tasks wait for the running one rather than racing it onto the same outputs."""
    r = mock_run(WIDENED, {"wide:a": ("RUNNING", "910")}, "submit",
                 extra_log=[done(NARROW_NODES, job_id="910", state="SUBMITTED")])
    assert r.ok, r.stderr
    assert not r.submitted
    assert [u for u, _ in r.skipped] == ["wide:a"]


def test_status_folds_a_partial_resubmission_over_the_run_that_preceded_it(mock_run):
    """The newest record covers two tasks; the other six keep the verdict of the run
    that did cover them, so the histogram reads 6 COMPLETED + 2 of the new state."""
    partial = {"unit": "wide:a", "kind": "array", "job_id": "920", "state": "FAILED",
               "event": "submit", "indices": NEW_INDICES,
               "nodes": ["wide-a-1-1", "wide-a-1-10", "wide-a-1-100", "wide-a-1-5",
                         "wide-a-10-1", "wide-a-10-10", "wide-a-10-100", "wide-a-10-5"]}
    r = mock_run(WIDENED, {}, "status", "--local", "--verbose",
                 extra_log=[done(NARROW_NODES), partial])
    assert r.ok, r.stderr
    assert "6 COMPLETED" in r.stdout and "2 FAILED" in r.stdout
    # -v names the tasks that did not complete: exactly the two just resubmitted.
    assert "wide-a-1-5" in r.stdout and "wide-a-10-5" in r.stdout
    assert "wide-a-1-1 " not in r.stdout


def test_a_reconcile_record_does_not_overwrite_untouched_tasks(mock_run):
    """A reconcile record carries neither `nodes` nor `indices`; it inherits both, so
    a PENDING observation of the two-task resubmission cannot claim the other six."""
    partial = {"unit": "wide:a", "kind": "array", "job_id": "930", "state": "SUBMITTED",
               "event": "submit", "indices": NEW_INDICES,
               "nodes": ["wide-a-1-1", "wide-a-1-10", "wide-a-1-100", "wide-a-1-5",
                         "wide-a-10-1", "wide-a-10-10", "wide-a-10-100", "wide-a-10-5"]}
    reconciled = {"unit": "wide:a", "kind": "array", "job_id": "930",
                  "state": "PENDING", "event": "reconcile"}
    r = mock_run(WIDENED, {}, "status", "--local",
                 extra_log=[done(NARROW_NODES), partial, reconciled])
    assert r.ok, r.stderr
    assert "6 COMPLETED" in r.stdout and "2 PENDING" in r.stdout


def test_a_sparse_child_still_takes_element_wise_edges(mock_run):
    """`aftercorr` maps child task i to parent task i. Task scripts keep their
    spec-order indices under a sparse submit, so the correspondence survives; the
    edge must not silently degrade to whole-array afterok."""
    spec = GRID + """
[recipe.after]
array      = true
array_axes = ["size", "batch"]
deps       = ["wide(dataset=${dataset}, size=${size}, batch=${batch})"]
params     = { dataset = ["a"], size = ["1", "10"], batch = ["1", "10", "100"] }
command    = "echo after ${dataset} ${size} ${batch}"
"""
    r = mock_run(spec, {}, "submit")
    assert r.ok, r.stderr
    child = r.submitted["after:a"]
    assert child.aftercorr == [r.submitted["wide:a"].job_id]
    assert child.afterok == []


def test_a_task_sacct_never_reported_keeps_the_units_verdict(mock_run):
    """sacct sometimes omits a task's row: the real log has a COMPLETED array whose
    final table names five of six tasks. A record speaks for everything it covered,
    so the missing one reads COMPLETED rather than becoming unrun work."""
    partial_table = done(NARROW_NODES, job_id="940")
    partial_table["tasks"] = {str(i): {"state": "COMPLETED"} for i in range(6) if i != 2}

    r = mock_run(GRID, {}, "status", "--local", extra_log=[partial_table])
    assert r.ok, r.stderr
    assert "6 COMPLETED" in r.stdout

    s = mock_run(GRID, {}, "submit", "--local", extra_log=[partial_table])
    assert s.ok, s.stderr
    assert not s.submitted, "an unreported task must not be mistaken for unrun work"
