"""D1: per-task sacct state. The engine folds sacct rows twice — per task, then per
unit — so a failure inside a 240-task array is attributable and each task keeps its
own elapsed/peak RSS, while the per-unit verdict `submit` reads stays what it was.
"""
import pathlib

from specs import FANIN, SPLIT

GB = 1024 ** 3
KB = 1024
SAMPLE = pathlib.Path(__file__).with_name("sacct_sample.txt")


def test_fold_matches_real_sacct_output(engine):
    """White-box, against **real** `sacct --parsable2` rows captured from the cluster
    (tests/sacct_sample.txt) rather than the fake's idea of them: a task's main row
    carries no MaxRSS, `.batch` carries it in K, `.extern` carries none and can report
    one second *more* elapsed than the main row -- which is why elapsed is taken from
    the main row and not maxed over the steps."""
    rows = [ln.split("|") for ln in SAMPLE.read_text().splitlines() if ln.strip()]
    rec = engine.Engine({})._parse_sacct(rows)["9789233"]
    assert rec["state"] == "COMPLETED"
    assert set(rec["tasks"]) == {"0", "5", "8", "9"}
    assert rec["tasks"]["0"]["max_rss"] == 5252468 * KB
    assert rec["tasks"]["8"]["max_rss"] == 2130912 * KB
    assert rec["tasks"]["5"]["elapsed"] == "00:16:27"       # not the .extern's 00:16:28
    assert rec["max_rss"] == 5259164 * KB                   # worst task, for the unit


def tasks(**states):
    """A `_seed` array entry: index -> state (or a per-task dict)."""
    return {"tasks": states, "id": "700"}


def test_per_task_states_are_recorded(mock_run):
    # `down` is one 6-task array (dataset x rep, no array_axes). Two tasks failed.
    seed = tasks(**{"0": "COMPLETED", "1": "COMPLETED", "2": "FAILED",
                    "3": "COMPLETED", "4": "FAILED", "5": "COMPLETED"})
    r = mock_run(FANIN, {"down": seed}, "status")
    assert r.ok, r.stderr
    assert r.tasks("down") == {"0": "COMPLETED", "1": "COMPLETED", "2": "FAILED",
                              "3": "COMPLETED", "4": "FAILED", "5": "COMPLETED"}


def test_unit_state_folds_from_tasks(mock_run):
    # The per-unit verdict is unchanged: any non-COMPLETED task fails the unit.
    seed = tasks(**{"0": "COMPLETED", "1": "FAILED", "2": "COMPLETED"})
    r = mock_run(FANIN, {"down": seed}, "status")
    assert r.latest["down"]["state"] == "FAILED"


def test_all_tasks_completed_folds_to_completed(mock_run):
    r = mock_run(FANIN, {"down": tasks(**{"0": "COMPLETED", "1": "COMPLETED"})}, "status")
    assert r.latest["down"]["state"] == "COMPLETED"


def test_one_running_task_keeps_the_unit_live(mock_run):
    # Live beats terminal, so the unit is still RUNNING and submit must not touch it.
    seed = tasks(**{"0": "COMPLETED", "1": "RUNNING", "2": "FAILED"})
    r = mock_run(FANIN, {"down": seed}, "submit", "--only", "down*")
    assert r.ok, r.stderr
    assert ("down", "RUNNING") in r.skipped
    assert "down" not in r.submitted


def test_peak_rss_is_per_task_not_smeared(mock_run):
    """The bug this fold fixes: one flat group per base id attributed the worst
    task's MaxRSS to every task, and mixed the main row with its `.batch` step."""
    seed = {"id": "700", "tasks": {
        "0": {"state": "COMPLETED", "maxrss": "1G", "elapsed": "00:01:00"},
        "1": {"state": "COMPLETED", "maxrss": "40G", "elapsed": "02:00:00"}}}
    r = mock_run(FANIN, {"down": seed}, "status")
    per_task = r.latest["down"]["tasks"]
    assert per_task["0"]["max_rss"] == 1 * GB
    assert per_task["1"]["max_rss"] == 40 * GB
    assert per_task["0"]["elapsed"] == "00:01:00"
    assert r.latest["down"]["max_rss"] == 40 * GB      # unit still reports the worst


def test_individual_job_gets_no_tasks_key(mock_run):
    # No `_<idx>` in the job id, so there is nothing to tabulate and the record
    # keeps exactly its old shape.
    spec = '[recipe.solo]\ncommand = "echo hi"\n'
    r = mock_run(spec, {"solo": ("RUNNING", "700")}, "status")
    assert "tasks" not in r.latest["solo"]


def test_pending_array_range_row_is_not_a_task(mock_run):
    """A pending array appears as `<base>_[5-9]`, which is not a task index. It must
    still fold into the unit state without polluting the task table."""
    seed = {"id": "700", "tasks": {"0": "COMPLETED", "[1-5]": "PENDING"}}
    r = mock_run(FANIN, {"down": seed}, "status")
    assert r.latest["down"]["state"] == "RUNNING"       # pending is live
    assert set(r.tasks("down")) == {"0"}


def test_unchanged_observation_is_not_appended(mock_run):
    """A running array is re-observed on every `status`; re-appending an identical
    240-task table would grow the log without recording anything new."""
    seed = tasks(**{"0": "COMPLETED", "1": "RUNNING"})
    prior = {"unit": "down", "kind": "array", "job_id": "700", "state": "RUNNING",
             "event": "reconcile", "elapsed": "00:00:30", "max_rss": 100 * 1024 ** 2,
             "tasks": {"0": {"state": "COMPLETED", "elapsed": "00:00:30",
                             "max_rss": 100 * 1024 ** 2},
                       "1": {"state": "RUNNING", "elapsed": "00:00:30",
                             "max_rss": 100 * 1024 ** 2}}}
    r = mock_run(FANIN, {"down": seed}, "status", extra_log=[prior])
    assert r.ok, r.stderr
    assert len([x for x in r.log if x["unit"] == "down"]) == 2      # seed + prior only


def test_changed_observation_is_appended(mock_run):
    # The dedup must not swallow a real transition.
    seed = tasks(**{"0": "COMPLETED", "1": "COMPLETED"})
    prior = {"unit": "down", "kind": "array", "job_id": "700", "state": "RUNNING",
             "event": "reconcile"}
    r = mock_run(FANIN, {"down": seed}, "status", extra_log=[prior])
    assert r.latest["down"]["state"] == "COMPLETED"
    assert len([x for x in r.log if x["unit"] == "down"]) == 3


# ---- status output ------------------------------------------------------------

def test_status_rollup_counts_tasks_not_units(mock_run):
    # `wide` splits into 2 arrays of 3 tasks; one task of one array failed.
    state = {"wide:a": tasks(**{"0": "COMPLETED", "1": "FAILED", "2": "COMPLETED"}),
             "wide:b": {"tasks": {"0": "COMPLETED", "1": "COMPLETED",
                                  "2": "COMPLETED"}, "id": "800"}}
    r = mock_run(SPLIT, state, "status")
    assert r.ok, r.stderr
    line = next(ln for ln in r.stdout.splitlines() if ln.startswith("wide "))
    assert "[2 arrays, 6 tasks]" in line
    assert "5 COMPLETED" in line and "1 FAILED" in line


def test_status_verbose_names_the_failed_tasks(mock_run):
    state = {"wide:a": tasks(**{"0": "COMPLETED", "1": "FAILED", "2": "TIMEOUT"}),
             "wide:b": {"tasks": {"0": "COMPLETED", "1": "COMPLETED",
                                  "2": "COMPLETED"}, "id": "800"}}
    r = mock_run(SPLIT, state, "status", "-v")
    assert r.ok, r.stderr
    named = [ln.split()[-1] for ln in r.stdout.splitlines()
             if ln.startswith("      ")]
    assert named == ["wide-a-1", "wide-a-2"]
    assert "failed" in r.stdout and "timeout" in r.stdout


def test_whole_unit_failure_names_no_tasks(mock_run):
    # Every task failed: the unit row already says so, and listing all of them
    # would be noise.
    state = {"wide:a": tasks(**{"0": "FAILED", "1": "FAILED", "2": "FAILED"}),
             "wide:b": {"tasks": {"0": "COMPLETED", "1": "COMPLETED",
                                  "2": "COMPLETED"}, "id": "800"}}
    r = mock_run(SPLIT, state, "status", "-v")
    assert not [ln for ln in r.stdout.splitlines() if ln.startswith("      ")]


def test_record_without_tasks_still_renders(mock_run):
    """Every record the pre-R5 engine wrote lacks `tasks`; status must degrade to
    the unit-level line rather than failing."""
    state = {"wide:a": ("FAILED", "700"), "wide:b": ("COMPLETED", "800")}
    r = mock_run(SPLIT, state, "status", "-v")
    assert r.ok, r.stderr
    assert "FAILED" in r.stdout
    assert not [ln for ln in r.stdout.splitlines() if ln.startswith("      ")]


def test_submit_still_resubmits_the_whole_array(mock_run):
    """Per-task *state* is R5; per-task *resubmission* is R6. A partly-failed array
    is resubmitted whole, and this pins that R6 has not leaked in early."""
    seed = tasks(**{"0": "COMPLETED", "1": "FAILED", "2": "COMPLETED"})
    r = mock_run(FANIN, {"down": seed, "up": ("COMPLETED", "1"),
                         "side": ("COMPLETED", "2")}, "submit", "--only", "down*")
    assert r.ok, r.stderr
    assert "down" in r.submitted
    assert r.submitted["down"].kind == "array"
