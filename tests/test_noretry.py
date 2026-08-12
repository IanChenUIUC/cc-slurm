"""spec.md §11 `--no-retry`: run only what has never been attempted, and never
submit a child whose afterok parent can no longer succeed."""
from specs import CHAIN, FANIN


def reason(r, unit):
    return next(why for name, why in r.skipped if name == unit)


def test_a_past_failure_is_not_retried(mock_run):
    """The whole point: a plain run resubmits `root`, `--no-retry` leaves it."""
    plain = mock_run(CHAIN, {"root": "FAILED"}, "submit")
    assert "root" in plain.submitted

    r = mock_run(CHAIN, {"root": "FAILED"}, "submit", "--no-retry")
    assert r.ok, r.stderr
    assert "root" not in r.submitted
    assert reason(r, "root") == "FAILED"


def test_invalidated_counts_as_attempted(mock_run):
    r = mock_run(CHAIN, {"root": "INVALIDATED"}, "submit", "--no-retry")
    assert r.ok, r.stderr
    assert "root" not in r.submitted


def test_the_subtree_of_a_skipped_failure_is_skipped_too(mock_run):
    """`mid` and `leaf` are absent, so they are eligible -- but their input is never
    going to exist, and the dependency edge to a skipped parent gets dropped, so
    submitting them would just fail on missing input."""
    r = mock_run(CHAIN, {"root": "FAILED"}, "submit", "--no-retry")
    assert r.submitted == {}
    assert reason(r, "mid") == "blocked by root FAILED"
    assert reason(r, "leaf") == "blocked by root FAILED"


def test_rerun_overrides_no_retry(mock_run):
    r = mock_run(CHAIN, {"root": "FAILED"}, "submit", "--no-retry", "--rerun", "root*")
    assert r.ok, r.stderr
    assert "root" in r.submitted
    assert "mid" in r.submitted and "leaf" in r.submitted   # no longer blocked


def test_a_clean_dag_decides_identically(mock_run):
    """With nothing attempted, `--no-retry` is a no-op -- it only ever removes work
    the log already knows about."""
    plain = mock_run(CHAIN, {}, "submit")
    quiet = mock_run(CHAIN, {}, "submit", "--no-retry")
    assert sorted(plain.submitted) == sorted(quiet.submitted) == ["leaf", "mid", "root"]


def test_completed_work_is_still_skipped_for_being_done(mock_run):
    r = mock_run(CHAIN, {"root": "COMPLETED"}, "submit", "--no-retry")
    assert reason(r, "root") == "COMPLETED"
    assert "mid" in r.submitted                            # a done parent blocks nothing


def test_child_of_a_live_afterok_parent_with_failed_tasks_is_skipped(mock_run):
    """afterok needs *every* task of the parent to succeed, so this edge can never be
    satisfied: slurm would kill the child, and it would read as the child's failure."""
    seed = {"up": {"id": "700", "tasks": {"0": "FAILED", "1": "RUNNING"}},
            "side": "COMPLETED"}
    r = mock_run(FANIN, seed, "submit", "--no-retry")
    assert r.ok, r.stderr
    assert "down" not in r.submitted
    assert reason(r, "down") == "afterok parent up has 1 failed task"


def test_a_healthy_live_parent_still_gets_its_children(mock_run):
    seed = {"up": {"id": "700", "tasks": {"0": "RUNNING", "1": "RUNNING"}},
            "side": "COMPLETED"}
    r = mock_run(FANIN, seed, "submit")
    assert "down" in r.submitted
    assert r.submitted["down"].afterok == ["700"]
