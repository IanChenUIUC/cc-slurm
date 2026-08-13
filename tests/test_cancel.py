"""spec.md §11, the three manual-override verbs. They share `_force_state`, so what
separates them is what they write, whether they talk to the cluster, and what each
does to a unit that is still live."""
from specs import CHAIN


def states(r):
    return {rec["unit"]: rec["state"] for rec in r.log}


def test_cancel_needs_no_flag_for_work_that_never_ran(mock_run):
    """The motivating case: a subtree whose dependency died, so it was never submitted
    and has no live job to stop. `cancel` is for work that is over either way."""
    r = mock_run(CHAIN, {}, "cancelled", "leaf*")
    assert r.ok, r.stderr
    assert states(r)["leaf"] == "CANCELLED"


def test_cancel_marks_a_live_unit_without_warning(mock_run):
    """It cannot orphan anything -- the scancel half stops the job -- so it is the one
    state verb with nothing to say about liveness."""
    r = mock_run(CHAIN, {"root": ("RUNNING", "700")}, "cancelled", "root*")
    assert r.ok, r.stderr
    assert states(r)["root"] == "CANCELLED"
    assert "warning" not in r.stderr


def test_invalidate_refuses_a_live_unit_and_writes_nothing(mock_run):
    """INVALIDATED is resubmit-eligible, so marking a running job puts a second job on
    the same output files. The refusal has to leave the log untouched."""
    r = mock_run(CHAIN, {"root": ("RUNNING", "700")}, "invalidate", "root*")
    assert not r.ok
    assert "root (job 700) is live" in r.stderr
    assert "INVALIDATED" not in [rec["state"] for rec in r.log]


def test_force_invalidates_a_live_unit_and_says_it_is_orphaned(mock_run):
    r = mock_run(CHAIN, {"root": ("RUNNING", "700")}, "invalidate", "root*", "--force")
    assert r.ok, r.stderr
    assert states(r)["root"] == "INVALIDATED"
    assert "warning: root is live (job 700) -- orphaned" in r.stderr


def test_complete_only_warns_on_a_live_unit(mock_run):
    """It loses that job's result, but submit skips COMPLETED, so nothing is
    resubmitted -- which is why it warns where `invalidate` refuses."""
    r = mock_run(CHAIN, {"root": ("RUNNING", "700")}, "complete", "root*")
    assert r.ok, r.stderr
    assert states(r)["root"] == "COMPLETED"
    assert "warning: root is live (job 700) -- orphaned" in r.stderr


def test_dry_writes_nothing(mock_run):
    for verb, label in (("cancelled", "cancelled"), ("invalidate", "invalidated"),
                        ("complete", "completed")):
        r = mock_run(CHAIN, {"root": "FAILED"}, verb, "root*", "--dry")
        assert r.ok, r.stderr
        assert f"(dry) {label} root" in r.stdout
        assert states(r)["root"] == "FAILED"


def test_print_ids_reads_liveness_before_it_marks(mock_run):
    """The bug that made `cancel` a no-op against the cluster: run as a second process
    after the mark, `cancel-ids` sees CANCELLED everywhere and prints nothing, so the
    job it was supposed to kill kept running."""
    r = mock_run(CHAIN, {"root": ("RUNNING", "700")}, "cancelled", "root*", "--print-ids")
    assert r.ok, r.stderr
    assert r.stdout.split() == ["700"]                # stdout is ids alone
    assert "cancelled root" in r.stderr               # ...and the prose moved aside
    assert states(r)["root"] == "CANCELLED"


def test_print_ids_still_reaches_a_job_whose_recipe_is_gone(mock_run):
    """The two halves match different things on purpose: the mark matches the spec,
    the kill matches the log, so a renamed-away recipe's job is still stoppable."""
    r = mock_run(CHAIN, {"retired-unit": ("RUNNING", "701")}, "cancelled",
                 "retired-unit*", "--print-ids")
    assert r.ok, r.stderr
    assert r.stdout.split() == ["701"]
    assert "no nodes matched" in r.stderr


def test_cancelled_work_is_still_rerunnable(mock_run):
    """A mark, not a tombstone: an ordinary terminal failure afterwards."""
    r = mock_run(CHAIN, {"root": "CANCELLED"}, "submit")
    assert "root" in r.submitted

    quiet = mock_run(CHAIN, {"root": "CANCELLED"}, "submit", "--no-retry")
    assert "root" not in quiet.submitted             # ...and --no-retry still skips it


def test_a_mistaken_cancel_is_recoverable(mock_run):
    """Nothing is protected, not even COMPLETED, because the log is append-only: the
    prior record survives in `history` and `complete` puts the state back."""
    r = mock_run(CHAIN, {"root": "COMPLETED"}, "cancelled", "root*")
    assert states(r)["root"] == "CANCELLED"

    back = mock_run(CHAIN, {"root": "CANCELLED"}, "complete", "root*")
    assert states(back)["root"] == "COMPLETED"

    hist = mock_run(CHAIN, {"root": "COMPLETED"}, "cancelled", "root*")
    assert [rec["state"] for rec in hist.log if rec["unit"] == "root"] == \
        ["COMPLETED", "CANCELLED"]                   # both attempts survive
