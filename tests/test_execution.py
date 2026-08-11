"""spec.md §11 execution: reconcile / submit skip-completed / resubmit-eligibility /
downstream-of-rerun staleness / the completed-parent edge drop.
"""
from specs import FANIN


def test_completed_unit_is_skipped(mock_run):
    r = mock_run(FANIN, {"up": ("COMPLETED", "700")}, "submit", "--only", "up*")
    assert r.ok
    assert ("up", "COMPLETED") in r.skipped
    assert "up" not in r.submitted


def test_live_unit_is_left_untouched(mock_run):
    # reconcile folds sacct: a SUBMITTED+id seed surfaced as RUNNING is live, so
    # submit skips it rather than resubmitting.
    r = mock_run(FANIN, {"up": ("RUNNING", "700")}, "submit", "--only", "up*")
    assert r.ok
    assert ("up", "RUNNING") in r.skipped
    assert "up" not in r.submitted


def test_failed_unit_is_resubmitted(mock_run):
    # Only COMPLETED is success; FAILED is resubmit-eligible.
    r = mock_run(FANIN, {"up": ("FAILED", "700")}, "submit", "--only", "up*")
    assert r.ok
    assert "up" in r.submitted
    assert not any(u == "up" for u, _ in r.skipped)


def test_rerun_propagates_downstream(mock_run):
    # Everything COMPLETED; force-rerun `up`. Its downstream `down` is now stale
    # and reruns too; the independent `side` stays skipped.
    state = {"up": ("COMPLETED", "700"), "side": ("COMPLETED", "800"),
             "down": ("COMPLETED", "900")}
    r = mock_run(FANIN, state, "submit", "--rerun", "up*")
    assert r.ok, r.stderr
    assert "up" in r.submitted
    assert "down" in r.submitted                      # downstream went stale
    assert ("side", "COMPLETED") in r.skipped         # independent branch untouched
    # down now targets up's fresh id (both in this wave's `keep`).
    assert r.submitted["down"].afterok == [r.submitted["up"].job_id]


def test_completed_parent_edge_is_dropped(engine):
    """White-box (migrated): a skipped COMPLETED parent must not be targeted by
    afterok — its old job id may have aged out of slurmctld. The edge is emitted
    only when the parent is in this run's `keep` set."""
    spec = {
        "recipe": {
            "build": {
                "params": {"dataset": ["abm14"]},
                "command": "./build ${dataset}",
                "output": "out/${dataset}.bin",
            },
            "gbbs-format": {
                "params": {"dataset": ["abm14"]},
                "deps": ["build(dataset=${dataset})"],
                "command": "./format ${build.output}",
            },
        }
    }
    eng = engine.Engine(spec)
    by_name = {u.name: u for u in eng.unit_order}
    build_u, child_u = by_name["build-abm14"], by_name["gbbs-format-abm14"]
    build_u.job_id = "9424587"
    uid = lambda u: u.job_id

    # keep excludes build (COMPLETED, skipped) -> no dependency emitted
    dropped = eng._cmd(child_u, uid, "script", keep={child_u})
    assert "-d 9424587" not in dropped
    assert "afterok" not in dropped and "-d " not in dropped

    # keep includes build (resubmitted or live) -> dependency emitted
    kept = eng._cmd(child_u, uid, "script", keep={child_u, build_u})
    assert "-d 9424587" in kept

    # default (keep=None) shows the full structural edge for dag/dry
    assert "-d 9424587" in eng._cmd(child_u, uid, "script")
