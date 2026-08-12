"""spec.md Sec.5 per-param slurm mapping: one resource value that varies with a
param, without re-inlining the dataset list as record-form `params`. Motivated by
`gbbs-format`, which needs 256GB for two of nine networks and 128GB for the rest.
"""
MAP = """
[recipe.fmt]
array   = true
params  = { dataset = ["small", "huge"] }
command = "echo ${dataset}"
slurm   = { cpus = 1, mem = { param = "dataset", default = "128GB", huge = "256GB" } }
"""


JOBS = MAP.replace("array   = true\n", "")


def flags(r, unit):
    return r.planned[unit].flags


def test_mapping_picks_the_value_per_node(engine):
    import tomllib
    eng = engine.Engine(tomllib.loads(JOBS))
    mem = {n.ident: n.slurm["mem"] for n in eng.order}
    assert mem == {"fmt-small": "128GB", "fmt-huge": "256GB"}


def test_unmapped_keys_are_untouched(engine):
    import tomllib
    eng = engine.Engine(tomllib.loads(JOBS))
    assert all(n.slurm["cpus"] == "1" for n in eng.order)


NO_PARAM = MAP.replace('{ param = "dataset", default = "128GB", huge = "256GB" }',
                       '{ default = "128GB", huge = "256GB" }')
WRONG_PARAM = MAP.replace('param = "dataset"', 'param = "network"')
NO_DEFAULT = MAP.replace('default = "128GB", ', "")


def test_mapping_without_param_is_a_hard_error(mock_run):
    r = mock_run(NO_PARAM, {}, "dag")
    assert not r.ok
    assert "no 'param' key" in (r.error or "")


def test_mapping_on_an_unknown_param_is_a_hard_error(mock_run):
    """Naming a param the recipe lacks is a typo, not a request for the default --
    silently defaulting would hand every node the same resources."""
    r = mock_run(WRONG_PARAM, {}, "dag")
    assert not r.ok
    assert "not a param of this recipe" in (r.error or "")


def test_unlisted_value_without_default_is_a_hard_error(mock_run):
    r = mock_run(NO_DEFAULT, {}, "dag")
    assert not r.ok
    assert "no entry for dataset='small'" in (r.error or "")


def test_mapped_value_reaches_the_runner(mock_run):
    r = mock_run(JOBS, {}, "submit", "--dry")
    assert r.ok, r.stderr + (r.error or "")
    assert flags(r, "fmt-small")["-m"] == "128GB"
    assert flags(r, "fmt-huge")["-m"] == "256GB"


def test_mapping_on_an_array_axis_is_ineligible(mock_run):
    """Sec.9's uniform-resources rule still governs: a mapping keyed on the very
    param an array sweeps gives its cells different resources, so the recipe must
    either split on another param or stop being an array. The error is the one place
    that trade-off is visible, so it is pinned here."""
    r = mock_run(MAP, {}, "dag")
    assert not r.ok
    assert "non-uniform resources across cells" in (r.error or "")
