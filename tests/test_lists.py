"""spec.md §3/§4: declared lists, inclusive ranges, and `params` references.

A string in `params` names a declared list; it is never a literal value. Before
this existed, `dataset = "all"` was fed straight to itertools.product and
iterated as *characters*.
"""

LISTS = """
[defaults]
all      = ["cora", "citeseer"]
training = ["abm14"]

[recipe.genquery]
array   = true
reps    = "0..3"
sizes   = [1, 10]
params  = { dataset = "all" }
command = "gen ${dataset} sizes=${sizes}"

[recipe.consume]
array   = true
deps    = ["genquery(dataset=${dataset})"]
params  = { dataset = "all", rep = "genquery.reps", size = "genquery.sizes" }
command = "run ${dataset} ${rep} ${size} of ${genquery.sizes}"
"""


def idents(r, prefix):
    """Node identities the dag printed for one recipe. An array recipe prints
    `task <i>: <ident>` rows under `-v`; an individual job prints `[job] <ident>`."""
    out = []
    for ln in r.stdout.splitlines():
        tok = ln.split()[-1] if ln.split() else ""
        if tok.startswith(prefix + "-") and ":" not in tok:
            out.append(tok)
    return sorted(out)


def test_bare_reference_reads_a_defaults_list(mock_run):
    r = mock_run(LISTS, {}, "dag", "-v")
    assert r.ok, r.stderr + (r.error or "")
    assert idents(r, "genquery") == ["genquery-citeseer", "genquery-cora"]


def test_qualified_reference_reads_a_recipe_list(mock_run):
    """`rep = "genquery.reps"` is a static lookup of recipe.key in the TOML, not a
    DAG-time parent alias -- params fix node identity before deps are wired."""
    r = mock_run(LISTS, {}, "dag", "-v")
    assert r.ok, r.stderr + (r.error or "")
    # 2 datasets x 4 reps (0..3, inclusive) x 2 sizes
    assert len(idents(r, "consume")) == 16
    assert "consume-cora-0-1" in idents(r, "consume")
    assert "consume-citeseer-3-10" in idents(r, "consume")


def test_range_is_inclusive_and_keeps_string_identities(mock_run):
    """A range yields ints where a literal list of strings yielded str. Identities
    render via str() either way, so node names must be unaffected."""
    r = mock_run(LISTS, {}, "dag", "-v")
    ids = idents(r, "consume")
    assert sum(i.startswith("consume-cora-") for i in ids) == 8      # reps 0..3 x 2 sizes
    assert not any(i.endswith("-4") or "-4-" in i for i in ids)      # 4 is out of range


def test_list_interpolates_space_joined(mock_run):
    """The join is what lets a driver recover the list with .split()."""
    r = mock_run(LISTS, {}, "submit", "--dry")
    assert r.ok, r.stderr + (r.error or "")
    body = (r.workdir / ".pipeline" / "scripts" / "genquery.tasks" / "task-0").read_text()
    assert "sizes=1 10" in body


def test_parent_list_alias_interpolates_space_joined(mock_run):
    r = mock_run(LISTS, {}, "submit", "--dry")
    assert r.ok, r.stderr + (r.error or "")
    body = (r.workdir / ".pipeline" / "scripts" / "consume.tasks" / "task-0").read_text()
    assert "of 1 10" in body


BAD_BARE = """
[defaults]
all = ["cora"]
[recipe.solo]
params  = { dataset = "testing" }
command = "echo ${dataset}"
"""


def test_unknown_bare_reference_is_a_hard_error(mock_run):
    """The old behavior was to iterate 't','e','s','t',... silently."""
    r = mock_run(BAD_BARE, {}, "dag")
    assert not r.ok
    assert "solo.params.dataset" in (r.error or "") and "names no declared list" in r.error


BAD_QUALIFIED = """
[recipe.solo]
params  = { rep = "nosuch.reps" }
command = "echo ${rep}"
"""


def test_unknown_qualified_reference_is_a_hard_error(mock_run):
    r = mock_run(BAD_QUALIFIED, {}, "dag")
    assert not r.ok
    assert "no recipe 'nosuch'" in (r.error or "")


NOT_A_LIST = """
[defaults]
root = "/data"
[recipe.solo]
params  = { dataset = "root" }
command = "echo ${dataset}"
"""


def test_reference_to_a_non_list_is_a_hard_error(mock_run):
    r = mock_run(NOT_A_LIST, {}, "dag")
    assert not r.ok
    assert "not a list" in (r.error or "")


BACKWARD_RANGE = """
[recipe.solo]
params  = { rep = "5..2" }
command = "echo ${rep}"
"""


def test_backward_range_is_a_hard_error(mock_run):
    r = mock_run(BACKWARD_RANGE, {}, "dag")
    assert not r.ok
    assert "below start" in (r.error or "")
