"""spec.md §9 arrays. (Migrated from the original test_multiline_array.py.)"""

MULTILINE = """\
[recipe.build]
array   = true
params  = { dataset = ["cora", "citeseer", "pubmed"] }
command = \"\"\"
echo "start ${dataset}"
./build ${dataset} > out/${dataset}.log
echo "done ${dataset}"
\"\"\"
output  = "out/${dataset}.bin"
"""

DATASETS = ["cora", "citeseer", "pubmed"]


def test_multiline_array_command_materializes_one_script_per_task(mock_run):
    """A multi-line `command` in an `array = true` recipe becomes one intact
    script per task (`task-<idx>`), not one physical line per command line."""
    r = mock_run(MULTILINE, {}, "submit", "--dry")
    assert r.ok, r.stderr

    tasks_dir = r.workdir / ".pipeline" / "scripts" / "build.tasks"
    assert tasks_dir.is_dir()
    scripts = sorted(tasks_dir.glob("task-*"))
    # One task per node -- NOT one per physical command line (the original bug).
    assert {s.name for s in scripts} == {f"task-{i}" for i in range(len(DATASETS))}

    bodies = [s.read_text() for s in scripts]
    for ds in DATASETS:
        body = next(b for b in bodies if f'echo "start {ds}"' in b)
        assert f"./build {ds} > out/{ds}.log" in body
        assert f'echo "done {ds}"' in body
        assert body.count("\n") >= 4                       # all lines in one script

    assert not (r.workdir / ".pipeline" / "scripts" / "build.cmds").exists()
    assert "cc-submit array" in r.stdout and "build.tasks" in r.stdout
