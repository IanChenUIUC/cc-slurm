"""spec.md §4/§9: `command_file` + `interpreter`, and the materialized artifact.

A materialized unit is a self-contained executable that declares its own
interpreter in a shebang, so the runners exec it directly instead of assuming
bash. `set -euo pipefail` is bash-specific and is injected only when no
`interpreter` is declared.
"""
import os
import stat

PY_BODY = '''\
import sys
DATASET = "${dataset}"
CPUS    = int("${slurm.cpus}")
print(f"{DATASET} on {CPUS}")
sys.exit(0)
'''

SPEC = """
[recipe.drive]
params       = { dataset = ["cora"] }
interpreter  = "/usr/bin/env python3"
command_file = "body.py"
slurm        = { cpus = 4 }
"""


def run_with_body(mock_run, tmp_path, spec, body, *args, name="body.py"):
    """mock_run writes the spec into `tmp_path`, so a command_file relative to the
    spec dir lands next to it."""
    (tmp_path / name).write_text(body)
    return mock_run(spec, {}, "submit", "--dry", *args, workdir=tmp_path)


def script(r, unit="drive-cora"):
    """An individual job's unit is named per node, so one-node `drive` is `drive-cora`."""
    return r.workdir / ".pipeline" / "scripts" / unit


def test_command_file_is_interpolated_and_gets_a_shebang(mock_run, tmp_path):
    r = run_with_body(mock_run, tmp_path, SPEC, PY_BODY)
    assert r.ok, r.stderr + (r.error or "")
    text = script(r).read_text()
    assert text.startswith("#!/usr/bin/env python3\n")
    assert 'DATASET = "cora"' in text
    assert 'CPUS    = int("4")' in text          # ${slurm.KEY} works in a command_file
    assert "${" not in text                      # nothing left unsubstituted
    assert "set -euo pipefail" not in text       # bash-only, not injected here


def test_materialized_script_is_executable(mock_run, tmp_path):
    r = run_with_body(mock_run, tmp_path, SPEC, PY_BODY)
    assert r.ok, r.stderr + (r.error or "")
    assert stat.S_IMODE(script(r).stat().st_mode) == 0o755
    assert os.access(script(r), os.X_OK)


def test_the_script_actually_runs_under_its_interpreter(mock_run, tmp_path):
    """The point of the shebang + exec bit: the artifact is directly runnable, which
    is exactly what cc-local and *.sbatch.sh now rely on."""
    import subprocess
    r = run_with_body(mock_run, tmp_path, SPEC, PY_BODY)
    assert r.ok, r.stderr + (r.error or "")
    proc = subprocess.run([str(script(r))], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "cora on 4"


BASH_SPEC = """
[recipe.drive]
params  = { dataset = ["cora"] }
command = "echo ${dataset}"
"""


def test_without_an_interpreter_the_script_is_bash_as_before(mock_run, tmp_path):
    """Regression guard: the default path must keep bash's strict-mode preamble."""
    r = mock_run(BASH_SPEC, {}, "submit", "--dry", workdir=tmp_path)
    assert r.ok, r.stderr + (r.error or "")
    assert script(r).read_text() == "#!/bin/bash\nset -euo pipefail\necho cora\n"


INLINE_INTERP = """
[recipe.drive]
params      = { dataset = ["cora"] }
interpreter = "/usr/bin/env python3"
command     = "print('hi ${dataset}')"
"""


def test_interpreter_without_command_file_is_allowed(mock_run, tmp_path):
    """`interpreter` describes how the body runs; it does not require the body to
    live in a file."""
    r = mock_run(INLINE_INTERP, {}, "submit", "--dry", workdir=tmp_path)
    assert r.ok, r.stderr + (r.error or "")
    assert script(r).read_text() == "#!/usr/bin/env python3\nprint('hi cora')\n"


BOTH = """
[recipe.drive]
params       = { dataset = ["cora"] }
command      = "echo ${dataset}"
command_file = "body.py"
"""


def test_declaring_both_command_and_command_file_is_a_hard_error(mock_run, tmp_path):
    r = run_with_body(mock_run, tmp_path, BOTH, PY_BODY)
    assert not r.ok
    assert "both command and command_file" in (r.error or "")


MISSING = """
[recipe.drive]
params       = { dataset = ["cora"] }
command_file = "nope.py"
"""


def test_missing_command_file_is_a_hard_error(mock_run, tmp_path):
    """Before command_file was reserved, it was swallowed as an alias and the job
    body silently became empty."""
    r = mock_run(MISSING, {}, "submit", "--dry", workdir=tmp_path)
    assert not r.ok
    assert "cannot read command_file" in (r.error or "")


ARRAY_SPEC = """
[recipe.drive]
array        = true
params       = { dataset = ["cora", "citeseer"] }
interpreter  = "/usr/bin/env python3"
command_file = "body.py"
slurm        = { cpus = 2 }
"""


def test_array_tasks_each_get_the_shebang_and_exec_bit(mock_run, tmp_path):
    r = run_with_body(mock_run, tmp_path, ARRAY_SPEC, PY_BODY)
    assert r.ok, r.stderr + (r.error or "")
    tasks = sorted((r.workdir / ".pipeline" / "scripts" / "drive.tasks").glob("task-*"))
    assert [t.name for t in tasks] == ["task-0", "task-1"]
    for t in tasks:
        assert t.read_text().startswith("#!/usr/bin/env python3\n")
        assert stat.S_IMODE(t.stat().st_mode) == 0o755
    bodies = "".join(t.read_text() for t in tasks)
    assert 'DATASET = "cora"' in bodies and 'DATASET = "citeseer"' in bodies
