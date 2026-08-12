# Test coverage ledger

Maps each `spec.md` section (and the §10 error list) to the test that pins it, or
`TODO`. Completeness is a work-in-progress; this table is the source of truth for
what's covered and where a new test belongs.

**Run:** `uv run pytest` (from the repo root). Tests are dev-only — not shipped in
the template (`gen-template.sh` copies only the four runners).

## Harness

- `mockpipe.py` — mock a pipeline's state and run a `pipeline.py` action against it,
  no cluster. Dual-use: a CLI ("is this fixed?" checker) and the `mock_run` library
  the tests import. Fakes under `fakes/` stand in for `cc-submit` (records what would
  be submitted) and `sacct` (scripts job states). See its module docstring.
  A unit seeded as `{"tasks": {0: "COMPLETED", 1: "FAILED"}}` is surfaced through the
  fake `sacct` as per-task main + `.batch` rows, so the *engine's* fold decides the
  unit state rather than the test asserting it. `extra_log=` seeds prior records for
  cases that need a specific history.
- `conftest.py` — fixtures `mock_run` (black-box, primary) and `engine` (white-box,
  the loaded module for `Engine(...)` internals).
- `specs.py` — reusable spec fragments.

## Sections

| spec § | Behavior | Status | Test |
|--------|----------|--------|------|
| §3  | `params` product / record / jagged; `${node}` identity; uniqueness | TODO | — |
| §3  | `params` list references (bare + qualified); inclusive ranges; identities unaffected by int-vs-str | ✅ | `test_lists.py` |
| §4  | interpolation: simple / `${parent.alias}` / `${slurm.KEY}` / list join-vs-splice | TODO | — |
| §4  | a list alias interpolates space-joined, own and via `${parent.alias}` | ✅ | `test_lists.py` |
| §8  | `command_file` interpolated + shebang from `interpreter`; verbatim body (no bash preamble); mode 0755; the artifact actually executes; bash default unchanged; `interpreter` alone is legal; array tasks each get both | ✅ | `test_command_file.py` |
| §5  | aliases: topo-order resolution, alias cycle | TODO | — |
| §6  | captures wire; dashed recipe names | partial | `test_deps.py::test_dashed_recipe_captures_parse_and_wire` |
| §6  | explicit-fan-in rule, zero-match, `${listvar}` splice | TODO | — |
| §7  | slurm three-level precedence; unknown-key error | TODO | — |
| §7  | per-param `slurm` mapping: value per node, other keys untouched, reaches the runner, ineligible on an array axis | ✅ | `test_slurm_map.py` |
| §9  | multi-line array → one intact script per task | partial | `test_arrays.py::test_multiline_array_command_materializes_one_script_per_task` |
| §9  | array eligibility errors, `array_axes` split, `max_array_size`, dep-translation table (`aftercorr` vs `afterok`) | TODO | — |
| §11 | reconcile folds sacct; skip-COMPLETED; FAILED resubmit-eligible; live left untouched; rerun → downstream stale; completed-parent edge dropped | ✅ | `test_execution.py` |
| §11 | `--only`: parent COMPLETED / live(→afterok) / in-run / absent / FAILED; `--local` rejects stale live | ✅ | `test_only.py` |
| §11 | `status` roll-up: task-level histogram, `-v` expands and names non-COMPLETED tasks, silent when the whole unit shares one state, degrades for a record with no `tasks` | ✅ | `test_tasks.py` |
| §11 | reconcile's two-level fold: per-task state/elapsed/RSS, unit verdict unchanged, RSS not smeared across tasks, no `tasks` key for an individual job, pending `_[a-b]` range is not a task, unchanged observation not re-appended | ✅ | `test_tasks.py` |
| §11 | `history`: every attempt in order, event labels (incl. inferred for pre-`event` records), `(no history)` vs a failed attempt, glob restriction, task histogram | ✅ | `test_history.py` |
| §11 | a partly-failed array is still resubmitted **whole** (per-task resubmission is not implemented) | ✅ | `test_tasks.py` |
| §11 | `submit --dry`: submits nothing, logs no submission, still materializes; plans only what needs running; matches a real submit's units/deps/flags; in-wave parents as `<placeholders>`; honors `--rerun`; reports unmet `--only` prerequisites (transitively, topo order) instead of erroring | ✅ | `test_dry.py` |
| §11 | `--deps`: pulls the whole upstream chain, stops at a COMPLETED or live ancestor, no downstream propagation, no-op without `--only` | ✅ | `test_only.py` |
| §11 | `cancel-ids`: omits terminal, honors globs, still reaches units absent from the spec | ✅ | `test_verbs.py` |
| §11 | `dag <glob>` restricts units but keeps edges to parents outside the glob | ✅ | `test_verbs.py` |
| §11 | `dag` rolls arrays up; `-v` expands to tasks and is otherwise identical | ✅ | `test_verbs.py` |
| §11 | `status <glob>` restricts rows, keeps a whole array whose task matched; `--local` never consults sacct | ✅ | `test_verbs.py` |
| §11 | `invalidate` / `complete` / `--rerun` / `log-ids` | TODO | — |
| §7  | valueless slurm flag: `true` emits `-x`; a non-boolean is a hard error | ✅ | `test_verbs.py` |

## §10 hard-error list

| Error | Status | Test |
|-------|--------|------|
| undefined `${variable}` | TODO | — |
| `${ref.alias}` where ref is not a dependency | TODO | — |
| capture names unknown recipe | TODO | — |
| capture omits a matched node's binding key | TODO | — |
| capture matches zero nodes | TODO | — |
| duplicate node identity | TODO | — |
| dependency cycle / alias cycle | TODO | — |
| reserved word used as alias | TODO | — |
| unknown `slurm.*` key | TODO | — |
| slurm mapping: no `param`, unknown `param`, no entry and no `default` | ✅ | `test_slurm_map.py` |
| `array = true` on ineligible recipe | TODO | — |
| `params` string naming no declared list / a non-list | ✅ | `test_lists.py` |
| range with end below start | ✅ | `test_lists.py` |
| both `command` and `command_file`; unreadable `command_file` | ✅ | `test_command_file.py` |

## Notes / spec drift

- **The fake `sacct`'s row shape is confirmed against the cluster** (2026-08-11). Real
  rows are in `tests/sacct_sample.txt` and `test_tasks.py::test_fold_matches_real_sacct_output`
  folds them directly, so the assumption is now pinned rather than believed: a task's
  main row carries **no** MaxRSS, `.batch` carries it in `K`, `.extern` carries none —
  and `.extern` can report one second *more* elapsed than the main row, which is why
  elapsed comes from the main row rather than a max over the steps. `sacct` still does
  not exist on the dev box, so refresh the sample if the cluster's format ever changes.

- **`-C` vs `--aftercorr`** — resolved. `spec.md` §8 now documents `-C <id>` (and `-d`
  for `afterok`), matching what `_cmd` emits and what the tests assert.
- **No escape for `${...}`.** Deliberate (spec.md §4), but it bites hardest in a
  `command_file`: a placeholder-looking string in a *comment or docstring* is
  interpolated too and fails as an undefined variable. Hit while writing
  `examples/cmds/analyze.py`, which is why that file says so out loud.
- **`${parent.alias}` joins across every matched parent**, so a `rep=*` fan-in reading
  an alias its parents share repeats it once per parent (`reps 0 1 2 0 1 2 0 1 2`).
  Pre-existing and unchanged — list aliases only made it visible. Untested; would need
  a decision on whether de-duplication is wanted.
- **`just` flag syntax.** Behaviour flags (`local`/`force`/`only`/`verbose`) are
  top-level `justfile` variables, so they must precede the recipe name
  (`just force=1 run 'g'`). `just` treats `name=value` *after* a recipe as a
  positional argument — the older `just status verbose=1` silently passed
  `verbose=1` as the spec path. Verified on just 1.45.0.
