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
- `conftest.py` — fixtures `mock_run` (black-box, primary) and `engine` (white-box,
  the loaded module for `Engine(...)` internals).
- `specs.py` — reusable spec fragments.

## Sections

| spec § | Behavior | Status | Test |
|--------|----------|--------|------|
| §3  | `params` product / record / jagged; `${node}` identity; uniqueness | TODO | — |
| §4  | interpolation: simple / `${parent.alias}` / `${slurm.KEY}` / list join-vs-splice | TODO | — |
| §5  | aliases: topo-order resolution, alias cycle | TODO | — |
| §6  | captures wire; dashed recipe names | partial | `test_deps.py::test_dashed_recipe_captures_parse_and_wire` |
| §6  | explicit-fan-in rule, zero-match, `${listvar}` splice | TODO | — |
| §7  | slurm three-level precedence; unknown-key error | TODO | — |
| §9  | multi-line array → one intact script per task | partial | `test_arrays.py::test_multiline_array_command_materializes_one_script_per_task` |
| §9  | array eligibility errors, `array_axes` split, `max_array_size`, dep-translation table (`aftercorr` vs `afterok`) | TODO | — |
| §11 | reconcile folds sacct; skip-COMPLETED; FAILED resubmit-eligible; live left untouched; rerun → downstream stale; completed-parent edge dropped | ✅ | `test_execution.py` |
| §11 | `--only`: parent COMPLETED / live(→afterok) / in-run / absent / FAILED; `--local` rejects stale live | ✅ | `test_only.py` |
| §11 | `status` roll-up (`-v` expand) | TODO | — |
| §11 | `submit --dry`: submits nothing, logs no submission, still materializes; plans only what needs running; matches a real submit's units/deps/flags; in-wave parents as `<placeholders>`; honors `--only` preconditions and `--rerun` | ✅ | `test_dry.py` |
| §11 | `cancel-ids`: omits terminal, honors globs, still reaches units absent from the spec | ✅ | `test_verbs.py` |
| §11 | `dag <glob>` restricts units but keeps edges to parents outside the glob | ✅ | `test_verbs.py` |
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
| `array = true` on ineligible recipe | TODO | — |

## Notes / spec drift

- **`-C` vs `--aftercorr`** — resolved. `spec.md` §8 now documents `-C <id>` (and `-d`
  for `afterok`), matching what `_cmd` emits and what the tests assert.
- **`just` flag syntax.** Behaviour flags (`local`/`force`/`only`/`verbose`) are
  top-level `justfile` variables, so they must precede the recipe name
  (`just force=1 run 'g'`). `just` treats `name=value` *after* a recipe as a
  positional argument — the older `just status verbose=1` silently passed
  `verbose=1` as the spec path. Verified on just 1.45.0.
