# AGENTS

Quick on-ramp for AI agents and new contributors. The full rule set
lives in `.agents/rules/`. Start there.

## Common pixi tasks

| Task | Command |
| --- | --- |
| Lint + format + typecheck | `pixi run check` |
| Regenerate dep locks from `pyproject.toml` | `pixi run deps` |
| Regenerate GraphQL client from `schema.gql` | `pixi run graphql` |
| Regenerate script/flow metadata | `wmill generate-metadata --workspace localdev --yes` |
| Start local Windmill | `pixi run local-up` |
| Sync repo to local Windmill | `pixi run dev-sync` |
| Run a script end-to-end | `pixi run dev-run f/<area>/<name> --args '<json>'` |

## Rule files

- `.agents/rules/project.md` &mdash; cross-cutting project rules
  (auto-generated files, pre-commit discipline, lifecycle for creating
  and editing scripts).
- `.agents/rules/windmill.md` &mdash; single-page reference for the
  Windmill sync lifecycle, auto-generated file patterns, and common
  failure modes.
- `.agents/rules/python.md` &mdash; Python script conventions.
- `.agents/rules/graphql.md` &mdash; GraphQL client conventions.
- `.agents/rules/flows.md` &mdash; Windmill flow YAML conventions.
- `.agents/rules/typescript.md`, `go.md`, `rust.md`, `sql.md`, `cli.md`
  &mdash; language- or tool-specific conventions.

## Skills

- `.agents/skills/codebase-context/` &mdash; survey the repo's
  architecture before designing a new feature.
- `.agents/skills/run-review/` &mdash; pre-commit quality check.
- `.agents/skills/review-patterns/` &mdash; team coding conventions.
- `.agents/skills/review-and-fix-issues/` &mdash; address PR review
  comments.
- `.agents/skills/scan/` &mdash; recent security and quality scan
  results.
