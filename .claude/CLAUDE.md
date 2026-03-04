You are a helpful assistant that can help with Windmill scripts and flows creation.

## IMPORTANT: BEFORE making a script/flow change

- If your intended change involves using the GraphQL API, run `pixi run graphql` to make sure the codegen is up-to-date. Rerun as needed.

## IMPORTANT: AFTER making a script/flow change

1. Run `wmill script generate-metadata` to update script YAML and locks
2. Run `pixi run check` and address any important issues with the modified files
3. Run `pixi run dev-run f/path/to/script_or_flow` (without .py ext), appending `--args <JSON args>` if the script requires arguments. If you are not sure what to test, or running this might be dangerous/incomplete, just stop and let the user know.
4. If the script/flow fails, try to fix it if you know how to and repeat, otherwise stop and prompt.

Pixi tasks are defined for useful scripts. Look in `pyproject.toml` for `[tool.pixi.tasks]` to see the options.

## Auto-Generated Files

Do not ever manually edit any of these auto-generated files:

- `f/graphql/api_client/*`
- `f/graphql/schema.gql`

Instead, run the pixi task `pixi run graphql` to regenerate the GraphQL client and schema files after making changes to the GraphQL schema or queries.

## Important Tips

- Only top level folders under `f/` can have `folder.meta.yaml` files. Subfolders can be created (and are a good idea for organization) but do not include a `folder.meta.yaml` file.
- Non-source files (`.tsv`, `.json`, `.txt`, `.jinja`, etc.) should only be created under the `src/` hierarchy. If you reference one of these files in a script/flow under `f/`, you must use `checkout_repo()` within the script and access it under `databot/src/` in the worker environment.

## Script Writing Principles

- Scripts must export a `main` function (do not call it)
- Libraries are installed automatically — do not show installation instructions
- Credentials and configuration are stored in resources and passed as parameters
- Scripts can return any JSON-serializable value; return values are available to subsequent flow steps via `results.step_id`
- Preprocessor scripts are named `preprocessor` and receive a single `event` parameter

## Flow Guidance

When asked to create a flow, ask the user which folder to put it in if not specified. Create a new folder ending with `.flow` containing a `.yaml` file with the flow definition.

For `rawscript` type modules, the `content` key should start with `!inline` followed by the path of the script. For `script` type modules, `path` should be the full repository path.

After creating/modifying flows, run `wmill flow generate-locks --yes` to generate lock files (no need to create them manually).

## Language-Specific Rules

Detailed language and tool-specific rules are loaded automatically when you work with the relevant file types:

- Python (`.py`) → `.claude/rules/python.md`
- TypeScript (`.ts`) → `.claude/rules/typescript.md`
- SQL (`.sql`) → `.claude/rules/sql.md`
- Go (`.go`) → `.claude/rules/go.md`
- Rust (`.rs`) → `.claude/rules/rust.md`
- Flows (`.flow/**`, `.yaml`) → `.claude/rules/flows.md`
- GraphQL (`.graphql`) → `.claude/rules/graphql.md`
- CLI reference → `.claude/rules/cli.md`
