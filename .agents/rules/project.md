---
name: project-rules
glob: '**/*'
---

# Project Rules

## Windmill Development

### Before making a script/flow change

- If your intended change involves using the GraphQL API, run `pixi run graphql` to make sure the codegen is up-to-date. Rerun as needed.

### After making a script/flow change

1. Run `wmill script generate-metadata` to update script YAML and locks
2. Run `pixi run check` and address any important issues with the modified files
3. Run `pixi run dev-run f/path/to/script_or_flow` (without .py ext), appending `--args <JSON args>` if the script requires arguments.
4. If the script/flow fails, try to fix it if you know how to and repeat, otherwise stop and prompt.

### Auto-Generated Files

Do not ever manually edit any of these auto-generated files:

- `f/graphql/api_client/*`
- `f/graphql/schema.gql`

Instead, run the pixi task `pixi run graphql` to regenerate the GraphQL client and schema files.

### Important Tips

- Only top level folders under `f/` can have `folder.meta.yaml` files. Subfolders can be created but do not include a `folder.meta.yaml` file.
- Non-source files (`.tsv`, `.json`, `.txt`, `.jinja`, etc.) should only be created under the `src/` hierarchy. If you reference one of these files in a script/flow under `f/`, you must use `checkout_repo()` within the script and access it under `databot/src/` in the worker environment.

## Script Writing Principles

- Scripts must export a `main` function (do not call it)
- Libraries are installed automatically — do not show installation instructions
- Credentials and configuration are stored in resources and passed as parameters
- Scripts can return any JSON-serializable value; return values are available to subsequent flow steps via `results.step_id`
- Preprocessor scripts are named `preprocessor` and receive a single `event` parameter

## Flow Guidance

When asked to create a flow, create a new folder ending with `.flow` containing a `.yaml` file with the flow definition.

For `rawscript` type modules, the `content` key should start with `!inline` followed by the path of the script. For `script` type modules, `path` should be the full repository path.

After creating/modifying flows, run `wmill flow generate-locks --yes` to generate lock files.
