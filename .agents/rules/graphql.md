---
glob: '**/*.graphql'
---

# GraphQL

## Sage API

The Sage API has a defined GraphQL schema located in `f/graphql/schema.gql`.
The `ariadne_codegen` tool is used for generating a Python client from the schema.

For Sage API GraphQL queries/mutations:

- Place them in `f/graphql/queries/*.graphql` files instead of embedding them in scripts
- Use the generated client (in `f/graphql/api_client/`) to execute them
- Make sure the queries have unique names to be easily tied back to the script(s) using them

After adding or modifying queries, run `pixi run graphql` to regenerate the client.
