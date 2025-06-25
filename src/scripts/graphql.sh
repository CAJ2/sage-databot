#!/bin/bash
# Usage: src/scripts/graphql.sh <path_to_schema_file>
# Check if the first argument exists
if [ -z "$1" ]; then
  echo "Usage: $0 <path_to_schema_file>"
  exit 1
fi
# Check if the file ends with .gql
if [[ ! "$1" =~ \.gql$ ]]; then
  echo "Error: The file must have a .gql extension"
  exit 1
fi
# Check if the current file is in the correct place
if [ ! -f "src/graphql/schema.gql" ]; then
  echo "Error: The file src/graphql/schema.gql does not exist, make sure you are in the root directory"
  exit 1
fi
cp $1 src/graphql/schema.gql && \
echo "Schema file copied to src/graphql/schema.gql" && \
echo "Running ariadne-codegen..." && \
ariadne-codegen