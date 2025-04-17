## Sage Databot

This repository contains code dedicated to populating and maintaining the sage database. It is primarily a data science project, intended to handle as many sources of circular economy information as possible.

### Features

There are several main features that Databot intends to implement. These are definitely a work in progress.

- [ ] Validating and integrating a set of "staged" changes into the database (using the Change model from the Sage API)
- [ ] Creating exports of the main database
- [ ] Connect with circular economy APIs
- [ ] Support importing external datasets + periodically integrating new data
- [ ] Extract information from HTML sources
- [ ] Extract information from PDF sources
- [ ] Extract information from image sources
- [ ] Creating questions for human contributors validation

### Architecture

The Databot is built using Python and Prefect for workflow orchestration. Prefect provides the concept of flows, which are entrypoints into the code meant to perform an action, such as one listed in the features above.

Dependencies:
- Sage API
- Prefect + PostgreSQL
- Meilisearch
- Redis
