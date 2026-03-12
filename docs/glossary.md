# Glossary

This glossary explains common words used across ukpyn docs in plain language.

## API

**Application Programming Interface.** A way for your code to ask another service
for data using standard requests.

## ODP

**Open Data Portal.** The UK Power Networks data portal where datasets are
published and queried.

## Dataset

A named collection of related data in the portal, similar to a table.

## Record

One row (one item) returned from a dataset query.

## Orchestrator

A helper module in ukpyn that groups related dataset calls by topic (for
example LTDS, flexibility, or network data).

## Sync

Short for **synchronous**: your code waits for a request to finish before moving
on.

## Async

Short for **asynchronous**: your code can continue other work while waiting for
requests to complete.

## Licence area

A UKPN service region (such as EPN, LPN, or SPN) used to filter many datasets.

## Filter

A query condition that narrows results (for example by date, area, or status).

## Response object

The Python object returned by ukpyn calls, usually containing metadata and a
list of records.

## Notebook kernel

The Python runtime used by a notebook. It must match the environment where
ukpyn is installed.

## Environment variable

A named setting provided by your shell, often used for secrets like
`UKPN_API_KEY`.

## Pagination

When results are too large for one response, data is split into pages that can
be fetched in steps.

## API key

A private token that proves your request is allowed to access the API.
