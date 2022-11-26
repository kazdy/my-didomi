# How to setup devenv and run tests?

## Prerequisites
Python 3.10 installed
Poetry 1.2 [installed](https://python-poetry.org/docs/#installation)

## Available commands
Create venv and install project dependencies:
`make install`

Run tests:
`make test` - runs all tests
`make unit` - runs unit tests only
`make functional` - runs functional tests only
`make e2e` - runs e2e tests only


Run as spark program:
`make run`

Run as python app:
`poetry run src/main.py --input_path ./tests/data/input/ --output_path ./run-output/ --partition_filter True`

Run as shell app:
`sh run.sh`

Run as shell app with args:
`sh run.sh ./tests/data/input/ ./run-output/ True`

## Requirements
In some specific cases, companies need to collect consent from consumers before using their data. For instance, app users might need to explicitly consent to share their geolocation before a company can use it for advertising.

As users interact with the Didomi platform, we collect different types of events like:

- "Page view" when a user visits a webpage
- "Consent asked" when a user is asked for consent (ie a consent notice is displayed)
- "Consent given" when a user gives consent (ie has clicked on a Agree or Disagree in the notice)

The goal of this challenge is to build a very simple Spark app that processes events and summarizes various metrics as time-series data.

## Input

### Format

Events are stored as JSON Lines files with the following format:

```js
{
    "datetime":"2021-01-23 11:28:37",
    "id": "94cabac0-088c-43d3-976a-88756d21132a",
    "type": "pageview",
    "domain": "www.website.com",
    "user": {
        "id": "09fcb803-2779-4096-bcfd-0fb1afef684a",
        "country": "US",
        "token": "{\"vendors\":{\"enabled\":[\"vendor\"],\"disabled\":[]},\"purposes\":{\"enabled\":[\"analytics\"],\"disabled\":[]}}",
    }
}
```

| Property       | Values                                       | Description                          |
| -------------- | -------------------------------------------- | ------------------------------------ |
| `datetime`     | ISO 8601 date                                | Date of the event                    |
| `id`           | UUID                                         | Unique event ID                      |
| `type`         | `pageview`, `consent.given`, `consent.asked` | Event type                           |
| `domain`       | Domain name                                  | Domain where the event was collected |
| `user.id`      | UUID                                         | Unique user ID                       |
| `user.token`   | JSON-String                                  | Contains status of purposes/vendors  |
| `user.country` | ISO 3166-1 alpha-2 country code              | Country of the user                  |

### Consent status

We consider an event as positive consent when at least one purpose is enabled.

### Partitioning

The data is partitioned by date/hour with Hive partition structure.

## Output

The Spark job is expected to output the following grouped metrics as a Parquet table:

| Column                        | Type      | Description                                                                      |
| ----------------------------- | --------- | -------------------------------------------------------------------------------- |
| `datehour`                    | Dimension | Date and hour (YYYY-MM-DD-HH) dimension                                          |
| `domain`                      | Dimension | Domain                                                                           |
| `country`                     | Dimension | User country                                                                     |
| `pageviews`                   | Metric    | Number of events of type `pageview`                                              |
| `pageviews_with_consent`      | Metric    | Number of events of type `pageview` with consent (ie `user.consent = true`)      |
| `consents_asked`              | Metric    | Number of events of type `consent.asked`                                         |
| `consents_asked_with_consent` | Metric    | Number of events of type `consent.asked` with consent (ie `user.consent = true`) |
| `consents_given`              | Metric    | Number of events of type `consent.given`                                         |
| `consents_given_with_consent` | Metric    | Number of events of type `consent.given` with consent (ie `user.consent = true`) |
| `avg_pageviews_per_user`      | Metric    | Average number of events of type `pageview` per user                             |

## Processing

On top of computing the metrics listed above, the following operations must be run:

- Deduplication of events based on event ID