# db-migration-pipeline

Migration pipeline to move data from Fauna DB into Postgres.

## Setup

```sh
npm install
```

To run locally you will need the following in your `.env.local` file:

```ini
# Running environment (dev, staging, production)
ENV=dev

# Fauna keys
DEV_FAUNA_KEY=<TOKEN>
STAGING_FAUNA_KEY=<TOKEN>
PRODUCTION_FAUNA_KEY=<TOKEN>

# Postgres keys
DEV_PG_CONNECTION=<CONNECTION_STRING>
STAGING_PG_CONNECTION=<CONNECTION_STRING>
PRODUCTION_PG_CONNECTION=<CONNECTION_STRING>
```

## Usage

TODO
