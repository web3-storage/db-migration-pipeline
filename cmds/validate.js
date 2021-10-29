import faunadb from 'faunadb'
import retry from 'p-retry'
import pg from 'pg'
import ora from 'ora'
import { getFaunaKey, getPgConnectionString, getSslState } from "./utils.js"

const { Client } = pg
const q = faunadb.query

export async function validateCmd (opts) {
  const startTs = opts['start-ts'] ? new Date(opts['start-ts']).toISOString() :
    new Date(2010, 1, 1).toISOString()
  let faunaMetrics
  let endTs

  if (opts['end-ts']) {
    endTs = new Date(opts['end-ts']).toISOString()
  }
  console.log('end ts', endTs)

  const spinnerFauna = ora('fetching fauna metrics')
  if (!endTs) {
    faunaMetrics = await fetchFullFaunaMetrics()
    endTs = faunaMetrics.updated
  } else {
    faunaMetrics = await fetchPartialFaunaMetrics(endTs, startTs)
  }
  spinnerFauna.stopAndPersist()

  const spinnerPostgres = ora('fetching postgres metrics')
  const postgresMetrics = await fetchPostgresMetrics(endTs, startTs)
  spinnerPostgres.stopAndPersist()

  console.log('fauna metrics', faunaMetrics)
  console.log('postgres metrics', postgresMetrics)
}

async function fetchPostgresMetrics (endTs, startTs) {
  const connectionString = getPgConnectionString()
  const ssl = getSslState()

  const client = await retry(
    async () => {
      const c = new Client({
        connectionString,
        ssl: ssl && {
          rejectUnauthorized: false
        }
      })
      await c.connect()
      return c
    },
    { minTimeout: 100 }
  )

  const [
    { rows: [{ count: users }] },
    { rows: [{ count: uploads }] },
    { rows: [{ count: pins }] },
    { rows: [{ sum: contentBytes }] }
  ] = await Promise.all([
    client.query(getCountQuery('user', endTs)),
    client.query(getCountQuery('upload', endTs)),
    client.query(getCountQuery('pin', endTs)),
    client.query(getSumQuery('content', 'dag_size', endTs))
  ])

  await client.end()

  return {
    updated: endTs,
    data: {
      users: Number(users),
      uploads: Number(uploads),
      pins: Number(pins),
      contentBytes: Number(contentBytes)
    }
  }
}

async function fetchPartialFaunaMetrics (endTs, startTs) {
  const faunaKey = getFaunaKey()
  const client = new faunadb.Client({ secret: faunaKey })

  const countQueries = ['functions/countUsers', 'functions/countUploads', 'functions/countPins', 'functions/sumContentDagSize']
  const [users, uploads, pins, contentBytes] = await Promise.all([
    ...countQueries.map(query => getPartialMetrics(client, query, endTs, startTs))
  ])

  return {
    updated: endTs,
    data: {
      users,
      uploads,
      pins,
      contentBytes
    }
  }
}

async function getPartialMetrics (client, query, endTs, startTs) {
  const size = 10000
  let after
  let total = 0

  while (true) {
    const res = await client.query(
      q.Call(q.Ref(query), q.Time(startTs), q.Time(endTs), size, after, undefined),
      { onFailedAttempt: console.error }
    )
    total += res.data[0] || 0
    after = res.after
    if (!after) break
  }
  return total
}

async function fetchFullFaunaMetrics () {
  const faunaKey = getFaunaKey()
  const client = new faunadb.Client({ secret: faunaKey })

  const [
    { data: users },
    { data: uplods },
    { data: pins },
    { data: contentBytes }
  ] = await Promise.all(faunaMetricsIds.map(id => client.query(
    q.Call(q.Ref('functions/findMetricByKey'), id)
  )))

  return {
    // All will have same ts, this is a cron job computation
    updated: users.updated.value,
    data: {
      users: users.value,
      uploads: uplods.value,
      pins: pins.value,
      contentBytes: contentBytes.value
    }
  }
}

const faunaMetricsIds = ['users_total', 'uploads_total', 'pins_total', 'content_bytes_total']

const getCountQuery = (table, endTs) => ({
  text: `
      SELECT COUNT(*)
      FROM public.${table}
      WHERE updated_at < $1
    `,
  values: [endTs]
})

const getSumQuery = (table, column, endTs) => ({
  text: `
      SELECT SUM(${column})
      FROM public.${table}
      WHERE updated_at < $1
    `,
  values: [endTs]
})
