import faunadb from 'faunadb'
import retry from 'p-retry'
import pg from 'pg'
import ora from 'ora'
import { getFaunaKey, getPgConnectionString, getSslState } from "./utils.js"

const { Client } = pg
const q = faunadb.query

export async function validateCmd (opts = {}) {
  const startTs = opts['start-ts'] ? new Date(opts['start-ts']).toISOString() :
    new Date(2010, 1, 1).toISOString()
  let reportedFaunaMetrics

  const spinnerPostgres = ora('fetching postgres metrics')
  const postgresMetrics = await fetchPostgresMetrics(new Date().toISOString(), startTs)
  spinnerPostgres.stopAndPersist()

  // Get date of last migration for partial fauna metrics
  const latestMigrationTs = await getMigrationEndTs()
  postgresMetrics.updated = latestMigrationTs

  const spinnerReportedFauna = ora('fetching fauna reported metrics')
  reportedFaunaMetrics = await fetchReportedFaunaMetrics()
  const reportedEndTs = reportedFaunaMetrics.updated
  spinnerReportedFauna.stopAndPersist()

  // Get diff from Fauna
  const spinnerDiffFauna = ora('fetching fauna diff metrics')
  const toMigrateFaunaMetrics = await fetchPartialFaunaMetrics(reportedEndTs, latestMigrationTs)
  spinnerDiffFauna.stopAndPersist()

  const diff = {
    users: reportedFaunaMetrics.data.users - (postgresMetrics.data.users + toMigrateFaunaMetrics.data.users),
    cids: reportedFaunaMetrics.data.cids - (postgresMetrics.data.cids + toMigrateFaunaMetrics.data.cids),
    uploads: reportedFaunaMetrics.data.uploads - (postgresMetrics.data.uploads + toMigrateFaunaMetrics.data.uploads),
    pins: reportedFaunaMetrics.data.pins - (postgresMetrics.data.pins + toMigrateFaunaMetrics.data.pins),
    contentBytes: reportedFaunaMetrics.data.contentBytes - (postgresMetrics.data.contentBytes + toMigrateFaunaMetrics.data.contentBytes)
  }

  console.log('-------------------------------------------------------------------')
  console.log('Summary')
  console.table([
    { name: 'Fauna previous report', ...reportedFaunaMetrics.data, updated: reportedFaunaMetrics.updated} ,
    { name: 'Fauna pending migr', ...toMigrateFaunaMetrics.data, updated: toMigrateFaunaMetrics.updated },
    { name: 'Postgres migrated', ...postgresMetrics.data, updated: postgresMetrics.updated },
    { name: 'Difference', ...diff}
  ])
  console.log('-------------------------------------------------------------------')
}

// Get latest timestamp of migration
async function getMigrationEndTs () {
  const client = await getPgClient()
  const { rows: [{ dump_ended_at }] } = await client.query(`
    SELECT id, dump_ended_at, MAX(id) FROM migration_tracker
    GROUP BY id
  `)

  await client.end()
  return new Date(dump_ended_at).toISOString()
}

async function fetchPostgresMetrics (endTs, startTs) {
  const client = await getPgClient()
  const [
    { rows: [{ count: users }] },
    { rows: [{ count: cids }] },
    { rows: [{ count: uploads }] },
    { rows: [{ count: pins }] },
    { rows: [{ sum: contentBytes }] }
  ] = await Promise.all([
    client.query(getCountQuery('user', endTs)),
    client.query(getCountQuery('content', endTs)),
    client.query(getCountQuery('upload', endTs)),
    client.query(getCountQuery('pin', endTs)),
    client.query(getSumQuery('content', 'dag_size', endTs))
  ])

  await client.end()

  return {
    updated: endTs,
    data: {
      users: Number(users),
      cids: Number(cids),
      uploads: Number(uploads),
      pins: Number(pins),
      contentBytes: Number(contentBytes)
    }
  }
}

async function getPgClient () {
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
  return client
}

async function fetchPartialFaunaMetrics (endTs, startTs) {
  const faunaKey = getFaunaKey()
  const client = new faunadb.Client({ secret: faunaKey })

  const countQueries = ['functions/countUsers', 'functions/countContent', 'functions/countUploads', 'functions/countPins', 'functions/sumContentDagSize']
  const [users, cids, uploads, pins, contentBytes] = await Promise.all([
    ...countQueries.map(query => getPartialMetrics(client, query, endTs, startTs))
  ])

  return {
    updated: endTs,
    data: {
      users,
      cids,
      uploads,
      pins,
      contentBytes
    }
  }
}

async function getPartialMetrics (client, query, endTs, startTs) {
  const size = 1200
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

async function fetchReportedFaunaMetrics () {
  const faunaKey = getFaunaKey()
  const client = new faunadb.Client({ secret: faunaKey })

  const [
    { data: users },
    { data: uplods },
    { data: cids },
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
      cids: cids.value,
      uploads: uplods.value,
      pins: pins.value,
      contentBytes: contentBytes.value
    }
  }
}

const faunaMetricsIds = ['users_total_v2', 'content_total', 'uploads_total_v2', 'pins_total', 'content_bytes_total_v2']

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
