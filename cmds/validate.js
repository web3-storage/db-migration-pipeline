import faunadb from 'faunadb'
import { GraphQLClient, gql } from 'graphql-request'
import retry from 'p-retry'
import pg from 'pg'
import ora from 'ora'

import { getFaunaKey, getPgConnectionString, getSslState } from "./utils.js"

const { Client } = pg
const q = faunadb.query

export async function validateCmd (opts = {}) {
  const startTs = opts['start-ts'] ? new Date(opts['start-ts']).toISOString() :
    new Date(2010, 1, 1).toISOString()

  let [postgresMetrics, reportedFaunaMetrics] = await Promise.all([
    (async () => {
      const spinnerPostgres = ora('fetching postgres metrics')
      const postgresMetrics = await fetchPostgresMetrics(new Date().toISOString(), startTs)
      spinnerPostgres.stopAndPersist()
      return postgresMetrics
    })(),
    (async () => {
      const spinnerUpdateMetrics = ora('updating fauna metrics')
      const reportedFaunaMetrics = await updateMetrics()
      spinnerUpdateMetrics.stopAndPersist()
      return reportedFaunaMetrics
    })()
  ])
  
  // Get date of last migration for partial fauna metrics
  const latestMigrationTs = await getMigrationEndTs()
  postgresMetrics.updated = latestMigrationTs

  const diff = {
    users: reportedFaunaMetrics.data.users - postgresMetrics.data.users,
    cids: reportedFaunaMetrics.data.cids - postgresMetrics.data.cids,
    uploads: reportedFaunaMetrics.data.uploads - postgresMetrics.data.uploads,
    pins: reportedFaunaMetrics.data.pins - postgresMetrics.data.pins,
    contentBytes: reportedFaunaMetrics.data.contentBytes - postgresMetrics.data.contentBytes
  }

  console.log('-------------------------------------------------------------------')
  console.log('Summary')
  console.table([
    { name: 'Fauna previous report', ...reportedFaunaMetrics.data, updated: reportedFaunaMetrics.updated} ,
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

// async function fetchReportedFaunaMetrics () {
//   const faunaKey = getFaunaKey()
//   const endpoint = 'https://graphql.fauna.com/graphql'
//   const client = new GraphQLClient(endpoint, {
//     headers: { Authorization: `Bearer ${faunaKey}` }
//   })

//   const [
//     users,
//     uplods,
//     cids,
//     pins,
//     contentBytes
//   ] = await Promise.all(faunaMetricsIds.map(id => getMetric(client, id)))

//   return {
//     // All will have same ts, this is a cron job computation
//     updated: cids.updated,
//     data: {
//       users: users.value,
//       cids: cids.value,
//       uploads: uplods.value,
//       pins: pins.value,
//       contentBytes: contentBytes.value
//     }
//   }
// }

async function updateMetrics () {
  const faunaKey = getFaunaKey()
  const endpoint = 'https://graphql.fauna.com/graphql'
  const client = new GraphQLClient(endpoint, {
    headers: { Authorization: `Bearer ${faunaKey}` }
  })

  const [
    users,
    uplods,
    cids,
    pins,
    contentBytes
  ] = await Promise.all([
    updateMetric(client, 'users_total_v2', COUNT_USERS, {}, 'countUsers'),
    updateMetric(client, 'uploads_total_v2', COUNT_UPLOADS, {}, 'countUploads'),
    updateMetric(client, 'content_total', COUNT_CIDS, {}, 'countContent'),
    updateMetric(client, 'pins_total_v2', COUNT_PINS, {}, 'countPins'),
    updateMetric(client, 'content_bytes_total_v2', SUM_CONTENT_DAG_SIZE, {}, 'sumContentDagSize')
  ])

  return {
    // All will have same ts, this is a cron job computation
    updated: cids.updated,
    data: {
      users: users.value,
      cids: cids.value,
      uploads: uplods.value,
      pins: pins.value,
      contentBytes: contentBytes.value
    }
  }
}

/**
 * @param {import('@web3-storage/db').DBClient} db
 * @param {string} key
 * @param {typeof gql} query
 * @param {any} vars
 * @param {string} dataProp
 */
async function updateMetric (db, key, query, vars, dataProp) {
  const to = new Date(Date.now())
  console.log(`🦴 Fetching current metric "${key}"...`)

  const metric = await getMetric(db, key)
  console.log(`ℹ️ Updating "${key}" metric from ${metric.updated} to ${to.toISOString()}`)

  vars = { ...vars, from: metric.updated, to: to.toISOString() }
  const total = await sumPaginate(db, query, vars, dataProp, total => {
    if (total) console.log(`➕ Incrementing "${key}" to ${metric.value + total}`)
  })

  if (!total) {
    console.log(`🙅 "${key}" did not change value (${metric.value})`)
    return {
      value: metric.value,
      updated: metric.updated
    }
  }

  const value = metric.value + total
  const updated = to.toISOString()

  console.log(`💾 Saving new value for "${key}": ${value}`)
  await db.request(CREATE_OR_UPDATE_METRIC, { data: { key, value, updated } })
  return {
    value,
    updated
  }
}

/**
 * @param {import('@web3-storage/db').DBClient} db
 * @param {string} key
 */
async function getMetric (db, key) {
  const { findMetricByKey } = await retry(() => db.request(FIND_METRIC, { key }))
  return findMetricByKey || { key, value: 0, updated: EPOCH }
}

/**
 * @param {import('@web3-storage/db').DBClient} db
 * @param {typeof gql} query
 * @param {any} vars
 * @param {string} dataProp
 */
async function sumPaginate (db, query, vars, dataProp, onPage) {
  let after
  let total = 0
  while (true) {
    const res = await retry(() => db.request(query, { after, ...vars }), { onFailedAttempt: console.error })
    const data = res[dataProp]
    total += data.data[0] || 0
    onPage(total)
    after = data.after
    if (!after) break
  }
  return total
}

const CREATE_OR_UPDATE_METRIC = gql`
  mutation CreateOrUpdateMetric($data: CreateOrUpdateMetricInput!) {
    createOrUpdateMetric(data: $data) {
      key
      value
      updated
    }
  }
`

const FIND_METRIC = gql`
  query FindMetric($key: String!) {
    findMetricByKey(key: $key) {
      key
      value
      updated
    }
  }
`

const SUM_CONTENT_DAG_SIZE = gql`
  query SumContentDagSize($from: Time!, $to: Time!, $after: String) {
    sumContentDagSize(from: $from, to: $to, _size: 25000, _cursor: $after) {
      data
      after
    }
  }
`

const EPOCH = '2021-07-01T00:00:00.000Z'

const COUNT_USERS = gql`
  query CountUsers($from: Time!, $to: Time!, $after: String) {
    countUsers(from: $from, to: $to, _size: 80000, _cursor: $after) {
      data,
      after
    }
  }
`

const COUNT_UPLOADS = gql`
  query CountUploads($from: Time!, $to: Time!, $after: String) {
    countUploads(from: $from, to: $to, _size: 80000, _cursor: $after) {
      data,
      after
    }
  }
`

const COUNT_CIDS = gql`
  query countContent($from: Time!, $to: Time!, $after: String) {
    countContent(from: $from, to: $to, _size: 80000, _cursor: $after) {
      data,
      after
    }
  }
`

const COUNT_PINS = gql`
  query CountPins($from: Time!, $to: Time!, $after: String) {
    countPins(from: $from, to: $to, _size: 80000, _cursor: $after) {
      data,
      after
    }
  }
`

const faunaMetricsIds = ['users_total_v2', 'content_total', 'uploads_total_v2', 'pins_total_v2', 'content_bytes_total_v2']

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
