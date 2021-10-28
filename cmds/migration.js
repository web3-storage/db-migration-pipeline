import fs from 'fs'
import os from 'os'
import pg from 'pg'
import path from 'path'
import { fileURLToPath } from 'url'
import retry from 'p-retry'
import pDefer from 'p-defer'
import pQueue from 'p-queue'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const { Client } = pg

import { customFaunaDump } from '../lib/fauna-dump.js'
import { postgresIngest } from "../lib/postgres-ingest.js"
import { getFaunaKey, getPgConnectionString, getSslState } from "./utils.js"
import { dataLayers } from '../lib/protocol.js'

/**
 * @return {Promise<void>}
 */
export async function fullMigrationCmd (options = {}) {
  const faunaKey = getFaunaKey()
  const connectionString = getPgConnectionString()
  const ssl = getSslState()
  const outputPath = `${os.tmpdir()}/${(parseInt(String(Math.random() * 1e9), 10)).toString() + Date.now()}`

  console.log('output path', outputPath)
  const initialTs = await dataMigrationPipeline(faunaKey, outputPath, connectionString, { ssl })

  // Teardown fauna dump
  if (options.clean) {
    await fs.promises.rm(outputPath, { recursive: true, force: true })
  }
  console.log('done with initial TS of:', initialTs)
}

/**
 * @param {string} startTs
 */
export async function partialMigrationCmd (startTs, options) {
  const faunaKey = getFaunaKey()
  const connectionString = getPgConnectionString()
  const ssl = getSslState()
  const outputPath = `${os.tmpdir()}/${(parseInt(String(Math.random() * 1e9), 10)).toString() + Date.now()}`

  const client = await retry(
    async () => {
      const c = new Client({ connectionString })
      await c.connect()
      return c
    },
    { minTimeout: 100 }
  )

  // Create import tables
  const tablesSql = await fs.promises.readFile(path.join(__dirname, '../postgres/import-tables.sql'), {
    encoding: 'utf-8'
  })
  await client.query(tablesSql)

  const initialTs = await dataMigrationPipeline(faunaKey, outputPath, connectionString, { isPartialUpdate: true, ssl })

  // Upsert partial data
  console.log('upsert data')
  const upsertSql = await fs.promises.readFile(path.join(__dirname, '../postgres/import-upsert.sql'), {
    encoding: 'utf-8'
  })
  await client.query(upsertSql)

  // Close SQL client
  await client.end()

  // Teardown fauna dump
  if (options.clean) {
    await fs.promises.rm(outputPath, { recursive: true, force: true })
  }

  console.log('done with initial TS of:', initialTs)
}

/**
 * @typedef {Object} DataLayer
 * @property {string} postgres
 * @property {string} fauna
 * @property {Array<string>} blockers
 */

/**
 * @typedef {Object} DataLayerPromisified
 * @property {string} postgres
 * @property {string} fauna
 * @property {Array<string>} blockers
 * @property {Array<Record<string, pDefer>>} dumpBlockerPromises
 * @property {Array<Record<string, pDefer>>} ingestBlockerPromises
 */

async function dataMigrationPipeline (faunaKey, outputPath, connectionString, { isPartialUpdate = false, ssl = false } = {}) {
  const createBlockerPromises = (blockers) => {
    const blockerPromises = {}
    blockers.forEach(blocker => blockerPromises[blocker] = new pDefer())
    return blockerPromises
  }
  const dataLayersPromisified = dataLayers.map(layer => ({
    ...layer,
    dumpBlockerPromises: createBlockerPromises(layer.dumpBlockers),
    ingestBlockerPromises: createBlockerPromises(layer.ingestBlockers)
  }))

  const dumpFn = async (layer) => {
    console.log('start dump', layer.fauna)
    await customFaunaDump(faunaKey, outputPath, [layer.fauna])
    setDumpLayerReady(layer, dataLayersPromisified)
    console.log('end dump', layer.fauna)
  }

  const dumpQueue = new pQueue({ concurrency: 3 })
  const dumpPromiseAll = dumpQueue.addAll(Array.from(
    { length: dataLayers.length }, (_, i) => () => dumpFn(dataLayers[i])
  ))

  const ingestFn = async (layer) => {
    layer.dumpBlockers.length && console.log(`ingest for ${layer.postgres} waiting for dump of ${layer.dumpBlockers.concat(',')}`)
    await isDumpLayerBlockersReady(layer, dataLayersPromisified)
    layer.ingestBlockers.length && console.log(`ingest for ${layer.postgres} waiting for ingest of ${layer.ingestBlockers.concat(',')}`)
    await isIngestLayerBlockersReady(layer, dataLayersPromisified)
    console.log('start ingest', layer.postgres)
    await postgresIngest(connectionString, outputPath, [layer.postgres], { isPartialUpdate, ssl })
    setIngestLayerReady(layer, dataLayersPromisified)
    console.log('end ingest', layer.postgres)
  }

  const ingestQueue = new pQueue({ concurrency: 3 })
  const ingestPromiseAll = ingestQueue.addAll(Array.from(
    { length: dataLayers.length }, (_, i) => () => ingestFn(dataLayers[i])
  ))

  const [dumpRes] = await Promise.all([
    dumpPromiseAll,
    ingestPromiseAll
  ])

  // TODO: dumpRes should return ts of first dump
  return 'ts'
}

/**
 * @param {DataLayer} dLayer
 * @param {Array<DataLayerPromisified>} dataLayersPromisified
 * @return {Promise<void>}
 */
async function isDumpLayerBlockersReady (dLayer, dataLayersPromisified) {
  const layer = dataLayersPromisified.find(layer => layer.postgres === dLayer.postgres)
  await Promise.all(Object.values(layer.dumpBlockerPromises).map(d => d.promise))
}

/**
 * @param {DataLayer} dLayer
 * @param {Array<DataLayerPromisified>} dataLayersPromisified
 * @return {Promise<void>}
 */
async function isIngestLayerBlockersReady (dLayer, dataLayersPromisified) {
  const layer = dataLayersPromisified.find(layer => layer.postgres === dLayer.postgres)
  await Promise.all(Object.values(layer.ingestBlockerPromises).map(d => d.promise))
}

/**
 * @param {DataLayer} dLayer
 * @param {Array<DataLayerPromisified>} dataLayersPromisified
 */
function setDumpLayerReady (dLayer, dataLayersPromisified) {
  dataLayersPromisified.forEach(layer => {
    const blockerPromise = layer.dumpBlockerPromises[dLayer.postgres]
    if (blockerPromise) {
      blockerPromise.resolve()
    }
  })
}

/**
 * @param {DataLayer} dLayer
 * @param {Array<DataLayerPromisified>} dataLayersPromisified
 */
function setIngestLayerReady (dLayer, dataLayersPromisified) {
  dataLayersPromisified.forEach(layer => {
    const blockerPromise = layer.ingestBlockerPromises[dLayer.postgres]
    if (blockerPromise) {
      blockerPromise.resolve()
    }
  })
}
