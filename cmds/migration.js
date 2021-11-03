import fs from 'fs'
import os from 'os'
import pg from 'pg'
import path from 'path'
import { fileURLToPath } from 'url'
import retry from 'p-retry'
import pDefer from 'p-defer'
import pQueue from 'p-queue'
import ora from 'ora'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const { Client } = pg

import { customFaunaDump } from '../lib/fauna-dump.js'
import { postgresIngest } from "../lib/postgres-ingest.js"
import { getFaunaKey, getPgConnectionString, getSslState } from "./utils.js"
import { dataLayers, pinRequestDataLayers } from '../lib/protocol.js'

/**
 * @return {Promise<void>}
 */
export async function fullMigrationCmd (options = {}) {
  const faunaKey = getFaunaKey()
  const connectionString = getPgConnectionString()
  const ssl = getSslState()
  const outputPath = `${os.tmpdir()}/${(parseInt(String(Math.random() * 1e9), 10)).toString() + Date.now()}`

  console.log('output path', outputPath)
  const dLayers = options['pin-requests'] ? pinRequestDataLayers : dataLayers
  console.log('layers to migrate', dLayers.map(dl => dl.postgres))
  const initialTs = await dataMigrationPipeline(faunaKey, outputPath, connectionString, {
    ssl,
    dLayers
  })

  // Teardown fauna dump
  if (options.clean) {
    await fs.promises.rm(outputPath, { recursive: true, force: true })
  }
  console.log('done with initial TS of:', initialTs)
}

/**
 * @param {string} startTs
 */
export async function partialMigrationCmd (startTs, options = {}) {
  const faunaKey = getFaunaKey()
  const connectionString = getPgConnectionString()
  const ssl = getSslState()
  const outputPath = `${os.tmpdir()}/${(parseInt(String(Math.random() * 1e9), 10)).toString() + Date.now()}`

  console.log('output path', outputPath)
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

  // Create import tables
  console.log('create temporary tables')
  const tablesSql = await fs.promises.readFile(path.join(__dirname, '../postgres/import-tables.sql'), {
    encoding: 'utf-8'
  })
  await client.query(tablesSql)

  const endTime = options['end-ts']
  const dLayers = options['pin-requests'] ? pinRequestDataLayers : dataLayers
  console.log('layers to migrate', dLayers.map(dl => dl.postgres))
  const initialTs = await dataMigrationPipeline(faunaKey, outputPath, connectionString, {
    isPartialUpdate: true,
    startTime: startTs,
    endTime,
    ssl,
    dLayers
  })

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

  return {
    outputPath
  }
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

async function dataMigrationPipeline (faunaKey, outputPath, connectionString, { isPartialUpdate = false, ssl = false, startTime, endTime, dLayers = dataLayers } = {}) {
  const createBlockerPromises = (blockers) => {
    const blockerPromises = {}
    blockers.forEach(blocker => blockerPromises[blocker] = new pDefer())
    return blockerPromises
  }
  const dataLayersPromisified = dLayers.map(layer => ({
    ...layer,
    dumpBlockerPromises: createBlockerPromises(layer.dumpBlockers),
    ingestBlockerPromises: createBlockerPromises(layer.ingestBlockers)
  }))

  if (!endTime) {
    endTime = new Date()
  }
  console.log('--------------------------------------')
  console.log('start time', startTime)
  console.log('end time:', endTime.toISOString())
  console.log('end time (epoch ms):', endTime.getTime())
  console.log('--------------------------------------')

  const dumpFn = async (layer) => {
    const spinner = ora(`Dumping ${layer.fauna}`)
    await customFaunaDump(faunaKey, outputPath, {
      collections: [layer.fauna],
      onCollectionProgress: (message) => spinner.info(message),
      startTime,
      endTime
    })
    setDumpLayerReady(layer, dataLayersPromisified)
    spinner.stopAndPersist()
  }

  const dumpQueue = new pQueue({ concurrency: 2 })
  const dumpPromiseAll = dumpQueue.addAll(Array.from(
    { length: dLayers.length }, (_, i) => () => dumpFn(dLayers[i])
  ))

  const ingestFn = async (layer) => {
    const spinnerWaitDump = ora(`ingest for ${layer.postgres} waiting for dump of ${layer.dumpBlockers.concat(',')}`)
    await isDumpLayerBlockersReady(layer, dataLayersPromisified)
    spinnerWaitDump.stop()
    const spinnerWaitIngest = ora(`ingest for ${layer.postgres} waiting for ingest of ${layer.ingestBlockers.concat(',')}`)
    await isIngestLayerBlockersReady(layer, dataLayersPromisified)
    spinnerWaitIngest.stop()
    const spinnerIngest = ora(`start ingest ${layer.postgres}`)
    await postgresIngest(connectionString, outputPath, [layer.postgres], { isPartialUpdate, ssl })
    setIngestLayerReady(layer, dataLayersPromisified)
    spinnerIngest.stopAndPersist()
  }

  const ingestQueue = new pQueue({ concurrency: 3 })
  const ingestPromiseAll = ingestQueue.addAll(Array.from(
    { length: dLayers.length }, (_, i) => () => ingestFn(dLayers[i])
  ))

  await Promise.all([
    dumpPromiseAll,
    ingestPromiseAll
  ])
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
