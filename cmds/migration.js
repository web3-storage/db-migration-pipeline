import fs from 'fs'
import os from 'os'
import pg from 'pg'
import path from 'path'
import { fileURLToPath } from 'url'
import retry from 'p-retry'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const { Client } = pg

import { customFaunaDump } from '../lib/fauna-dump.js'
import { postgresIngest } from "../lib/postgres-ingest.js"
import { getFaunaKey, getPgConnectionString, getSslState } from "./utils.js"
import { dataLayers } from '../lib/protocol.js'

async function dataMigrationPipeline (faunaKey, outputPath, connectionString, { isPartialUpdate = false, ssl = false } = {}) {
  // Get first collections layer fauna dump
  console.log('dump', dataLayers[0])
  const initialTs = await customFaunaDump(faunaKey, outputPath, dataLayers[0].map(dl => dl.fauna))

  // Iterate over all data layers
  for (let i = 0; i < dataLayers.length - 1; i++) {
    console.log('ingest', dataLayers[i].map(dl => dl.postgres))
    console.log('dump', dataLayers[i + 1].map(dl => dl.fauna))
    await Promise.all([
      postgresIngest(connectionString, outputPath, dataLayers[i].map(dl => dl.postgres), { isPartialUpdate, ssl }),
      customFaunaDump(faunaKey, outputPath, dataLayers[i + 1].map(dl => dl.fauna))
    ])
  }

  // Ingest last collections layer
  console.log('ingest', dataLayers[dataLayers.length - 1].map(dl => dl.postgres))
  await postgresIngest(connectionString, outputPath, dataLayers[dataLayers.length - 1].map(dl => dl.postgres), { isPartialUpdate, ssl })

  return initialTs
}

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
