import fs from 'fs'
import retry from 'p-retry'
import dotenv from 'dotenv'
import pg from 'pg'
import path from 'path'
import { fileURLToPath } from 'url'
import { Web3Storage, getFilesFromPath } from 'web3.storage'

import { validateCmd } from '../cmds/validate.js'
import { partialMigrationCmd } from '../cmds/migration.js'
import { getPgConnectionString, getSslState } from "../cmds/utils.js"

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const { Client } = pg

dotenv.config({
  path: path.join(__dirname, '/.env.local')
})

async function main () {
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

  const startTs = await getStartTs(client)
  const endTs = new Date()
  console.log(`from ${startTs} to ${endTs}`)
  const {
    outputPath
  } = await partialMigrationCmd(startTs, { 'end-ts': endTs, clean: false })

  console.log('CSVs created at', outputPath)
  // Compute duration
  const duration = new Date(new Date().toISOString()).getTime() - endTs.getTime()

  console.log('duration', duration)
  // Store data to web3.storage
  const w3Client = new Web3Storage({
    token: process.env.WEB3_STORAGE,
    endpoint: 'https://api-staging.web3.storage'
  })
  const files = await getFilesFromPath(outputPath)
  const cid = await w3Client.put(files, {
    name: `migration-${new Date(endTs).getTime()}`
  })
  console.log('cid', cid)

  // Insert migration metadata
  await insertMigrationMetadata(client, cid, duration, startTs, endTs.toISOString())

  await Promise.all([
    // Run Validation
    validateCmd(),
    // Clean files
    fs.promises.rm(outputPath, { recursive: true, force: true }),
    client.end()
  ])
}

// Read Postgres migration table latest table if existent
// Otherwise return really old time
async function getStartTs (client) {
  const { rows: [{ dump_ended_at }] } = await client.query(`
    SELECT * FROM migration_tracker WHERE id=(SELECT max(id) FROM migration_tracker)
  `)

  return dump_ended_at
}

async function insertMigrationMetadata (client, cid, duration, startTs, endTs) {
  await client.query({
    text: `
      INSERT INTO migration_tracker (cid, duration, dump_started_at, dump_ended_at, inserted_at)
      VALUES ($1, $2, $3, $4, $5)
      `,
    values: [cid, duration, startTs, endTs, new Date().toISOString()]
  })
}

main()
