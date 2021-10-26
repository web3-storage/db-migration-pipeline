import fs from 'fs'
import { pipeline } from 'stream/promises'
import pg from 'pg'
import pgCopyStreams from 'pg-copy-streams'

const { Pool } = pg
const { from: copyFrom } = pgCopyStreams

// TODO: Make transformer to change table name
/**
 * @typedef {Object} IngestOptions
 * @property {boolean} [isPartialUpdate=false]
 */

/**
 * @param {string} connectionString
 * @param {string} csvDirPath
 * @param {string[]} collections
 * @param {IngestOptions} [options]
 */
export async function postgresIngest (connectionString, csvDirPath, collections, options = {}) {
  // Setup postgres client pool
  const pool = new Pool({
    connectionString,
    max: collections.length
  })

  // Validate all csv files exist
  if (!fs.existsSync(csvDirPath)) {
    throw new Error(`Could not open ${csvDirPath}`)
  }
  const dir = await fs.promises.readdir(csvDirPath)
  collections.forEach(collection => {
    if (!dir.includes(`${collection}.csv`)) {
      throw new Error(`${collection}.csv does not exist in ${csvDirPath}`)
    }
  })

  await Promise.all(collections.map(async collection => {
    const client = await pool.connect()
    const importedName = options.isPartialUpdate ? '_imported' : ''
    const tableName = `public.${collection}${importedName}`
    await pipeline(
      fs.createReadStream(`${csvDirPath}/${collection}.csv`),
      client.query(copyFrom(
        `
        COPY ${tableName}
        FROM STDIN
        DELIMITER ','
        CSV HEADER;
        `
      ))
    )
    client.release()
  }))

  await pool.end()
}
