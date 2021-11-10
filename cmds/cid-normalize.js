#!/usr/bin/env node
import path from 'path'
import dotenv from 'dotenv'
import { fileURLToPath } from 'url'
import retry from 'p-retry'
import pg from 'pg'
import { CID } from 'multiformats/cid'

import { getPgConnectionString, getSslState } from "./utils.js"

const __dirname = path.dirname(fileURLToPath(import.meta.url))

dotenv.config({
  path: path.join(__dirname, '/.env.local')
})
const { Client } = pg

async function main () {
  const client = await getPgClient()
  const { rows } = await client.query(`
    SELECT cid
    FROM content
    WHERE cid LIKE 'Qm%'
  `)

  for (let i = 0; i < rows.length; i++) {
    const cid = rows[i].cid
    const nCid = normalizeCid(cid)

    console.log(`transform ${cid} -> ${nCid}`)
    await client.query({
      text: `
        UPDATE content
        SET cid=$1
        WHERE cid=$2
      `,
      values: [nCid, cid]
    })
  }

  await client.end()
}

main()

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

/**
 * Parse CID and return normalized b32 v1
 *
 * @param {string} cid
 */
export function normalizeCid (cid) {
  try {
    const c = CID.parse(cid)
    return c.toV1().toString()
  } catch (err) {
    return cid
  }
}
