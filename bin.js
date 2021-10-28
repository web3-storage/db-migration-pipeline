#!/usr/bin/env node
import path from 'path'
import dotenv from 'dotenv'
import sade from 'sade'
import { fileURLToPath } from 'url'

import { faunaDumpCmd } from './cmds/fauna.js'
import { fullMigrationCmd, partialMigrationCmd } from './cmds/migration.js'
import { validateCmd } from './cmds/validate.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const prog = sade('db-migration')

dotenv.config({
  path: path.join(__dirname, '/.env.local')
})

prog
  .option('--env', 'Environment to perform the operation', 'dev')
  .option('--clean', 'Clean temporary files created', true)

prog
  .command('full')
  .describe('Full Database migration')
  .action(fullMigrationCmd)
  .command('partial <startTs>')
  .describe('Partial Database migration by updating DB with new data')
  .action(partialMigrationCmd)
  .command('validate')
  .describe('Validate Database migration')
  .action(validateCmd)
  .command('fauna-dump')
  .describe('complete fauna dump')
  .action(faunaDumpCmd)

prog.parse(process.argv)
