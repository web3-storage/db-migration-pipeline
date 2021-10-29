import os from 'os'
import pQueue from 'p-queue'
import ora from 'ora'

import { customFaunaDump } from '../lib/fauna-dump.js'
import { getFaunaKey} from "./utils.js"

export async function faunaDumpCmd () {
  const faunaKey = getFaunaKey()
  const outputPath = `${os.tmpdir()}/${(parseInt(String(Math.random() * 1e9), 10)).toString() + Date.now()}`

  console.log('output path', outputPath)

  const collections = ['User', 'Content', 'PinLocation', 'AuthToken', 'Pin', 'PinRequest', 'Upload', 'PinSyncRequest', 'Backup']

  const dumpFn = async (collection) => {
    const spinner = ora(`Dumping ${collection}`)
    await customFaunaDump(faunaKey, outputPath, [collection], spinner.info)
    spinner.stopAndPersist()
  }

  const dumpQueue = new pQueue({ concurrency: 3 })
  const dumpRes = await dumpQueue.addAll(Array.from(
    { length: collections.length }, (_, i) => () => dumpFn(collections[i])
  ))

  // TODO: dumpRes should return ts of first dump
  return 'ts'
}
