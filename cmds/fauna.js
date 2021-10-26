import os from 'os'

import { customFaunaDump } from '../lib/fauna-dump.js'
import { getFaunaKey} from "./utils.js"

export async function faunaDumpCmd () {
  const faunaKey = getFaunaKey()
  const outputPath = `${os.tmpdir()}/${(parseInt(String(Math.random() * 1e9), 10)).toString() + Date.now()}`

  console.log('output path', outputPath)

  const collections = ['User', 'Content', 'PinLocation', 'AuthToken', 'Pin', 'PinRequest', 'Upload', 'PinSyncRequest', 'Backup']
  await customFaunaDump(faunaKey, outputPath, collections)
}
