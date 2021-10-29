import faunaDump from 'fauna-dumpify'
import { snakeCase } from 'snake-case'

import { collectionTableMapping } from './protocol.js'

export async function customFaunaDump (faunaKey, dumpFolder, collections, onCollectionProgress) {
  const initialTs = await faunaDump(faunaKey, dumpFolder, {
    collections,
    pageSize: 100000,
    faunaLambda: (q, collection) => {
      // Pin and Upload Collection need Content ref to be fetched
      if (collection === 'Pin' || collection === 'Upload') {
        return q.Lambda(['ref'], q.Let({
          collection: q.Get(q.Var('ref'))
        }, {
          collection: q.Var('collection'),
          relations: {
            cid: q.Select(['data', 'cid'], q.Get(q.Select(['data', 'content'], q.Var('collection'))))
          }
        }))
      }
      return q.Lambda(['ref'], q.Let({
        collection: q.Get(q.Var('ref'))
      }, {
        collection: q.Var('collection'),
        relations: {}
      }))
    },
    headers: (collection) => {
      const tableMapping = collectionTableMapping[collection]
      if (tableMapping) {
        return tableMapping.columns
      }
      throw new Error('unrecognized collection received')
    },
    dataTransformer: (header, allData) => {
      // General
      if (header === 'inserted_at') return allData.created?.value
      if (header === 'updated_at') return allData.updated?.value || new Date().toISOString()

      // User
      if (header === 'public_address') return allData.publicAddress
      // Content
      if (header === 'dag_size') return allData.dagSize || 0
      // Pin Location
      if (header === 'peer_id') return allData.peerId || ''
      if (header === 'peer_name') return allData.peerName || ''

      // Relations
      if (header === 'user_id') return allData.user.value.id
      if (header === 'content_cid') return allData.cid
      if (header === 'source_cid') return allData.cid
      if (header === 'pin_location_id') return allData.location?.value.id
      if (header === 'auth_key_id') return allData.authToken?.value.id
      if (header === 'pin_id') return allData.pin.id
      if (header === 'upload_id') return allData.upload.id

      return allData[header]
    },
    filenameTransformer: (name) => {
      if (name === 'AuthToken') {
        return 'auth_key'
      }
      return snakeCase(name).toLowerCase()
    },
    onCollectionProgress,
    collectionIndex: (collection) => {
      if(collection === 'AuthToken') return 'authtoken_migration_timestamp'
      if(collection === 'Backup') return 'backup_migration_timestamp'
      if(collection === 'Content') return 'content_migration_timestamp'
      if(collection === 'Pin') return 'pin_migration_timestamp'
      if(collection === 'PinLocation') return ''
      if(collection === 'PinRequest') return 'pinrequest_migration_timestamp'
      if(collection === 'PinSyncRequest') return 'pinSyncRequest_migration_timestamp'
      if(collection === 'Upload') return 'upload_migration_timestamp'
      if(collection === 'User') return 'user_migration_timestamp'
    }
  })

  return initialTs
}
