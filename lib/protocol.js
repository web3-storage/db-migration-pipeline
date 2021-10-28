export const collectionTableMapping = {
  'User': {
    table: 'user',
    columns: ['id', 'name', 'picture', 'email', 'issuer', 'github', 'public_address', 'inserted_at', 'updated_at']
  },
  'Content': {
    table: 'content',
    columns: ['cid', 'dag_size', 'inserted_at', 'updated_at']
  },
  'PinLocation': {
    table: 'pin_location',
    columns: ['id', 'peer_id', 'peer_name', 'region']
  },
  'AuthToken': {
    table: 'auth_key',
    columns: ['id', 'name', 'secret', 'user_id', 'inserted_at', 'updated_at', 'deleted_at']
  },
  'Pin': {
    table: 'pin',
    columns: ['id', 'status', 'content_cid', 'pin_location_id', 'inserted_at', 'updated_at']
  },
  'PinRequest': {
    table: 'pin_request',
    columns: ['id', 'content_cid', 'attempts', 'inserted_at', 'updated_at']
  },
  'Upload': {
    table: 'upload',
    columns: ['id', 'user_id', 'auth_key_id', 'content_cid', 'source_cid', 'type', 'name', 'inserted_at', 'updated_at', 'deleted_at']
  },
  'PinSyncRequest': {
    table: 'pin_sync_request',
    columns: ['id', 'pin_id', 'inserted_at']
  },
  'Backup': {
    table: 'backup',
    columns: ['id', 'upload_id', 'url', 'inserted_at']
  }
}

export const dataLayers = [
  {
    postgres: 'user',
    fauna: 'User',
    dumpBlockers: ['user'],
    ingestBlockers: []
  },
  {
    postgres: 'content',
    fauna: 'Content',
    dumpBlockers: ['content'],
    ingestBlockers: []
  },
  {
    postgres: 'pin_location',
    fauna: 'PinLocation',
    dumpBlockers: ['pin_location'],
    ingestBlockers: []
  },
  {
    postgres: 'auth_key',
    fauna: 'AuthToken',
    dumpBlockers: ['auth_key'],
    ingestBlockers: ['user']
  },
  {
    postgres: 'pin_request',
    fauna: 'PinRequest',
    dumpBlockers: ['pin_request'],
    ingestBlockers: ['content']
  },
  {
    postgres: 'pin',
    fauna: 'Pin',
    dumpBlockers: ['pin'],
    ingestBlockers: ['content', 'pin_location']
  },
  {
    postgres: 'upload',
    fauna: 'Upload',
    dumpBlockers: ['upload'],
    ingestBlockers: ['user', 'content', 'auth_key']
  },
  {
    postgres: 'pin_sync_request',
    fauna: 'PinSyncRequest',
    dumpBlockers: ['pin_sync_request'],
    ingestBlockers: ['pin']
  },
  {
    postgres: 'backup',
    fauna: 'Backup',
    dumpBlockers: ['backup'],
    ingestBlockers: ['upload']
  }
]
