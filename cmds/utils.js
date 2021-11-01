export function getFaunaKey () {
  let faunaKey
  if (process.env.ENV === 'staging') {
    faunaKey = process.env.STAGING_FAUNA_KEY
  } else if (process.env.ENV === 'production') {
    faunaKey = process.env.PRODUCTION_FAUNA_KEY
  } else {
    faunaKey = process.env.DEV_FAUNA_KEY
  }

  if (!faunaKey) {
    throw new Error('Environment variables for ENV and/or FAUNA_KEY are not set')
  }

  return faunaKey
}

export function getPgConnectionString () {
  console.log('env', process.env)
  let connectionString
  if (process.env.ENV === 'staging') {
    connectionString = process.env.STAGING_PG_CONNECTION
  } else if (process.env.ENV === 'production') {
    connectionString = process.env.PRODUCTION_PG_CONNECTION
  } else {
    connectionString = process.env.DEV_PG_CONNECTION
  }

  if (!connectionString) {
    throw new Error('Environment variables for ENV and/or PG_CONNECTION are not set')
  }

  return connectionString
}

export function getSslState () {
  if (process.env.ENV === 'staging' || process.env.ENV === 'production') {
    return true
  }

  return false
}
