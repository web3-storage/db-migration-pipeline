-- A user of web3.storage.
CREATE TABLE IF NOT EXISTS public.user_imported
(
  id              BIGSERIAL PRIMARY KEY,
  name            TEXT                                                          NOT NULL,
  picture         TEXT,
  email           TEXT                                                          NOT NULL,
  -- The Decentralized ID of the Magic User who generated the DID Token.
  issuer          TEXT                                                          NOT NULL,
  -- GitHub user handle, may be null if user logged in via email.
  github          TEXT,
  -- Cryptographic public address of the Magic User.
  public_address  TEXT                                                          NOT NULL,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- User authentication keys.
CREATE TABLE IF NOT EXISTS auth_key_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- User assigned name.
  name            TEXT                                                          NOT NULL,
  -- Secret that corresponds to this token.
  secret          TEXT                                                          NOT NULL,
  -- User this token belongs to.
  user_id         BIGINT                                                        NOT NULL,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  deleted_at      TIMESTAMP WITH TIME ZONE
);

-- Details of the root of a file/directory stored on web3.storage.
CREATE TABLE IF NOT EXISTS content_imported
(
  -- Root CID for this content. Normalized as v1 base32.
  cid             TEXT PRIMARY KEY,
  -- Size of the DAG in bytes. Either the cumulativeSize for dag-pb or the sum of block sizes in the CAR.
  dag_size        BIGINT,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- An IPFS node that is pinning content.
CREATE TABLE IF NOT EXISTS pin_location_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- Libp2p peer ID of the node pinning this pin.
  peer_id         TEXT                                                          NOT NULL,
  -- Name of the peer pinning this pin.
  peer_name       TEXT,
  -- Geographic region this node resides in.
  region          TEXT
);

-- Information for piece of content pinned in IPFS.
CREATE TABLE IF NOT EXISTS pin_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- Pinning status at this location.
  status          pin_status_type                                               NOT NULL,
  -- The content being pinned.
  content_cid     TEXT                                                          NOT NULL,
  -- Identifier for the service that is pinning this pin.
  pin_location_id BIGINT                                                        NOT NULL,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- An upload created by a user.
CREATE TABLE IF NOT EXISTS upload_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- User that uploaded this content.
  user_id         BIGINT                                                        NOT NULL,
  -- User authentication token that was used to upload this content.
  -- Note: nullable, because the user may have used a Magic.link token.
  auth_key_id     BIGINT,
  -- The root of the uploaded content (base32 CIDv1 normalised).
  content_cid     TEXT                                                          NOT NULL,
  -- CID in the from we found in the received file.
  source_cid      TEXT                                                          NOT NULL,
  -- Type of received upload data.
  type            upload_type                                                   NOT NULL,
  -- User provided name for this upload.
  name            TEXT,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  deleted_at      TIMESTAMP WITH TIME ZONE
);

-- Details of the backups created for an upload.
CREATE TABLE IF NOT EXISTS backup_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- Upload that resulted in this backup.
  upload_id       BIGINT                                                        NOT NULL,
  -- Backup url location.
  url             TEXT                                                          NOT NULL,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- Tracks requests to replicate content to more nodes.
CREATE TABLE IF NOT EXISTS pin_request_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- Root CID of the Pin we want to replicate.
  content_cid     TEXT                                                          NOT NULL,
  attempts        INT DEFAULT 0,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- A request to keep a Pin in sync with the nodes that are pinning it.
CREATE TABLE IF NOT EXISTS pin_sync_request_imported
(
  id              BIGSERIAL PRIMARY KEY,
  -- Identifier for the pin to keep in sync.
  pin_id          BIGINT                                                        NOT NULL,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);
