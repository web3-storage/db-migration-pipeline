-- 0. Create safe tables
CREATE TABLE IF NOT EXISTS upload_safe
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

CREATE TABLE IF NOT EXISTS backup_safe
(
  id              BIGSERIAL PRIMARY KEY,
  -- Upload that resulted in this backup.
  upload_id       BIGINT                                                        NOT NULL REFERENCES upload_safe (id) ON DELETE CASCADE,
  -- Backup url location.
  url             TEXT                                                          NOT NULL,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 1. Create mig tables
CREATE TABLE IF NOT EXISTS upload_migration
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

CREATE TABLE IF NOT EXISTS backup_migration
(
  id              BIGSERIAL PRIMARY KEY,
  -- Upload that resulted in this backup.
  upload_id       BIGINT                                                        NOT NULL REFERENCES upload_migration (id) ON DELETE CASCADE,
  -- Backup url location.
  url             TEXT                                                          NOT NULL UNIQUE,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 2.1. Insert into Safe Tables
INSERT INTO upload_safe (id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at)
  SELECT id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at
  FROM upload;

INSERT INTO backup_safe (id, upload_id, url, inserted_at)
  SELECT id, upload_id, url, inserted_at
  FROM backup;

-- 2.2. Insert into migration tables
INSERT INTO upload_migration (id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at)
  SELECT id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at
  FROM upload
ON CONFLICT (id) DO UPDATE SET
  name = excluded.name,
  updated_at = excluded.updated_at,
  deleted_at = excluded.deleted_at
;

INSERT INTO backup_migration (id, upload_id, url, inserted_at)
  SELECT id, upload_id, url, inserted_at
  FROM backup
ON CONFLICT (id, url) DO NOTHING;

-- 3. Drop constraints from Upload, Backup

DROP TABLE IF EXISTS upload CASCADE;
DROP TABLE IF EXISTS backup;

CREATE TABLE IF NOT EXISTS upload
(
  id              BIGSERIAL PRIMARY KEY,
  -- User that uploaded this content.
  user_id         BIGINT                                                        NOT NULL REFERENCES public.user (id),
  -- User authentication token that was used to upload this content.
  -- Note: nullable, because the user may have used a Magic.link token.
  auth_key_id     BIGINT REFERENCES auth_key (id),
  -- The root of the uploaded content (base32 CIDv1 normalised).
  content_cid     TEXT                                                          NOT NULL REFERENCES content (cid),
  -- CID in the from we found in the received file.
  source_cid      TEXT                                                          NOT NULL,
  -- Type of received upload data.
  type            upload_type                                                   NOT NULL,
  -- User provided name for this upload.
  name            TEXT,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at      TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  deleted_at      TIMESTAMP WITH TIME ZONE,
  UNIQUE (user_id, source_cid)
);

CREATE INDEX IF NOT EXISTS upload_updated_at_idx ON upload (updated_at);

-- Details of the backups created for an upload.
CREATE TABLE IF NOT EXISTS backup
(
  id              BIGSERIAL PRIMARY KEY,
  -- Upload that resulted in this backup.
  upload_id       BIGINT                                                        NOT NULL REFERENCES upload (id) ON DELETE CASCADE,
  -- Backup url location.
  url             TEXT                                                          NOT NULL UNIQUE,
  inserted_at     TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 4. Insert into original tables with constraints

INSERT INTO upload AS u (id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at)
  SELECT id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at
  FROM upload_migration
ON CONFLICT (id, user_id, source_cid) DO UPDATE SET
  name = excluded.name,
  updated_at = excluded.updated_at,
  deleted_at = excluded.deleted_at
WHERE excluded.updated_at > u.updated_at;

INSERT INTO backup (id, upload_id, url, inserted_at)
  SELECT id, upload_id, url, inserted_at
  FROM backup_migration
ON CONFLICT (id, url) DO NOTHING;
