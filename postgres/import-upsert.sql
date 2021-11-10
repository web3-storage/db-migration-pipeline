INSERT INTO public.user (id, name, picture, email, issuer, github, public_address, inserted_at, updated_at)
  SELECT id, name, picture, email, issuer, github, public_address, inserted_at, updated_at
  FROM public.user_imported
ON CONFLICT (id) DO UPDATE SET
  name = excluded.name,
  picture = excluded.picture,
  email = excluded.email,
  github = excluded.github,
  updated_at = excluded.updated_at
;

INSERT INTO content (cid, dag_size, inserted_at, updated_at)
  SELECT cid, dag_size, inserted_at, updated_at
  FROM content_imported
ON CONFLICT (cid) DO UPDATE SET
  dag_size = excluded.dag_size,
  updated_at = excluded.updated_at
;

INSERT INTO pin_location (id, peer_id, peer_name, region)
  SELECT id, peer_id, peer_name, region
  FROM pin_location_imported
ON CONFLICT (id) DO UPDATE SET
  peer_id = excluded.peer_id,
  peer_name = excluded.peer_name,
  region = excluded.region
;

INSERT INTO auth_key (id, name, secret, user_id, inserted_at, updated_at, deleted_at)
  SELECT id, name, secret, user_id, inserted_at, updated_at, deleted_at
  FROM auth_key_imported
ON CONFLICT (id) DO UPDATE SET
  name = excluded.name,
  updated_at = excluded.updated_at,
  deleted_at = excluded.deleted_at
;

INSERT INTO pin (id, status, content_cid, pin_location_id, inserted_at, updated_at)
  SELECT id, status, content_cid, pin_location_id, inserted_at, updated_at
  FROM pin_imported
ON CONFLICT (id) DO UPDATE SET
  status = excluded.status,
  updated_at = excluded.updated_at
;

INSERT INTO pin_request (id, content_cid, attempts, inserted_at, updated_at)
  SELECT id, content_cid, attempts, inserted_at, updated_at
  FROM pin_request_imported
ON CONFLICT (id) DO UPDATE SET
  attempts = excluded.attempts,
  updated_at = excluded.updated_at
;

INSERT INTO upload AS u (id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at)
  SELECT DISTINCT ON (user_id, source_cid) id, user_id, auth_key_id, content_cid, source_cid, type, name, inserted_at, updated_at, deleted_at
  FROM upload_imported AS ui
ON CONFLICT (user_id, source_cid) DO UPDATE SET
  name = excluded.name,
  updated_at = excluded.updated_at,
  deleted_at = excluded.deleted_at
WHERE excluded.updated_at > u.updated_at;

INSERT INTO pin_sync_request (id, pin_id, inserted_at)
  SELECT id, pin_id, inserted_at
  FROM pin_sync_request_imported
ON CONFLICT (id) DO NOTHING;

INSERT INTO backup (id, upload_id, url, inserted_at)
  SELECT id, upload_id, url, inserted_at
  FROM backup_imported
ON CONFLICT (url) DO NOTHING;

DROP TABLE public.user_imported;
DROP TABLE auth_key_imported;
DROP TABLE content_imported;
DROP TABLE pin_location_imported;
DROP TABLE pin_imported;
DROP TABLE upload_imported;
DROP TABLE backup_imported;
DROP TABLE pin_request_imported;
DROP TABLE pin_sync_request_imported;
