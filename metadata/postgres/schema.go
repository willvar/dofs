package postgres

const schemaVersion = 2

const createSchema = `
CREATE TABLE IF NOT EXISTS dofs_namespaces (
    id              VARCHAR(128) PRIMARY KEY,
    label           TEXT NOT NULL DEFAULT '',
    wrapped_kek     BYTEA NOT NULL,
    quota_bytes     BIGINT NOT NULL DEFAULT 0 CHECK (quota_bytes >= 0),
    used_bytes      BIGINT NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    next_inode      BIGINT NOT NULL DEFAULT 2 CHECK (next_inode >= 2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dofs_nodes (
    namespace_id   VARCHAR(128) NOT NULL REFERENCES dofs_namespaces(id) ON DELETE CASCADE,
    inode          BIGINT NOT NULL CHECK (inode > 0),
    parent_inode   BIGINT NOT NULL DEFAULT 0 CHECK (parent_inode >= 0),
    name            TEXT NOT NULL DEFAULT '',
    kind            TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
    state           TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'deleted')),
    mode            INTEGER NOT NULL CHECK (mode >= 0 AND mode <= 511),
    size            BIGINT NOT NULL DEFAULT 0 CHECK (size >= 0),
    generation      BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    object_key      TEXT NOT NULL DEFAULT '',
    wrapped_dek     BYTEA NOT NULL DEFAULT ''::bytea,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    changed_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at      TIMESTAMPTZ,
    PRIMARY KEY (namespace_id, inode),
    CHECK (
        (inode = 1 AND parent_inode = 0 AND name = '' AND kind = 'directory' AND state = 'ready') OR
        (inode <> 1 AND name <> '' AND name <> '.' AND name <> '..' AND position('/' IN name) = 0)
    ),
    CHECK ((kind = 'directory' AND wrapped_dek = ''::bytea) OR kind = 'file'),
    CHECK ((state = 'deleted' AND parent_inode = 0 AND deleted_at IS NOT NULL) OR state <> 'deleted')
);

CREATE UNIQUE INDEX IF NOT EXISTS dofs_nodes_active_name
    ON dofs_nodes(namespace_id, parent_inode, name)
    WHERE state IN ('creating', 'ready');
CREATE INDEX IF NOT EXISTS dofs_nodes_parent
    ON dofs_nodes(namespace_id, parent_inode, name)
    WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS dofs_nodes_state
    ON dofs_nodes(namespace_id, state, inode);

CREATE TABLE IF NOT EXISTS dofs_generations (
    namespace_id   VARCHAR(128) NOT NULL,
    inode          BIGINT NOT NULL,
    number         BIGINT NOT NULL CHECK (number > 0),
    object_key     TEXT NOT NULL,
    size            BIGINT NOT NULL CHECK (size >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (namespace_id, inode, number),
    UNIQUE (object_key),
    FOREIGN KEY (namespace_id, inode) REFERENCES dofs_nodes(namespace_id, inode) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dofs_events (
    sequence        BIGSERIAL PRIMARY KEY,
    namespace_id   VARCHAR(128) NOT NULL REFERENCES dofs_namespaces(id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK (kind IN ('create', 'write', 'rename', 'remove', 'chmod')),
    inode           BIGINT NOT NULL,
    generation      BIGINT NOT NULL DEFAULT 0,
    parent_inode   BIGINT NOT NULL DEFAULT 0,
    name            TEXT NOT NULL DEFAULT '',
    old_parent_inode BIGINT NOT NULL DEFAULT 0,
    old_name        TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS dofs_events_namespace_sequence
    ON dofs_events(namespace_id, sequence);
`

const createExternalUploadSchema = `
CREATE TABLE IF NOT EXISTS dofs_external_uploads (
    namespace_id        VARCHAR(128) NOT NULL,
    upload_id           VARCHAR(128) NOT NULL,
    inode               BIGINT NOT NULL CHECK (inode > 0),
    expected_generation BIGINT NOT NULL CHECK (expected_generation >= 0),
    target_generation   BIGINT NOT NULL CHECK (target_generation = expected_generation + 1),
    object_key          TEXT NOT NULL UNIQUE,
    size                BIGINT NOT NULL CHECK (size >= 0),
    wrapped_dek         BYTEA NOT NULL CHECK (octet_length(wrapped_dek) > 0),
    creates_node        BOOLEAN NOT NULL,
    state               TEXT NOT NULL CHECK (state IN ('active', 'committed')),
    expires_at          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    committed_at        TIMESTAMPTZ,
    PRIMARY KEY (namespace_id, upload_id),
    FOREIGN KEY (namespace_id, inode) REFERENCES dofs_nodes(namespace_id, inode) ON DELETE CASCADE,
    CHECK ((state = 'committed' AND committed_at IS NOT NULL) OR (state = 'active' AND committed_at IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS dofs_external_uploads_active_inode
    ON dofs_external_uploads(namespace_id, inode) WHERE state = 'active';
CREATE INDEX IF NOT EXISTS dofs_external_uploads_state_expiry
    ON dofs_external_uploads(namespace_id, state, expires_at, upload_id);
`
