package sqlite

const schemaVersion = 2

var schemaV1Statements = []string{
	`CREATE TABLE IF NOT EXISTS dofs_namespaces (
        id          TEXT PRIMARY KEY,
        label       TEXT NOT NULL DEFAULT '',
        wrapped_kek BLOB NOT NULL,
        quota_bytes INTEGER NOT NULL DEFAULT 0 CHECK (quota_bytes >= 0),
        used_bytes  INTEGER NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
        next_inode  INTEGER NOT NULL DEFAULT 2 CHECK (next_inode >= 2),
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
    )`,
	`CREATE TABLE IF NOT EXISTS dofs_nodes (
        namespace_id TEXT NOT NULL REFERENCES dofs_namespaces(id) ON DELETE CASCADE,
        inode         INTEGER NOT NULL CHECK (inode > 0),
        parent_inode  INTEGER NOT NULL DEFAULT 0 CHECK (parent_inode >= 0),
        name           TEXT NOT NULL DEFAULT '',
        kind           TEXT NOT NULL CHECK (kind IN ('file', 'directory')),
        state          TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'deleted')),
        mode           INTEGER NOT NULL CHECK (mode >= 0 AND mode <= 511),
        size           INTEGER NOT NULL DEFAULT 0 CHECK (size >= 0),
        generation     INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
        object_key     TEXT NOT NULL DEFAULT '',
        wrapped_dek    BLOB NOT NULL DEFAULT X'',
        created_at     INTEGER NOT NULL,
        modified_at    INTEGER NOT NULL,
        changed_at     INTEGER NOT NULL,
        deleted_at     INTEGER,
        PRIMARY KEY (namespace_id, inode),
        CHECK (
            (inode = 1 AND parent_inode = 0 AND name = '' AND kind = 'directory' AND state = 'ready') OR
            (inode <> 1 AND name <> '' AND name <> '.' AND name <> '..' AND instr(name, '/') = 0)
        ),
        CHECK ((kind = 'directory' AND length(wrapped_dek) = 0) OR kind = 'file'),
        CHECK ((state = 'deleted' AND parent_inode = 0 AND deleted_at IS NOT NULL) OR state <> 'deleted')
    )`,
	`CREATE UNIQUE INDEX IF NOT EXISTS dofs_nodes_active_name
        ON dofs_nodes(namespace_id, parent_inode, name)
        WHERE state IN ('creating', 'ready')`,
	`CREATE INDEX IF NOT EXISTS dofs_nodes_parent
        ON dofs_nodes(namespace_id, parent_inode, name)
        WHERE state = 'ready'`,
	`CREATE INDEX IF NOT EXISTS dofs_nodes_state
        ON dofs_nodes(namespace_id, state, inode)`,
	`CREATE TABLE IF NOT EXISTS dofs_generations (
        namespace_id TEXT NOT NULL,
        inode         INTEGER NOT NULL,
        number        INTEGER NOT NULL CHECK (number > 0),
        object_key    TEXT NOT NULL,
        size           INTEGER NOT NULL CHECK (size >= 0),
        created_at     INTEGER NOT NULL,
        PRIMARY KEY (namespace_id, inode, number),
        UNIQUE (object_key),
        FOREIGN KEY (namespace_id, inode) REFERENCES dofs_nodes(namespace_id, inode) ON DELETE CASCADE
    )`,
	`CREATE TABLE IF NOT EXISTS dofs_events (
        sequence         INTEGER PRIMARY KEY AUTOINCREMENT,
        namespace_id    TEXT NOT NULL REFERENCES dofs_namespaces(id) ON DELETE CASCADE,
        kind             TEXT NOT NULL CHECK (kind IN ('create', 'write', 'rename', 'remove', 'chmod')),
        inode            INTEGER NOT NULL,
        generation       INTEGER NOT NULL DEFAULT 0,
        parent_inode    INTEGER NOT NULL DEFAULT 0,
        name             TEXT NOT NULL DEFAULT '',
        old_parent_inode INTEGER NOT NULL DEFAULT 0,
        old_name         TEXT NOT NULL DEFAULT '',
        created_at       INTEGER NOT NULL
    )`,
	`CREATE INDEX IF NOT EXISTS dofs_events_namespace_sequence
        ON dofs_events(namespace_id, sequence)`,
	`CREATE TABLE IF NOT EXISTS dofs_writer_leases (
        namespace_id TEXT PRIMARY KEY REFERENCES dofs_namespaces(id) ON DELETE CASCADE,
        owner         TEXT NOT NULL,
        acquired_at   INTEGER NOT NULL,
        expires_at    INTEGER NOT NULL CHECK (expires_at > acquired_at)
    )`,
}

var schemaV2Statements = []string{
	`CREATE TABLE IF NOT EXISTS dofs_external_uploads (
        namespace_id       TEXT NOT NULL,
        upload_id          TEXT NOT NULL,
        inode              INTEGER NOT NULL CHECK (inode > 0),
        expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
        target_generation  INTEGER NOT NULL CHECK (target_generation = expected_generation + 1),
        object_key         TEXT NOT NULL UNIQUE,
        size               INTEGER NOT NULL CHECK (size >= 0),
        wrapped_dek        BLOB NOT NULL,
        creates_node       INTEGER NOT NULL CHECK (creates_node IN (0, 1)),
        state              TEXT NOT NULL CHECK (state IN ('active', 'committed')),
        expires_at         INTEGER NOT NULL,
        created_at         INTEGER NOT NULL,
        committed_at       INTEGER,
        PRIMARY KEY (namespace_id, upload_id),
        FOREIGN KEY (namespace_id, inode) REFERENCES dofs_nodes(namespace_id, inode) ON DELETE CASCADE,
        CHECK (length(upload_id) BETWEEN 1 AND 128),
        CHECK (length(wrapped_dek) > 0),
        CHECK ((state = 'committed' AND committed_at IS NOT NULL) OR (state = 'active' AND committed_at IS NULL))
    )`,
	`CREATE UNIQUE INDEX IF NOT EXISTS dofs_external_uploads_active_inode
        ON dofs_external_uploads(namespace_id, inode) WHERE state = 'active'`,
	`CREATE INDEX IF NOT EXISTS dofs_external_uploads_state_expiry
        ON dofs_external_uploads(namespace_id, state, expires_at, upload_id)`,
}
