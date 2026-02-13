import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Database from '../src/index.js';
import type { RunResult } from '../src/index.js';
import { rmSync, existsSync } from 'node:fs';

describe('Database (in-memory)', () => {
  let db: InstanceType<typeof Database>;

  beforeEach(() => {
    db = new Database(':memory:');
    db.exec(`
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
      INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
      INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
      INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
    `);
  });

  afterEach(() => {
    db.close();
  });

  describe('prepare().all()', () => {
    it('returns all rows', () => {
      const rows = db.prepare('SELECT * FROM users').all();
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ id: 1, name: 'Alice', email: 'alice@example.com' });
    });

    it('filters with positional params', () => {
      const rows = db.prepare('SELECT * FROM users WHERE id > ?').all(1);
      expect(rows).toHaveLength(2);
      expect(rows[0]).toMatchObject({ name: 'Bob' });
    });

    it('filters with multiple positional params', () => {
      const rows = db.prepare('SELECT * FROM users WHERE id >= ? AND id <= ?').all(1, 2);
      expect(rows).toHaveLength(2);
    });

    it('filters with named params (@)', () => {
      const rows = db.prepare('SELECT * FROM users WHERE name = @name').all({ name: 'Alice' });
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ id: 1 });
    });

    it('returns empty array for no matches', () => {
      const rows = db.prepare('SELECT * FROM users WHERE id = ?').all(999);
      expect(rows).toEqual([]);
    });
  });

  describe('prepare().get()', () => {
    it('returns single row', () => {
      const row = db.prepare('SELECT * FROM users WHERE id = ?').get(1);
      expect(row).toEqual({ id: 1, name: 'Alice', email: 'alice@example.com' });
    });

    it('returns undefined for no match', () => {
      const row = db.prepare('SELECT * FROM users WHERE id = ?').get(999);
      expect(row).toBeUndefined();
    });

    it('works with named params', () => {
      const row = db.prepare('SELECT * FROM users WHERE name = @name').get({ name: 'Bob' });
      expect(row).toMatchObject({ id: 2, name: 'Bob' });
    });
  });

  describe('prepare().run()', () => {
    it('inserts and returns changes', () => {
      const result: RunResult = db
        .prepare('INSERT INTO users VALUES (?, ?, ?)')
        .run(4, 'Diana', 'diana@example.com');
      expect(result.changes).toBe(1);
      expect(result.lastInsertRowid).toBe(4);
    });

    it('updates and returns changes count', () => {
      const result = db
        .prepare("UPDATE users SET email = ? WHERE name = ?")
        .run('newalice@example.com', 'Alice');
      expect(result.changes).toBe(1);
    });

    it('deletes and returns changes count', () => {
      const result = db.prepare('DELETE FROM users WHERE id > ?').run(1);
      expect(result.changes).toBe(2);
    });

    it('works with named params', () => {
      const result = db
        .prepare('INSERT INTO users VALUES (@id, @name, @email)')
        .run({ id: 5, name: 'Eve', email: 'eve@example.com' });
      expect(result.changes).toBe(1);
    });
  });

  describe('db.exec()', () => {
    it('runs multi-statement SQL', () => {
      db.exec(`
        CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT);
        INSERT INTO logs VALUES (1, 'test');
      `);
      const row = db.prepare('SELECT * FROM logs').get();
      expect(row).toMatchObject({ id: 1, msg: 'test' });
    });
  });

  describe('pragma()', () => {
    it('returns result with simple option', () => {
      const result = db.pragma('journal_mode', { simple: true });
      expect(result).toBeDefined();
    });

    it('sets and reads foreign_keys', () => {
      db.pragma('foreign_keys = ON');
      const val = db.pragma('foreign_keys', { simple: true });
      expect(val).toBe(1);
    });

    it('returns array without simple option', () => {
      const result = db.pragma('table_info(users)');
      expect(Array.isArray(result)).toBe(true);
      expect((result as any[]).length).toBe(3);
    });
  });

  describe('transaction()', () => {
    it('commits on success', () => {
      const insertMany = db.transaction((items: { name: string; email: string }[]) => {
        const stmt = db.prepare('INSERT INTO users (name, email) VALUES (@name, @email)');
        for (const item of items) {
          stmt.run(item);
        }
      });

      insertMany([
        { name: 'Diana', email: 'diana@example.com' },
        { name: 'Eve', email: 'eve@example.com' },
      ]);

      const count = db.prepare('SELECT COUNT(*) as count FROM users').get() as any;
      expect(count.count).toBe(5);
    });

    it('rolls back on error', () => {
      const badTransaction = db.transaction(() => {
        db.prepare('INSERT INTO users VALUES (?, ?, ?)').run(10, 'Test', 'test@example.com');
        throw new Error('Intentional failure');
      });

      expect(() => badTransaction()).toThrow('Intentional failure');

      const row = db.prepare('SELECT * FROM users WHERE id = ?').get(10);
      expect(row).toBeUndefined();
    });
  });

  describe('FTS5 support', () => {
    it('works with FTS5 tables', () => {
      db.exec(`
        CREATE VIRTUAL TABLE docs USING fts5(title, body);
        INSERT INTO docs VALUES ('Security Policy', 'All users must use strong passwords');
        INSERT INTO docs VALUES ('Privacy Notice', 'Personal data is protected under GDPR');
        INSERT INTO docs VALUES ('Incident Response', 'Report security incidents within 24 hours');
      `);

      const results = db
        .prepare("SELECT * FROM docs WHERE docs MATCH ?")
        .all('security');

      expect(results.length).toBeGreaterThanOrEqual(1);
      expect(results.some((r: any) => r.title === 'Security Policy')).toBe(true);
    });
  });
});

describe('Database (file-based)', () => {
  let testDbPath: string;

  beforeEach(() => {
    testDbPath = `/tmp/mcp-sqlite-test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  });

  afterEach(() => {
    // Clean up db file and any WAL/journal/shm files
    for (const suffix of ['', '-wal', '-journal', '-shm']) {
      const p = testDbPath + suffix;
      if (existsSync(p)) rmSync(p, { recursive: true, force: true });
    }
  });

  it('persists data to file and re-opens', () => {
    const fileDb = new Database(testDbPath);
    fileDb.exec('CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)');
    fileDb.prepare('INSERT INTO test VALUES (?, ?)').run(1, 'hello');
    fileDb.close();

    const readDb = new Database(testDbPath, { readonly: true });
    const row = readDb.prepare('SELECT * FROM test WHERE id = ?').get(1);
    expect(row).toMatchObject({ id: 1, val: 'hello' });
    readDb.close();
  });

  it('throws on missing file with fileMustExist', () => {
    expect(() => {
      new Database('/tmp/nonexistent-db-file.db', { fileMustExist: true });
    }).toThrow();
  });
});
