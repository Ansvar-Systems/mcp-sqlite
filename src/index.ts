/**
 * @ansvar/mcp-sqlite
 *
 * Drop-in replacement for better-sqlite3 using node-sqlite3-wasm (WebAssembly).
 * Zero native modules — works on any Node.js version and platform.
 * Includes FTS5 full-text search support.
 *
 * Bridges the small API differences between better-sqlite3 and node-sqlite3-wasm:
 *   - Constructor option: readonly → readOnly
 *   - Named params: {name: val} → {'@name': val}
 *   - Adds .pragma() method
 *   - Adds .transaction() method
 */

import pkg from 'node-sqlite3-wasm';
const { Database: WasmDatabase } = pkg;
type WasmStatement = import('node-sqlite3-wasm').Statement;
import { rmSync, existsSync } from 'node:fs';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface DatabaseOptions {
  readonly?: boolean;
  fileMustExist?: boolean;
}

export interface RunResult {
  changes: number;
  lastInsertRowid: number | bigint;
}

// ---------------------------------------------------------------------------
// Statement wrapper
// ---------------------------------------------------------------------------

class Statement<T = unknown> {
  /** The WASM database instance, used to re-prepare statements */
  private db: InstanceType<typeof WasmDatabase>;
  /** Current WASM statement (null after finalize, re-prepared on next use) */
  private stmt: WasmStatement | null;
  private sql: string;
  private prefix: string;

  constructor(db: InstanceType<typeof WasmDatabase>, stmt: WasmStatement, sql: string) {
    this.db = db;
    this.stmt = stmt;
    this.sql = sql;
    this.prefix = detectParamPrefix(sql);
  }

  /**
   * Get or re-prepare the WASM statement.
   * node-sqlite3-wasm statements hold file-level locks until finalized.
   * We finalize after each operation and re-prepare as needed to prevent
   * "database is locked" errors from accumulated unfiled statements.
   */
  private ensure(): WasmStatement {
    if (!this.stmt) {
      this.stmt = this.db.prepare(this.sql);
    }
    return this.stmt;
  }

  /** Finalize the WASM statement to release locks. */
  private release(): void {
    if (this.stmt) {
      try { this.stmt.finalize(); } catch {}
      this.stmt = null;
    }
  }

  /**
   * Run query and return all matching rows.
   * Accepts positional params as varargs, or a single object for named params.
   */
  all(...params: unknown[]): T[] {
    const bound = normalizeParams(params, this.prefix);
    const result = this.ensure().all(bound) as T[];
    this.release();
    return result;
  }

  /**
   * Run query and return the first matching row, or undefined.
   */
  get(...params: unknown[]): T | undefined {
    const bound = normalizeParams(params, this.prefix);
    const result = this.ensure().get(bound);
    this.release();
    return (result ?? undefined) as T | undefined;
  }

  /**
   * Run a mutation (INSERT/UPDATE/DELETE) and return RunResult.
   */
  run(...params: unknown[]): RunResult {
    const bound = normalizeParams(params, this.prefix);
    const result = this.ensure().run(bound) as RunResult;
    this.release();
    return result;
  }
}

// ---------------------------------------------------------------------------
// Database wrapper
// ---------------------------------------------------------------------------

class Database {
  private db: InstanceType<typeof WasmDatabase>;
  private path: string;

  constructor(path: string, options?: DatabaseOptions) {
    this.path = path;
    this.db = new WasmDatabase(path, {
      readOnly: options?.readonly ?? false,
      fileMustExist: options?.fileMustExist ?? false,
    });
  }

  /**
   * Prepare a SQL statement for repeated use.
   */
  prepare<T = unknown>(sql: string): Statement<T> {
    return new Statement<T>(this.db, this.db.prepare(sql), sql);
  }

  /**
   * Run one or more SQL statements directly (no return value).
   */
  exec(sql: string): void {
    this.db.exec(sql);
  }

  /**
   * Run a PRAGMA statement.
   * With { simple: true }, returns a single scalar value.
   * Without options, returns an array of result rows.
   */
  pragma(
    pragmaStr: string,
    options?: { simple?: boolean },
  ): unknown {
    const sql = pragmaStr.toUpperCase().startsWith('PRAGMA')
      ? pragmaStr
      : `PRAGMA ${pragmaStr}`;

    try {
      if (options?.simple) {
        const row = this.db.get(sql) as Record<string, unknown> | null;
        if (!row) return undefined;
        const values = Object.values(row);
        return values.length > 0 ? values[0] : undefined;
      }
      return this.db.all(sql);
    } catch {
      // Some pragmas (like optimize, wal_checkpoint) may not return results
      return options?.simple ? undefined : [];
    }
  }

  /**
   * Create a transaction wrapper. The returned function runs the
   * callback inside BEGIN/COMMIT, with ROLLBACK on error.
   */
  transaction<F extends (...args: any[]) => any>(fn: F): F {
    const self = this;
    const wrapper = function (this: any, ...args: any[]) {
      self.db.run('BEGIN TRANSACTION');
      try {
        const result = fn.apply(this, args);
        self.db.run('COMMIT');
        return result;
      } catch (err) {
        self.db.run('ROLLBACK');
        throw err;
      }
    } as unknown as F;
    return wrapper;
  }

  /**
   * Close the database connection.
   * Idempotent — safe to call multiple times (matches better-sqlite3 behavior).
   * Also cleans up .lock directories created by node-sqlite3-wasm's VFS.
   */
  close(): void {
    try {
      this.db.close();
    } catch {
      // Ignore "Database already closed" errors (idempotent close)
    }
    // node-sqlite3-wasm creates <path>.lock/ directories for VFS locking.
    // These persist after close() and cause "database is locked" on re-open.
    if (this.path !== ':memory:') {
      const lockDir = this.path + '.lock';
      if (existsSync(lockDir)) {
        try { rmSync(lockDir, { recursive: true, force: true }); } catch {}
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Detect which named parameter prefix (@, $, :) is used in the SQL.
 * Defaults to '@' which is the better-sqlite3 convention.
 */
function detectParamPrefix(sql: string): string {
  // Match @word, $word, or :word (but not inside strings)
  const match = sql.match(/[@$:]([A-Za-z_]\w*)/);
  if (match) return match[0][0];
  return '@'; // default to better-sqlite3 convention
}

/**
 * Normalize better-sqlite3 parameter conventions to node-sqlite3-wasm format.
 *
 * better-sqlite3: stmt.all({name: 'foo'}) with @name in SQL (bare keys)
 * node-sqlite3-wasm: stmt.all({'@name': 'foo'}) with @name in SQL (prefixed keys)
 *
 * For positional params, both use the same convention.
 */
function normalizeParams(params: unknown[], prefix: string = '@'): any {
  if (params.length === 0) return undefined;

  // Single object arg = named parameters
  if (
    params.length === 1 &&
    params[0] !== null &&
    typeof params[0] === 'object' &&
    !Array.isArray(params[0]) &&
    !(params[0] instanceof Uint8Array)
  ) {
    const named = params[0] as Record<string, unknown>;
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(named)) {
      if (key.startsWith('@') || key.startsWith('$') || key.startsWith(':')) {
        // Already prefixed — pass through as-is
        result[key] = value;
      } else {
        // Add the prefix that matches the SQL placeholder
        result[`${prefix}${key}`] = value;
      }
    }
    return result;
  }

  // Single positional arg — pass directly
  if (params.length === 1) return params[0];

  // Multiple positional args — pass as array
  return params;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

export default Database;
export { Database, Statement };
