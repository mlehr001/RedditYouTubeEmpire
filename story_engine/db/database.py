"""
Database connection pool and base query helpers.
Uses psycopg2 with a connection pool — never raw connections in pipeline stages.
"""

import logging
import os
from contextlib import contextmanager
from typing import Any, Generator, Optional

import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger(__name__)


class Database:
    """Thread-safe PostgreSQL connection pool wrapper."""

    _instance: Optional["Database"] = None
    _pool: Optional[pool.ThreadedConnectionPool] = None

    def __init__(self, dsn: str, min_conn: int = 2, max_conn: int = 10):
        self.dsn = dsn
        self.min_conn = min_conn
        self.max_conn = max_conn
        self._pool = pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dsn=dsn,
        )
        logger.info("Database pool initialized (min=%d, max=%d)", min_conn, max_conn)

    @classmethod
    def get_instance(cls) -> "Database":
        if cls._instance is None:
            dsn = os.environ.get("DATABASE_URL")
            if not dsn:
                raise RuntimeError("DATABASE_URL environment variable not set")
            cls._instance = cls(dsn)
        return cls._instance

    @contextmanager
    def get_conn(self) -> Generator:
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    @contextmanager
    def cursor(self) -> Generator:
        with self.get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur

    def execute(self, query: str, params: tuple = ()) -> list[dict]:
        """Execute a query and return all rows as dicts."""
        with self.cursor() as cur:
            cur.execute(query, params)
            try:
                return [dict(row) for row in cur.fetchall()]
            except psycopg2.ProgrammingError:
                return []

    def execute_one(self, query: str, params: tuple = ()) -> Optional[dict]:
        """Execute and return a single row or None."""
        results = self.execute(query, params)
        return results[0] if results else None

    def execute_many(self, query: str, values: list[tuple]) -> int:
        """Bulk insert using execute_values. Returns row count."""
        with self.cursor() as cur:
            execute_values(cur, query, values)
            return cur.rowcount

    def close(self):
        if self._pool:
            self._pool.closeall()
            logger.info("Database pool closed")


def get_db() -> Database:
    return Database.get_instance()