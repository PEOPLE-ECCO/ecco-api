from dataclasses import dataclass

import sqlalchemy
from quart import request, current_app
from sqlalchemy import create_engine, event, Connection, Engine
from sqlalchemy.orm import Session

from .definitions import *

root_uuid = "52000000-0000-0000-0000-000000000052"


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str
    recreate_tables = False
    debug = False


class DBSession(Session):
    is_root: False

    def __init__(self, admin=False, engine=None):
        if not engine:
            if hasattr(current_app, "db_engine"):
                engine = current_app.db_engine
            else:
                engine = connect_db(current_app.config["db"])
        super().__init__(engine)
        self.is_root = admin


def connect_db(config: DBConfig) -> Engine:
    engine = create_engine(
        f"postgresql+psycopg://{config.user}:{config.password}@{config.host}:{config.port}/{config.dbname}",
        echo=config.debug
    )

    @event.listens_for(Session, 'after_begin')
    def switch_to_user(session, transaction, connection):
        ## TODO: ensure that this is really a uuid. this smells like sql injection
        if session.is_root:
            connection.execute(sqlalchemy.sql.text(f"SET local jwt.claims.roles = \'{root_uuid}\';"))
        else:
            ids = ",".join(request.scope['uuid'])
            connection.execute(sqlalchemy.sql.text(f"SET local jwt.claims.roles = \'{ids}\';"))

    if current_app:
        current_app.db_engine = engine
    return engine


def recreate_tables(engine: Engine):
    """
    Initalizes the databases - adds schemas if required
    """

    conn = engine.connect()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    _setup_rls_users(conn)
    _setup_rls_scenarios(conn)
    _setup_rls_jobs(conn)
    _setup_rls_processes(conn)
    _setup_rls_timeseries(conn)


def _setup_rls_users(conn: Connection):
    """
    Sets up Postgres RLS for `users` table
    Users are readable by all uuid in kc_uuid
    Users are writable by root
    """
    name = "users"
    stmnts = _default_rls(name) + [
        sqlalchemy.sql.text(f"SET local jwt.claims.roles = \'{root_uuid}\';"),
        sqlalchemy.sql.text(f"INSERT INTO users VALUES(1, 'root', 'people-ecco@52north.org', '{root_uuid}') ON CONFLICT DO NOTHING;"),
        sqlalchemy.sql.text(f"DROP POLICY IF EXISTS acl_{name} on {name};"),
        sqlalchemy.sql.text(f"""
                            create policy acl_{name}
                                on {name}
                                as permissive
                                for ALL
                                using (
                                  ARRAY[users.kc_uuid]::UUID[] && regexp_split_to_array(current_setting('jwt.claims.roles'), ',')::uuid[]
                                )
                                with check (
                                  ARRAY[users.kc_uuid::UUID] && regexp_split_to_array(current_setting('jwt.claims.roles'), ',')::uuid[]
                                );
                            """)
    ]

    for stmnt in stmnts:
        conn.execute(stmnt)
    conn.commit()


def _setup_rls_jobs(conn: Connection):
    """
    Sets up Postgres RLS for `jobs` table.
    Jobs are readable for by all uuids in acl_read
    Jobs are writable by root
    Jobs are writable by everyone
    """
    name = "jobs"
    stmnts = _default_rls(name) + _default_aclread_rls(name) + [
        sqlalchemy.sql.text(f"DROP POLICY IF EXISTS acl_write_{name} on {name};"),
        sqlalchemy.sql.text(f"""
                            create policy acl_write_{name}
                                on {name}
                                as permissive
                                for INSERT
                                with check (
                                  true
                                );
                            """)
    ]

    for stmnt in stmnts:
        conn.execute(stmnt)
    conn.commit()


def _setup_rls_scenarios(conn: Connection):
    """
    Sets up Postgres RLS for `scenarios` table.
    Scenarios are readable by all uuids in acl_read
    Scenarios are writable by root
    """
    name = "scenarios"
    stmnts = _default_rls(name) + _default_aclread_rls(name)

    for stmnt in stmnts:
        conn.execute(stmnt)
    conn.commit()


def _setup_rls_processes(conn: Connection):
    """
    Sets up Postgres RLS for `processes` table.
    Processes are readable by all uuids in acl_read
    Processes are writable by root
    """
    name = "processes"
    stmnts = _default_rls(name) + _default_aclread_rls(name)

    for stmnt in stmnts:
        conn.execute(stmnt)
    conn.commit()


def _setup_rls_timeseries(conn: Connection):
    """
    Sets up Postgres RLS for `timeseries` table.
    Timeseries are readable by all uuids in acl_read
    Timeseries are writable by root
    """
    name = "timeseries"
    stmnts = _default_rls(name) + _default_aclread_rls(name)

    for stmnt in stmnts:
        conn.execute(stmnt)
    conn.commit()


def _default_aclread_rls(name: str) -> [str]:
    return [
        sqlalchemy.sql.text(f"DROP POLICY IF EXISTS acl_read_{name} on {name};"),
        sqlalchemy.sql.text(f"""
        create policy acl_read_{name}
            on {name}
            as permissive
            for SELECT
            using (
              acl_read && regexp_split_to_array(current_setting('jwt.claims.roles'), ',')::uuid[]
            );
        """)
    ]


def _default_rls(name: str) -> [str]:
    return [
        sqlalchemy.sql.text(f"ALTER TABLE public.{name} ENABLE ROW LEVEL SECURITY;"),
        sqlalchemy.sql.text(f"ALTER TABLE public.{name} FORCE ROW LEVEL SECURITY;"),
        sqlalchemy.sql.text(f"DROP POLICY IF EXISTS acl_root_write_{name} on {name};"),
        sqlalchemy.sql.text(f"""
        create policy acl_root_write_{name}
            on {name}
            as permissive
            for ALL
            using (
              ARRAY['{root_uuid}'::UUID] && regexp_split_to_array(current_setting('jwt.claims.roles'), ',')::uuid[]
            );
        """)
    ]
