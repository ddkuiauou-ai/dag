from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Optional
import os

import psycopg2
from dagster import ConfigurableResource
from dagster._utils.backoff import backoff
from psycopg2.extensions import connection as PGConnection
from pydantic import Field
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import libsql_experimental as libsql


class PostgresResource(ConfigurableResource):
    """
    PostgreSQL 데이터베이스와 상호작용하기 위한 Resource.

    예시:
        from dagster import Definitions, asset, EnvVar
        from postgres_wrapper import PostgresResource

        @asset
        def my_table(postgres: PostgresResource):
            with postgres.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM table;")

        defs = Definitions(
            assets=[my_table],
            resources={
                "postgres": PostgresResource(
                    host="localhost",
                    port=5432,
                    user="postgres",
                    password=EnvVar("POSTGRES_PASSWORD"),
                    database="mydb"
                )
            }
        )
    """

    host: Optional[str] = Field(
        description="PostgreSQL 서버의 호스트 이름이나 IP 주소. 제공하지 않으면 기본값으로 localhost가 사용됩니다.",
        default=None,
    )
    port: Optional[int] = Field(
        description="PostgreSQL 서버의 포트 번호. 기본값은 5432입니다.",
        default=5432,
    )
    user: str = Field(
        description="PostgreSQL 서버에 인증할 때 사용할 사용자 이름입니다.",
    )
    password: Optional[str] = Field(
        description="PostgreSQL 서버에 인증할 때 사용할 비밀번호입니다.",
        default=None,
    )
    database: Optional[str] = Field(
        description="연결할 PostgreSQL 데이터베이스 이름입니다.",
        default=None,
    )
    additional_parameters: dict[str, Any] = Field(
        description="psycopg2.connect()에 전달할 추가 파라미터들입니다.",
        default={},
    )

    @classmethod
    def _is_dagster_maintained(cls):
        return True

    def _drop_none_values(self, dictionary: dict) -> dict:
        return {key: value for key, value in dictionary.items() if value is not None}

    @contextmanager
    def get_connection(self) -> Generator[PGConnection, None, None]:
        """
        psycopg2를 사용하여 PostgreSQL 데이터베이스에 연결합니다.
        연결 시 backoff 기법을 사용하여 재시도하며, 연결 후에는 컨텍스트 매니저를 통해 연결 객체를 반환합니다.
        """
        connection = backoff(
            fn=psycopg2.connect,
            retry_on=(psycopg2.OperationalError,),
            kwargs=self._drop_none_values(
                {
                    "host": self.host,
                    "port": self.port,
                    "user": self.user,
                    "password": self.password,
                    "dbname": self.database,  # psycopg2는 데이터베이스 이름에 "dbname" 사용
                    **self.additional_parameters,
                }
            ),
            max_retries=3,
        )
        try:
            yield connection
        finally:
            connection.close()

    def get_sqlalchemy_engine(self) -> Engine:
        """
        SQLAlchemy 엔진을 반환합니다. pandas의 read_sql_query에서 사용할 수 있습니다.
        """
        connection_params = self._drop_none_values(
            {
                "host": self.host or "localhost",
                "port": self.port or 5432,
                "user": self.user,
                "password": self.password,
                "database": self.database,
            }
        )
        
        # PostgreSQL 연결 URL 생성
        if connection_params.get("password"):
            url = f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        else:
            url = f"postgresql://{connection_params['user']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        
        return create_engine(url)


class TursoResource(ConfigurableResource):
    """
    Turso 데이터베이스와 상호작용하기 위한 Resource.

    예시:
        from dagster import Definitions, asset, EnvVar
        from resources import TursoResource

        @asset
        def my_turso_table(turso: TursoResource):
            with turso.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT * FROM my_table;")
                return cur.fetchall()

        defs = Definitions(
            assets=[my_turso_table],
            resources={
                "turso": TursoResource(
                    url=EnvVar("TURSO_DATABASE_URL"),
                    auth_token=EnvVar("TURSO_AUTH_TOKEN")
                )
            }
        )
    """
    url: str = Field(
        description="Turso 데이터베이스 URL (예: TURSO_DATABASE_URL 환경 변수)",
    )
    auth_token: str = Field(
        description="Turso 인증 토큰 (예: TURSO_AUTH_TOKEN 환경 변수)",
    )


    @classmethod
    def _is_dagster_maintained(cls):
        return True

    @contextmanager
    def get_connection(self) -> Generator[libsql.Connection, None, None]:
        """
        libsql_experimental을 사용하여 Turso 데이터베이스에 연결합니다.
        연결 시 backoff 기법을 사용하여 재시도하며, 연결 후에는 컨텍스트 매니저를 통해 연결 객체를 반환합니다.
        """
        connection = backoff(
            fn=libsql.connect,
            retry_on=(ValueError,),  # Retry on LibsqlError
            kwargs={
                "database": "test.db",
                "sync_url": self.url, # libsql.connect uses db_url
                "auth_token": self.auth_token,
            },
            max_retries=3,
        )
        try:
            yield connection
        finally:
            connection.close()
