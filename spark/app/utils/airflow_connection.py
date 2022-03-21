from airflow.hooks.base_hook import BaseHook

def get_postgres_connection_credentials(postgres_conn_id: str):
    c = BaseHook.get_connection(postgres_conn_id)
    if not c.host or not c.schema:
        raise ValueError("Must specify a postgres host and schema")
    resolved_port = c.port or 5432
    return {
        "postgres_uri": "jdbc:postgresql://{h}:{p}/{d}".format(
            h=c.host,
            p=resolved_port,
            d=c.schema
        ),
        "postgres_user": c.login or "",
        "postgres_password": c.password or "",
        "postgres_host": c.host,
        "postgres_database": c.schema,
        "postgres_port": resolved_port
    }
