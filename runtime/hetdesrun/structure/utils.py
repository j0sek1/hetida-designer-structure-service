from sqlalchemy.engine import Engine


def is_postgresql(engine: Engine) -> bool:
    return engine.dialect.name == "postgresql"


def is_sqlite(engine: Engine) -> bool:
    return engine.dialect.name == "sqlite"
