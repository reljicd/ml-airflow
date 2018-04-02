from sqlalchemy import MetaData
from sqlalchemy.engine.base import Engine


class BaseRepository(object):
    # All repository classes share this object, which is bind with engine only once
    metadata = MetaData()

    def __init__(self, engine: Engine):
        # Bind engine to metadata only if it is not bind already
        if self.metadata.bind is None:
            self.metadata.bind = engine
