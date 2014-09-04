from uuid import uuid4
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import Insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String

_Model = declarative_base()


INITIALIZING = 0
RUNNING = 1
TERMINATED = 2


class Run(_Model):
    __tablename__ = 'run'
    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String)
    state = Column(Integer)
    ami = Column(String)
    nodes = Column(Integer)

    def __init__(self, *args, **kw):
        if 'uuid' not in kw:
            kw['uuid'] = str(uuid4())
        if 'state' not in kw:
            kw['state'] = INITIALIZING
        super(Run, self).__init__(*args, **kw)


run_table = Run.__table__



class Database(object):

    def __init__(self, uri, create=True, echo=False):
        self.engine = create_engine(uri)
        self.session_factory = sessionmaker(
            bind=self.engine, autocommit=False, autoflush=False)

        # create tables
        if create:
            with self.engine.connect() as conn:
                trans = conn.begin()
                _Model.metadata.create_all(self.engine)
                trans.commit()

    @contextmanager
    def session(self):
        try:
            _session = self.session_factory()
            yield _session
        except Exception:
            _session.rollback()
            raise
        finally:
            _session.close()
