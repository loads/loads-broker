from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
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

    def json(self):
        data = {}
        for key, val in self.__dict__.items():
            if key.startswith('_'):
                continue
            data[key] = val
        return data


run_table = Run.__table__


class Database(object):

    def __init__(self, uri, create=True, echo=False):
        self.engine = create_engine(uri)
        self.session = sessionmaker(bind=self.engine)

        # create tables
        if create:
            _Model.metadata.create_all(self.engine)
