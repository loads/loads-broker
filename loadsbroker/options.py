"""Convenience objects for broker options"""
from typing import Any, Dict

from attr import asdict, attrib, attrs


@attrs
class HekaOptions:
    """Heka options"""

    host = attrib()  # type: str
    port = attrib()  # type: int
    secure = attrib()  # type: bool


@attrs
class InfluxDBOptions:
    """InfluxDB options"""

    host = attrib()  # type: str
    port = attrib()  # type: int
    username = attrib()  # type: str
    password = attrib()  # type: str
    database = attrib()  # type: str
    secure = attrib()  # type: bool

    @property
    def client_args(self) -> Dict[str, Any]:
        """args for InfluxDBClient"""
        args = asdict(self)
        if args.pop('secure'):
            args.update(ssl=True, verify_ssl=True)
        return args

    @property
    def database_url(self) -> str:
        return "{}://{self.host}:{self.port}/write?db={self.database}".format(
            "https" if self.secure else "http",
            self=self
        )
