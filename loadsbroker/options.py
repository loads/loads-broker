from collections import namedtuple

class HekaOptions(namedtuple("HekaOptions", "host port")):
    """Named tuple containing Heka options."""

class InfluxOptions(namedtuple("InfluxOptions", "host port user password")):
    """Named tuple containing InfluxDB options."""
