from collections import namedtuple


class HekaOptions(namedtuple("HekaOptions", "host port secure")):
    """Named tuple containing Heka options."""


class InfluxOptions(namedtuple("InfluxOptions",
                               "host port user password secure")):
    """Named tuple containing InfluxDB options."""
