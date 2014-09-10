"""Loads Exceptions"""


class LoadsException(Exception):
    """Base Loads Exception class"""


class TimeoutException(LoadsException):
    """Raised when a timeout occurs"""
