"""Basic ssh utility functions"""
import os
import stat
from collections import deque

from loadsbroker import logger


def makedirs(sftp, dirname, mode=511):
    """Creates a directory with the given dirname and mode on a remote server,
    including any intermediate-level directories."""

    if not dirname:
        raise OSError('Missing directory name')

    dirnames = deque([dirname])
    while True:
        dirname, basename = os.path.split(dirname)
        if not basename:
            dirname, basename = os.path.split(dirname)
        if not dirname or not basename:
            break
        dirnames.appendleft(dirname)

    for dirname in dirnames:
        try:
            attrs = sftp.stat(dirname)
        except OSError:
            logger.debug("Creating directory %s..." % dirname)
            sftp.mkdir(dirname, mode)
            continue

        if not stat.S_ISDIR(attrs.st_mode):
            raise OSError("%s exists and is not a directory" % dirname)
