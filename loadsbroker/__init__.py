import logging

logger = logging.getLogger('loads')
__version__ = '0.1'

# silly patch for AWS because https://github.com/boto/boto/issues/2617
try:
    from boto.pyami.config import Config, ConfigParser

    def get(self, section, name, default=None, **kw):
        try:
            val = ConfigParser.get(self, section, name, **kw)
        except:
            val = default
        return val

    Config.get = get

except ImportError:
    pass
