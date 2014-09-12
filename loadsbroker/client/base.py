
class BaseCommand(object):
    """Base Command Class"""

    arguments = {}

    def __init__(self, session, root):
        self.session = session
        self.root = root

    def __call__(self, args):
        return self.session.get(self.root).json()

    def args2options(self, args):
        options = {}
        for option in self.arguments:
            if option.startswith('--'):
                option = option[2:]

            normalized = option.replace('-', '_')

            if normalized in args:
                options[normalized] = getattr(args, normalized)

        return options
