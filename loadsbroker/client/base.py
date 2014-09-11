
class BaseCommand(object):
    options = {}

    def __init__(self, session, root):
        self.session = session
        self.root = root

    def __call__(self):
        return self.session.get(self.root).json()
