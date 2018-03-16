import logging


class LoggingMixin(object):
    """ Convenience super-class to have a logger configured with the class name """

    @property
    def log(self):
        try:
            return self._log
        except AttributeError:
            self._log = logging.root.getChild(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._log
