import requests
import urlparse

class CouchDBError(Exception):
    def __init__(self, status_code, body=''):
        self.status_code = status_code
        self.body = body

    def __str__(self):
        return 'CouchDBError %d %r' % (self.status_code, self.body)

class Server(object):
    """
    A CouchDB server.
    """

    def __init__(self, url, username=None, password=None):
        self._url = url
        self._auth = None
        if username and password:
            self._auth = (username, password)
        r = requests.get(url, self._auth)
        if r.status_code != 200:
            raise CouchDBError(r.status_code, r.text)
        self._info = r.json

    def __getitem__(self, name):
        r = requests.head(urlparse.urljoin(self._url, name), auth=self._auth)
        if r.status_code == 200:
            return Database(self, name)
        raise CouchDBError(404, 'no such database')

    

class Database(object):
    def __init__(self, server, name):
        self._server = server