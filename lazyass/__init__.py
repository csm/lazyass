import requests
from urlparse import urljoin
from urllib import quote_plus
try:
    import simplejson as json
except:
    import json

def _quote_slash(s):
    """
    Quotes a URL part, leaving / unquoted.
    """
    '/'.join(map(quote_plus, s.split('/')))

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

    def __init__(self, url, username=None, password=None, session=None):
        """Create a new CouchDB client.

        :param url: the base URL of the CouchDB host.
        :param username: optional, the username to use for authenticating.
        :param password: optional, the password to use for authenticating.
        :param session: optional, to reuse an existing requests session.
        """
        self._url = url
        auth = None
        if username and password:
            auth = (username, password)
        if session is None:
            self._session = requests.session(auth=auth)
        else:
            self._session = session
        r = self._session.get(url)
        if r.status_code != 200:
            raise CouchDBError(r.status_code, r.text)
        self._info = r.json

    def __iter__(self):
        """Returns an iterable over all database names."""
        r = self._session.get(urljoin(self._url, "_all_dbs"))
        if r.status_code == 200:
            return iter(r.json)
        raise CouchDBError(r.status_code, r.text)

    def __getitem__(self, name):
        """
        Fetches the named database.
        """
        r = self._session.head(urljoin(self._url, name))
        if r.status_code == 200:
            return Database(self, name)
        raise CouchDBError(404, 'no such database')

    def __delitem__(self, name):
        r = self._session.delete(urljoin(self._url, name))
        if r.status_code != 200:
            raise CouchDBError(r.status_code, r.text)

    def delete(self, name):
        del self[name]

    def config(self):
        r = self._session.get(urljoin(self._url, "_config"))
        if r.status_code == 200:
            return r.json
        raise CouchDBError(r.status_code, r.text)

    def create(self, name):
        r = self._session.put(urljoin(self._url, name))
        if r.status_code == 201 or r.status_code == 412:
            return Database(self, name)
        raise CouchDBError(r.status_code, r.text)

    def session():
        doc = "The requests session object; this can be used to manipulate the underlying connections."
        def fget(self):
            return self._session
        def fset(self, value):
            self._session = value
        def fdel(self):
            del self._session
        return locals()

    session = property(**session())

class Database(object):
    """
    A CouchDB database.
    """
    def __init__(self, server, name):
        self._server = server
        self._name = name
        self._url = urljoin(server._url, name + "/")

    def info(self):
        r = self._server.session.get(self._url)
        if r.status_code == 200:
            return r.json
        raise CouchDBError(r.status_code, r.text)

    def __iter__(self):
        results = self.view('_all_docs')
        for row in results['rows']:
            yield row['id']        

    def __getitem__(self, docname):
        r = self._server.session.get(urljoin(self._url, quote_plus(docname)))
        if r.status_code == 200:
            return r.json
        raise CouchDBError(r.status_code, r.text)

    def getrev(self, docname):
        r = self._server.session.head(urljoin(self._url, quote_plus(docname)))
        if r.status_code == 200:
            return json.loads(r.headers['Etag'])
        raise CouchDBError(r.status_code, r.text)

    def __setitem__(self, id, doc):
        r = self._server.session.put(urljoin(self._url, quote_plus(id)), data=json.dumps(doc), headers={'content-type' : 'application/json'})
        if r.status_code != 200 and r.status_code != 201:
            raise CouchDBError(r.status_code, r.text)
        result = r.json
        doc.update({'_id' : id, '_rev' : result['rev']})

    def save(self, doc):
        if not '_id' in doc:
            r = self._server.session.post(self._url, data=json.dumps(doc), headers={'content-type' : 'application/json'})
        else:
            r = self._server.session.put(urljoin(self._url, quote_plus(doc['_id'])), data=json.dumps(doc), headers={'content-type' : 'application/json'})
        if r.status_code != 200 and r.status_code != 201:
            raise CouchDBError(r.status_code, r.text)
        result = r.json
        return (result['id'], result['rev'])

    def __delitem__(self, id):
        rev = self.getrev(id)
        self.delete(id, rev)

    def delete(self, id, rev):
        r = self._server.session.delete(urljoin(self._url, quote_plus(id)), params={'rev' : rev })
        if r.status_code == 200:
            result = r.json
            return result['rev']
        raise CouchDBError(r.status_code, r.text)

    def view(self, viewname, keys=None, **options):
        """
        Execute a named view.

        :param viewname: the view name of the form desgin_name/view_name, or the string _all_docs.
        :param keys: optional array of keys, if supplied, makes a bulk query.
        """
        if viewname == '_all_docs':
            url = urljoin(self._url, '_all_docs')
        else:
            url = urljoin(self._url, '_design/%s/_view/%s' % map(quote_plus, viewname.split('/', 1)))
        if keys == None:
            r = self._server.session.get(url, params=options)
        else:
            r = self._server.session.post(url, params=options, data=json.dumps({'keys':keys}), headers={'content-type': 'application/json'})
        if r.status_code == 200:
            return r.json
        raise CouchDBError(r.status_code, r.text)

    def _continuous(self, **options):
        url = urljoin(self._url, '_changes')
        r = self._server.session.get(url, params=options, prefetch=False)
        if r.status_code == 200:
            buf = b''
            while True:
                ch = r.raw.read(1)
                if ch == None or len(ch) == 0:
                    if len(buf) > 0:
                        yield json.loads(buf)
                    raise StopIteration()
                if ch == '\n':
                    if len(buf) > 0:
                        change = json.loads(buf)
                        buf = b''
                        yield change
                else:
                    buf += ch
        raise CouchDBError()

    def changes(self, **options):
        if 'feed' in options and options['feed'] == 'continuous':
            return self._continuous(**options)
        else:
            url = urljoin(self._url, '_changes')
            r = self._server.session.get(url, params=options)
            if r.status_code == 200:
                return r.json
            raise CouchDBError(r.status_code, r.text)

__all__ = [ "CouchDBError", "Server", "Database" ]