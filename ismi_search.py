import urllib, urllib2, json, itertools, os
from whoosh import fields, index, qparser, query, analysis


class Globals:
    BASE_DIR = os.path.dirname(__file__)
    DATA_DIR = os.path.join(BASE_DIR, 'data')

    @classmethod
    def populate_db(cls):
        for d in Definitions.get_defs():
            if 'ov' in d:
                filename = '{0}.json'.format(d['ov'].lower())
                if not filename in os.listdir(cls.DATA_DIR):
                    print 'populating {0}...'.format(d['ov'])
                    fp = open(os.path.join(cls.DATA_DIR, filename), 'w+')
                    json.dump(JsonInterface.method('get_ents', oc=d['ov']), fp)
                    fp.close()

    @classmethod
    def parse_filters(cls, request):
        filters = request.arguments.get('filter', [])
        for filt in filters:
            try:
                f = Filter(*json.loads(filt))
                yield f
            except:
                continue


class JsonInterface(object):
    JSON_INTERFACE = 'https://openmind-ismi-dev.mpiwg-berlin.mpg.de/om4-ismi/jsonInterface?'

    @classmethod
    def method(cls, method, **kwargs):
        kwargs.update(method=method)
        req = urllib2.Request(cls.JSON_INTERFACE + urllib.urlencode(kwargs))
        req.add_header('Accept', 'text/json')
        resp = urllib2.urlopen(req)
        return json.load(resp)


class Definitions(object):
    defs = {}

    @classmethod
    def all_atts(cls):
        return itertools.chain(*[d['atts'] for d in cls.get_defs()])

    @classmethod
    def get_defs(cls):
        if not cls.defs:
            cls.defs = JsonInterface.method('get_defs')['defs']
        return cls.defs


class Objects(object):
    INDEX_DIR = os.path.join(Globals.BASE_DIR, 'objects')
    INDEX = None
    SCHEMA = None

    TYPES = {
        u'escidoc-objid': fields.TEXT,
        u'old': fields.TEXT,
        u'text': fields.TEXT(analyzer=analysis.FancyAnalyzer(), stored=True, chars=True),
        u'num': fields.NUMERIC(stored=True),
        u'boolean': fields.BOOLEAN(stored=True),
        u'bool': fields.BOOLEAN(stored=True),
        u'date': fields.TEXT(stored=True),
        u'arabic': fields.TEXT(analyzer=analysis.FancyAnalyzer(), stored=True, chars=True),
        u'geoname-id': fields.TEXT
    }

    @classmethod
    def schema_fields(cls):
        sfields = set(
            (att['ov'], att['content_type'])
            for att in Definitions.all_atts()
        )
        sfields = {k: cls.TYPES[v] for k,v in sfields}
        sfields.update(ov=fields.TEXT(analyzer=analysis.FancyAnalyzer(), stored=True, chars=True),
                       nov=fields.TEXT(analyzer=analysis.FancyAnalyzer(), stored=True, chars=True),
                       oc=fields.ID,
                       id=fields.ID(stored=True, unique=True))
        return sfields

    @classmethod
    def get_schema(cls):
        if cls.SCHEMA is None:
            cls.SCHEMA = fields.Schema(**cls.schema_fields())
        return cls.SCHEMA

    @classmethod
    def get_index(cls):
        if cls.INDEX is None:
            if not os.path.exists(cls.INDEX_DIR):
                os.mkdir(cls.INDEX_DIR)
            if index.exists_in(cls.INDEX_DIR):
                cls.INDEX = index.open_dir(cls.INDEX_DIR)
            else:
                cls.INDEX = index.create_in(cls.INDEX_DIR, cls.get_schema())
                for json_file in filter(lambda s: ".json" in s, os.listdir(Globals.DATA_DIR)):
                    fp = open(os.path.join(Globals.DATA_DIR, json_file), 'r')
                    resp = json.load(fp)
                    fp.close()
                    for ent in resp['ents']:
                        cls.add_ent(ent)
        return cls.INDEX

    @classmethod
    def add_ent(cls, ent):
        writer = cls.get_index().writer()
        kwargs = {
            'ov': unicode(ent.get('ov', '')),
            'nov': unicode(ent.get('nov', '')),
            'oc': unicode(ent.get('oc', '')),
            'id': unicode(ent.get('id', ''))
        }
        if 'atts' in ent:
            kwargs.update({att['name'].lower(): unicode(att.get('ov', '')) for att in ent['atts']})
        writer.add_document(**kwargs)
        writer.commit()

    @classmethod
    def delete(cls, iden):
        writer = cls.get_index().writer()
        writer.delete_by_term('id', iden)
        writer.commit()

    @classmethod
    def sync_db(cls):
        for d in Definitions.get_defs():
            resp = JsonInterface.method('get_ents', oc=d['ov'], include_content='true')
            with cls.get_index().searcher() as s:
                for ent in resp['ents']:
                    stored_fields = s.document(id=unicode(ent['id']))
                    if stored_fields:
                        cls.delete(unicode(ent['id']))
                    cls.add_ent(ent)

    @classmethod
    def search(cls, filters, **kwargs):
        schema = cls.get_schema()
        def parse(filt):
            if filt.query_type == Filter.Q_APPROX:
                mp = qparser.MultifieldParser(filt.get_fields(), schema=schema)
                return mp.parse(unicode(filt.query_string))
            elif filt.query_type == Filter.Q_EXACT:
                s = cls.get_index().searcher()
                qs = filt.query_string
                f = lambda d: qs in [d.get(field) for field in filt.get_fields()]
                ids = [unicode(d['id']) for d in filter(f,s.documents())]
                return query.Or([query.Term('id', iden) for iden in ids])
        queries = [parse(filt) for filt in filters]
        q = query.And(queries)
        kwargs.update(limit=None, groupedby='oc')
        results = cls.get_index().searcher().search(q, **kwargs)
        return Results(results)


class Fields(object):
    INDEX_DIR = os.path.join(Globals.BASE_DIR, 'fields')
    INDEX = None
    SCHEMA = fields.Schema(name=fields.TEXT(analyzer=analysis.FancyAnalyzer(), stored=True, chars=True),
                           tags=fields.KEYWORD(scorable=True))

    @classmethod
    def get_index(cls):
        if cls.INDEX is None:
            if not os.path.exists(cls.INDEX_DIR):
                os.mkdir(cls.INDEX_DIR)
            if index.exists_in(cls.INDEX_DIR):
                cls.INDEX = index.open_dir(cls.INDEX_DIR)
            else:
                cls.INDEX = index.create_in(cls.INDEX_DIR, cls.SCHEMA)
                writer = cls.INDEX.writer()
                for att in Definitions.all_atts():
                    writer.add_document(name=unicode(att['ov']))
                writer.add_document(name=u'ov')
                writer.add_document(name=u'nov')
                writer.add_document(name=u'id')
                writer.commit()
        return cls.INDEX

    @classmethod
    def search(cls, query_string):
        qp = qparser.MultifieldParser(cls.SCHEMA.names(), schema=cls.SCHEMA)
        q = qp.parse(query_string)
        s = cls.get_index().searcher()
        results = s.search(q, limit=None)
        return [r['name'] for r in results]


class Filter(object):
    Q_APPROX = 0
    Q_EXACT = 1
    F_ANY = 0
    F_EXACT = 1
    F_APPROX = 2

    def __init__(self, query_string, query_type, fquery_string, fquery_type):
        super(Filter, self).__init__()
        cls = self.__class__
        self.query_string = query_string
        self.query_type = query_type
        self.fquery_string = fquery_string
        self.fquery_type = fquery_type

    def get_fields(self):
        cls = self.__class__
        if self.fquery_type == cls.F_ANY:
            return Objects.get_schema().names()
        elif self.fquery_type == cls.F_EXACT:
            return [self.fquery_string]
        elif self.fquery_type == cls.F_APPROX:
            return Fields.search(self.fquery_string)

    def __repr__(self):
        args = (self.query_string, self.query_type,
                self.fquery_string, self.fquery_type)
        args = map(repr, args)
        return 'Filter({0})'.format(", ".join(args))


class Results(object):
    def __init__(self, results=None):
        self._results = results
        if results is None:
            self._results = Objects.get_index().searcher().search(query.Every())

    def get_dict(self):
        s = Objects.get_index().searcher()
        items = self._results.groups().iteritems()
        d = {k: map(s.stored_fields, v) for k,v in items}
        d.update(num_results=len(self._results))
        return d

    def __repr__(self):
        s = ', '.join('{0}: {1}'.format(k, len(v)) for k,v in self._results.groups().items())
        return '<{0} Results {1}>'.format(len(self._results), s)