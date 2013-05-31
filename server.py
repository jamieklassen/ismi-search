import os

import tornado.httpserver
import tornado.httpclient
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.template

from ismi_search import Objects, Globals


class MainHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.loader = tornado.template.Loader('templates')

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        filts = Globals.parse_filters(self.request)
        filters = [[f.query_string, f.query_type, f.fquery_string, f.fquery_type] for f in filts]
        self.render('templates/index.html')


class ResultsHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        filters = Globals.parse_filters(self.request)
        results_dict = Objects.search(filters).get_dict()
        num_results = results_dict.pop('num_results')
        class Group:
            def __init__(self, name, rslts):
                self.name = name
                self.num_results = len(rslts)
                self.headers = list(set.union(
                    *(set(a.keys()) for a in rslts)
                ))
                class Item:
                    def __init__(self, d, headers):
                        self.fields = [
                            d.get(h, '') for h in headers
                        ]
                self.items = [Item(r, self.headers) for r in rslts]
        groups = enumerate(Group(*t) for t in results_dict.iteritems())
        self.render('templates/results.html', num_results=num_results, groups=groups)


def main(port):
    settings = {
        'static_path': os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static'),
        'autoescape' : None,
        'debug': True
    }

    application = tornado.web.Application([
        (r'/?', MainHandler),
        (r'/results/?', ResultsHandler)
    ], **settings)

    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 8888
    main(port)
