import os

import tornado.httpserver
import tornado.httpclient
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.template

from ismi_search import Results, Filter


class MainHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.loader = tornado.template.Loader('templates')

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        req_filters = self.request.arguments.get('filter', [])
        filters = []
        for filt in req_filters:
            try:
                f = eval(filt)
                if isinstance(f, Filter):
                    filters.append(f)
            except:
                continue
        self.render('templates/index.html')

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        pass


settings = {
    'static_path': os.path.join(os.path.dirname(__file__), 'static'),
    'autoescape' : None,
    'debug': True
}

application = tornado.web.Application([
    (r'/?', MainHandler)
], **settings)

def main(port):
    application.filters = []
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 8888
    main(port)
