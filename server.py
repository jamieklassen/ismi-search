import tornado.httpserver
import tornado.httpclient
import tornado.ioloop
import tornado.web
import tornado.escape
import tornado.template
from ismi_search import Results, Filter


class SearchHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        self.render('templates/results.html')


class MainHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.loader = tornado.template.Loader('templates')

    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        keywords = {
            'options':self.options(),
            'table_headers':self.table_headers()
        }
        self.render('templates/index.html',**keywords)

    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        pass


settings = {
    'static_path': os.path.join(os.path.dirname(__file__), 'static'),
    'autoescape' : None,
    'debug': True
}

application = tornado.web.Application([
    (r'/?', MainHandler),
    (r'/search/?', SearchHandler)
], **settings)

def main(port):
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 8888
    main(port)
