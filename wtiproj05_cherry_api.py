import cherrypy


@cherrypy.expose
@cherrypy.tools.json_out()
class ratings(object):
    @cherrypy.tools.accept(media='application/json')
    def DELETE(self):
        return {}


@cherrypy.expose
@cherrypy.tools.json_out()
class rating(object):
    @cherrypy.tools.accept(media="application/json'")
    def POST(self):
        return

    def GET(self):
        return


@cherrypy.expose
@cherrypy.tools.json_out()
class avg_all(object):
    @cherrypy.tools.accept(media="application/json'")
    def GET(self):
        return


@cherrypy.expose
@cherrypy.tools.json_out()
class user_profile(object):
    @cherrypy.tools.accept(media="application/json'")
    def GET(self):
        return


if __name__ == '__main__':
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
            'tools.response_headers.on': True,
            'tools.response_headers.headers': [('Content-Type', 'application/json')],
        },
        'global': {
            'engine.autoreload.on': False
        }
    }

    cherrypy.config.update({'server.socket_port': 9898})
    cherrypy.tree.mount(ratings(), '/ratings', conf)
    cherrypy.tree.mount(rating(), "/rating", conf)
    cherrypy.tree.mount(avg_all(), "/avg-genre-ratings", conf)
    cherrypy.tree.mount(user_profile(), "/user-profile", conf)

    cherrypy.engine.start()
    cherrypy.engine.block()
