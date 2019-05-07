import cherrypy
import wtiproj05_api_logic
import json

@cherrypy.expose
@cherrypy.tools.json_out()
class ratings(object):
    @cherrypy.tools.accept(media='application/json')
    def DELETE(self):
        wtiproj05_api_logic.delete_ratings()
        return {}


@cherrypy.expose
@cherrypy.tools.json_out()
class rating(object):
    @cherrypy.tools.accept(media="application/json'")
    def POST(self):
        cl = cherrypy.request.headers['Content-Length']
        body = json.loads(cherrypy.request.body.read(int(cl)))
        wtiproj05_api_logic.add_rating(body)
        return body

    def GET(self, args):
        return wtiproj05_api_logic.get_user_ratings(args)


@cherrypy.expose
@cherrypy.tools.json_out()
class avg(object):
    @cherrypy.tools.accept(media="application/json'")
    def GET(self, args):
        if args == 'all-users':
            avg_for_all = wtiproj05_api_logic.get_avg_for_all()
            return avg_for_all
        else:
            avg_for_user = wtiproj05_api_logic.get_avg_for_user(args)
            return avg_for_user


@cherrypy.expose
@cherrypy.tools.json_out()
class user_profile(object):
    @cherrypy.tools.accept(media="application/json'")
    def GET(self, args):
        profile = wtiproj05_api_logic.get_user_profile(args)
        return profile


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
    cherrypy.tree.mount(ratings(), '/api/ratings', conf)
    cherrypy.tree.mount(rating(), "/api/rating", conf)
    cherrypy.tree.mount(avg(), "/api/avg-genre-ratings", conf)
    cherrypy.tree.mount(user_profile(), "/api/user-profile", conf)

    cherrypy.engine.start()
    cherrypy.engine.block()
