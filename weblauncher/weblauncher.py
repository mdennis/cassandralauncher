#!/usr/bin/env python

import os
import uuid
import subprocess
import cherrypy
from datetime import datetime
from genshi.template import TemplateLoader

loader = TemplateLoader(
    os.path.join(os.path.dirname(__file__), 'genshi'),
    auto_reload=True
)

class Launcher(object):
    def __init__(self, result_directory):
        self.result_directory = result_directory
        
    def index(self):
        return loader.load('index.html').generate(title='weblauncher').render('html', doctype='html')

    def launch(self, c_nodes, h_nodes, s_nodes, ttl):
        req_uuid = str(uuid.uuid4())
        cherrypy.log('launching with c_nodes=%s, h_nodes=%s, s_nodes=%s' % (c_nodes, h_nodes, s_nodes))
        stdout = open(os.path.join(self.result_directory, '%s.stdout' % req_uuid), 'w')
        stderr = open(os.path.join(self.result_directory, '%s.stderr' % req_uuid), 'w')
        cmd = [
            'python',
            '-u',
            os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__))), '..', 'cassandralauncher', 'cassandralauncher.py'),
            '--analyticsnodes=%s' % h_nodes,
            '--searchnodes=%s' % s_nodes,
            '--totalnodes=%s' % (int(s_nodes) + int(h_nodes) + int(c_nodes)),
            '--clustername=temporary_cluster',
            '--demotime=%s' % ttl,
            '--noprompts',
            '--opscenterinterface=80',
            '--result_directory=%s' % self.result_directory,
            '--launch_id=%s' % req_uuid,
            '--handle=weblauncher'
        ]
        cherrypy.log('running %s' % ' '.join(cmd))
        #child = subprocess.Popen(['python', '-u', '/home/mdennis/mdev/cassandralauncher/weblauncher/test.py'], stdout=stdout, stderr=stderr, bufsize=0)
        child = subprocess.Popen(cmd, stderr=stderr, stdout=stdout, bufsize=0)
        raise cherrypy.HTTPRedirect("/ping/%s" % req_uuid)

    def ping(self, uuid):
        #obviously not secure
        result_path = os.path.join(self.result_directory, '%s.results' % uuid)
        with open(os.path.join(self.result_directory, '%s.stdout' % uuid), 'r') as f:
            stdout = f.readlines()[-10:]
        with open(os.path.join(self.result_directory, '%s.stderr' % uuid), 'r') as f:
            stderr = f.readlines()[-10:]
        if os.path.exists(result_path):
            with open(result_path, 'r') as f:
                results = dict(map(lambda l: l.strip().split('='), f))
                opsc_ip = results['opsc_ip']
                opsc_port = results['opsc_port']
                if _opsc_up(opsc_ip, opsc_port):
                    raise cherrypy.HTTPRedirect("http://%s:%s" % (opsc_ip, opsc_port))
                
        asof = datetime.now().ctime()
        return loader.load('ping.html').generate(title=uuid, stdout=stdout, stderr=stderr, asof=asof).render('html', doctype='html')

def _opsc_up(ip, port):
    import socket
    import httplib
    try:
        c = httplib.HTTPConnection(ip, port, timeout=3)
        c.request("HEAD","/opscenter/index.html")
        r = c.getresponse()
        return r.status == httplib.OK
    except socket.error, se:
        #cherrypy.log('error (%s, %s) connecting to opsc at %s on port %s' % (type(se), se.message, ip, port))
        return False

def init_logging():
    from logging import handlers

    log = cherrypy.log
    log.error_file = ""
    log.access_file = ""
    maxBytes = 10*1024*1024
    backupCount = 1

    h = handlers.RotatingFileHandler('/var/log/wl/error.log', 'a', maxBytes, backupCount)
    log.error_log.addHandler(h)

    h = handlers.RotatingFileHandler('/var/log/wl/access.log', 'a', maxBytes, backupCount)
    log.access_log.addHandler(h)

    cherrypy.config.update({
        'log.screen' : False,
    })

def load_users(password_file):
    with open(password_file) as f:
        return dict(map(lambda l: l.strip().split(':'), f))
    
def run(result_directory, password_file):
    if not os.path.exists(result_directory):
        os.mkdir(result_directory)

    launcher = Launcher(result_directory)
    
    d = cherrypy.dispatch.RoutesDispatcher()
    d.connect(name='root', route='/', controller=launcher, action='index')
    d.connect(name='launch', route='/launch', controller=launcher, action='launch')
    d.connect(name='ping', route='/ping/:uuid', controller=launcher, action='ping')
    
    conf = {
        '/' : {
            'request.dispatch' : d,
            'tools.staticdir.root' : os.getcwd(),
            'tools.encode.on': True,
            'tools.encode.encoding': 'utf-8',
            'tools.decode.on': True,
            'tools.trailing_slash.on': True,
            'tools.basic_auth.on' : True if password_file != None else False,
            'tools.basic_auth.realm' : 'riptano' if password_file != None else False,
            'tools.basic_auth.users' : load_users(password_file) if password_file != None else None
        },
        '/css' : {
            'tools.staticdir.on' : True,
            'tools.staticdir.dir' : os.path.join(os.path.dirname(__file__), 'css')
        },
        '/js' : {
            'tools.staticdir.on' : True,
            'tools.staticdir.dir' : os.path.join(os.path.dirname(__file__), 'js')
        },

    }

    init_logging()
    cherrypy.server.socket_host = '0.0.0.0'
    cherrypy.tree.mount(None, "/", config=conf)
    cherrypy.quickstart(None, config=conf)

def main():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-r', '--result_directory', dest='result_directory', help='direcotry to search for running clusters', metavar='RESULT_DIRECTORY', default='/tmp/wl')
    parser.add_option('-p', '--password_file', dest='password_file', help='list of users, one per line, in the form: username:md5(password)', metavar='PASSWORD_FILE', default=None)
    (options, args) = parser.parse_args()
    run(options.result_directory, options.password_file)    

if __name__ == '__main__':
    main()

