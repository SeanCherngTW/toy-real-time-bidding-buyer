import argparse
from flask import Flask
from src.api.ad_api import ad_api
from src.config.application import app_context

HOST = app_context.server_config['host']
PORT = app_context.server_config['port']

app = Flask(__name__)
app.register_blueprint(ad_api)


def create_app(arg_host, arg_port, arg_debug):
    app.config['app_context'] = app_context
    arg_debug = False if arg_debug == 'False' else True
    app.run(host=arg_host, port=int(arg_port), debug=arg_debug, threaded=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', '-i', dest='host', default=str(HOST), help='bind host')
    parser.add_argument('--port', '-p', dest='port', default=str(PORT), help='bind port')
    parser.add_argument('--debug', '-d', dest='debug', default='False', help='bind debug')
    args = parser.parse_args()
    if args.debug.lower() not in ['true', 'false']:
        raise ValueError('debug should be True / False')

    create_app(args.host, args.port, args.debug)


if __name__ == '__main__':
    main()
