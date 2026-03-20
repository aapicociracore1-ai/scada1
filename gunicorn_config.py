"""
Gunicorn config for Flask-SocketIO on Render.com
gevent worker is required for WebSocket support.
"""
import os

worker_class = "geventwebsocket.gunicorn.workers.GeventWebSocketWorker"
workers      = 1          # SocketIO requires exactly 1 worker (no sticky sessions on free tier)
bind         = "0.0.0.0:" + str(os.environ.get("PORT", 5000))
timeout      = 120
keepalive    = 5
loglevel     = "info"
accesslog    = "-"
errorlog     = "-"