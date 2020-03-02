import logging; logging.basicConfig(level=logging.INFO)

import asyncio,os,json,time
from datetime import datetime
from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Awesome</h1>')

@asyncio.coroutine
async def init(loop):
    #创建web服务器实例app
    app = web.Application(loop=loop)
    app.router.add_route('GET','/',index)
    app_runner = web.AppRunner(app)
    #利用event_loop.create_server()创建TCP服务
    #srv = yield from loop.create_server(app.make_handler(),'127.0.0.1',9000)
    #srv = await event_loop.create_server(web.AppRunner(app),'127.0.0.1',9000)
    await app_runner.setup() 
    srv = await loop.create_server(app_runner.server, '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()