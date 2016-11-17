import sys
import orm, asyncio
from models import User, Blog, Comment


async def test(loop):
    await orm.create_pool(loop=loop, user='liuyongbo', password='lyb97427', db='webdata')
    u = User(name='test2', email='test2@test.com', passwd='test', image='about:blank')
    await u.save()

if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([test(loop)]))
    loop.close()
    if loop.is_closed():
        sys.exit(0)
