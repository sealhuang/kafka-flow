# vi: set ft=python sts=4 ts=4 sw=4 et:

from urllib.parse import quote_plus
import pymongo


def conn2db(dbconfig):
    """Connect to mongoDB."""
    if dbconfig.get('user')=='XXX':
        user = ''
        pwd = ''
    else:
        user = dbconfig.get('user')
        pwd = dbconfig.get('pwd')

    user_info = ':'.join([quote_plus(item) for item in [user, pwd] if item])
    if user_info:
        user_info = user_info + '@'
    url = 'mongodb://%s%s' % (user_info, dbconfig.get('url'))
    #print(url)
    dbclient = pymongo.MongoClient(url)

    # check if the client is valid
    try:
        dbclient.server_info()
        print('Connect to db successfully.')
        return 'ok', dbclient
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        return 'err', dbclient

