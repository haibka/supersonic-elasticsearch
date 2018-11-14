import redis
r = redis.StrictRedis(host='supersonic.rexvi8.0001.apne1.cache.amazonaws.com', port=6379, db=0)
example_data_to_cache = {
    "friends": [
      {
	    "id": 0,
	    "name": "Pierce Mccall"
      },
      {
	    "id": 1,
	    "name": "Dana Sweeney"
      },
      {
	    "id": 2,
	    "name": "Cheryl Fischer"
      }
    ],
    "greeting": "Hello, WhileTrue! You have 7 unread messages.",
    "favoriteFruit": "banana"
}
r.set('myKey',example_data_to_cache)
