##Command Supported
proxyLayer.c takes the retrival and update commands from user to alter redis and mySQL simultaneously. Currently it serves following commands:
- get key
- set key value
- expire key ttl
	
##Flow of proxyLayer.c: 

```c
	if redis is up and query is RETRIEVAL, get from redis
	else if redis is down and query is RETRIVAL, get from mySQL
		if mySQL is down return null
	else if redis is up and query is UPDATE, update to redis and mySQL
		if mySQL is down return null
	else if redis down and query is UPDATE, do not serve the update query
```
##Ruuning proxyLayer.c

###Compiling: 
- gcc proxyLayer.c libhiredis.a `mysql_config --cflags --libs`
- hiredis.h, libhiredis.a, read.h, sds.h should be in to your current directory of proxyLayer.c
