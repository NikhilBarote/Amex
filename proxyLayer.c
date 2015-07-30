/* 
	Take commands from user
	Pass commands to redis
	Parse commands and form sql queries
	Pass commands to mySQL server

	if redis is down and query is RETRIEVAL, get from mySQL
		if mySQL is down return null
	else if redis is down and query is UPDATE, put query in mySQL queue
	else if redis is up and query is RETRIEVAL and value not found, get from mySQL and sync to redis
		if mySQL is down return null
	else redis up and query is UPDATE, update redis and put query in mySQL queue

	Contention Problem:
	- For mySQL queue we can use ActiveMQ, ironMQ
	- For own queue implementation we can use simple queue data structure backed up with file system and memory mapped file retrieval (for less IO overhead).
	- For faster retrival from queue we can increase mySQL threads, use batch or parallel query processing and for repeated queries mySQL cache can be enabled.
	- Executing whole mySQL queue as atomic transaction for consistency?
	- Distributed cluster of mySQL can solve contention problem significantly
	- Partition of mySQL data can also help to reduce contention

	Failure Recovery:
	- Redis up --> down : all commands passed to mySQL
	- Redis down --> up : on demand synchronization from mySQL if it is up
	- mySQL up --> down : all commands passed to local file system
	- mySQL down --> up : synchronize mySQL from saved commands in local file system
*/
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <my_global.h>
#include <mysql.h>
#include "hiredis.h"

void *isRedisUp();
void *isMySqlUp();
void *commandExec();

int REDIS_STATUS=0;
int SQL_STATUS=0;
redisContext *c;
MYSQL *con;

int main(){

    pthread_t thread1, thread2, thread3;

    pthread_create( &thread1, NULL, &isRedisUp, NULL);
    pthread_create( &thread2, NULL, &isMySqlUp, NULL);
    pthread_create( &thread3, NULL, &commandExec, NULL);

    pthread_join( thread1, NULL);
    pthread_join( thread2, NULL);
    pthread_join( thread3, NULL);

    exit(0);
}

void connectRedis(){
	c = redisConnect("127.0.0.1", 6379);
	if (c->err){
		REDIS_STATUS=0;
    }else{
    	REDIS_STATUS=1;
        //printf("Connection Made! \n");
    }
}

void *isRedisUp(){
	connectRedis();

    while(1){
    	redisReply *reply1 = redisCommand(c, "ping");
		if(reply1==NULL || reply1->type == REDIS_REPLY_ERROR){
			REDIS_STATUS=0;
			connectRedis();
			//printf("Redis down trying to reconnect\n");
		}
		else{
			REDIS_STATUS=1;
			//printf("Reply PONG!\n");
		}
		sleep(5);
    }
    return NULL;
}

void connectMySQL(){
	con = mysql_init(NULL);
	if (con == NULL){
		SQL_STATUS=0;
      	//fprintf(stderr, "%s\n", mysql_error(con));
      	//exit(1);
  	}

  	if (mysql_real_connect(con, "localhost", "root", "my_password", "testdb", 0, NULL, 0) == NULL){
      	//fprintf(stderr, "%s\n", mysql_error(con));
      	SQL_STATUS=0;
      	mysql_close(con);
      	//exit(1);
  	}
  	else{
  		SQL_STATUS=1;
  		//printf("Connected to mySQL\n");
  	}  
}

void *isMySqlUp(){
	connectMySQL();
	int status;
	while(1){
		status=mysql_ping(con);
		if(status!=0){
			SQL_STATUS=0;
			//printf("Down trying to reconnect\n");
			connectMySQL();
		}
		else{
			SQL_STATUS=1;
			//printf("SQL Running\n");
		}
		sleep(5);
	}
	return NULL;
}

void parseCommand(char * parser, int i, int j, char *command){
	while(command[i]==' ')
			i++;
	for(; i<strlen(command); i++){
		if(command[i]==' ')
			break;
		parser[j]=command[i];
		j++;
	}
}

int getFromSQL(char *command){
	int i=0,j=0,k=0;
	time_t seconds;
	//printf("%s\n",command);
	char* key = malloc(256);
	char* name = malloc(256);

	while(command[i]==' ')
		i++;
	for(; i<strlen(command); i++){
		if(command[i]==' ')
			break;
		name[k]=command[i];
		k++;
	}
	while(command[i]==' ')
		i++;
	for(; i<strlen(command); i++){
		if(command[i]==' ')
			break;
		key[j]=command[i];
		j++;
	}
	//printf("%s\n",name);
	//printf("%s\n",key);
	
	char statement[512];
	snprintf(statement, 512, "select val, ttl from redisStore where keyId =('%s')", key);
	if(mysql_query(con, statement)){
		//finish_with_error(con);
		printf("error SQL down\n");
		return 1;
	}

	MYSQL_RES *result = mysql_store_result(con);
  
  	if (result == NULL){
    	//finish_with_error(con);
    	printf("error SQL down\n");
    	return 1;
  	}

  	int num_fields = mysql_num_fields(result);
  	int rows = mysql_num_rows(result);
  	if(rows==0){
  		printf("Value not found!\n");
  		return 1;
  	}
  	//printf("%d\n", num_fields);
  	MYSQL_ROW row;
  	seconds=time(NULL);
  	printf("Getting From sql\n");
  	
  	while ((row = mysql_fetch_row(result))){
  		if(row[1]==NULL){
  			printf("%s\n",row[0]); 
  		} 
      	else{
      		if(seconds>atol(row[1]))
				printf("Entry Expired\n");
			else
				printf("%s\n",row[0]);
		}		
  	}
  
  	mysql_free_result(result);	
	return 0;
}

int getCommand(char * command){
	//printf("%s\n",command);
	if(REDIS_STATUS==1){
		//printf("%d\n",REDIS_STATUS);
		redisReply *reply2 = redisCommand(c, command);
		if(reply2==NULL || reply2->type == REDIS_REPLY_ERROR || reply2->str==NULL){
			if(SQL_STATUS==1)
				getFromSQL(command);
		}
		else{
			//printf("%d\n",REDIS_STATUS);
			//printf("Hello\n");
			printf("Getting from Redis\n");
			printf("%s\n",reply2->str);
		}
	}
	else if(SQL_STATUS==1){
		//printf("%d\n",REDIS_STATUS);
		getFromSQL(command);
	}
	else{
		printf("Both are down\n");
	}
	return 0;
}

int updateToSQL(char * command){
	int i=0,j=0,k=0,l=0;
	char* name = malloc(256);
	char* key = malloc(256);
	char* value = malloc(256);
	time_t seconds;

	while(command[i]==' ')
		i++;
	for(; i<strlen(command); i++){
		if(command[i]==' ')
			break;
		name[k]=command[i];
		k++;
	}
	while(command[i]==' ')
		i++;
	for(; i<strlen(command); i++){
		if(command[i]==' ')
			break;
		key[j]=command[i];
		j++;
	}
	while(command[i]==' ')
		i++;
	for(; i<strlen(command); i++){
		if(command[i]==' ')
			break;
		value[l]=command[i];
		l++;
	}
	//printf("%s %s %s\n",name,key,value);
	if(!strcasecmp(name,"expire")){
		//printf("Updating ttl sql\n");
		seconds=time(NULL)+atol(value);
  		char statement[512];
		snprintf(statement, 512, "update redisStore set ttl=('%ld') where keyId=('%s')",seconds,key);
		if(mysql_query(con, statement)){
			//finish_with_error(con);
			printf("error SQL down\n");
			return 1;
		}
  		
	}
	else if(!strcasecmp(name,"set")){
		char statement[512];
		snprintf(statement, 512, "insert into redisStore (keyId, val) values('%s','%s') on duplicate key update val=values(val)", key,value);
		if(mysql_query(con, statement)){
			//finish_with_error(con);
			printf("error SQL down\n");
			return 1;
		}
	}
	return 0;
}

int updateCommand(char *command){
	if(REDIS_STATUS==1){
		//printf("%d\n",REDIS_STATUS);
		redisReply *reply2 = redisCommand(c, command);
		if(reply2==NULL || reply2->type == REDIS_REPLY_ERROR /*|| reply2->str==NULL*/){
			printf("Only Retrival Queries are Served!\n");
		}
		else{
			//printf("%d\n",REDIS_STATUS);
			//printf("Hello\n");
			if(reply2->str!=NULL)
				printf("%s\n",reply2->str);
			updateToSQL(command);
		}
	}
	else{
		printf("Only Retrival Queries are Served!\n");	
	}
	
	return 0;
}

void *commandExec(){
	char *command = malloc(256);
	int replyStatus;
	if (command==NULL){
		printf("No memory allocated\n");
		return NULL;
	}

	while(1){
		//printf("%s",command);
		char *parser = malloc(256);
		char *get = "get";
		char *set = "set";
		int i=0,j=0;

		fgets(command,256,stdin);
		
		if(command!=NULL && command[strlen(command)-1]=='\n')
			command[strlen(command)-1]='\0';
		
		parseCommand(parser,i,j,command);
		//printf("%s\n",parser);

		if(!strcasecmp(parser,get))
			replyStatus=getCommand(command);
		else if(!strcasecmp(parser,set)|| !strcasecmp(parser, "expire"))
			replyStatus=updateCommand(command);	
	}
	return NULL;
}