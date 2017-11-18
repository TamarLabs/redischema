#include "redismodule.h"
#include <string.h>
#include <stdlib.h>

#define MODULE_NAME "redischema"

#define SCHEMA_LOAD_ARGS_LIMIT 2
#define SCHEMA_LOAD_ARG_LIST 1
#define MODULE_ERROR_INVALID_INPUT -1

#define SCHEMA_START_DICT '{'
#define SCHEMA_END_DICT '}'
#define WILDCARD "*"
#define SCHEMA_START_ARR '['
#define SCHEMA_END_ARR ']'
#define SCHEMA_KV_DELIM ':'
#define SCHEMA_VAL_DELIM ','
#define QUOTE_CHAR "\""
#define SPACE_DELIMS " \t\r\n"
#define SCHEMA_LOCATION "module:schema:order"
#define SCHEMA_ELEM_PREFIX "module:schema:elements:"
#define OK_STR "OK"
#define ZRANGE_COMMAND "ZRANGE"
#define ZRANGE_FORMAT "cll"
#define END_OF_STR '\0' //TODO: maybe delete
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))
typedef enum { PARSER_INIT, PARSER_KEY, PARSER_VAL, PARSER_DONE, PARSER_ERR } parser_status;
typedef enum { false, true } bool; //TODO: maybe delete


#define RMUtil_RegisterReadCmd(ctx, cmd, f) \
    if (RedisModule_CreateCommand(ctx, cmd, f, "readonly fast allow-loading allow-stale", \
        1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;

int delete_key(RedisModuleCtx *ctx, const char *key) {
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleKey *redis_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_DeleteKey(redis_key);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, key_str);
  return rsp;
}

char* zset_get_element_by_index(RedisModuleCtx *ctx, const char *key, int index) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANGE_COMMAND,ZRANGE_FORMAT,key,index,index);
  RedisModuleCallReply *ele = RedisModule_CallReplyArrayElement(reply,0);
  if(ele == NULL) {
    RedisModule_FreeCallReply(reply);
    return NULL;
  }
  size_t len=0;
  const char *ele_str = RedisModule_CallReplyStringPtr(ele, &len);
  char *ret = malloc((len+1)*sizeof(char));
  memcpy(ret,ele_str,len);
  ret[len] = END_OF_STR;
  RedisModule_FreeCallReply(reply);
  return ret;
}

int delete_key_with_prefix(RedisModuleCtx *ctx, const char *prefix, const char *key) {
  char *key_arr = malloc((strlen(prefix) + strlen(key) + 1) * sizeof(char));
  strcpy(key_arr, prefix);
  strcat(key_arr, key);
  int rsp = delete_key(ctx, key_arr);
  free(key_arr);
  return rsp;
}

int cleanup_schema(RedisModuleCtx *ctx) {
  int i=0;
  char *key = zset_get_element_by_index(ctx, SCHEMA_LOCATION,i++);
  while(key != NULL) {
    delete_key_with_prefix(ctx, SCHEMA_ELEM_PREFIX, key);
    free(key);
    key = zset_get_element_by_index(ctx, SCHEMA_LOCATION,i++);
  }
  return delete_key(ctx,SCHEMA_LOCATION);
}

int add_element_to_zset(RedisModuleCtx *ctx, const char *ele, const char *key, int ordinal) {
  RedisModuleString *ele_str = RM_CreateString(ctx, ele);
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleKey *redis_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int flag = REDISMODULE_ZADD_NX; //element must not exist
  int rsp = RedisModule_ZsetAdd(redis_key, ordinal, ele_str, &flag);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, key_str);
  RedisModule_FreeString(ctx, ele_str);
  return (flag == REDISMODULE_ZADD_NOP)? flag : rsp;
}

int insert_schema_key(RedisModuleCtx *ctx, const char *key, int ordinal){
  return add_element_to_zset(ctx, key, SCHEMA_LOCATION, ordinal);
}

int insert_schema_val_for_key(RedisModuleCtx *ctx, const char* val, const char* key, int ordinal){
  char *key_arr = malloc((strlen(SCHEMA_ELEM_PREFIX) + strlen(key) + 1) * sizeof(char));
  strcpy(key_arr, SCHEMA_ELEM_PREFIX);
  strcat(key_arr, key);
  int rsp = add_element_to_zset(ctx, val, key_arr, ordinal);
  free(key_arr);
  return rsp;
}

char* skip_spaces(char *pos) {
    int len = sizeof(SPACE_DELIMS) / sizeof(SPACE_DELIMS[0]);
    do {
        bool is_space_found = false;
        for(int i=0; i<len; ++i) {
            if(*pos == SPACE_DELIMS[i]) {
                is_space_found = true;
            }
        }
        if(!is_space_found)
            return pos;
        pos++;
    } while(*pos != END_OF_STR);
    return pos;
}

char* validate_next_char(char* pos, char c) {
    pos = skip_spaces(pos);
    //pos = strtok(pos, SPACE_DELIMS);
    if(*pos != c)
        return NULL;
    ////return strtok(NULL, ""); //get rest of string
    //pos = strtok(NULL, ""); //get rest of string
    //printf("validate for %c remaining |%s|\n", c, pos++);
    //return pos;
    return skip_spaces(++pos);
}

parser_status parse_next_token(char **pos, parser_status status, char **token) {
  char *old_pos;
  switch (status) {
    case PARSER_INIT:
      *pos = validate_next_char(*pos, SCHEMA_START_DICT);
      if( *pos == NULL)
        return PARSER_ERR;
      return PARSER_KEY;
    case PARSER_KEY:
      *token = strtok(*pos, QUOTE_CHAR);
      *pos = *token + strlen(*token)+1; //skip key
      *pos = validate_next_char(*pos, SCHEMA_KV_DELIM);
      if( *pos == NULL)
        return PARSER_ERR;
      *pos = validate_next_char(*pos, SCHEMA_START_ARR);
      if(*pos == NULL)
        return PARSER_ERR;
      return PARSER_VAL;
    case PARSER_VAL:
      *token = strtok(*pos, QUOTE_CHAR);
      *pos = *token + strlen(*token)+1; //skip *token
      old_pos = *pos;
      *pos = validate_next_char(*pos, SCHEMA_VAL_DELIM);
      if(*pos != NULL)
        return PARSER_VAL;
      *pos = old_pos;
      *pos = validate_next_char(*pos, SCHEMA_END_ARR);
      if(*pos == NULL)
          return PARSER_ERR;
      *pos = validate_next_char(*pos, SCHEMA_VAL_DELIM);
      if(*pos != NULL)
        return PARSER_KEY;
      return PARSER_DONE;
    default:
      return PARSER_ERR;
  }
}

int load_schema_str_to_redis(RedisModuleCtx *ctx, char *pos){
  char *key = NULL, *val = NULL;
  int resp=0, key_ord = 0, val_ord = 0;
  cleanup_schema(ctx);
  /***********************************************************
  *************************************************************/
  resp++;
  parser_status status = PARSER_INIT;
  status = parse_next_token(&pos, PARSER_INIT, NULL);
  while(status == PARSER_KEY) {
    status = parse_next_token(&pos, status, &key);
    resp = insert_schema_key(ctx, key, key_ord++);
    val_ord = 0;
    while(status == PARSER_VAL) {
      status = parse_next_token(&pos, status, &val);
      resp = insert_schema_val_for_key(ctx, val, key, val_ord++);
    }
  }
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return REDISMODULE_OK;
}

//TODO: fix reply
int SchemaCleanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int resp = cleanup_schema(ctx);
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return resp;
}

int SchemaLoadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    const char *schema_def = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    char *schema = strdup(schema_def);
    load_schema_str_to_redis(ctx, schema);
    free(schema);
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx)
{
    // Register the module itself
    if (RedisModule_Init(ctx, MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // register Commands - using the shortened utility registration macro
    RMUtil_RegisterReadCmd(ctx, "SchemaLoad",        SchemaLoadCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaClean",        SchemaCleanCommand);

    return REDISMODULE_OK;
}
