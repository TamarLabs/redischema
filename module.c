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
#define QUERY_LOCATION "module:query:order"
#define QUERY_ELEM_PREFIX "module:query:elements:"
#define OK_STR "OK"
#define ZRANGE_COMMAND "ZRANGE"
#define ZRANGE_FORMAT "cll"
#define END_OF_STR '\0' //TODO: maybe delete
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))
typedef enum { PARSER_INIT, PARSER_ELEM, PARSER_VAL, PARSER_DONE, PARSER_ERR } parser_status;
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
  RedisModuleCallReply *elem = RedisModule_CallReplyArrayElement(reply,0);
  if(elem == NULL) {
    RedisModule_FreeCallReply(reply);
    return NULL;
  }
  size_t len=0;
  const char *elem_str = RedisModule_CallReplyStringPtr(elem, &len);
  char *ret = malloc((len+1)*sizeof(char));
  memcpy(ret,elem_str,len);
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

int cleanup_schema(RedisModuleCtx *ctx, const char *elem_loc, const char *val_prefix) {
  int i=0;
  char *elem = zset_get_element_by_index(ctx, elem_loc,i++);
  while(elem != NULL) {
    delete_key_with_prefix(ctx, val_prefix, elem);
    free(elem);
    elem = zset_get_element_by_index(ctx, elem_loc,i++);
  }
  return delete_key(ctx,elem_loc);
}

int add_element_to_zset(RedisModuleCtx *ctx, const char *elem, const char *key, int ordinal) {
  RedisModuleString *elem_str = RM_CreateString(ctx, elem);
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleKey *redis_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int flag = REDISMODULE_ZADD_NX; //element must not exist
  int rsp = RedisModule_ZsetAdd(redis_key, ordinal, elem_str, &flag);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, key_str);
  RedisModule_FreeString(ctx, elem_str);
  return (flag == REDISMODULE_ZADD_NOP)? flag : rsp;
}

int insert_schema_element(RedisModuleCtx *ctx, const char *elem, const char* key, int ordinal){
  return add_element_to_zset(ctx, elem, key, ordinal);
}

int insert_schema_val_for_element(RedisModuleCtx *ctx, const char* val, const char* elem, const char* prefix, int ordinal){
  char *key_arr = malloc((strlen(prefix) + strlen(elem) + 1) * sizeof(char));
  strcpy(key_arr, prefix);
  strcat(key_arr, elem);
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
      return PARSER_ELEM;
    case PARSER_ELEM:
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
        return PARSER_ELEM;
      return PARSER_DONE;
    default:
      return PARSER_ERR;
  }
}

int load_schema_str_to_redis(RedisModuleCtx *ctx, char *pos, const char *elem_loc, const char *val_prefix){
  char *elem = NULL, *val = NULL;
  int resp=0, elem_ord = 0, val_ord = 0;
  cleanup_schema(ctx, elem_loc, val_prefix);
  /***********************************************************
  *************************************************************/
  resp++;
  parser_status status = PARSER_INIT;
  status = parse_next_token(&pos, PARSER_INIT, NULL);
  while(status == PARSER_ELEM) {
    status = parse_next_token(&pos, status, &elem);
    resp = insert_schema_element(ctx, elem, elem_loc,elem_ord++);
    val_ord = 0;
    while(status == PARSER_VAL) {
      status = parse_next_token(&pos, status, &val);
      resp = insert_schema_val_for_element(ctx, val, elem, val_prefix, val_ord++);
    }
  }
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return REDISMODULE_OK;
}

//TODO: fix reply
int SchemaCleanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int resp = cleanup_schema(ctx, SCHEMA_LOCATION, SCHEMA_ELEM_PREFIX);
  resp = cleanup_schema(ctx, QUERY_LOCATION, QUERY_ELEM_PREFIX);
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
    load_schema_str_to_redis(ctx, schema, SCHEMA_LOCATION, SCHEMA_ELEM_PREFIX);
    free(schema);
    return REDISMODULE_OK;
}

int SchemaGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    const char *schema_def = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    char *schema = strdup(schema_def);
    load_schema_str_to_redis(ctx, schema, QUERY_LOCATION, QUERY_ELEM_PREFIX);
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
    RMUtil_RegisterReadCmd(ctx, "SchemaClean",       SchemaCleanCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaGet",         SchemaGetCommand);

    return REDISMODULE_OK;
}
