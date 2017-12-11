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
#define REDIS_HIERARCHY_DELIM ":"
#define SCHEMA_LOCATION "module:schema:order"
#define SCHEMA_SIZE "module:schema:size"
#define SCHEMA_ELEM_PREFIX "module:schema:elements:"
#define QUERY_LOCATION "module:query:order"
#define QUERY_ELEM_PREFIX "module:query:elements:"
#define OK_STR "OK"
#define ZRANGE_COMMAND "ZRANGE"
#define ZRANGE_FORMAT "cll"
#define ZRANK_COMMAND "ZRANK"
#define ZRANK_FORMAT "cc"
#define ZCOUNT_COMMAND "ZCOUNT"
#define ZCOUNT_FORMAT "cll"
#define KEYS_COMMAND "keys"
#define KEYS_FORMAT "c"
#define ALL_KEYS "*"
#define END_OF_STR '\0' //TODO: maybe delete
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))
//#define SCHEMALOAD_LOCATIONS { SCHEMA_LOCATION, SCHEMA_ELEM_PREFIX }
typedef enum { PARSER_INIT, PARSER_ELEM, PARSER_VAL, PARSER_DONE, PARSER_ERR } PARSER_STAGE;
typedef enum { false, true } bool; //TODO: maybe delete
typedef enum { ELEM_LOC, VAL_PREFIX } LOCATIONS_OPTS;
typedef char* schema_elem_t;
typedef struct  SCHEMALOAD_LOCATIONS {
  char *schema_loc;
  char *elem_prefix;
} SCHEMALOAD_LOCATIONS;
typedef struct PARSER_STATE {
  char *elem;
  int elem_ord;
  char *val;
  int val_ord;
} PARSER_STATE;
typedef struct Query {
  schema_elem_t **elems;
  size_t elems_size;
  size_t *val_sizes;
} Query;
typedef union parse_params {
		SCHEMALOAD_LOCATIONS locations;
		Query query;
} PARSE_PARAMS;
typedef int (*parser_handler)(RedisModuleCtx*, PARSER_STATE*, PARSER_STAGE, PARSE_PARAMS*);


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

char *get_reply_element_at(RedisModuleCallReply *reply, int index) {
  RedisModuleCallReply *elem = RedisModule_CallReplyArrayElement(reply,index);
  if(elem == NULL)
    return NULL;
  size_t len=0;
  const char *elem_str = RedisModule_CallReplyStringPtr(elem, &len);
  char *ret = malloc((len+1)*sizeof(char));
  memcpy(ret,elem_str,len);
  ret[len] = END_OF_STR;
  return ret;
}

int get_zset_size(RedisModuleCtx *ctx, const char *key) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZCOUNT_COMMAND,ZCOUNT_FORMAT,key,0,REDISMODULE_POSITIVE_INFINITE);
  int size = RedisModule_CallReplyInteger(reply);
  RedisModule_FreeCallReply(reply);
  return size;
}

char* zset_get_element_by_index(RedisModuleCtx *ctx, const char *key, int index) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANGE_COMMAND,ZRANGE_FORMAT,key,index,index);
  char *elem = get_reply_element_at(reply,0);
  RedisModule_FreeCallReply(reply);
  return elem;
}

int zset_get_rank(RedisModuleCtx *ctx, const char *key, const char *member) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANK_COMMAND,ZRANK_FORMAT,key,member);
  char *rank_p = get_reply_element_at(reply,0);
  RedisModule_FreeCallReply(reply);
  if(rank_p == NULL)
    return REDISMODULE_NEGATIVE_INFINITE;
  return atoi(rank_p);
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

PARSER_STAGE parse_next_token(char **pos, PARSER_STAGE stage, char **token) {
  char *old_pos;
  switch (stage) {
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

int SchemaLoad_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  char *elem = parser->elem;
  char *val = parser->val;
  const char *load_loc = params->locations.schema_loc;
  const char *val_prefix = params->locations.elem_prefix;
  switch(stage) {
    case PARSER_ELEM:
      return insert_schema_element(ctx, elem, load_loc ,parser->elem_ord);
    case PARSER_VAL:
      return insert_schema_val_for_element(ctx, val, elem, val_prefix, parser->val_ord);
    default:
      return REDISMODULE_ERR;
  }
}

int SchemaGet_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  switch(stage) {
    case PARSER_VAL:
      params->query.elems[parser->elem_ord][parser->val_ord] = parser->val;
      return REDISMODULE_OK;
    default:
      return REDISMODULE_ERR;
  }
}

int parse_input(RedisModuleCtx *ctx, char *pos, PARSE_PARAMS *params, parser_handler handler) {
  PARSER_STATE parser;
  PARSER_STAGE stage = PARSER_INIT;
  parser.elem_ord =0; parser.val_ord = 0;
  int resp=0;
  /***********************************************************
  *************************************************************/
  resp++;

  stage = parse_next_token(&pos, PARSER_INIT, NULL);
  while(stage == PARSER_ELEM) {
    stage = parse_next_token(&pos, stage, &parser.elem);
    resp = handler(ctx, &parser, PARSER_ELEM, params);
    parser.val_ord = 0;
    while(stage == PARSER_VAL) {
      stage = parse_next_token(&pos, stage, &parser.val);
      resp = handler(ctx, &parser, PARSER_VAL, params);
      parser.val_ord++;
    }
    parser.elem_ord++;
  }
  return REDISMODULE_OK;
}

//TODO: fix reply
int SchemaCleanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int resp = cleanup_schema(ctx, SCHEMA_LOCATION, SCHEMA_ELEM_PREFIX);
  resp = cleanup_schema(ctx, QUERY_LOCATION, QUERY_ELEM_PREFIX);
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return resp;
}

int SchemaLoadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    const char *schema_def = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    char *schema = strdup(schema_def);
    cleanup_schema(ctx, SCHEMA_LOCATION, SCHEMA_ELEM_PREFIX);
    PARSE_PARAMS params;
    params.locations.schema_loc = SCHEMA_LOCATION;
    params.locations.elem_prefix = SCHEMA_ELEM_PREFIX;
    parse_input(ctx, schema, &params, SchemaLoad_handler);
    free(schema);
    RedisModule_ReplyWithSimpleString(ctx, OK_STR);
    return REDISMODULE_OK;
}

bool match_key_to_schema(char* key, Query *query) {
  ////////////////////////////////char *pos = strdup(key);
  /*char *token = strtok(key,REDIS_HIERARCHY_DELIM);
  int elem_ord = 0;
  while (token != NULL && elem_ord < query->elems_size) {
    if(query->elems[elem_ord] == NULL) {
      token = strtok (NULL, REDIS_HIERARCHY_DELIM);
      elem_ord++;
      continue;
    }
    int vals_len = query->val_sizes[elem_ord];
    bool match_val = false;
    for(int val_ord=0; val_ord < vals_len; ++val_ord) {
      elem = query->elems[elem_ord][val_ord];
      if(elem == NULL)
        continue;
      if(strcmp(elem, query.elems[elem_ord][val_ord]) == 0)
        match_val = true;
    }
    if(! match_val)
      return false;
    token = strtok (NULL, REDIS_HIERARCHY_DELIM);
    elem_ord++;
  }
  return token == NULL && elem_ord = query->elem_size;*/
  return true;
}

int filter_query_results(RedisModuleCtx *ctx, Query *query) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,KEYS_COMMAND,KEYS_FORMAT,ALL_KEYS);
  size_t keys_length = RedisModule_CallReplyLength(reply);
  for(int i=0; i < keys_length; ++i) {
    char* key = get_reply_element_at(reply,i);
    if(match_key_to_schema(key, query))
      /*aaaaaaaaaa*/RedisModule_ReplyWithSimpleString(ctx, key);
    free(key);
  }
  RedisModule_FreeCallReply(reply);
  return keys_length;//////////////////////////////////TODO:change
}

/*
typedef struct Query {
  schema_elem_t **elems;
  size_t elems_size;
  size_t *val_sizes;
} Query;
*/

char* concat_prefix(const char* prefix, const char* str) {
  char *ret = malloc((strlen(prefix) + strlen(str) + 1) * sizeof(char));
  strcpy(ret, prefix);
  strcat(ret, str);
  return ret;
}

void build_query(RedisModuleCtx *ctx, Query *query) {
  query->elems_size = get_zset_size(ctx, SCHEMA_LOCATION);
  query->elems = malloc(sizeof(schema_elem_t*) * query->elems_size);
  query->val_sizes = malloc(sizeof(size_t) * query->elems_size);
  for(int i=0; i<query->elems_size; ++i) {
    schema_elem_t elem = zset_get_element_by_index(ctx, SCHEMA_LOCATION, i);
    schema_elem_t elem_key = concat_prefix(SCHEMA_ELEM_PREFIX, elem);
    query->val_sizes[i] = get_zset_size(ctx, elem_key);
    query->elems[i] = malloc(sizeof(char*) * query->val_sizes[i]);
    ///////////////for (int j=0; j<query->val_sizes[i]; ++j)
      ////////////////query->elems[i][j] = NULL;
    free(elem_key);
    free(elem);
  }
}

void free_query(Query *query) {
  for(int i=0; i< query->elems_size; ++i) {
    for(int j=0; j<query->val_sizes[i]; ++j) {
      if(query->elems[i][j] != NULL)
        free(query->elems[i][j]);
    }
    free(query->elems[i]);
  }
  free(query->val_sizes);
  free(query->elems);
}

int SchemaGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    const char *schema_def = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    char *schema = strdup(schema_def);
    PARSE_PARAMS params;
    build_query(ctx, &params.query);
    parse_input(ctx, schema, &params, SchemaGet_handler);
    ///////TODO: delete this
        //RedisModule_Log(ctx, "warning", "after build %s %s %s %s", params.query.elems[0][0],params.query.elems[0][1],params.query.elems[1][0],params.query.elems[1][1]);/////////////////
    ///////TODO: delete this
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    int rep_size = filter_query_results(ctx, &params.query);
    RedisModule_ReplySetArrayLength(ctx,rep_size);
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
