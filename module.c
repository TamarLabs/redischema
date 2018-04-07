#include "redismodule.h"
#include <string.h>
#include <stdlib.h>
#include "jsmn.h"

#define MODULE_NAME "redischema"

#define SCHEMA_LOAD_ARGS_LIMIT 2
#define SCHEMA_LOAD_ARG_LIST 1
#define MODULE_ERROR_INVALID_INPUT -1

#define REDIS_HIERARCHY_DELIM ":"
#define SCHEMA_KEY_SET "module:schema:order"
#define SCHEMA_SIZE "module:schema:size"
#define SCHEMA_KEY_PREFIX "module:schema:keys:"
#define QUERY_KEY_SET "module:query:order"
#define QUERY_KEY_PREFIX "module:query:keys:"
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
#define INCR_COMMAND "INCR"
#define INCR_FORMAT "c"
#define GET_COMMAND "GET"
#define GET_FORMAT "c"
#define END_OF_STR '\0'
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))
typedef enum { PARSER_INIT, PARSER_KEY, PARSER_VAL, PARSER_DONE, PARSER_ERR } PARSER_STAGE;
typedef enum { OP_INIT, OP_MID, OP_DONE, OP_ERR } OP_STAGE;
typedef enum { false, true } bool;
typedef enum { S_OP_SUM, S_OP_AVG, S_OP_MIN, S_OP_MAX, S_OP_CLR, S_OP_INC, S_OP_GET } SCHEMA_OP;
typedef char* schema_elem_t;
typedef struct PARSER_STATE {
  jsmntok_t *key;
  int key_ord;
  jsmntok_t *val;
  int val_ord;
  int schema_elem_ord; //this field is used in get parser, remembers the current elem ord in schema
  bool single_value; //this field is used in parse_next_token, false if vale is in an array
} PARSER_STATE;
typedef struct  SCHEMALOAD {
  const char *input;
  const char *key_set;
  const char *key_prefix;
} SCHEMALOAD;
typedef struct Query {
  const char *input;
  schema_elem_t **elems;
  size_t elems_size;
  size_t *val_sizes;
} Query;
typedef union PARSE_PARAMS {
		SCHEMALOAD load;
		Query query;
} PARSE_PARAMS;
typedef struct op_state {
  OP_STAGE stage;
  SCHEMA_OP op;
  size_t match_count;
  float aggregate;
} OP_STATE;
typedef int (*parser_handler)(RedisModuleCtx*, PARSER_STATE*, PARSER_STAGE, PARSE_PARAMS*);

#define RMUtil_RegisterReadCmd(ctx, cmd, f) \
    if (RedisModule_CreateCommand(ctx, cmd, f, "readonly fast allow-loading allow-stale", \
        1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;


int json_walk(RedisModuleCtx *ctx, jsmntok_t *t, PARSE_PARAMS *params, parser_handler handler) {
  int i, j, key_count, val_count;
  PARSER_STATE parser;
  parser.key_ord=0; parser.val_ord= 0;
  int resp=0; //TODO: delete this if not used

  if (t->type != JSMN_OBJECT) {
    return REDISMODULE_ERR;
  }
  key_count = t->size; //TODO: add check that there are no more than key_count tokens
  t++;
  for (i=0; i < key_count; ++i) {
    if (t->type != JSMN_STRING)
      return REDISMODULE_ERR; //TODO: handle error
    parser.key = t; parser.val=NULL;
    resp = handler(ctx, &parser, PARSER_KEY, params);
    parser.val_ord = 0;
    t++;
    if (t->type != JSMN_ARRAY && t->type != JSMN_PRIMITIVE) //TODO: maybe add string
      return REDISMODULE_ERR; //TODO handle error
    val_count = (t->type == JSMN_PRIMITIVE)? 1 : t->size;
    parser.single_value = (t->type == JSMN_PRIMITIVE);
    if(val_count > 0)
      t++;
    for (j=0; j < val_count; ++j) {
      parser.val = t;
      t++;
      resp = handler(ctx, &parser, PARSER_VAL, params);
      parser.val_ord++;
    }
    parser.key_ord++;
  }
  return resp;
}

int delete_key(RedisModuleCtx *ctx, const char *key) {
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleKey *redis_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_DeleteKey(redis_key);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, key_str);
  return rsp;
}

char* concat_prefix(const char* prefix, const char* str) {
  char *ret = malloc((strlen(prefix) + strlen(str) + 1) * sizeof(char));
  strcpy(ret, prefix);
  strcat(ret, str);
  return ret;
}

char* strcpy_len(const char *src, int size) {
  char *str = malloc(sizeof(char)*(size+1));
  memcpy(str, src, size);
  str[size] = END_OF_STR;
  return str;
}

char *get_string_from_reply(RedisModuleCallReply *reply) {
  if(reply == NULL)
    return NULL;
  size_t len=0;
  const char *reply_str = RedisModule_CallReplyStringPtr(reply, &len);
  char *ret = strcpy_len(reply_str, len);
  return ret;
}

char *get_reply_element_at(RedisModuleCallReply *reply, int index) {
  RedisModuleCallReply *elem = RedisModule_CallReplyArrayElement(reply,index);
  return get_string_from_reply(elem);
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

//TODO: test for increment of float and string keys, check for errors
int increment_key(RedisModuleCtx *ctx, const char *key) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,INCR_COMMAND,INCR_FORMAT,key);
  if(reply == NULL)
    return MODULE_ERROR_INVALID_INPUT;
  RedisModule_FreeCallReply(reply);
  return REDISMODULE_OK;
}

int zset_get_rank(RedisModuleCtx *ctx, const char *key, const char *member) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANK_COMMAND,ZRANK_FORMAT,key,member);
  if(reply == NULL)
    return MODULE_ERROR_INVALID_INPUT;
  int rank = RedisModule_CallReplyInteger(reply);
  RedisModule_FreeCallReply(reply);
  return rank;
}

int delete_key_with_prefix(RedisModuleCtx *ctx, const char *prefix, const char *key) {
  char *elem_key = concat_prefix(prefix, key);
  int rsp = delete_key(ctx, elem_key);
  free(elem_key);
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

int add_element_to_zset(RedisModuleCtx *ctx, const char *key, const char *key_set, int ordinal) {
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleString *key_set_str = RM_CreateString(ctx, key_set);
  RedisModuleKey *redis_key_set = RedisModule_OpenKey(ctx,key_set_str,REDISMODULE_WRITE);
  int flag = REDISMODULE_ZADD_NX; //element must not exist
  int rsp = RedisModule_ZsetAdd(redis_key_set, ordinal, key_str, &flag);
  RedisModule_CloseKey(redis_key_set);
  RedisModule_FreeString(ctx, key_set_str);
  RedisModule_FreeString(ctx, key_str);
  return (flag == REDISMODULE_ZADD_NOP)? flag : rsp;
}

int insert_schema_key(RedisModuleCtx *ctx, const char *key, const char* key_set, int ordinal){
  return add_element_to_zset(ctx, key, key_set, ordinal);
}

int insert_schema_val_for_element(RedisModuleCtx *ctx, const char* val, const char* elem, const char* prefix, int ordinal){
  char *elem_key = concat_prefix(prefix, elem);
  int rsp = add_element_to_zset(ctx, val, elem_key, ordinal);
  free(elem_key);
  return rsp;
}

char* token_to_string(jsmntok_t *token, const char *input) {
  if(token == NULL)
    return NULL;
  return strcpy_len(input + token->start, token->end - token->start);
}

int SchemaLoad_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  char *key = token_to_string(parser->key, params->load.input);
  char *val = token_to_string(parser->val, params->load.input);
  const char *key_set = params->load.key_set;
  const char *key_prefix = params->load.key_prefix;
  int ret = REDISMODULE_ERR;
  if(stage == PARSER_KEY)
    ret = insert_schema_key(ctx, key, key_set ,parser->key_ord);
  else if (stage == PARSER_VAL)
      ret = insert_schema_val_for_element(ctx, val, key, key_prefix, parser->val_ord);
  free(val);
  free(key);
  return ret;
}

int check_elem_update_parser(RedisModuleCtx *ctx, PARSER_STATE* parser, PARSE_PARAMS* params) {
  char *key = token_to_string(parser->key, params->query.input);
  int rank = zset_get_rank(ctx, SCHEMA_KEY_SET, key), ret = MODULE_ERROR_INVALID_INPUT;
  if(rank == MODULE_ERROR_INVALID_INPUT) {
    parser->schema_elem_ord = MODULE_ERROR_INVALID_INPUT;
    ret = MODULE_ERROR_INVALID_INPUT;
  } else {
    parser->schema_elem_ord = rank;
    ret = REDISMODULE_OK;
  }
  free(key);
  return ret;
}

int check_val_update_parser(RedisModuleCtx *ctx, PARSER_STATE* parser, PARSE_PARAMS* params) {
  char *schema_key = token_to_string(parser->key, params->query.input);
  char* full_key = concat_prefix(SCHEMA_KEY_PREFIX, schema_key);
  char* val = token_to_string(parser->val, params->query.input);
  int resp = REDISMODULE_ERR;
  int rank = zset_get_rank(ctx, full_key, val);
  if(rank == MODULE_ERROR_INVALID_INPUT)
    resp = MODULE_ERROR_INVALID_INPUT;
  else {
    params->query.elems[parser->schema_elem_ord][parser->val_ord] = val;
    resp = REDISMODULE_OK;
  }
  free(val);
  free(schema_key);
  free(full_key);
  return resp;
}

int SchemaGet_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  switch(stage) {
    case PARSER_KEY:
      return check_elem_update_parser(ctx, parser, params);
    case PARSER_VAL:
      return check_val_update_parser(ctx, parser, params);
    default:
      return MODULE_ERROR_INVALID_INPUT;
  }
}

int parse_input(RedisModuleCtx *ctx, const char *input, PARSE_PARAMS *params, parser_handler handler) {
  jsmn_parser p;
	jsmntok_t *tok;
	size_t tokcount = strlen(input)/2;
  int r;

	/* Prepare parser */
	jsmn_init(&p);

	/* Allocate some tokens as a start */
	tok = malloc(sizeof(*tok) * tokcount);
	if (tok == NULL) {
		return -1;//TODO: notify out of memory error
	}
  r = jsmn_parse(&p, input, strlen(input), tok, tokcount);//TODO: check return value of parse
  if(r<0)
    return JSMN_ERROR_NOMEM;
  json_walk(ctx, tok, params, handler);
  return REDISMODULE_OK;
}

/*int parse_input(RedisModuleCtx *ctx, char *pos, PARSE_PARAMS *params, parser_handler handler) {
  PARSER_STATE parser;
  PARSER_STAGE stage = PARSER_INIT;
  parser.elem_ord =0; parser.val_ord = 0;
  int resp=0;
  ///////////////////////////////////////////////////////
  resp++;

  stage = parse_next_token(&pos, &parser, PARSER_INIT, NULL);
  while(stage == PARSER_ELEM) {
    stage = parse_next_token(&pos, &parser, stage, &parser.elem);
    resp = handler(ctx, &parser, PARSER_ELEM, params);
    parser.val_ord = 0;
    while(stage == PARSER_VAL) {
      stage = parse_next_token(&pos, &parser, stage, &parser.val);
      resp = handler(ctx, &parser, PARSER_VAL, params);
      parser.val_ord++;
    }
    parser.elem_ord++;
  }
  return REDISMODULE_OK;
}*/

//TODO: fix reply
int SchemaCleanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int resp = cleanup_schema(ctx, SCHEMA_KEY_SET, SCHEMA_KEY_PREFIX);
  resp = cleanup_schema(ctx, QUERY_KEY_SET, QUERY_KEY_PREFIX);
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return resp;
}

int SchemaLoadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len;
    cleanup_schema(ctx, SCHEMA_KEY_SET, SCHEMA_KEY_PREFIX);
    PARSE_PARAMS params;
    params.load.key_set = SCHEMA_KEY_SET;
    params.load.key_prefix = SCHEMA_KEY_PREFIX;
    params.load.input = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    parse_input(ctx, params.load.input, &params, SchemaLoad_handler);
    RedisModule_ReplyWithSimpleString(ctx, OK_STR);
    return REDISMODULE_OK;
}

bool match_key_to_schema(const char* key, Query *query) {
  char *key_dup = strdup(key);
  int e_ord = 0;
  char *token = strtok(key_dup,REDIS_HIERARCHY_DELIM);
  while (token != NULL && e_ord < query->elems_size) {
    bool val_match = false, all_nulls = true;
    for(int v_ord=0; v_ord < query->val_sizes[e_ord]; v_ord++) {
      if(query->elems[e_ord][v_ord] == NULL)
        continue;
      if(all_nulls)
        all_nulls = false;
      if(strcmp(query->elems[e_ord][v_ord], token) == 0) {
        val_match = true;
        break;
      }
    }
    token = strtok(NULL, REDIS_HIERARCHY_DELIM);
    e_ord++;
    if(val_match)
      continue;
    if( !all_nulls) {
      free(key_dup);
      return false;
    }
  }
  free(key_dup);
  return true;
}

float get_numeric_key(RedisModuleCtx *ctx, char const *key, OP_STATE* state) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,GET_COMMAND,GET_FORMAT,key);
  if(reply == NULL) {
    state->stage = OP_ERR;
    return REDISMODULE_ERR;
  }
  //TODO: go over negative infinite and change
  float ret = atof(get_string_from_reply(reply));
  RedisModule_FreeCallReply(reply);
  return ret;
}

int schema_op_init(RedisModuleCtx *ctx, char *key, OP_STATE* state) {
  switch (state->op) {
    case S_OP_GET:
      RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
      break;
    case S_OP_AVG:
    case S_OP_SUM:
      state->aggregate = 0;
      break;
    case S_OP_MIN:
    case S_OP_MAX:
      state->aggregate = get_numeric_key(ctx, key, state);
      break;
    default:
      break;
  }
  state->stage = OP_MID;
  return REDISMODULE_OK;
}

int schema_op_mid(RedisModuleCtx *ctx, char *key, OP_STATE* state) {
  long result=0;
  switch (state->op) {
    case S_OP_GET:
      RedisModule_ReplyWithSimpleString(ctx, key);
      break;
    case S_OP_INC:
      increment_key(ctx, key);
      break;
    case S_OP_CLR:
      delete_key(ctx,key);
      break;
    case S_OP_AVG:
    case S_OP_SUM:
      result = get_numeric_key(ctx, key, state);
      state->aggregate += result;
      break;
    case S_OP_MIN:
      result = get_numeric_key(ctx, key, state);
      if(result < state->aggregate)
        state->aggregate = result;
      break;
    case S_OP_MAX:
      result = get_numeric_key(ctx, key, state);
      if(result > state->aggregate)
        state->aggregate = result;
      break;
    default:
      break;
  }
  state->match_count++;
  return REDISMODULE_OK;
}

//TODO: error handling + reply for all cases + reply during error
int schema_op_done(RedisModuleCtx *ctx, char *key, OP_STATE* state) {
  switch (state->op) {
    case S_OP_GET:
      RedisModule_ReplySetArrayLength(ctx,state->match_count);
      break;
    case S_OP_SUM:
    case S_OP_MIN:
    case S_OP_MAX:
      RedisModule_ReplyWithLongLong(ctx, state->aggregate);
      break;
    case S_OP_AVG:
      RedisModule_ReplyWithLongLong(ctx, state->aggregate/state->match_count);
      break;
    default:
      RedisModule_ReplyWithSimpleString(ctx, OK_STR);
      break;
  }
  return REDISMODULE_OK;
}

int found_matched_key(RedisModuleCtx *ctx, char *key, OP_STATE* state) {
  switch (state->stage) {
    case OP_INIT:
      schema_op_init(ctx, key, state);
    case OP_MID:
      schema_op_mid(ctx, key, state);
      break;
    case OP_DONE:
      schema_op_done(ctx, key, state);
      break;
    default:
      break;
  }
  return REDISMODULE_OK;
}

int filter_results_and_reply(RedisModuleCtx *ctx, Query *query, SCHEMA_OP op) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,KEYS_COMMAND,KEYS_FORMAT,ALL_KEYS);
  size_t keys_length = RedisModule_CallReplyLength(reply);
  OP_STATE state;
  state.op = op;
  state.stage = OP_INIT;
  state.match_count = 0;
  for(int i=0; i < keys_length; ++i) {
    char* key = get_reply_element_at(reply,i);
    if(match_key_to_schema(key, query)) {
      found_matched_key(ctx, key, &state);
    }
    free(key);
  }
  RedisModule_FreeCallReply(reply);
  state.stage = OP_DONE;
  found_matched_key(ctx, NULL, &state);
  return REDISMODULE_OK;
}

void build_query(RedisModuleCtx *ctx, Query *query) {
  query->elems_size = get_zset_size(ctx, SCHEMA_KEY_SET);
  query->elems = malloc(sizeof(schema_elem_t*) * query->elems_size);
  query->val_sizes = malloc(sizeof(size_t) * query->elems_size);
  for(int i=0; i<query->elems_size; ++i) {
    schema_elem_t elem = zset_get_element_by_index(ctx, SCHEMA_KEY_SET, i);
    schema_elem_t elem_key = concat_prefix(SCHEMA_KEY_PREFIX, elem);
    query->val_sizes[i] = get_zset_size(ctx, elem_key);
    query->elems[i] = malloc(sizeof(char*) * query->val_sizes[i]);
    for (int j=0; j<query->val_sizes[i]; ++j)
      query->elems[i][j] = NULL;
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

int schemaOperationsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SCHEMA_OP op) {
  if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
      return RedisModule_WrongArity(ctx);
  }
  size_t len;
  PARSE_PARAMS params;
  params.query.input = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
  build_query(ctx, &params.query);
  parse_input(ctx, params.query.input, &params, SchemaGet_handler);
  filter_results_and_reply(ctx, &params.query, op);
  return REDISMODULE_OK;
}

int SchemaGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_GET);
}

int SchemaSumCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_SUM);
}

int SchemaAvgCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_AVG);
}

int SchemaMinCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_MIN);
}

int SchemaMaxCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_MAX);
}

int SchemaClrCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_CLR);
}

int SchemaIncCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_INC);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    // Register the module itself
    if (RedisModule_Init(ctx, MODULE_NAME, 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // register Commands - using the shortened utility registration macro
    RMUtil_RegisterReadCmd(ctx, "SchemaLoad",        SchemaLoadCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaClean",       SchemaCleanCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaGet",         SchemaGetCommand);
    //schema operations
    RMUtil_RegisterReadCmd(ctx, "SchemaSUM",         SchemaSumCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaAVG",         SchemaAvgCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaMIN",         SchemaMinCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaMAX",         SchemaMaxCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaCLR",         SchemaClrCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaINC",         SchemaIncCommand);

    return REDISMODULE_OK;
}
