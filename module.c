#include "redismodule.h"
#include <string.h>
#include <stdlib.h>
#include "jsmn.h"

#define MODULE_NAME "redischema"

#define SCHEMA_LOAD_ARGS_LIMIT 2
#define SCHEMA_LOAD_ARG_LIST 1
#define SET_MEMBER_NOT_FOUND -1

#define SINGLE_VALUE_ERR -1
#define INVALID_INPUT "json input is invalid"
#define NOMEM_ERROR_MSG "not enough tokens provided"
#define VALUE_NOT_FOUND_ERROR_MSG " key or value not found in schema"
#define NO_KEYS_MATCHED "no keys matched the given filter"
#define SCHEMA_SET_OK_STR "schema values loaded"

#define REDIS_HIERARCHY_DELIM ":"
#define SCHEMA_KEY_SET "module:schema:order"
#define SCHEMA_KEY_PREFIX "module:schema:keys:"
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
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))
typedef enum { PARSER_INIT, PARSER_KEY, PARSER_VAL, PARSER_DONE, PARSER_ERR } PARSER_STAGE;
typedef enum { OP_INIT, OP_MID, OP_DONE, OP_ERR } OP_STAGE;
typedef enum { false, true } bool;
typedef enum { S_OP_SUM, S_OP_AVG, S_OP_MIN, S_OP_MAX, S_OP_CLR, S_OP_INC, S_OP_GET, S_OP_SET } SCHEMA_OP;
typedef char* schema_elem_t;
typedef struct PARSER_STATE {
  const char *input;
  jsmntok_t *key;
  int key_ord;
  jsmntok_t *val;
  int val_ord;
  int schema_key_ord; //this field is used in operation parser, remembers the current elem ord in schema
  bool single_value; //this field is used in parse_next_token, false if vale is in an array
} PARSER_STATE;
typedef struct  SCHEMALOAD {
  const char *key_set;
  const char *key_prefix;
} SCHEMALOAD;
typedef struct Query {
  schema_elem_t **key_set;
  size_t key_set_size;
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

int report_error(RedisModuleCtx *ctx, const char *msg, PARSER_STATE *parser, PARSE_PARAMS *params) {
  RedisModule_ReplyWithSimpleString(ctx, msg);
  return REDISMODULE_ERR;
}

/* walk a jason array
   the function allows an array or a single value
*/
int walk_array(RedisModuleCtx *ctx, jsmntok_t *root, PARSER_STATE *parser, PARSE_PARAMS *params, parser_handler handler) {
  int steps = 0, resp = 0;
  int val_count = (root->type != JSMN_ARRAY)? 1 : root->size;
  parser->single_value = (root->type != JSMN_ARRAY);
  if(! parser->single_value)
    steps++;
  for (int i=0; i < val_count; ++i) {
    parser->val = root+steps;
    steps++;
    resp = handler(ctx, parser, PARSER_VAL, params);
    if(resp < 0)
      return resp;
    parser->val_ord++;
  }
  return steps;
}

/* walk a json map walk_object
*/
int walk_object(RedisModuleCtx *ctx, jsmntok_t *root, PARSER_STATE *parser, PARSE_PARAMS *params, parser_handler handler) {
  int key_count = root->size; //TODO: add check that there are no more than key_count tokens
  int steps = 1; //root is the object opener '{' - skipping it
  for (int i=0; i < key_count; ++i) {
    jsmntok_t *node = root+steps;
    if (node->type != JSMN_STRING)
      return REDISMODULE_ERR; // ERR: INVALID_INPUT
    parser->key = node; parser->val=NULL;
    int resp = handler(ctx, parser, PARSER_KEY, params);
    if(resp < 0)
      return resp; // ERR: INVALID_INPUT
    parser->val_ord = 0;
    steps++;
    node = root + steps;
    if (node->type != JSMN_ARRAY && node->type != JSMN_PRIMITIVE && node->type != JSMN_STRING)
      return REDISMODULE_ERR; // ERR: INVALID_INPUT
    resp = walk_array(ctx, node, parser, params, handler);
    if(resp < 0)
      return resp; // ERR: VALUE_NOT_FOUND_ERROR_MSG
    steps += resp;
    parser->key_ord++;
  }
  return steps;
}

/* walk a json from user input
*/
int json_walk(RedisModuleCtx *ctx, jsmntok_t *root, PARSE_PARAMS *params, PARSER_STATE *parser, parser_handler handler) {
  parser->key_ord=0; parser->val_ord= 0;
  if (root->type != JSMN_OBJECT) {
    return REDISMODULE_ERR; // ERR: INVALID_INPUT
  }
  return walk_object(ctx, root, parser, params, handler);
}

/* delete a key from redis
*/
int delete_key(RedisModuleCtx *ctx, const char *key) {
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleKey *redis_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_DeleteKey(redis_key);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, key_str);
  return rsp;
}

/* returned pointer must be freed
*/
char* concat_prefix(const char* prefix, const char* str) {
  char *ret = malloc((strlen(prefix) + strlen(str) + 1) * sizeof(char));
  strcpy(ret, prefix);
  strcat(ret, str);
  return ret;
}

/* returned pointer must be freed
*/
char *get_string_from_reply(RedisModuleCallReply *reply) {
  if(reply == NULL)
    return NULL;
  size_t len=0;
  const char *reply_str = RedisModule_CallReplyStringPtr(reply, &len);
  char *ret = strndup(reply_str, len);
  return ret;
}

/* returned pointer must be freed
*/
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

/* returned pointer must be freed
*/
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
    return SET_MEMBER_NOT_FOUND;//TODO: change err msg
  RedisModule_FreeCallReply(reply);
  return REDISMODULE_OK;
}

int zset_get_rank(RedisModuleCtx *ctx, const char *key, const char *member) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANK_COMMAND,ZRANK_FORMAT,key,member);
  if(reply == NULL || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_NULL)
    return SET_MEMBER_NOT_FOUND;
  int rank = RedisModule_CallReplyInteger(reply);
  RedisModule_FreeCallReply(reply);
  return rank;
}

int delete_key_with_prefix(RedisModuleCtx *ctx, const char *prefix, const char *key) {
  char *full_key = concat_prefix(prefix, key);
  int rsp = delete_key(ctx, full_key);
  free(full_key);
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

int insert_schema_val_for_key(RedisModuleCtx *ctx, const char* val, const char* key, const char* prefix, int ordinal){
  char *full_key = concat_prefix(prefix, key);
  int rsp = add_element_to_zset(ctx, val, full_key, ordinal);
  free(full_key);
  return rsp;
}

/* returned pointer must be freed
*/
char* token_to_string(jsmntok_t *token, const char *input) {
  if(token == NULL)
    return NULL;
  return strndup(input + token->start, token->end - token->start);
}

int SchemaLoad_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  char *key = token_to_string(parser->key, parser->input);
  char *val = token_to_string(parser->val, parser->input);
  const char *key_set = params->load.key_set;
  const char *key_prefix = params->load.key_prefix;
  int ret = REDISMODULE_ERR;
  if(stage == PARSER_KEY)
    ret = insert_schema_key(ctx, key, key_set ,parser->key_ord);
  else if (stage == PARSER_VAL)
      ret = insert_schema_val_for_key(ctx, val, key, key_prefix, parser->val_ord);
  free(val);
  free(key);
  return ret;
}

int check_key_update_parser(RedisModuleCtx *ctx, PARSER_STATE* parser, PARSE_PARAMS* params) {
  char *key = token_to_string(parser->key, parser->input);
  int rank = zset_get_rank(ctx, SCHEMA_KEY_SET, key), ret = SET_MEMBER_NOT_FOUND;
  if(rank == SET_MEMBER_NOT_FOUND) {
    //char *msg = concat_prefix(key, VALUE_NOT_FOUND_ERROR_MSG);
    ret = SET_MEMBER_NOT_FOUND; // ERR: VALUE_NOT_FOUND_ERROR_MSG
  }
  else {
    parser->schema_key_ord = rank;
    ret = REDISMODULE_OK;
  }
  free(key);
  return ret;
}

int check_val_update_parser(RedisModuleCtx *ctx, PARSER_STATE* parser, PARSE_PARAMS* params) {
  char *schema_key = token_to_string(parser->key, parser->input);
  char* full_key = concat_prefix(SCHEMA_KEY_PREFIX, schema_key);
  char* val = token_to_string(parser->val, parser->input);
  int resp = REDISMODULE_ERR;
  int rank = zset_get_rank(ctx, full_key, val);
  if(rank == SET_MEMBER_NOT_FOUND)
    resp = SET_MEMBER_NOT_FOUND;
  else if (parser->val_ord >= params->query.val_sizes[parser->schema_key_ord])
    resp = SET_MEMBER_NOT_FOUND; // ERR: too many values in query
  else {
    params->query.key_set[parser->schema_key_ord][parser->val_ord] = val;
    resp = REDISMODULE_OK;
  }
  //val is not freed - will be freed when params are freed
  free(schema_key);
  free(full_key);
  return resp;
}

bool match_key_to_query(const char* key, Query *query) {
  char *key_dup = strdup(key);
  int k_ord = 0;
  char *token = strtok(key_dup,REDIS_HIERARCHY_DELIM);
  while (token != NULL && k_ord < query->key_set_size) {
    bool val_match = false;
    if(query->key_set[k_ord][0] == NULL) {
      val_match = true;
      goto advance_while; //No match required for this k_ord
    }
    for(int v_ord=0; v_ord < query->val_sizes[k_ord]; v_ord++) {
      if(query->key_set[k_ord][v_ord] == NULL)
        break;
      if(strcmp(query->key_set[k_ord][v_ord], token) == 0) {
        val_match = true;
        break;
      }
    }
    advance_while:
    token = strtok(NULL, REDIS_HIERARCHY_DELIM);
    k_ord++;
    if(val_match)
      continue;
    free(key_dup);
    return false;
  }
  free(key_dup);
  return true;
}

int validate_key(PARSER_STATE *parser, Query* query) {
  char *key = token_to_string(parser->key, parser->input);
  bool match = match_key_to_query(key, query);
  free(key);
  return (match)? REDISMODULE_OK : REDISMODULE_ERR;
}

int SchemaOperations_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  switch(stage) {
    case PARSER_KEY:
      return check_key_update_parser(ctx, parser, params);
    case PARSER_VAL:
      return check_val_update_parser(ctx, parser, params);
    default:
      return REDISMODULE_ERR; // ERR: INVALID_INPUT
  }
}

int schema_set_val(RedisModuleCtx *ctx, PARSER_STATE *parser) {
  char *key = token_to_string(parser->key, parser->input);
  char *val = token_to_string(parser->val, parser->input);
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleString *val_str = RM_CreateString(ctx, val);
  RedisModuleKey *redis_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_StringSet(redis_key, val_str);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, val_str);
  RedisModule_FreeString(ctx, key_str);
  free(val);
  free(key);
  return rsp;
}

int SchemaSet_handler(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSER_STAGE stage, PARSE_PARAMS *params) {
  switch(stage) {
    case PARSER_KEY:
      return validate_key(parser, &params->query);
    case PARSER_VAL:
      if(! parser->single_value)
        return SINGLE_VALUE_ERR; // ERR: not single value
      return schema_set_val(ctx, parser);
    default:
      return REDISMODULE_ERR; // ERR: INVALID_INPUT
  }
}

int parse_input(RedisModuleCtx *ctx, PARSER_STATE *parser, PARSE_PARAMS *params, parser_handler handler) {
  jsmn_parser p;
	jsmntok_t *tok;
	size_t tokcount = strlen(parser->input)/2;
  int r;

	/* Prepare parser */
	jsmn_init(&p);
	tok = malloc(sizeof(*tok) * tokcount);
	if (tok == NULL)
		return REDISMODULE_ERR; // ERR: NOMEM_ERROR_MSG
  r = jsmn_parse(&p, parser->input, strlen(parser->input), tok, tokcount);//TODO: check return value of parse
  if(r<0) {
    //const char *msg = (r == JSMN_ERROR_NOMEM)? NOMEM_ERROR_MSG: INVALID_INPUT;
    return REDISMODULE_ERR; // ERR: NOMEM_ERROR_MSG / INVALID_INPUT
  }
  return json_walk(ctx, tok, params, parser, handler);
}

//TODO: fix reply
int SchemaCleanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int resp = cleanup_schema(ctx, SCHEMA_KEY_SET, SCHEMA_KEY_PREFIX);
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return resp;
}

int SchemaLoadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len; int resp;
    cleanup_schema(ctx, SCHEMA_KEY_SET, SCHEMA_KEY_PREFIX);
    PARSE_PARAMS params; PARSER_STATE parser;
    params.load.key_set = SCHEMA_KEY_SET;
    params.load.key_prefix = SCHEMA_KEY_PREFIX;
    parser.input = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    resp = parse_input(ctx, &parser, &params, SchemaLoad_handler);
    if(resp < 0)
      return report_error(ctx, INVALID_INPUT, &parser, &params); // ERR: change error message
    RedisModule_ReplyWithSimpleString(ctx, OK_STR);
    return REDISMODULE_OK;
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
int schema_op_done(RedisModuleCtx *ctx, OP_STATE* state) {
  switch (state->op) {
    case S_OP_GET:
      RedisModule_ReplySetArrayLength(ctx,state->match_count);
      if(state->match_count == 0)
        RedisModule_ReplyWithSimpleString(ctx, NO_KEYS_MATCHED);
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
      schema_op_done(ctx, state);
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
    if(match_key_to_query(key, query)) {
      found_matched_key(ctx, key, &state);
    }
    free(key);
  }
  RedisModule_FreeCallReply(reply);
  state.stage = OP_DONE;
  found_matched_key(ctx, NULL, &state);
  return REDISMODULE_OK;
}

void build_query(RedisModuleCtx *ctx, Query *query, bool fill) {
  query->key_set_size = get_zset_size(ctx, SCHEMA_KEY_SET);
  query->key_set = malloc(sizeof(schema_elem_t*) * query->key_set_size);
  query->val_sizes = malloc(sizeof(size_t) * query->key_set_size);
  for(int i=0; i<query->key_set_size; ++i) {
    schema_elem_t key = zset_get_element_by_index(ctx, SCHEMA_KEY_SET, i);
    schema_elem_t full_key = concat_prefix(SCHEMA_KEY_PREFIX, key);
    query->val_sizes[i] = get_zset_size(ctx, full_key);
    query->key_set[i] = malloc(sizeof(char*) * query->val_sizes[i]);
    for (int j=0; j<query->val_sizes[i]; ++j)
      query->key_set[i][j]=fill?zset_get_element_by_index(ctx,full_key,j):NULL;
    free(full_key);
    free(key);
  }
}

void free_query(Query *query) {
  for(int i=0; i< query->key_set_size; ++i) {
    for(int j=0; j<query->val_sizes[i]; ++j) {
      if(query->key_set[i][j] != NULL)
        free(query->key_set[i][j]);
    }
    free(query->key_set[i]);
  }
  free(query->val_sizes);
  free(query->key_set);
}

int schemaOperationsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, SCHEMA_OP op) {
  if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
      return RedisModule_WrongArity(ctx);
  }
  size_t len; int resp;
  PARSE_PARAMS params; PARSER_STATE parser;
  parser.input = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
  bool fill = (op == S_OP_SET);
  parser_handler handler = (op == S_OP_SET)? SchemaSet_handler : SchemaOperations_handler;
  build_query(ctx, &params.query, fill);
  resp = parse_input(ctx, &parser, &params, handler);
  if(resp<0)
    return report_error(ctx, INVALID_INPUT, &parser, &params); // ERR: change error message
  if(op != S_OP_SET)
    filter_results_and_reply(ctx, &params.query, op);
  else
    RedisModule_ReplyWithSimpleString(ctx, SCHEMA_SET_OK_STR);
  return REDISMODULE_OK;
}

int SchemaSetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    return schemaOperationsCommand(ctx, argv, argc, S_OP_SET);
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
    RMUtil_RegisterReadCmd(ctx, "SchemaSet",         SchemaSetCommand);
    //schema operations
    RMUtil_RegisterReadCmd(ctx, "SchemaSUM",         SchemaSumCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaAVG",         SchemaAvgCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaMIN",         SchemaMinCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaMAX",         SchemaMaxCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaCLR",         SchemaClrCommand);
    RMUtil_RegisterReadCmd(ctx, "SchemaINC",         SchemaIncCommand);

    return REDISMODULE_OK;
}
