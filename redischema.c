#include "redismodule.h"
#include <string.h>
#include <stdlib.h>
#include "jsmn.h"
#include "redischema.h"

int report_error(RedisModuleCtx *ctx, C_CHARS msg, PARSER_STATE *parser) {
  RedisModule_ReplyWithSimpleString(ctx, msg);
  return REDISMODULE_ERR;
}

/* walk a jason array
   the function allows an array or a single value
*/
int walk_array(RedisModuleCtx *ctx, jsmntok_t *root, PARSER_STATE *parser) {
  int steps = 0, resp = 0;
  int val_count = (root->type != JSMN_ARRAY)? 1 : root->size;
  parser->single_value = (root->type != JSMN_ARRAY);
  if(! parser->single_value)
    steps++;
  for (int i=0; i < val_count; ++i) {
    parser->val = root+steps;
    steps++;
    parser->stage = PARSER_VAL;
    resp = parser->handler(ctx, parser);
    if(resp < 0)
      return resp;
    parser->val_ord++;
  }
  return steps;
}

/* walk a json map walk_object
*/
int walk_object(RedisModuleCtx *ctx, jsmntok_t *root, PARSER_STATE *parser) {
  int key_count = root->size; //TODO: check are there more than key_count tokens
  if(root->type != JSMN_OBJECT) {
    parser->err_msg = ERR_MSG_INVALID_INPUT; return MODULE_ERROR;
  }
  int steps = 1; //skip the object opener '{'
  for (int i=0; i < key_count; ++i) {
    jsmntok_t *node = root+steps;
    if (node->type != JSMN_STRING) {
      parser->err_msg = ERR_MSG_INVALID_INPUT; return MODULE_ERROR;
    }
    parser->key = node; parser->val=NULL; parser->stage = PARSER_KEY;
    int resp = parser->handler(ctx, parser);
    if(resp < 0)
      return MODULE_ERROR;
    parser->val_ord = 0;
    steps++;
    node = root + steps;
    if (node->type != JSMN_ARRAY && node->type != JSMN_PRIMITIVE &&
      node->type != JSMN_STRING) {
      parser->err_msg = ERR_MSG_INVALID_INPUT; return MODULE_ERROR;
    }
    resp = walk_array(ctx, node, parser);
    if(resp < 0)
      return MODULE_ERROR;
    steps += resp;
    parser->key_ord++;
  }
  return steps;
}

/* walk a json from user input
*/
int json_walk(RedisModuleCtx *ctx, jsmntok_t *root, PARSER_STATE *parser) {
  parser->key_ord=0; parser->val_ord= 0;
  if (root->type != JSMN_OBJECT) {
    parser->err_msg = ERR_MSG_INVALID_INPUT;
    return MODULE_ERROR;
  }
  return walk_object(ctx, root, parser);
}

/* delete a key from redis
*/
int delete_key(RedisModuleCtx *ctx, C_CHARS key) {
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleKey *redis_key=RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_DeleteKey(redis_key);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, key_str);
  return rsp;
}

/* returned pointer must be freed
*/
char* concat_prefix(C_CHARS prefix, C_CHARS str) {
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
  C_CHARS reply_str = RedisModule_CallReplyStringPtr(reply, &len);
  char *ret = strndup(reply_str, len);
  return ret;
}

/* returned pointer must be freed
*/
char *get_reply_element_at(RedisModuleCallReply *reply, int index) {
  RedisModuleCallReply *elem = RedisModule_CallReplyArrayElement(reply,index);
  return get_string_from_reply(elem);
}

int get_zset_size(RedisModuleCtx *ctx, C_CHARS key) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZCOUNT_CMD,ZCOUNT_FMT,
    key,0,REDISMODULE_POSITIVE_INFINITE);
  int size = RedisModule_CallReplyInteger(reply);
  RedisModule_FreeCallReply(reply);
  return size;
}

/* returned pointer must be freed
*/
char* zset_get_element_by_index(RedisModuleCtx *ctx, C_CHARS key, int index) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANGE_CMD,ZRANGE_FMT,
    key,index,index);
  char *elem = get_reply_element_at(reply,0);
  RedisModule_FreeCallReply(reply);
  return elem;
}

//TODO: test for increment of float and string keys, check for errors
int increment_key(RedisModuleCtx *ctx, C_CHARS key) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,INCR_CMD,INCR_FMT,key);
  if(reply == NULL)
    return MODULE_ERROR;
  RedisModule_FreeCallReply(reply);
  return REDISMODULE_OK;
}

int zset_get_rank(RedisModuleCtx *ctx, C_CHARS key, C_CHARS member) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,ZRANK_CMD,ZRANK_FMT,
    key,member);
  if(reply == NULL || RedisModule_CallReplyType(reply)==REDISMODULE_REPLY_NULL)
    return MODULE_ERROR;
  int rank = RedisModule_CallReplyInteger(reply);
  RedisModule_FreeCallReply(reply);
  return rank;
}

int delete_key_with_prefix(RedisModuleCtx *ctx, C_CHARS prefix, C_CHARS key) {
  char *full_key = concat_prefix(prefix, key);
  int rsp = delete_key(ctx, full_key);
  free(full_key);
  return rsp;
}

int cleanup_schema(RedisModuleCtx *ctx, C_CHARS elem_loc, C_CHARS val_prefix) {
  int i=0;
  char *elem = zset_get_element_by_index(ctx, elem_loc,i++);
  while(elem != NULL) {
    delete_key_with_prefix(ctx, val_prefix, elem);
    free(elem);
    elem = zset_get_element_by_index(ctx, elem_loc,i++);
  }
  return delete_key(ctx,elem_loc);
}

int add_elm_to_zset(RedisModuleCtx *ctx, C_CHARS key, C_CHARS key_set,int ord) {
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleString *key_set_str = RM_CreateString(ctx, key_set);
  RedisModuleKey *redis_key_set = RedisModule_OpenKey(ctx,key_set_str,
    REDISMODULE_WRITE);
  int flag = REDISMODULE_ZADD_NX; //element must not exist
  int rsp = RedisModule_ZsetAdd(redis_key_set, ord, key_str, &flag);
  RedisModule_CloseKey(redis_key_set);
  RedisModule_FreeString(ctx, key_set_str);
  RedisModule_FreeString(ctx, key_str);
  return (flag == REDISMODULE_ZADD_NOP)? flag : rsp;
}

int add_schema_key(RedisModuleCtx *ctx, C_CHARS key, C_CHARS key_set, int ord) {
  return add_elm_to_zset(ctx, key, key_set, ord);
}

int add_schema_val(RedisModuleCtx *ctx, C_CHARS val, C_CHARS key,
  C_CHARS prefix, int ord) {
  char *full_key = concat_prefix(prefix, key);
  int rsp = add_elm_to_zset(ctx, val, full_key, ord);
  free(full_key);
  return rsp;
}

/* returned pointer must be freed
*/
char* token_to_string(jsmntok_t *token, C_CHARS input) {
  if(token == NULL)
    return NULL;
  return strndup(input + token->start, token->end - token->start);
}

int SchemaLoad_handler(RedisModuleCtx *ctx, PARSER_STATE *parser) {
  char *key = token_to_string(parser->key, parser->input);
  char *val = token_to_string(parser->val, parser->input);
  int ret = REDISMODULE_ERR;
  if(parser->stage == PARSER_KEY)
    ret = add_schema_key(ctx, key, SCHEMA_KEY_SET ,parser->key_ord);
  else if (parser->stage == PARSER_VAL)
      ret = add_schema_val(ctx, val, key, SCHEMA_KEY_PREFIX, parser->val_ord);
  free(val);
  free(key);
  return ret;
}

int check_key_update_parser(RedisModuleCtx *ctx, PARSER_STATE* parser) {
  char *key = token_to_string(parser->key, parser->input);
  int rank = zset_get_rank(ctx, SCHEMA_KEY_SET, key), ret = MODULE_ERROR;
  if(rank == MODULE_ERROR) {
    parser->err_msg = ERR_MSG_MEMBER_NOT_FOUND;
    ret = MODULE_ERROR;
  }
  else {
    parser->schema_key_ord = rank;
    ret = REDISMODULE_OK;
  }
  free(key);
  return ret;
}

int check_val_update_parser(RedisModuleCtx *ctx, PARSER_STATE* parser) {
  char *schema_key = token_to_string(parser->key, parser->input);
  char* full_key = concat_prefix(SCHEMA_KEY_PREFIX, schema_key);
  char* val = token_to_string(parser->val, parser->input);
  int rank = zset_get_rank(ctx, full_key, val);
  if(rank == MODULE_ERROR)
    parser->err_msg = ERR_MSG_MEMBER_NOT_FOUND;
  else if (parser->val_ord >= parser->query.val_sizes[parser->schema_key_ord])
    parser->err_msg = ERR_MSG_TOO_MANY_KEYS;
  else
    parser->query.key_set[parser->schema_key_ord][parser->val_ord] = val;
  //val is not freed - will be freed when parser is freed
  free(schema_key);
  free(full_key);
  return (parser->err_msg == NULL)? REDISMODULE_OK : MODULE_ERROR;
}

bool match_key_to_query(C_CHARS key, Query *query) {
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

int validate_key(PARSER_STATE *parser) {
  char *key = token_to_string(parser->key, parser->input);
  bool match = match_key_to_query(key, &parser->query);
  free(key);
  return (match)? REDISMODULE_OK : REDISMODULE_ERR;
}

int SchemaOperations_handler(RedisModuleCtx *ctx, PARSER_STATE *parser) {
  switch(parser->stage) {
    case PARSER_KEY:
      return check_key_update_parser(ctx, parser);
    case PARSER_VAL:
      return check_val_update_parser(ctx, parser);
    default:
      parser->err_msg = ERR_MSG_GENERAL_ERROR;
      return MODULE_ERROR;
  }
}

int schema_set_val(RedisModuleCtx *ctx, PARSER_STATE *parser) {
  char *key = token_to_string(parser->key, parser->input);
  char *val = token_to_string(parser->val, parser->input);
  RedisModuleString *key_str = RM_CreateString(ctx, key);
  RedisModuleString *val_str = RM_CreateString(ctx, val);
  RedisModuleKey *redis_key= RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_StringSet(redis_key, val_str);
  RedisModule_CloseKey(redis_key);
  RedisModule_FreeString(ctx, val_str);
  RedisModule_FreeString(ctx, key_str);
  free(val);
  free(key);
  return rsp;
}

int SchemaSet_handler(RedisModuleCtx *ctx, PARSER_STATE *parser) {
  switch(parser->stage) {
    case PARSER_KEY:
      return validate_key(parser);
    case PARSER_VAL:
      if(! parser->single_value)
        parser->err_msg = ERR_MSG_SINGLE_VALUE;
      return (parser->err_msg==NULL)? schema_set_val(ctx, parser): MODULE_ERROR;
    default:
      parser->err_msg = ERR_MSG_GENERAL_ERROR;
      return MODULE_ERROR;
  }
}

int parse_input(RedisModuleCtx *ctx, PARSER_STATE *parser) {
  jsmn_parser p; int r; jsmntok_t *tok;
	size_t tokcount = strlen(parser->input)/2;

	jsmn_init(&p);
	tok = malloc(sizeof(*tok) * tokcount);
	if (tok == NULL) {
		parser->err_msg = ERR_MSG_NOMEM;
		return MODULE_ERROR;
	}
  r = jsmn_parse(&p, parser->input, strlen(parser->input), tok, tokcount);
  if(r<0) {
    parser->err_msg = (r == JSMN_ERROR_NOMEM)?
    ERR_MSG_NOMEM: ERR_MSG_INVALID_INPUT;
    return MODULE_ERROR;
  }
  return json_walk(ctx, tok, parser);
}

//TODO: fix reply
int SchemaCleanCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  int resp = cleanup_schema(ctx, SCHEMA_KEY_SET, SCHEMA_KEY_PREFIX);
  RedisModule_ReplyWithSimpleString(ctx, OK_STR);
  return resp;
}

int SchemaLoadCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
        return RedisModule_WrongArity(ctx);
    }
    size_t len; int resp;
    cleanup_schema(ctx, SCHEMA_KEY_SET, SCHEMA_KEY_PREFIX);
    PARSER_STATE parser;
    parser.err_msg = NULL;
    parser.input = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
    parser.handler = SchemaLoad_handler;
    resp = parse_input(ctx, &parser);
    if(resp < 0)
      return report_error(ctx, parser.err_msg, &parser); // ERR: change message
    RedisModule_ReplyWithSimpleString(ctx, OK_STR);
    return REDISMODULE_OK;
}

float get_numeric_key(RedisModuleCtx *ctx, char const *key, OP_STATE* state) {
  RedisModuleCallReply *reply = RedisModule_Call(ctx,GET_CMD,GET_FMT,key);
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
  RedisModuleCallReply *reply=RedisModule_Call(ctx,KEYS_CMD,KEYS_FMT,ALL_KEYS);
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

int schemaOperationsCommand(RedisModuleCtx *ctx, RedisModuleString **argv,
  int argc, SCHEMA_OP op) {
  if(argc != SCHEMA_LOAD_ARGS_LIMIT) {
      return RedisModule_WrongArity(ctx);
  }
  size_t len; int resp = REDISMODULE_OK;
  PARSER_STATE parser;
  parser.err_msg = NULL;
  parser.input = RedisModule_StringPtrLen(argv[SCHEMA_LOAD_ARG_LIST], &len);
  bool fill = (op == S_OP_SET);
  parser.handler= (op==S_OP_SET)? SchemaSet_handler : SchemaOperations_handler;
  build_query(ctx, &parser.query, fill);
  resp = parse_input(ctx, &parser);
  if(resp<0)
    resp = report_error(ctx, parser.err_msg, &parser); // ERR: change message
  else if(op != S_OP_SET)
    filter_results_and_reply(ctx, &parser.query, op);
  else
    RedisModule_ReplyWithSimpleString(ctx, SCHEMA_SET_OK_STR);
  free_query(&parser.query);
  return resp;
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
