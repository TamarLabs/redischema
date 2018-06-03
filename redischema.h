#ifndef REDISCHEMA_H
#define REDISCHEMA_H

#define MODULE_NAME "redischema"

#define SCHEMA_LOAD_ARGS_LIMIT 2
#define SCHEMA_LOAD_ARG_LIST 1

#define REDIS_HIERARCHY_DELIM ":"
#define SCHEMA_KEY_SET "module:schema:order"
#define SCHEMA_KEY_PREFIX "module:schema:keys:"
#define OK_STR "OK"
#define ZRANGE_CMD "ZRANGE"
#define ZRANGE_FMT "cll"
#define ZRANK_CMD "ZRANK"
#define ZRANK_FMT "cc"
#define ZCOUNT_CMD "ZCOUNT"
#define ZCOUNT_FMT "cll"
#define KEYS_CMD "keys"
#define KEYS_FMT "c"
#define ALL_KEYS "*"
#define INCR_CMD "INCR"
#define INCR_FMT "c"
#define GET_CMD "GET"
#define GET_FMT "c"
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))

#define MODULE_ERROR -1
#define ERR_MSG_NO_MEM "parse error - insufficient memory"
#define ERR_MSG_TOO_MANY_KEYS "the query has too many keys"
#define ERR_MSG_SINGLE_VALUE "key is expected to have a single value"
#define ERR_MSG_GENERAL_ERROR "a general error has occured"
#define ERR_MSG_INVALID_INPUT "json input is invalid"
#define ERR_MSG_NOMEM "not enough tokens provided"
#define ERR_MSG_MEMBER_NOT_FOUND "key or value not found in schema"
#define NO_KEYS_MATCHED "no keys matched the given filter"
#define SCHEMA_SET_OK_STR "schema values loaded"

typedef enum { PARSER_INIT, PARSER_KEY, PARSER_VAL, PARSER_DONE, PARSER_ERR } PARSER_STAGE;
typedef enum { OP_INIT, OP_MID, OP_DONE, OP_ERR } OP_STAGE;
typedef enum { false, true } bool;
typedef enum { S_OP_SUM, S_OP_AVG, S_OP_MIN, S_OP_MAX, S_OP_CLR, S_OP_INC, S_OP_GET, S_OP_SET } SCHEMA_OP;
typedef struct PARSER_STATE PARSER_STATE; //forward declaration
typedef int (*parser_handler)(RedisModuleCtx*, PARSER_STATE*);
typedef const char *C_CHARS;
typedef char* schema_elem_t;
typedef struct Query {
  schema_elem_t **key_set;
  size_t key_set_size;
  size_t *val_sizes;
} Query;
typedef struct PARSER_STATE {
  const char *input;
  jsmntok_t *key;
  int key_ord;
  jsmntok_t *val;
  int val_ord;
  int schema_key_ord; //this field is used in operation parser, remembers the current elem ord in schema
  bool single_value; //this field is used in parse_next_token, false if vale is in an array
  Query query;
  parser_handler handler;
  PARSER_STAGE stage;
  const char *err_msg;
} PARSER_STATE;
typedef struct op_state {
  OP_STAGE stage;
  SCHEMA_OP op;
  size_t match_count;
  float aggregate;
} OP_STATE;

#define RMUtil_RegisterReadCmd(ctx, cmd, f) \
    if (RedisModule_CreateCommand(ctx, cmd, f, "readonly fast allow-loading allow-stale", \
        1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;

#endif /* REDISCHEMA_H */
