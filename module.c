#include "redismodule.h"
#include <string.h>
#include <stdlib.h>

#define MODULE_NAME "redischema"

#define SCHEMA_LOAD_ARGS_LIMIT 2
#define SCHEMA_LOAD_ARG_LIST 1
#define MODULE_ERROR_INVALID_INPUT -1

#define SCHEMA_START_DICT '{'
#define SCHEMA_END_DICT '}'
#define SCHEMA_START_ARR '['
#define SCHEMA_END_ARR ']'
#define SCHEMA_KV_DELIM ':'
#define SCHEMA_VAL_DELIM ','
#define QUOTE_CHAR "\""
#define SPACE_DELIMS " \t\r\n"
#define SCHEMA_LOCATION "module:schema:order"
#define SCHEMA_ELEM_PREFIX "module:schema:elements:"
#define END_OF_STR '\0' //TODO: maybe delete
#define RM_CreateString(ctx, str) RedisModule_CreateString(ctx,str,strlen(str))
typedef enum { false, true } bool; //TODO: maybe delete


#define RMUtil_RegisterReadCmd(ctx, cmd, f) \
    if (RedisModule_CreateCommand(ctx, cmd, f, "readonly fast allow-loading allow-stale", \
        1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;

int insert_schema_key(RedisModuleCtx *ctx, const char *key){
    RedisModuleString *ele = RM_CreateString(ctx, key);
    RedisModuleString *schm_loc_str = RM_CreateString(ctx, SCHEMA_LOCATION);
    RedisModuleKey *schm_loc_key = RedisModule_OpenKey(ctx,schm_loc_str,REDISMODULE_WRITE);
    int rsp = RedisModule_ListPush(schm_loc_key,REDISMODULE_LIST_TAIL,ele);
    RedisModule_CloseKey(schm_loc_key);
    RedisModule_FreeString(ctx, schm_loc_str);
    RedisModule_FreeString(ctx, ele);
    return rsp;
}

int insert_schema_val_for_key(RedisModuleCtx *ctx, char* val, char* key){
  RedisModuleString *ele = RM_CreateString(ctx, val);
  char *key_arr = malloc((strlen(SCHEMA_ELEM_PREFIX) + strlen(key) + 1) * sizeof(char));
  strcpy(key_arr, SCHEMA_ELEM_PREFIX);
  strcat(key_arr, key);
  RedisModuleString *key_str = RM_CreateString(ctx, key_arr);
  RedisModuleKey *rds_key = RedisModule_OpenKey(ctx,key_str,REDISMODULE_WRITE);
  int rsp = RedisModule_ListPush(rds_key,REDISMODULE_LIST_TAIL,ele);
  RedisModule_CloseKey(rds_key);
  RedisModule_FreeString(ctx, key_str);
  free(key_arr);
  RedisModule_FreeString(ctx, ele);
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

//TODO: add docs
//TODO: split function
int load_schema_str_to_redis(RedisModuleCtx *ctx, char *pos) {
    char *key, *val;
    char *old_pos;
    pos = validate_next_char(pos, SCHEMA_START_DICT);
    if( pos == NULL)
        return MODULE_ERROR_INVALID_INPUT;
    do {
        key = strtok(pos, QUOTE_CHAR);
        insert_schema_key(ctx, key);
        pos = key + strlen(key)+1; //skip key
        pos = validate_next_char(pos, SCHEMA_KV_DELIM);
        if(pos == NULL)
            return MODULE_ERROR_INVALID_INPUT;
        pos = validate_next_char(pos, SCHEMA_START_ARR);
        if(pos == NULL)
            return MODULE_ERROR_INVALID_INPUT;
        do {
            val = strtok(pos, QUOTE_CHAR);
            insert_schema_val_for_key(ctx, val, key);
            pos = val + strlen(val)+1; //skip key
            old_pos = pos;
            pos = validate_next_char(pos, SCHEMA_VAL_DELIM);
        } while(pos != NULL);
        pos = old_pos;
        pos = validate_next_char(pos, SCHEMA_END_ARR);
        if(pos == NULL)
            return MODULE_ERROR_INVALID_INPUT;
        pos = validate_next_char(pos, SCHEMA_VAL_DELIM);
    } while(pos != NULL);
    return 0;
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

    return REDISMODULE_OK;
}
