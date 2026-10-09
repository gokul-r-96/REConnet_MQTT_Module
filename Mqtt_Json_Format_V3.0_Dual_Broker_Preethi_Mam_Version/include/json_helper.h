#ifndef JSON_HELPER_H
#define JSON_HELPER_H
#include <hiredis/hiredis.h>
#include <stdint.h>


#define JBUF_INIT_CAP  8192

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} jbuf_t;


int jbuf_init(jbuf_t *jb);
int jbuf_grow(jbuf_t *jb, size_t need);
void jbuf_append(jbuf_t *jb, const char *fmt, ...);
void jbuf_trim_comma(jbuf_t *jb);

void jbuf_append_escaped(jbuf_t *jb, const char *s);
void jbuf_append_coil_value(jbuf_t *jb, const char *val_str);
void jbuf_append_numeric_value(jbuf_t *jb, const char *val_str);

int rget_str(redisContext *ctx,
             const char *key,
             const char *field,
             char *buf,
             size_t buf_sz);

int rget_int(redisContext *ctx,
             const char *key,
             const char *field,
             int def);

int rhash_exists(redisContext *ctx,
                 const char *key);

void export_cmd_dcu_nameplate(jbuf_t *jb,
                              redisContext *ctx);

void export_dcu_details(jbuf_t *jb,
                        redisContext *ctx);

#endif