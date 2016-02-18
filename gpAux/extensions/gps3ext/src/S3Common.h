#ifndef __S3_COMMON_H__
#define __S3_COMMON_H__

#include <curl/curl.h>
#include "http_parser.h"

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "nodes/pg_list.h"

/* Compile-time Configuration */
#define MAX_FETCH_RETRIES 3
#define FETCH_RETRY_DELAY 3		/* in seconds */

typedef struct
{
    char	   *keyid;
    char	   *secret;
} S3Credential;

typedef enum {
    HOST,
    RANGE,
    DATE,
    CONTENTLENGTH,
    CONTENTMD5,
    CONTENTTYPE,
    EXPECT,
    AUTHORIZATION,
    ETAG,
} HeaderField;

typedef struct
{
	List *fields;
} HeaderContent;

typedef struct {
    xmlParserCtxtPtr ctxt;
} XMLInfo;

extern void s3_parse_url(const char *url, char **schema, char **host, char **path, char **fullurl);

extern bool HeaderContent_Add(HeaderContent *h, HeaderField f, const char *value);
extern struct curl_slist *HeaderContent_GetList(HeaderContent *h);
extern void HeaderContent_Destroy(HeaderContent *h);

extern void SignGETv2(HeaderContent* h, const char *path_with_query,
               const S3Credential *cred);

extern const char *GetFieldString(HeaderField f);

extern uint64_t ParserCallback(void* contents, uint64_t size, uint64_t nmemb,
                        void* userp);

extern char *get_opt_s3(const char* url, const char* key);

extern char *truncate_options(const char* url_with_options);

#endif  // __S3_COMMON_H__
