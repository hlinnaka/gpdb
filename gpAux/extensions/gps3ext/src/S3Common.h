#ifndef __S3_COMMON_H__
#define __S3_COMMON_H__

#include <curl/curl.h>

/* Compile-time Configuration */
#define MAX_FETCH_RETRIES 3
#define FETCH_RETRY_DELAY 3		/* in seconds */

typedef struct
{
    char	   *keyid;
    char	   *secret;
} S3Credential;

typedef enum
{
	HOST,
	RANGE,
	DATE,
	CONTENT_LENGTH,
	CONTENT_MD5,
	CONTENT_TYPE,
	EXPECT,
	AUTHORIZATION,
	ETAG,
	X_AMZ_DATE,
	X_AMZ_CONTENT_SHA256,
} HeaderField;

#define MAX_HEADERS		10

typedef struct
{
	int			nheaders;
	HeaderField keys[MAX_HEADERS];
	char	   *values[MAX_HEADERS];
} HeaderContent;

extern bool HeaderContent_Add(HeaderContent *h, HeaderField f, const char *value);
extern struct curl_slist *HeaderContent_GetList(HeaderContent *h);
extern void HeaderContent_Destroy(HeaderContent *h);

extern char *SignRequestV4(const char *HTTPRequestMethod,
			  const char *CanonicalURI,
			  const char *CanonicalQueryString,
			  HeaderContent *headers,
			  const char *payload,
			  const char *service,
			  const char *region,
			  const char *timestamp_str,
			  const char *accessid,
			  const char *secret);
extern void SignGETv2(HeaderContent* h, const char *path_with_query,
               const S3Credential *cred);

extern const char *GetFieldString(HeaderField f);

extern char *get_opt_s3(const char* url, const char* key);

extern char *truncate_options(const char* url_with_options);

#endif  // __S3_COMMON_H__
