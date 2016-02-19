#ifndef __S3DOWNLOADER_H__
#define __S3DOWNLOADER_H__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "S3Common.h"

typedef struct
{
    char	   *key;
    uint64		size;
} BucketContent;

typedef struct
{
    char	   *Name;
    char	   *Prefix;
    unsigned int MaxKeys;
	BucketContent *contents; /* palloc'd array of BucketContent */
	int			ncontents;
} ListBucketResult;

extern void reportAWSerror(int loglevel, int http_response_code,
			   char *response_body, int body_size);

extern BucketContent *CreateBucketContentItem(char *key, uint64 size);

// need free
extern ListBucketResult *ListBucket(const char *schema, const char *urlhost, const char *bucket,
									const char *path, const S3Credential *cred, const char *host);

#endif
