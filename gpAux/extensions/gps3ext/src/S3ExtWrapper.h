#ifndef __S3_EXT_WRAPPER__
#define __S3_EXT_WRAPPER__

#include "s3operations.h"
#include "DownloadPipeline.h"
#include "S3Common.h"

typedef struct
{
	/* This is the protocol URL (e.g. s3://s3.amazonaws.com/...) */
    char	   *url;

	/* These are extracted parts from the protocol URL */
    char	   *schema;
    char	   *host;
	char	   *path;
    char	   *bucket;
    char	   *prefix;
    char	   *region;

	char	   *endpoint;		/* same as conf_endpoint, if set, otherwise derived from url */

	/* Configurable parameters */
	char	   *conf_accessid;
	char	   *conf_secret;

	char	   *conf_endpoint;

	int			conf_nconnections;
	int			conf_chunksize;

	int			conf_low_speed_limit;
	int			conf_low_speed_time;

	bool		conf_encryption;

	/* copy of accessid and secret in convenient form */
	S3Credential cred;

	/* segment layout */
	int			segid;
	int			segnum;

	/* for readers */
	DownloadPipeline *pipeline;

} S3ExtBase;

extern S3ExtBase *S3ExtBase_create(const char *url, const char *conf_path);
extern void S3Reader_destroy(S3ExtBase *base);
extern int S3Reader_TransferData(S3ExtBase *base, char *buf, int bufsize);

#endif
