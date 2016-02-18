#ifndef __S3_EXT_WRAPPER__
#define __S3_EXT_WRAPPER__

#include "S3Downloader.h"
#include "DownloadPipeline.h"
#include "S3Common.h"

typedef struct
{
    char	   *url;

    char	   *schema;
    char	   *host;
	char	   *path;
    char	   *bucket;
    char	   *prefix;
    char	   *region;

	char	   *endpoint;		/* same as conf_endpoint, if set, otherwise derived from url */

	S3Credential cred;

	/* Configurable parameters */
	char	   *conf_accessid;
	char	   *conf_secret;
	char	   *conf_token;

	char	   *conf_endpoint;

	int			conf_nconnections;
	int			conf_chunksize;

	int			conf_low_speed_limit;
	int			conf_low_speed_time;

	bool		conf_encryption;

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
