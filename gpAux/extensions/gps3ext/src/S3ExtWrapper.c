#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "S3ExtWrapper.h"
#include "utils.h"

#include "ini.h"

#include "gps3ext.h"

static bool ValidateURL(S3ExtBase *base);
static void ReadConfig(S3ExtBase *bas, const char *conf_path);

static void S3Reader_SetupPipeline(S3ExtBase *base, ListBucketResult *keylist);

S3ExtBase *
S3ExtBase_create(const char *url, const char *conf_path)
{
	S3ExtBase *base;
	StringInfoData sstr;
    int			nfailures = 0;
	ListBucketResult *keylist;

	base = (S3ExtBase *) palloc(sizeof(S3ExtBase));
	
	ReadConfig(base, conf_path);

	base->url = pstrdup(url);
	
    // Validate url first
    if (!ValidateURL(base))
		ereport(ERROR,
				(errmsg("given s3 URL \"%s\" is not valid",
						base->url)));

    // TODO: As separated function for generating url
	/*
	 * Use the hostname specified in the config file, or the one from the URL
	 * if not given.
	 */
	initStringInfo(&sstr);
	appendStringInfo(&sstr, "s3-%s.amazonaws.com", base->region);
	base->host = sstr.data;
	if (strcmp(base->conf_endpoint, "") == 0)
		base->endpoint = sstr.data;
	else
		base->endpoint = base->conf_endpoint;
	elog(DEBUG1, "Host url is %s (endpoint %s)", sstr.data, base->endpoint);

	/*
	 * Copy the credentials into the S3Credential object, for passing around more
	 * easily.
	 */
	base->cred.keyid = base->conf_accessid;
	base->cred.secret = base->conf_secret;

	/*
	 * Now get the list of objects in the bucket from the server.
	 *
	 * If this fails, we retry a few times (MAX_FETCH_RETRIES)
	 */
retry:
	keylist = ListBucket(base->schema, sstr.data,
						 base->bucket, base->prefix, &base->cred, base->endpoint);
	if (!keylist)
	{
		nfailures++;

		if (nfailures >= MAX_FETCH_RETRIES)
			ereport(ERROR,
					(errmsg("could not list contents of bucket \"%s\"",
						base->bucket)));
		else
		{
			elog(INFO, "could not list contents for bucket, retrying");
			goto retry;
		}
	}

	/*
	 * FIXME: Is it really reasonable to retry, if the bucket is empty? Surely
	 * it's not going to suddenly be non-empty if we just try again?
	 */
	if (keylist->ncontents == 0)
	{
		nfailures++;

		if (nfailures >= MAX_FETCH_RETRIES)
			ereport(ERROR,
					(errmsg("bucket is empty")));
		else
		{
			elog(INFO, "bucket is empty, retrying");
			goto retry;
		}
	}

	elog(INFO, "got %d files to download", keylist->ncontents);
	S3Reader_SetupPipeline(base, keylist);

	return base;
}

/*
 * Form a DownloadPipeline that will fetch all objects listed in the given
 * "bucket result".
 */
static void
S3Reader_SetupPipeline(S3ExtBase *base, ListBucketResult *keylist)
{
	int			i;
	StringInfoData hoststr;

	initStringInfo(&hoststr);
	appendStringInfo(&hoststr, "%s.%s", base->bucket, base->host);
	
	base->pipeline = DLPipeline_create();
	
	/*
	 * Create a pipeline that will download all the contents 
	 *
	 *  for now, every segment downloads its assigned files(mod)
	 * better to build a workqueue in case not all segments are available
	 */
	for (i = base->segid; i < keylist->ncontents; i += base->segnum)
	{
		BucketContent *c = &keylist->contents[i];
		HTTPFetcher *fetcher;
		StringInfoData urlbuf;

		initStringInfo(&urlbuf);
		appendStringInfo(&urlbuf, "%s://%s/%s",
						 base->schema, base->endpoint, c->key);
		
		fetcher = HTTPFetcher_create(urlbuf.data, hoststr.data, base->bucket, base->path, &base->cred, 0, -1);
		elog(DEBUG1, "key: %s, size: " UINT64_FORMAT, urlbuf.data, c->size);

		DLPipeline_add(base->pipeline, fetcher);
	}
}

/*
 * Read at most *len bytes into *data. Returns the actual number of bytes read
 * in *len.
 */
int
S3Reader_TransferData(S3ExtBase *base, char *buf, int bufsize)
{
	bool		eof;

	return DLPipeline_read(base->pipeline, buf, bufsize, &eof);
}

void
S3Reader_destroy(S3ExtBase *base)
{
	DLPipeline_destroy(base->pipeline);
	/*
	 * Memory isn't explicitly pfree'd here. We assume the caller will destroy the
	 * containing MemoryContext soon.
	 */
}

/*
 * Note: This not only checks that the URL is valid, but also extracts some fields
 * from it into fields in 'base'.
 *
 * XXX: What exactly is the format of the url? Comment, please!
 */
static bool
ValidateURL(S3ExtBase *base)
{
    const char *awsdomain = ".amazonaws.com";
	char		*s_awsdomain;
	char	   *s_region;
	int			regionlen;
	char	   *s_bucket;
	char	   *s_prefix;
	int			bucketlen;

	if (strncmp(base->url, "s3://", 5) == 0)
	{
        if (base->conf_encryption)
            base->schema = "https";
        else
            base->schema = "http";
	}
	else
		return false;

	s_region = strchr(base->url, '-');
	if (!s_region)
		return false;
	s_region++;

	s_awsdomain = strstr(s_region, awsdomain);
	if (!s_awsdomain)
		return false;

	regionlen = s_awsdomain - s_region;
	base->region = palloc(regionlen + 1);
	memcpy(base->region, s_region, regionlen);
	base->region[regionlen] = '\0';

	s_bucket = strchr(s_awsdomain, '/');
	if (!s_bucket)
		return false;
	s_bucket++;

	s_prefix = strchr(s_bucket, '/');
	if (!s_prefix)
		return false;
	bucketlen = s_prefix - s_bucket;
	s_prefix++;

	base->bucket = palloc(bucketlen + 1);
	memcpy(base->bucket, s_bucket, bucketlen);
	base->bucket[bucketlen] = '\0';

	base->prefix = pstrdup(s_prefix);

    /*
    if (url.back() != '/') {
        return false;
    }
    */
    return true;
}

/**** Utility functions for reading the configuration file ****/
static char *
conf_str_get(ini_t *ini, const char *section, const char *key, const char *defval)
{
	const char *value;

	if (ini == NULL)
		return pstrdup(defval);

	value = ini_get(ini, section, key);
	if (value == NULL)
		return pstrdup(defval);
	else
		return pstrdup(value);
}

static int
conf_int_get(ini_t *ini, const char *section, const char *key, int defval)
{
	const char *value;

	if (ini == NULL)
		return defval;

	value = ini_get(ini, section, key);
	if (value == NULL)
		return defval;
	else
		return pg_atoi((char *) value, 4, 0);
}

static bool
conf_bool_get(ini_t *ini, const char *section, const char *key, bool defval)
{
	const char *value;

	if (ini == NULL)
		return defval;

	value = ini_get(ini, section, key);
	if (value == NULL)
		return defval;
	else
	{
		bool result;

		if (!parse_bool(value, &result))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type boolean: \"%s\"", value)));
		return result;
	}
}

/*
 * Read options from configuration file into S3ExtBase object.
 */
static void
ReadConfig(S3ExtBase *base, const char *conf_path)
{
	ini_t	   *inifile = NULL;

    if (strcmp(conf_path, "") == 0)
        elog(DEBUG1, "Config file is not specified");
	else
	{
		inifile = ini_load(conf_path);
		if (inifile == NULL)
			ereport(ERROR,
					(errmsg("could not load config file \"%s\"", conf_path)));
	}

	PG_TRY();
	{
		base->conf_accessid = conf_str_get(inifile, "default", "accessid", "");
		base->conf_secret = conf_str_get(inifile, "default", "secret", "");
		base->conf_token = conf_str_get(inifile, "default", "token", "");

        if (strcmp(base->conf_accessid, "") == 0)
            ereport(ERROR,
					(errmsg("access id is empty")));

        if (strcmp(base->conf_secret, "") == 0)
            ereport(ERROR,
					(errmsg("secret is empty")));

		base->conf_chunksize = conf_int_get(inifile, "default", "chunksize", 64 * 1024 * 1024);
		base->conf_low_speed_limit = conf_int_get(inifile, "default", "low_speed_limit", 10240);
		base->conf_low_speed_time = conf_int_get(inifile, "default", "low_speed_time", 60);

		base->conf_low_speed_limit = conf_int_get(inifile, "default", "low_speed_limit", 10240);
		base->conf_low_speed_time = conf_int_get(inifile, "default", "low_speed_time", 60);

		base->conf_encryption = conf_bool_get(inifile, "default", "encryption", true);

		base->conf_endpoint = conf_str_get(inifile, "default", "endpoint", "");

#ifdef DEBUGS3
		base->segid = 0;
		base->segnum = 1;
#else
		base->segid = GpIdentity.segindex;
		base->segnum = GpIdentity.numsegments;
#endif

		if (inifile)
			ini_free(inifile);
	}
	PG_CATCH();
	{
		if (inifile)
			ini_free(inifile);
		PG_RE_THROW();
	}
	PG_END_TRY();
}
