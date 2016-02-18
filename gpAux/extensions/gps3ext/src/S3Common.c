#include "postgres.h"

#include "S3Common.h"
#include "utils.h"

#include "lib/http_parser.h"

static char *extract_field(const char *url, const struct http_parser_url *u, enum http_parser_url_fields i);

	
void
SignGETv2(HeaderContent *h, const char *path_with_query,
		  const S3Credential *cred)
{
    char		timebuf[65];
    char		tmpbuf[20];  // SHA_DIGEST_LENGTH is 20
    struct tm tm_info;
    time_t t;
	StringInfoData sstr;

    /* CONTENT_LENGTH is not a part of StringToSign */
    HeaderContent_Add(h, CONTENTLENGTH, "0");

	/* DATE header */

    t = time(NULL);
    localtime_r(&t, &tm_info);
    strftime(timebuf, sizeof(timebuf), "%a, %d %b %Y %H:%M:%S %z", &tm_info);
    HeaderContent_Add(h, DATE, timebuf);

	initStringInfo(&sstr);
	appendStringInfo(&sstr, "GET\n\n\n%s\n%s", timebuf, path_with_query);

    sha1hmac(sstr.data, tmpbuf, cred->secret);

    // S3DEBUG("%s", sstr.str().c_str());
    char *signature = Base64Encode(tmpbuf, 20);  // SHA_DIGEST_LENGTH is 20
    // S3DEBUG("%s", signature);
	resetStringInfo(&sstr);
	appendStringInfo(&sstr, "AWS %s:%s", cred->keyid, signature);

	pfree(signature);
    // S3DEBUG("%s", sstr.str().c_str());
    HeaderContent_Add(h, AUTHORIZATION, sstr.data);
}

const char *
GetFieldString(HeaderField f)
{
    switch (f) {
        case HOST:
            return "Host";
        case RANGE:
            return "Range";
        case DATE:
            return "Date";
        case CONTENTLENGTH:
            return "Content-Length";
        case CONTENTMD5:
            return "Content-MD5";
        case CONTENTTYPE:
            return "Content-Type";
        case EXPECT:
            return "Expect";
        case AUTHORIZATION:
            return "Authorization";
        case ETAG:
            return "ETag";
        default:
            return "unknown";
    }
}

bool
HeaderContent_Add(HeaderContent *h, HeaderField f, const char *v)
{
	if (strcmp(v, "") != 0)
    {
		StringInfoData sinfo;

		initStringInfo(&sinfo);
		appendStringInfo(&sinfo, "%s: %s", GetFieldString(f), v);
		h->fields = lappend(h->fields, pstrdup(sinfo.data));
		pfree(sinfo.data);
		return true;
    } else {
        return false;
    }
}

struct curl_slist *
HeaderContent_GetList(HeaderContent *h)
{
    struct curl_slist *chunk = NULL;
	ListCell *lc;

	foreach(lc, h->fields)
	{
        chunk = curl_slist_append(chunk, (char *) lfirst(lc));
	}
    return chunk;
}

void
HeaderContent_Destroy(HeaderContent *h)
{
	list_free_deep(h->fields);
}

/*
 * Extract certain fields from URL.
 */
void
s3_parse_url(const char *url, char **schema, char **host, char **path, char **fullurl)
{
    struct http_parser_url u;
    int			len;
	int			result;

    if (!url)
		elog(ERROR, "no URL given"); /* shouldn't happen */

    len = strlen(url);
	if (fullurl)
		*fullurl = pstrdup(url);

    // only parse len, no need to memset this->fullurl
    result = http_parser_parse_url(url, len, false, &u);
    if (result != 0)
		elog(ERROR, "could not parse URL \"%s\": error code %d\n", url, result);

	if (schema)
		*schema = extract_field(url, &u, UF_SCHEMA);
	if (host)
		*host = extract_field(url, &u, UF_HOST);
	if (path)
		*path = extract_field(url, &u, UF_PATH);
}

static char *
extract_field(const char *url, const struct http_parser_url *u,
			  enum http_parser_url_fields i)
{
    char *ret = NULL;

    if ((u->field_set & (1 << i)) != 0)
	{
        ret = (char *) palloc(u->field_data[i].len + 1);
		memcpy(ret, url + u->field_data[i].off,
			   u->field_data[i].len);
		ret[u->field_data[i].len] = 0;
    }
    return ret;
}

uint64_t
ParserCallback(void *contents, uint64_t size, uint64_t nmemb,
			   void *userp) {
    uint64_t realsize = size * nmemb;
    int res;
    // printf("%.*s",realsize, (char*)contents);
    XMLInfo *pxml = (XMLInfo *) userp;

    if (!pxml->ctxt)
	{
        pxml->ctxt = xmlCreatePushParserCtxt(NULL, NULL, (const char *)contents,
                                             realsize, "resp.xml");
        return realsize;
    }
	else
	{
        res = xmlParseChunk(pxml->ctxt, (const char *)contents, realsize, 0);
    }
    return realsize;
}

char *
get_opt_s3(const char *url, const char *key)
{
    const char *key_f = NULL;
    const char *key_tailing = NULL;
    char *key_val = NULL;
    int val_len = 0;

    if (!url || !key)
        return NULL;

    char *key2search = (char *) palloc(strlen(key) + 3);
    int key_len = strlen(key);

    key2search[0] = ' ';
    memcpy(key2search + 1, key, key_len);
    key2search[key_len + 1] = '=';
    key2search[key_len + 2] = 0;

    // printf("key2search=%s\n", key2search);
    const char *delimiter = " ";
    const char *options = strstr(url, delimiter);
    if (!options)  /* no options string */
        goto FAIL;

    key_f = strstr(options, key2search);
    if (key_f == NULL)
        goto FAIL;

    key_f += strlen(key2search);
    if (*key_f == ' ')
        goto FAIL;

    // printf("key_f=%s\n", key_f);

    key_tailing = strstr(key_f, delimiter);
    // printf("key_tailing=%s\n", key_tailing);
    val_len = 0;
    if (key_tailing)
        val_len = strlen(key_f) - strlen(key_tailing);
    else
        val_len = strlen(key_f);

    key_val = (char *) palloc(val_len + 1);

    memcpy(key_val, key_f, val_len);
    key_val[val_len] = 0;

    pfree(key2search);

    return key_val;

FAIL:
	pfree(key2search);
    return NULL;
}

/*
 * Truncates the given URL at the first space character. Returns
 * the part before the space, as a palloc'd string
 */
char *
truncate_options(const char *url_with_options)
{
    const char *delimiter = " ";
    char	*options;
    int url_len;
    char *url;

    options = strstr((char *)url_with_options, delimiter);
    url_len = strlen(url_with_options);

    if (options)
        url_len = strlen(url_with_options) - strlen(options);

    url = (char *) palloc(url_len + 1);
    memcpy(url, url_with_options, url_len);
    url[url_len] = 0;

    return url;
}
