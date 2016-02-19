/*
 * Utility functions for calculating Authentication signatures etc. for S3.
 */
#include "postgres.h"

#define INCLUDE_S3_DEBUG_FUNCS

#include "S3Common.h"
#include "utils.h"

#ifdef INCLUDE_S3_DEBUG_FUNCS
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/array.h"
#endif

static const char *GetCanonicalizedFieldString(HeaderField f);

/*
 * Helper function to canonicalize a heea
 */
static void
appendCanonicalHeader(StringInfo buf, HeaderField f, const char *val)
{
	const char *vp;

	appendStringInfo(buf, "%s:", GetCanonicalizedFieldString(f));

	/*
	 * Output header value in canonical form: strip leading and trailing space, and
	 * convert sequential spaces to a single space
	 */
	vp = val;

	/* strip leading spaces */
	while (*vp == ' ')
		vp++;

	while (*vp)
	{
		if (vp[0] == ' ' && vp[1] == ' ')
		{
			vp++; /* skip double spaces */
			continue;
		}
		if (vp[0] == ' ' && vp[1] == '\0')
		{
			/* skip trailing space */
			break;
		}

		/* we're now located at a non-space character, output it */
		appendStringInfoChar(buf, *vp);

		vp++;

		continue;
	}

	appendStringInfoChar(buf, '\n');
}

/*
 * date must be on YYYYMMDD format.
 */
static void
DeriveSigningKey(const char *secret, const char *date, const char *region, const char *service,
				 unsigned char output[S3_SHA256_DIGEST_LENGTH])
{
	char	   *kSecret;
	int			kSecretLen;
	unsigned char kDate[S3_SHA256_DIGEST_LENGTH];
	unsigned char kRegion[S3_SHA256_DIGEST_LENGTH];
	unsigned char kService[S3_SHA256_DIGEST_LENGTH];

	/*
	 * Derive the signing key
	 *
	 * kSecret = Your AWS Secret Access Key
	 * kDate = HMAC("AWS4" + kSecret, Date)
	 * kRegion = HMAC(kDate, Region)
	 * kService = HMAC(kRegion, Service)
	 * kSigning = HMAC(kService, "aws4_request")
	 */
	kSecretLen = 4 + strlen(secret);
	kSecret = palloc(kSecretLen + 1);
	snprintf(kSecret, kSecretLen + 1, "AWS4%s", secret);

	sha256hmac(kDate, date, kSecret, kSecretLen);
	sha256hmac(kRegion, region, (char *) kDate, S3_SHA256_DIGEST_LENGTH);
	sha256hmac(kService, service, (char *) kRegion, S3_SHA256_DIGEST_LENGTH);
	sha256hmac(output, "aws4_request", (char *) kService, S3_SHA256_DIGEST_LENGTH);

	pfree(kSecret);
}

/*
 * Add an S3 Authorization header to a curl request.
 *
 * HTTPRequestMethod	"GET", "PUT, or similar
 * CanonicalURI			path of the URL, up to the question mark, e.g. "/foo"
 * CanonicalQueryString	part of the URL after question mark
 * headers				headers to sign.
 *
 * See https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html
 * for details.
 *
 * If timestamp_str is NULL, uses the current timestamp, and also adds
 * an x-amz-date header to the request.
 *
 * Returns the value to put in the Authorization header. Also adds the
 * fully-constructed  Authorization header to 'headers', as well as an
 * 'x-amz-date' header with the same timestamp as was used in the signing
 * process.
 *
 * If 'payload' is NULL, the "UNSIGNED-PAYLOAD" is used instead of the
 * Canonical hash.
 *
 * FIXME: The query string would need to be normalized, by percent-encoding.
 * That has not been implemented yet. We only use query strings in the 'prefix',
 * so as long as you stick to ascii-only prefixes, we don't need it.
 */
char *
SignRequestV4(const char *HTTPRequestMethod,
			  const char *CanonicalURI,
			  const char *CanonicalQueryString,
			  HeaderContent *headers,
			  const char *payload,
			  const char *service,
			  const char *region,
			  const char *timestamp_str, /* 20150830T123600Z */
			  const char *accessid,
			  const char *secret)
{
	StringInfoData CanonicalRequest;
	StringInfoData SignedHeaders;
	StringInfoData StringToSign;
	StringInfoData authbuf;
	char		HashedCanonicalRequest[65];
	char		signatureHex[65];
	unsigned char signatureDigest[S3_SHA256_DIGEST_LENGTH];
	char		payloadhash[65];
	unsigned char signingkey[S3_SHA256_DIGEST_LENGTH];
	int			i;
	char		date_str[9];
	char		timestamp_buf[17];

	if (timestamp_str == NULL)
	{
		time_t		t;
		struct tm	tm_info;

		/* YYYYMMDD'T'HHMMSS'Z' */
		t = time(NULL);
		gmtime_r(&t, &tm_info);
		strftime(timestamp_buf, 17, "%Y%m%dT%H%M%SZ", &tm_info);
		timestamp_str = timestamp_buf;

		HeaderContent_Add(headers, X_AMZ_DATE, timestamp_str);
	}
	else if (strlen(timestamp_str) != 16)
		elog(ERROR, "invalid timestamp argument to SignRequestV4");
	memcpy(date_str, timestamp_str, 8);
	date_str[8] = '\0';

	initStringInfo(&CanonicalRequest);
	appendStringInfo(&CanonicalRequest, "%s\n", HTTPRequestMethod);
	/* FIXME: the URI should be percent-encoded */
	appendStringInfo(&CanonicalRequest, "%s\n", CanonicalURI);
	appendStringInfo(&CanonicalRequest, "%s\n", CanonicalQueryString);

	/* append CanonicalHeaders */
	for (i = 0; i < headers->nheaders; i++)
	{
		appendCanonicalHeader(&CanonicalRequest,
							  headers->keys[i],
							  headers->values[i]);
	}
	appendStringInfoChar(&CanonicalRequest, '\n');

	/* Calculate SignedHeaders and append it */
	initStringInfo(&SignedHeaders);
	for (i = 0; i < headers->nheaders; i++)
	{
		if (i > 0)
			appendStringInfoChar(&SignedHeaders, ';');
		appendStringInfoString(&SignedHeaders,
							   GetCanonicalizedFieldString(headers->keys[i]));

	}

	appendStringInfo(&CanonicalRequest, "%s\n", SignedHeaders.data);

	/* Append HexEncode(Hash(RequestPayload)) */
	if (payload)
		sha256(payload, payloadhash);
	else
		snprintf(payloadhash, sizeof(payloadhash), "UNSIGNED-PAYLOAD");
	appendStringInfo(&CanonicalRequest, "%s", payloadhash);

	elog(DEBUG2, "CanonicalRequest:\n%s", CanonicalRequest.data);

	/* Hash the whole of the whole CanonicalRequest */
	sha256(CanonicalRequest.data, HashedCanonicalRequest);

	/*
	 * Great! Now let's calculate StringToSign:
	 *
	 * StringToSign  =
	 * Algorithm + '\n' +
	 * RequestDate + '\n' +
	 * CredentialScope + '\n' +
	 * HashedCanonicalRequest))
	 */
	initStringInfo(&StringToSign);
	appendStringInfoString(&StringToSign, "AWS4-HMAC-SHA256\n");
	appendStringInfo(&StringToSign, "%s\n", timestamp_str);

	/* Credential scope, e.g. "20150830/us-east-1/iam/aws4_request\n" */
	appendStringInfo(&StringToSign, "%s/%s/%s/aws4_request\n",
					 date_str, region, service);
	appendStringInfo(&StringToSign, "%s", HashedCanonicalRequest);

	elog(DEBUG2, "StringToSign:\n%s", StringToSign.data);

	/*
	 * Derive the signing key
	 */
	DeriveSigningKey(secret, date_str, region, service, signingkey);

	/* Finally, calculate the signature */
	sha256hmac(signatureDigest, StringToSign.data, (char *) signingkey, S3_SHA256_DIGEST_LENGTH);
	for (i = 0; i < S3_SHA256_DIGEST_LENGTH; i++)
        sprintf(signatureHex + (i * 2), "%02x", signatureDigest[i]);

	/*
	 * Now construct Authorization header:
	 *
	 * Authorization: algorithm Credential=access key ID/credential scope, SignedHeaders=SignedHeaders, Signature=signature
	 *
	 * Example:
	 * Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7
	 */
	initStringInfo(&authbuf);

	appendStringInfo(&authbuf, "AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
					 accessid, date_str, region, service, SignedHeaders.data, signatureHex);


	/* Add headers */
	HeaderContent_Add(headers, AUTHORIZATION, authbuf.data);

	/* clean up */
	pfree(CanonicalRequest.data);
	pfree(StringToSign.data);

	return authbuf.data;
}

const char *
GetFieldString(HeaderField f)
{
    switch (f)
	{
        case HOST:
            return "Host";
        case RANGE:
            return "Range";
        case DATE:
            return "Date";
        case CONTENT_LENGTH:
            return "Content-Length";
        case CONTENT_MD5:
            return "Content-MD5";
        case CONTENT_TYPE:
            return "Content-Type";
        case EXPECT:
            return "Expect";
        case AUTHORIZATION:
            return "Authorization";
        case ETAG:
            return "ETag";
		case X_AMZ_DATE:
			return "X-Amz-Date";
		case X_AMZ_CONTENT_SHA256:
			return "x-amz-content-sha256";
        default:
            return "unknown";
    }
}

static const char *
GetCanonicalizedFieldString(HeaderField f)
{
    switch (f)
	{
        case HOST:
            return "host";
        case RANGE:
            return "range";
        case DATE:
            return "date";
        case CONTENT_LENGTH:
            return "content-length";
        case CONTENT_MD5:
            return "content-md5";
        case CONTENT_TYPE:
            return "content-type";
        case EXPECT:
            return "expect";
        case AUTHORIZATION:
            return "authorization";
        case ETAG:
            return "etag";
		case X_AMZ_DATE:
			return "x-amz-date";
		case X_AMZ_CONTENT_SHA256:
			return "x-amz-content-sha256";
        default:
            return "unknown";
    }
}

bool
HeaderContent_Add(HeaderContent *h, HeaderField f, const char *v)
{
	if (strcmp(v, "") != 0)
    {
		if (h->nheaders >= MAX_HEADERS)
			elog(ERROR, "too many headers");

		h->keys[h->nheaders] = f;
		h->values[h->nheaders] = pstrdup(v);
		h->nheaders++;
		return true;
    }
	else
	{
        return false;
    }
}

struct curl_slist *
HeaderContent_GetList(HeaderContent *h)
{
	struct curl_slist *chunk = NULL;
	int			i;
	StringInfoData sbuf;

	initStringInfo(&sbuf);

	for (i = 0; i < h->nheaders; i++)
	{
		resetStringInfo(&sbuf);

		appendStringInfo(&sbuf, "%s: %s",
						 GetFieldString(h->keys[i]),
						 h->values[i]);

        chunk = curl_slist_append(chunk, pstrdup(sbuf.data));
	}
	pfree(sbuf.data);

	return chunk;
}

void
HeaderContent_Destroy(HeaderContent *h)
{
	int			i;

	for (i = 0; i < h->nheaders; i++)
		pfree(h->values[i]);

	h->nheaders = 0;
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



#ifdef INCLUDE_S3_DEBUG_FUNCS
/*
 * Expose the signing function to SQL level, for debugging purposes.
 *
 * The signatures ar:
 *
 * CREATE FUNCTION test_signrequestv4(method text, path text, querystring text, headers text[], payload text, service text, region text, date_time text, accessid text, secret text) RETURNS text AS '$libdir/gps3ext.so', 's3_test_signv4' LANGUAGE C STRICT;
 *
 * CREATE FUNCTION test_derivesigningkey(secret text, date_s text, region text, service text) RETURNS text AS '$libdir/gps3ext.so', 's3_test_derivesigningkey' LANGUAGE C STRICT;

 *
 */
PG_FUNCTION_INFO_V1(s3_test_signv4);
PG_FUNCTION_INFO_V1(s3_test_derivesigningkey);

Datum s3_test_signv4(PG_FUNCTION_ARGS);
Datum s3_test_derivesigningkey(PG_FUNCTION_ARGS);

Datum
s3_test_signv4(PG_FUNCTION_ARGS)
{
	char	   *method = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	   *path = TextDatumGetCString(PG_GETARG_DATUM(1));
	char	   *querystring = TextDatumGetCString(PG_GETARG_DATUM(2));
	ArrayType  *header_array = PG_GETARG_ARRAYTYPE_P(3);
	char	   *payload = TextDatumGetCString(PG_GETARG_DATUM(4));
	char	   *service = TextDatumGetCString(PG_GETARG_DATUM(5));
	char	   *region = TextDatumGetCString(PG_GETARG_DATUM(6));
	char	   *timestamp = TextDatumGetCString(PG_GETARG_DATUM(7));
	char	   *accessid = TextDatumGetCString(PG_GETARG_DATUM(8));
	char	   *secret = TextDatumGetCString(PG_GETARG_DATUM(9));
	char	   *auth_header;
	HeaderContent headers = { 0 };
	Datum	   *elems;
	int			nelems;
	int			i;

	deconstruct_array(header_array, TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems % 2 != 0)
		elog(ERROR, "headers argument must contain an even number of elements");

	for (i = 0; i < nelems; i+=2)
	{
		HeaderField f;
		char	   *h = TextDatumGetCString(elems[i]);
		char	   *v = TextDatumGetCString(elems[i + 1]);

		/*
		 * we don't bother to support every header type here, just a
		 * a few we will use in testing
		 */
		if (strcmp(h, "Host") == 0)
			f = HOST;
		else if (strcmp(h, "Range") == 0)
			f = RANGE;
		else if (strcmp(h, "Content-Length") == 0)
			f = CONTENT_LENGTH;
		else if (strcmp(h, "Content-MD5") == 0)
			f = CONTENT_MD5;
		else if (strcmp(h, "Content-Type") == 0)
			f = CONTENT_TYPE;
		else if (strcmp(h, "X-Amz-Date") == 0)
			f = X_AMZ_DATE;
		else
			elog(ERROR, "unknown header field %s", h);

		HeaderContent_Add(&headers, f, v);
	}

	auth_header = SignRequestV4(method, path, querystring, &headers,
								payload, service, region, timestamp, accessid, secret);

	PG_RETURN_DATUM(CStringGetTextDatum(auth_header));
}

Datum
s3_test_derivesigningkey(PG_FUNCTION_ARGS)
{
	char	   *secret = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	   *date = TextDatumGetCString(PG_GETARG_DATUM(1));
	char	   *region = TextDatumGetCString(PG_GETARG_DATUM(2));
	char	   *service = TextDatumGetCString(PG_GETARG_DATUM(3));
	StringInfoData sbuf;
	unsigned char digest[S3_SHA256_DIGEST_LENGTH];
	int			i;

	DeriveSigningKey(secret, date, region, service, digest);

	initStringInfo(&sbuf);
	for (i = 0; i < S3_SHA256_DIGEST_LENGTH; i++)
	{
		if (i > 0)
			appendStringInfoString(&sbuf, ", ");
		appendStringInfo(&sbuf, "%u", (unsigned int) digest[i]);
	}

	PG_RETURN_DATUM(CStringGetTextDatum(sbuf.data));
}
#endif
