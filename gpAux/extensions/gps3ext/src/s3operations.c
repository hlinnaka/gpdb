/*
 * Functions to issue various commands to S3.
 *
 * Currently, only List Bucket command is implemented. (The GET operation, to
 * fetch contents of an object, is implemented with the HTTPFetcher mechanism
 * instead)
 */
#include "postgres.h"

#include "s3operations.h"
#include "DownloadPipeline.h"

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "utils.h"
#include <curl/curl.h>

#include <unistd.h>

typedef struct
{
    xmlParserCtxtPtr ctxt;
} XMLInfo;

static void reportAWSerrorXML(int loglevel, int http_response_code, xmlNode *root_element);


/*---------
 * Parse an S3 error response, and report it with ereport()
 *
 * See https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#RESTErrorResponses
 *
 * Example:
 *
 * <?xml version="1.0" encoding="UTF-8"?>
 * <Error>
 *   <Code>NoSuchKey</Code>
 *   <Message>The resource you requested does not exist</Message>
 *   <Resource>/mybucket/myfoto.jpg</Resource>
 *   <RequestId>4442587FB7D0A2F9</RequestId>
 * </Error>
 *
 *---------
 */
void
reportAWSerror(int loglevel, int http_response_code, char *response_body, int body_size)
{
	xmlDocPtr doc;
	xmlNode *root = NULL;

	PG_TRY();
	{
		if (body_size > 0)
		{
			doc = xmlParseMemory(response_body, body_size);
			if (doc)
				root = xmlDocGetRootElement(doc);
		}

		reportAWSerrorXML(loglevel, http_response_code, root);

		xmlFreeDoc(doc);
	}
	PG_CATCH();
	{
		xmlFreeDoc(doc);
	}
	PG_END_TRY();
}

static void
reportAWSerrorXML(int loglevel, int http_response_code, xmlNode *root_element)
{
	char	   *message = NULL;
	xmlNodePtr cur;

	if (root_element)
	{
		/* Find the "Message" field, if any */
		cur = root_element->xmlChildrenNode;
		while (cur != NULL)
		{
			if (!xmlStrcmp(cur->name, (const xmlChar *) "Message"))
				message = (char *) xmlNodeGetContent(cur);
			cur = cur->next;
		}
	}

	if (message)
		ereport(loglevel,
				(errmsg("server reported error: %s", message)));
	else
		ereport(loglevel,
				(errmsg("operation failed with HTTP error %d", http_response_code)));
}

static int
BucketContentComp(const void *a_, const void *b_)
{
	const BucketContent *a = (const BucketContent *) a_;
	const BucketContent *b = (const BucketContent *) b_;

    return strcmp(a->key, b->key);
}

static bool
extractContent(ListBucketResult *result, xmlNode *root_element)
{
    if (!result || !root_element)
        return false;

    xmlNodePtr cur;
    cur = root_element->xmlChildrenNode;
    while (cur != NULL)
	{
        if (!xmlStrcmp(cur->name, (const xmlChar *) "Name"))
            result->Name = (char *) xmlNodeGetContent(cur);

        if (!xmlStrcmp(cur->name, (const xmlChar *) "Prefix"))
            result->Prefix = (char *) xmlNodeGetContent(cur);

        if (!xmlStrcmp(cur->name, (const xmlChar *) "Contents"))
		{
            xmlNodePtr contNode = cur->xmlChildrenNode;
            char *key = NULL;
            uint64_t size = 0;
            while (contNode != NULL)
			{
                if (!xmlStrcmp(contNode->name, (const xmlChar *) "Key"))
                    key = (char *) xmlNodeGetContent(contNode);

                if (!xmlStrcmp(contNode->name, (const xmlChar *) "Size"))
				{
                    xmlChar *v = xmlNodeGetContent(contNode);
                    size = atoll((const char *)v);
                }
                contNode = contNode->next;
            }
            if (size > 0)
			{
				/* skip empty item */
				if (strcmp(key, "") == 0)
				{
					/* FIXME: Can this happen? Should it be an error? */
					elog(WARNING, "did not to create item for empty key");
				}
				else
				{
					int		ncontents = result->ncontents;

					if (result->contents == NULL)
						result->contents = palloc((ncontents + 1) * sizeof(BucketContent));
					else
						result->contents = repalloc(result->contents,
													(ncontents + 1) * sizeof(BucketContent));

					BucketContent *item = &result->contents[ncontents];
					item->key = key;
					item->size = size;

					result->ncontents++;
				}
            }
			else
                elog(INFO, "object with key %s is empty, skipping", key);
        }
        cur = cur->next;
    }
    qsort(result->contents, result->ncontents, sizeof(BucketContent), BucketContentComp);
    return true;
}

/*
 * Curl Write callback, which pushes the incoming data to the XML parser.
 */
static uint64_t
ParserCallback(void *contents, uint64_t size, uint64_t nmemb, void *userp)
{
    uint64_t	realsize = size * nmemb;
    int			res;
    XMLInfo	   *pxml = (XMLInfo *) userp;

    if (!pxml->ctxt)
	{
        pxml->ctxt = xmlCreatePushParserCtxt(NULL, NULL,
											 (const char *) contents,
                                             realsize, "resp.xml");
    }
	else
	{
        res = xmlParseChunk(pxml->ctxt, (const char *) contents, realsize, 0);
    }
    return realsize;
}

/*
 * endpoint: host to use in the URL, and to connect to.
 * host: host to send as Host header.
 */
ListBucketResult *
ListBucket(const char *schema, const char *host, const char *bucket,
		   const char *prefix, const S3Credential *cred, const char *endpoint)
{
	StringInfoData hostbuf;
    ListBucketResult *result;
	CURL	   *curl;
	volatile XMLInfo xml = { NULL };
	struct curl_slist *chunk;
	StringInfoData urlbuf;
	HeaderContent headers = { NIL };
	StringInfoData sbuf;
	CURLcode res;
	xmlNode *root_element = NULL;
	int			respcode;

	/*
	 * We use S3 "virtual host" style URLs. This is what we send in the Host: field.
	 */
	initStringInfo(&hostbuf);
	appendStringInfo(&hostbuf, "%s.%s", bucket, host);

	/*
	 * This is the URL we pass to curl. The 'endpoint' part here specifies the
	 * host that we connect to. Normally, it's just "s3.amazonaws.com", or one
	 * of the region-specific hostnames, but it can also be e.g. "localhost"
	 * for testing.
	 */
	initStringInfo(&urlbuf);
	appendStringInfo(&urlbuf, "%s://%s/", schema, endpoint);
    if (strcmp(prefix, "") != 0)
		appendStringInfo(&urlbuf, "?prefix=%s", prefix);

	curl = curl_easy_init();
	if (!curl)
		elog(ERROR, "could not create curl instance");

	PG_TRY();
	{
		curl_easy_setopt(curl, CURLOPT_URL, urlbuf.data);
		// curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &xml);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ParserCallback);

		HeaderContent_Add(&headers, HOST, hostbuf.data);

		initStringInfo(&sbuf);
		appendStringInfo(&sbuf, "/%s/", bucket);
		SignGETv2(&headers, sbuf.data, cred);
		pfree(sbuf.data);

		chunk = HeaderContent_GetList(&headers);

		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

		res = curl_easy_perform(curl);

		if (res != CURLE_OK)
			ereport(ERROR,
					(errmsg("could not list bucket contents: %s", curl_easy_strerror(res))));

		res = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &respcode);
		if (res != CURLE_OK)
			elog(ERROR, "could not get HTTP response code");

		if (xml.ctxt)
		{
			/* Tell the parser that there is no more data coming */
			xmlParseChunk(xml.ctxt, "", 0, 1);

			root_element = xmlDocGetRootElement(xml.ctxt->myDoc);
		}

		/*
		 * If we could got no data, or the response code was anything else than OK,
		 * throw an error. If we got an XML-formatted error response, extract the
		 * error message from that.
		 */
		if (xml.ctxt == NULL || (respcode != 200 /* HTTP OK */))
			reportAWSerrorXML(ERROR, respcode, root_element);

		curl_easy_cleanup(curl);
		curl = NULL;
		curl_slist_free_all(chunk);
		chunk = NULL;
		HeaderContent_Destroy(&headers);


		result = palloc0(sizeof(ListBucketResult));
		if (!extractContent(result, root_element))
			elog(ERROR, "could not extract key from bucket list");
		if (xml.ctxt)
		{
			xmlFreeParserCtxt(xml.ctxt);
			xml.ctxt = NULL;
		}
	}
	PG_CATCH();
	{
		if (xml.ctxt)
			xmlFreeParserCtxt(xml.ctxt);

		curl_easy_cleanup(curl);
		curl_slist_free_all(chunk);

		PG_RE_THROW();
	}
	PG_END_TRY();
    return result;
}
