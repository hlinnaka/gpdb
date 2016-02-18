/*
 * Functions to issue various commands to S3.
 *
 * Currently, only List Bucket command is implemented. (The GET operation, to
 * fetch contents of an object, is implemented with the HTTPFetcher mechanism
 * instead)
 */
#include "postgres.h"

#include "S3Downloader.h"
#include "DownloadPipeline.h"

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "utils.h"
#include <curl/curl.h>

#include <unistd.h>


/* require curl 7.17 higher */
static xmlParserCtxtPtr
DoGetXML(const char *host, const char *url, const char *bucket, const S3Credential *cred)
{
    CURL	   *curl;
	char	   *path;
    volatile XMLInfo xml;
    xml.ctxt = NULL;
	StringInfoData sbuf;

	PG_TRY();
	{
		curl = curl_easy_init();
		if (!curl)
			elog(ERROR, "could not create curl instance");
		curl_easy_setopt(curl, CURLOPT_URL, url);
		// curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
		curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) &xml);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ParserCallback);

		HeaderContent *header = palloc0(sizeof(HeaderContent));
		HeaderContent_Add(header, HOST, host);

		s3_parse_url(url,
					 NULL /* schema */,
					 NULL /* host */,
					 &path,
					 NULL /* fullurl */);

		initStringInfo(&sbuf);
		appendStringInfo(&sbuf, "/%s%s", bucket, path);
		SignGETv2(header, sbuf.data, cred);
		pfree(sbuf.data);

		struct curl_slist *chunk = HeaderContent_GetList(header);

		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

		CURLcode res = curl_easy_perform(curl);

		if (res != CURLE_OK) {
			elog(ERROR, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
		}
		else
		{
			if (xml.ctxt)
				xmlParseChunk(xml.ctxt, "", 0, 1);
			else
				elog(ERROR, "XML is downloaded but failed to be parsed");
		}
		curl_slist_free_all(chunk);
		curl_easy_cleanup(curl);
		HeaderContent_Destroy(header);
	}
	PG_CATCH();
	{
		if (xml.ctxt)
		{
			xmlFreeParserCtxt(xml.ctxt);
			xml.ctxt = NULL;
		}

		PG_RE_THROW();
	}
	PG_END_TRY();
    return xml.ctxt;
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
 * endpoint: host to use in the URL, and to connect to.
 * host: host to send as Host header.
 */
ListBucketResult *
ListBucket(const char *schema, const char *host, const char *bucket,
		   const char *prefix, const S3Credential *cred, const char *endpoint)
{
	StringInfoData sstr;
    ListBucketResult *result;
	StringInfoData hoststr;

	initStringInfo(&hoststr);
	appendStringInfo(&hoststr, "%s.%s", bucket, host);

	initStringInfo(&sstr);

    if (strcmp(prefix, "") != 0)
	{
		appendStringInfo(&sstr, "%s://%s/?prefix=%s",
						 schema, endpoint, prefix);
    }
	else
	{
		appendStringInfo(&sstr, "%s://%s/",
						 schema, endpoint);
    }

	xmlParserCtxtPtr xmlcontext = DoGetXML(hoststr.data, sstr.data, bucket, cred);

	PG_TRY();
	{
		if (!xmlcontext)
			elog(ERROR, "could not list bucket \"%s\"", sstr.data);

		xmlNode *root_element = xmlDocGetRootElement(xmlcontext->myDoc);
		if (!root_element)
			elog(ERROR, "could not parse returned xml of bucket list");

		result = palloc0(sizeof(ListBucketResult));
		if (!extractContent(result, root_element))
			elog(ERROR, "could not extract key from bucket list");
		xmlFreeParserCtxt(xmlcontext);
	}
	PG_CATCH();
	{
		/* always cleanup */
		xmlFreeParserCtxt(xmlcontext);

		PG_RE_THROW();
	}
	PG_END_TRY();
    return result;
}
