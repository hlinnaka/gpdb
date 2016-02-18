/*
 * HTTPFetcher performs the transfer from a single URL. To use, it must be added
 * to a DownloadPipeline. The HTTPFetcher functions never block, DownloadPipeline
 * is responsible for waiting, and calling the CURL's perform function that advances
 * the state of the transfer.
 */
#include "postgres.h"

#include "S3Downloader.h"
#include "DownloadPipeline.h"

#include "utils.h"
#include <curl/curl.h>

static void HTTPFetcher_fail(HTTPFetcher *fetcher);

/* CURL callback that places incoming data in the buffer. */
static size_t
WriterCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
	size_t		realsize = size * nmemb;
	HTTPFetcher *fetcher = (HTTPFetcher *) userp;

	if (fetcher->nused == fetcher->readoff)
		fetcher->readoff = fetcher->nused = 0;

	if (realsize > fetcher->bufsize - fetcher->nused)
	{
		if (fetcher->readoff > 0)
		{
			/* Shift the valid data in the buffer to make room for more */
			memmove(fetcher->readbuf,
					fetcher->readbuf + fetcher->readoff,
					fetcher->nused - fetcher->readoff);
			fetcher->nused -= fetcher->readoff;
			fetcher->readoff = 0;
		}

		if (realsize > fetcher->bufsize - fetcher->nused)
		{
			fetcher->paused = true;
			elog(DEBUG2, "paused %s (%d bytes in buffer, size %d)", fetcher->url,
				 (int) fetcher->nused, (int) fetcher->bufsize);
			return CURL_WRITEFUNC_PAUSE;
		}
	}

	memcpy(fetcher->readbuf + fetcher->nused, contents, realsize);
	fetcher->nused += realsize;
	return realsize;
}

HTTPFetcher *
HTTPFetcher_create(const char *url, const char *host, const char *bucket,
				   S3Credential *cred, int bufsize, uint64 offset, int len)
{
	HTTPFetcher *fetcher;

	elog(DEBUG1, "created fetcher for %s, " UINT64_FORMAT " for %d bytes)", url, offset, len);

	fetcher = palloc0(sizeof(HTTPFetcher));
	fetcher->state = FETCHER_INIT;
	fetcher->conf_bufsize = bufsize;
	fetcher->url = url;
	fetcher->bucket = bucket;
	fetcher->host = host;
	fetcher->cred = cred;
	fetcher->offset = offset;
	fetcher->len = len;

	return fetcher;
}

/*
 * Starts a new HTTP fetch operation.
 *
 * The HTTPFetcher is registered with the given ResourceOwner. Deleting the
 * ResourceOwner will abort the transfer, mark this HTTPFetcher as failed,
 * and release the curl handle.
 */
void
HTTPFetcher_start(HTTPFetcher *fetcher, CURLM *curl_mhandle)
{
	CURL	   *curl;
	char	   *path;
	StringInfoData sbuf;
	HeaderContent headers = { NIL };

	if (!fetcher->readbuf)
	{
		fetcher->bufsize = Max(CURL_MAX_WRITE_SIZE, fetcher->conf_bufsize);
		fetcher->readbuf = palloc(fetcher->bufsize);
	}

	if (fetcher->state != FETCHER_INIT && fetcher->state != FETCHER_FAILED)
		elog(ERROR, "invalid fetcher state");

    curl = fetcher->curl = curl_easy_init();
    if (!curl)
        elog(ERROR, "could not create curl handle");

	curl_multi_add_handle(curl_mhandle, fetcher->curl);
	fetcher->parent = curl_mhandle;

	curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
	// curl_easy_setopt(curl, CURLOPT_PROXY, "127.0.0.1:8080");
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriterCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *) fetcher);
	curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1L);
	HeaderContent_Add(&headers, HOST, fetcher->host);

	curl_easy_setopt(curl, CURLOPT_URL, fetcher->url);

	// consider low speed as timeout
	//curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, s3ext_low_speed_limit);
	//curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, s3ext_low_speed_time);

	if (fetcher->offset + fetcher->bytes_done > 0 || fetcher->len >= 0)
	{
		uint64 offset = fetcher->offset + fetcher->bytes_done;
		int64 len = fetcher->len;
		char		rangebuf[128];

		if (len >= 0)
		{
			/* read 'len' bytes, starting from offset */
			snprintf(rangebuf, sizeof(rangebuf),
					 "bytes=" UINT64_FORMAT "-" UINT64_FORMAT,
					 offset, offset + len - 1);
		}
		else
		{
			/* Read from offset to the end */
			snprintf(rangebuf, sizeof(rangebuf),
					 "bytes=" UINT64_FORMAT "-",
					 offset);
		}
		HeaderContent_Add(&headers, RANGE, rangebuf);
	}

	s3_parse_url(fetcher->url,
				 NULL /* schema */,
				 NULL /* host */,
				 &path,
				 NULL /* fullurl */);

	initStringInfo(&sbuf);
	appendStringInfo(&sbuf, "/%s%s", fetcher->bucket, path);
	SignGETv2(&headers, sbuf.data, fetcher->cred);
	pfree(sbuf.data);
	pfree(path);

	fetcher->httpheaders = HeaderContent_GetList(&headers);
	curl_easy_setopt(fetcher->curl, CURLOPT_HTTPHEADER, fetcher->httpheaders);
	HeaderContent_Destroy(&headers);

	fetcher->state = FETCHER_RUNNING;

	elog(DEBUG1, "starting download from %s (off " UINT64_FORMAT ", len %d)",
		 fetcher->url, fetcher->offset, fetcher->len);
}

static void
HTTPFetcher_cleanup(HTTPFetcher *fetcher)
{
    if (fetcher->curl)
	{
		if (fetcher->parent)
		{
			curl_multi_remove_handle(fetcher->parent, fetcher->curl);
			fetcher->parent = NULL;
		}
		curl_easy_cleanup(fetcher->curl);
		fetcher->curl = NULL;
	}

	if (fetcher->httpheaders)
	{
		curl_slist_free_all(fetcher->httpheaders);
		fetcher->httpheaders = NULL;
	}
}

void
HTTPFetcher_destroy(HTTPFetcher *fetcher)
{
	HTTPFetcher_cleanup(fetcher);

	if (fetcher->readbuf)
		pfree(fetcher->readbuf);

	pfree(fetcher);
}

/*
 * Release resources, and mark the transfer as failed. It can be retried by
 * calling HTTPFetcher_start() again.
 */
static void
HTTPFetcher_fail(HTTPFetcher *fetcher)
{
	HTTPFetcher_cleanup(fetcher);
	fetcher->state = FETCHER_FAILED;
	fetcher->nfailures++;
	fetcher->failed_at = GetCurrentTimestamp();

	elog(DEBUG1, "download from %s (off " UINT64_FORMAT ", len %d) failed",
		 fetcher->url, fetcher->offset, fetcher->len);
}

/*
 * Mark the transfer as completed successfully. This releases the CURL handle and
 * other resources, except for the buffer. You may continue to read remaining unread
 * data with HTTPFetcher_get().
 */
static void
HTTPFetcher_done(HTTPFetcher *fetcher)
{
	HTTPFetcher_cleanup(fetcher);
	fetcher->state = FETCHER_DONE;

	elog(DEBUG1, "download from %s (off " UINT64_FORMAT ", len %d) is complete",
		 fetcher->url, fetcher->offset, fetcher->len);
}

/*
 * Read up to buflen bytes from the source. Will not block; if no data is
 * immediately available, returns 0.
 */
int
HTTPFetcher_get(HTTPFetcher *fetcher, char *buf, int buflen, bool *eof)
{
	int			avail;

retry:
	avail = fetcher->nused - fetcher->readoff;
	if (avail > 0)
	{
		if (avail >  buflen)
			avail = buflen;
		memcpy(buf, fetcher->readbuf + fetcher->readoff, avail);
		fetcher->readoff += avail;
		fetcher->bytes_done += avail;
		*eof = false;
		return avail;
	}

	if (fetcher->paused)
	{
		elog(DEBUG2, "unpaused %s", fetcher->url);
		fetcher->paused = false;
		curl_easy_pause(fetcher->curl, CURLPAUSE_CONT);
		/*
		 * curl_easy_pause() will typically call the write callback immediately,
		 * so loop back to see if we have more data now.
		 */
		goto retry;
	}

	if (fetcher->state == FETCHER_DONE)
	{
		elog(DEBUG2, "EOF from %s (off " UINT64_FORMAT ", len %d)",
			 fetcher->url, fetcher->offset, fetcher->len);
		*eof = true;
		return 0;
	}

	/* Need more data. */
	*eof = false;
	return 0;
}

/*
 * Called by DownloadPipeline when curl_multi_perform() reports that the
 * transfer is complete.
 */
void
HTTPFetcher_handleResult(HTTPFetcher *fetcher, CURLcode res)
{
	Assert(fetcher->curl);
	if (res == CURLE_OK)
	{
		long		respcode;
		CURLcode	eres;

		eres = curl_easy_getinfo(fetcher->curl, CURLINFO_RESPONSE_CODE, &respcode);
		if (eres != CURLE_OK)
			elog(ERROR, "could not get response code from handle %p: %s", fetcher->curl,
				 curl_easy_strerror(eres));

		Assert(respcode != 0);
		if ((respcode != 200) && (respcode != 206))
		{
//                elog(WARNING,
//					 (errmsg("get %.*s, retry", (int) bi.len, data)));
                //bi.len = -1;
			elog(WARNING, "received HTTP code %ld", respcode);
			HTTPFetcher_fail(fetcher);
		}
		else
		{
			/* Success! Note: We might still have unread data in the fetcher buffer */
			HTTPFetcher_done(fetcher);
		}
	}
	else if (res == CURLE_OPERATION_TIMEDOUT)
	{
		elog(WARNING, "net speed is too slow");
		HTTPFetcher_fail(fetcher);
	}
	else
	{
		elog(WARNING, "curl_multi_perform() failed: %s",
			 curl_easy_strerror(res));
		HTTPFetcher_fail(fetcher);
	}
}
