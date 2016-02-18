/*
 * DownloadPipeline is an object that manages the download of a bunch of files (or
 * ranges within files). Reading from the pipeline (with DLPipeline_read()) will
 * return the contents of each file in the given order, similar to "cat file1 file2 ..."
 *
 * Usage:
 *
 * First, create a new pipeline with DLPipeline_create(). Then add as many
 * fetchers to it as you like, with DLPipeline_add(). This will simply enlist the
 * URLs with the pipeline, it will not start any transfers yet. To perform the
 * transfers, call DLPipeline_read() until it returns with *eof_p = true.
 */
#include "postgres.h"

#include "S3Downloader.h"
#include "DownloadPipeline.h"

#include "utils.h"
#include <curl/curl.h>

#include "utils/memutils.h"

/*
 * ResourceOwner callback.
 *
 * To make sure we don't leak any CURL handles, pipelines are registered with a
 * ResourceOwner. Destroying the ResourceOwner will call the callback, and
 * release all resource. (Memory is handled separately: it is released by destroying
 * the containing MemoryContext.)
 */
static void
DLPipeline_ReleaseCallback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel,
				void *arg)
{
	DownloadPipeline *dp = (DownloadPipeline *) arg;

	if (dp->current_fetcher->state == FETCHER_RUNNING)
	{
		/*
		 * At commit, all the fetcher's should've been closed gracefully
		 * already.
		 */
		if (isCommit)
			elog(WARNING, "HTTPFetcher leaked");

		HTTPFetcher_destroy(dp->current_fetcher);
		dp->current_fetcher = NULL;
	}
}

DownloadPipeline *
DLPipeline_create(void)
{
	DownloadPipeline *dp;

	dp = palloc0(sizeof(DownloadPipeline));
	dp->current_fetcher = NULL;
	dp->pending_fetchers = NIL;
	dp->curl_mhandle = NULL;

	/* Register with the resource owner */
	dp->owner = CurrentResourceOwner;
	RegisterResourceReleaseCallback(DLPipeline_ReleaseCallback, dp);

	dp->curl_mhandle = curl_multi_init();
	if (dp->curl_mhandle == NULL)
		elog(ERROR, "could not create CURL multi-handle");

	return dp;
}

/*
 * Try to get at must buflen bytes from pipeline. Blocks.
 */
int
DLPipeline_read(DownloadPipeline *dp, char *buf, int buflen, bool *eof_p)
{
	HTTPFetcher *fetcher;
	int r;
	bool		eof;

	for (;;)
	{
		if (dp->current_fetcher == NULL)
		{
			/* Get next fetcher from list. */
			if (dp->pending_fetchers == NIL)
			{
				*eof_p = true;
				return 0;
			}

			dp->current_fetcher = linitial(dp->pending_fetchers);
			dp->pending_fetchers = list_delete_first(dp->pending_fetchers);

			/* Start up this fetcher */
			HTTPFetcher_start(dp->current_fetcher, dp->curl_mhandle);
		}
		fetcher = dp->current_fetcher;

		/* Try to read from the current fetcher */
		r = HTTPFetcher_get(fetcher, buf, buflen, &eof);
		if (eof)
		{
			HTTPFetcher_destroy(fetcher);
			dp->current_fetcher = NULL;
			continue;
		}

		if (r == 0 && fetcher->state == FETCHER_FAILED)
		{
			long		secs;
			int	   		microsecs;
			/* Consider restarting this */

			TimestampDifference(fetcher->failed_at, GetCurrentTimestamp(),
								&secs, &microsecs);
			if (secs > FETCH_RETRY_DELAY)
			{
				elog(INFO, "retrying now");
				HTTPFetcher_start(dp->current_fetcher, dp->curl_mhandle);
			}
			return -1;
		}
		else if (!eof && r == 0)
		{
			int		running_handles;
			struct CURLMsg *m;

			/* Got nothing. Wait until something happens */
			curl_multi_wait(dp->curl_mhandle, NULL, 0, 1000, NULL);

			curl_multi_perform(dp->curl_mhandle, &running_handles);
 
			do
			{
				int			msgq = 0;
				m = curl_multi_info_read(dp->curl_mhandle, &msgq);

				if (m && (m->msg == CURLMSG_DONE))
				{
					Assert(m->easy_handle == fetcher->curl);

					HTTPFetcher_handleResult(fetcher, m->data.result);
				}
			} while(m);

			continue;
		}
		else
		{
			*eof_p = eof;
			return r;
		}
	}
}

void
DLPipeline_add(DownloadPipeline *dp, HTTPFetcher *fetcher)
{
	dp->pending_fetchers = lappend(dp->pending_fetchers, fetcher);
}

void
DLPipeline_destroy(DownloadPipeline *dp)
{
	if (dp->current_fetcher)
	{
		HTTPFetcher_destroy(dp->current_fetcher);
		dp->current_fetcher = NULL;
	}
	if (dp->curl_mhandle)
		curl_multi_cleanup(dp->curl_mhandle);

	if (dp->owner)
		UnregisterResourceReleaseCallback(DLPipeline_ReleaseCallback, dp);

	pfree(dp);
}
