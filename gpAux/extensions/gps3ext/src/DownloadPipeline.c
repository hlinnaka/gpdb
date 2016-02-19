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

#include "s3operations.h"
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

	while (dp->active_fetchers != NIL)
	{
		HTTPFetcher *f;

		f = linitial(dp->active_fetchers);
		dp->active_fetchers = list_delete_first(dp->active_fetchers);

		if (f->state == FETCHER_RUNNING)
		{
			/*
			 * At commit, all the fetcher's should've been closed gracefully
			 * already.
			 */
			if (isCommit)
				elog(WARNING, "HTTPFetcher leaked");

			HTTPFetcher_destroy(f);
		}
	}
}

DownloadPipeline *
DLPipeline_create(int nconnections)
{
	DownloadPipeline *dp;

	dp = palloc0(sizeof(DownloadPipeline));
	dp->active_fetchers = NIL;
	dp->pending_fetchers = NIL;
	dp->curl_mhandle = NULL;

	Assert(nconnections > 0);
	dp->nconnections = nconnections;

	/* Register with the resource owner */
	dp->owner = CurrentResourceOwner;
	RegisterResourceReleaseCallback(DLPipeline_ReleaseCallback, dp);

	dp->curl_mhandle = curl_multi_init();
	if (dp->curl_mhandle == NULL)
		elog(ERROR, "could not create CURL multi-handle");

	return dp;
}

/*
 * If there are less than the requested number of downloads active currently,
 * launch more from the pending list.
 */
static void
DLPipeline_launch(DownloadPipeline *dp)
{
	while (list_length(dp->active_fetchers) < dp->nconnections)
	{
		HTTPFetcher *fetcher;

		/* Get next fetcher from pending list */
		if (dp->pending_fetchers == NIL)
			break;
		fetcher = linitial(dp->pending_fetchers);

		/* Move it to the active list, and start it  */	
		dp->active_fetchers = lappend(dp->active_fetchers, fetcher);
		dp->pending_fetchers = list_delete_first(dp->pending_fetchers);

		HTTPFetcher_start(fetcher, dp->curl_mhandle);
	}
}

/*
 * Try to get at must buflen bytes from pipeline. Blocks.
 */
int
DLPipeline_read(DownloadPipeline *dp, char *buf, int buflen, bool *eof_p)
{
	HTTPFetcher *current_fetcher;
	int			r;
	bool		eof;

	for (;;)
	{
		/* launch more connections if needed */
		DLPipeline_launch(dp);

		if (dp->active_fetchers == NIL)
		{
			/*
			 * DLPipeline_launch() should've launched at least one fetcher,
			 * if there were any still pending.
			 */
			Assert(dp->pending_fetchers == NIL);
			elog(DEBUG1, "all downloads completed");
			*eof_p = true;
			return 0;
		}
		current_fetcher = (HTTPFetcher *) linitial(dp->active_fetchers);

		/* Try to read from the current fetcher */
		r = HTTPFetcher_get(current_fetcher, buf, buflen, &eof);
		if (eof)
		{
			Assert(r == 0);
			HTTPFetcher_destroy(current_fetcher);
			dp->active_fetchers = list_delete_first(dp->active_fetchers);
			continue;
		}

		if (r > 0)
		{
			/* got some data */
			*eof_p = false;
			return r;
		}

		/*
		 * Consider restarting any failed fetchers.
		 * TODO: right now, we only look at the first in the queue, not the
		 * prefetching ones. Hopefully requests don't fail often enough for that to
		 * matter.
		 */
		if (current_fetcher->state == FETCHER_FAILED)
		{
			long		secs;
			int	   		microsecs;

			if (current_fetcher->nfailures >= MAX_FETCH_RETRIES)
				ereport(ERROR,
						(errmsg("could not download \"%s\", %d failed attempts",
								current_fetcher->url, current_fetcher->nfailures)));

			TimestampDifference(current_fetcher->failed_at, GetCurrentTimestamp(),
								&secs, &microsecs);
			if (secs > FETCH_RETRY_DELAY)
			{
				elog(DEBUG1, "retrying now");
				HTTPFetcher_start(current_fetcher, dp->curl_mhandle);
			}
		}

		{
			int		running_handles;
			struct CURLMsg *m;
			CURLMcode mres;

			/* Got nothing. Wait until something happens */
			mres = curl_multi_wait(dp->curl_mhandle, NULL, 0, 1000, NULL);
			if (mres != CURLM_OK)
				elog(ERROR, "curl_multi_perform returned error: %s",
					 curl_multi_strerror(mres));

			mres = curl_multi_perform(dp->curl_mhandle, &running_handles);
			if (mres != CURLM_OK)
				elog(ERROR, "curl_multi_perform returned error: %s",
					 curl_multi_strerror(mres));

			do
			{
				int			msgq = 0;
				m = curl_multi_info_read(dp->curl_mhandle, &msgq);

				if (m && (m->msg == CURLMSG_DONE))
				{
					/* find the fetcher this belongs to */
					ListCell   *lc;
					bool		found = false;

					Assert(m->easy_handle != NULL);

					foreach(lc, dp->active_fetchers)
					{
						HTTPFetcher *f = (HTTPFetcher *) lfirst(lc);

						if (f->curl == m->easy_handle)
						{
							found = true;
							HTTPFetcher_handleResult(f, m->data.result);
							break;
						}
					}
					if (!found)
						elog(ERROR, "got CURL result code for a fetcher that's not active");
				}
			} while(m);

			continue;
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
	while (dp->active_fetchers != NIL)
	{
		HTTPFetcher *f;

		f = linitial(dp->active_fetchers);
		dp->active_fetchers = list_delete_first(dp->active_fetchers);

		HTTPFetcher_destroy(f);
	}
	if (dp->curl_mhandle)
		curl_multi_cleanup(dp->curl_mhandle);

	if (dp->owner)
		UnregisterResourceReleaseCallback(DLPipeline_ReleaseCallback, dp);

	pfree(dp);
}
