#ifndef __S3DOWNLOADPIPELINE_H__
#define __S3DOWNLOADPIPELINE_H__

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <curl/curl.h>

#include "utils/timestamp.h"
#include "utils/resowner.h"

typedef enum
{
	FETCHER_INIT,
	FETCHER_RUNNING,
	FETCHER_FAILED,
	FETCHER_DONE
} FetcherState;

/*
 * Represents one URL to fetch from, and a connection to do so.
 *
 * Initially, when created, the 'curl' handle is NULL, and the HTTPFetcher
 * simply records the intention to download from the given URL. HTTPFetcher_start()
 * opens the connection and starts the download.
 */
typedef struct
{
	FetcherState state;
	
    CURL	   *curl;
	struct curl_slist *httpheaders;
	CURLM	   *parent;
	ResourceOwner *owner;

    const char *url;
	const char *bucket; 		/* bucket name, used to create AWS signature */
	const char *host;
	const char *path;

	S3Credential *cred;

	uint64		offset;
	int64		len;

	uint64		bytes_done;		/* Returned this many bytes to caller (allows retrying from where we left */
	
	HeaderContent headers;

	/* TODO: a ring buffer would be more efficient */
	char	   *readbuf;		/* buffer to read into */
	size_t		bufsize;		/* allocated size (at least CURL_MAX_WRITE_SIZE */
	size_t		nused;			/* amount of valid data in buffer */
	size_t		readoff;		/* how much of the valid data has been consumed */

	/* TODO: Retry support */
	int			nfailures;		/* how many times have we failed at connecting? */
	TimestampTz failed_at;		/* timestamp of last failure */
} HTTPFetcher;

extern HTTPFetcher *HTTPFetcher_create(const char *url, const char *host, const char * bucket,
									   const char *path,
									   S3Credential *cred, int offset, int len);
extern void HTTPFetcher_start(HTTPFetcher *fetcher, CURLM *curl_mhandle);
extern int HTTPFetcher_get(HTTPFetcher *fetcher, char *buf, int buflen, bool *eof);
extern void HTTPFetcher_destroy(HTTPFetcher *fetcher);
extern void HTTPFetcher_handleResult(HTTPFetcher *fetcher, CURLcode res);

/*
 * Represents a list of URLs, which together form the stream to return to the
 * caller of the "external table". 
 */
typedef struct
{
	HTTPFetcher *current_fetcher;

	List	   *pending_fetchers;

	/* Multi-handle that contains the currently active fetcher's CURL handle */
	CURLM	   *curl_mhandle;

	ResourceOwner owner;
} DownloadPipeline;


extern DownloadPipeline *DLPipeline_create(void);
extern int DLPipeline_read(DownloadPipeline *dp, char *buf, int buflen, bool *eof);
extern void DLPipeline_add(DownloadPipeline *dp, HTTPFetcher *fetcher);
extern void DLPipeline_destroy(DownloadPipeline *dp);

#endif
