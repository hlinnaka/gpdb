#include "postgres.h"

#include "funcapi.h"

#include "access/extprotocol.h"
#include "catalog/pg_proc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "fmgr.h"

#include "S3ExtWrapper.h"
#include "S3Common.h"
#include "utils.h"

/* Do the module magic dance */

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(s3_export);
PG_FUNCTION_INFO_V1(s3_import);
PG_FUNCTION_INFO_V1(s3_validate_urls);

Datum s3_export(PG_FUNCTION_ARGS);
Datum s3_import(PG_FUNCTION_ARGS);
Datum s3_validate_urls(PG_FUNCTION_ARGS);

/*
 * Import data into GPDB.
 */
Datum
s3_import(PG_FUNCTION_ARGS)
{
    S3ExtBase  *myData;
    char	   *data;
    int			data_len;
    size_t		nread = 0;

    /* Must be called via the external table format manager */
    if (!CALLED_AS_EXTPROTOCOL(fcinfo))
        elog(ERROR,
             "extprotocol_import: not called by external protocol manager");

    /* Get our internal description of the protocol */
    myData = (S3ExtBase *)EXTPROTOCOL_GET_USER_CTX(fcinfo);

    if (EXTPROTOCOL_IS_LAST_CALL(fcinfo))
	{
        if (myData)
            S3Reader_destroy(myData);
        PG_RETURN_INT32(0);
    }

    if (myData == NULL)
	{
        /* first call. do any desired init */
        curl_global_init(CURL_GLOBAL_ALL);
        char *url_with_options = EXTPROTOCOL_GET_URL(fcinfo);
        char *url = truncate_options(url_with_options);

        char *config_path = get_opt_s3(url_with_options, "config");
        if (!config_path)
		{
            // no config path in url, use default value
            // data_folder/gpseg0/s3/s3.conf
            config_path = pstrdup("s3/s3.conf");
        }
		
        myData = S3ExtBase_create(url, config_path);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, myData);

        pfree(url);
    }

    /* =======================================================================
     *                            DO THE IMPORT
     * =======================================================================
     */

    data = EXTPROTOCOL_GET_DATABUF(fcinfo);
    data_len = EXTPROTOCOL_GET_DATALEN(fcinfo);
    if (data_len > 0)
	{
        nread = S3Reader_TransferData(myData, data, data_len);
        // S3DEBUG("read %d data from S3", nread);
    }

    PG_RETURN_INT32((int)nread);
}

/*
 * Export data out of GPDB.
 */
Datum
s3_export(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(0);
}

Datum
s3_validate_urls(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}
