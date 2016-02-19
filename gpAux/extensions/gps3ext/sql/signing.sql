/*
 * Unit tests for the AWS V4 signing protocol.
 *
 * The test requests below have been copied from official AWS documentation.
 */
CREATE FUNCTION test_signrequestv4(method text, path text, querystring text, headers text[], payload text, service text, region text, date_time text, accessid text, secret text) RETURNS text AS '$libdir/gps3ext.so', 's3_test_signv4' LANGUAGE C STRICT;

CREATE FUNCTION test_derivesigningkey(secret text, date_s text, region text, service text) RETURNS text AS '$libdir/gps3ext.so', 's3_test_derivesigningkey' LANGUAGE C STRICT;

SELECT test_derivesigningkey('wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY', '20150830', 'us-east-1', 'iam');

/*
GET / HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
*/
SELECT test_signrequestv4('GET', '/', '',
       ARRAY['Host', 'example.amazonaws.com',
             'X-Amz-Date', '20150830T123600Z'],
       '',	/* payload */
       'service',/* service */
       'us-east-1', /* region */
       '20150830T123600Z',
       'AKIDEXAMPLE',
       'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY');


--
-- Query with parameters.
-- FIXME: Not implemented correctly yet. Fortunately, we don't issue
-- queries like this at the moment.
--
/*
GET /?Param1=value2&Param1=Value1 HTTP/1.1
Host:example.amazonaws.com
X-Amz-Date:20150830T123600Z
*/
/*
SELECT test_signrequestv4('GET', '/', 'Param1=value2&Param1=value1',
       '{Host, example.amazonaws.com, X-Amz-Date, 20150830T123600Z}',
       '',	/* payload */
       's3',	/* service */
       'us-east-1', /* region */
       'secret');
*/
