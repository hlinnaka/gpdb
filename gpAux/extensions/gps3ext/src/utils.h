#ifndef _UTILFUNCTIONS_
#define _UTILFUNCTIONS_

extern void sha256(const char *str, char outputBuffer[65]);

#define S3_SHA256_DIGEST_LENGTH 32

extern void sha256hmac(unsigned char out[S3_SHA256_DIGEST_LENGTH], const char *str, const char *secret, int secretlen);

#endif  // _UTILFUNCTIONS_
