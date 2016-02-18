#ifndef _UTILFUNCTIONS_
#define _UTILFUNCTIONS_

//! return value is malloced
extern char *Base64Encode(const char* buffer, size_t length);

extern bool sha256(const char *str, char outputBuffer[65]);

extern void sha1hmac(const char *str, char out[20], const char *secret);

extern bool sha256hmac(const char *str, char out[65], const char *secret);

// return malloced because Base64Encode
extern char *SignatureV2(const char* date, const char* path, const char* key);
extern char *SignatureV4(const char* date, const char* path, const char* key);

#endif  // _UTILFUNCTIONS_
