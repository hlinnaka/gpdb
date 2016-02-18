#include <postgres.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <time.h>
#include <string.h>
#include <stdio.h>

#include <openssl/hmac.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/buffer.h>
#include <openssl/sha.h>

#include <curl/curl.h>

#include "utils.h"

/* Encodes a binary safe base 64 string */
char *
Base64Encode(const char *buffer, size_t length)
{
    BIO *bio, *b64;
    BUF_MEM *bufferPtr;
    char *ret;
    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);

    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);  // Ignore newlines - write
                                                 // everything in one line
    BIO_write(bio, buffer, length);
    (void) BIO_flush(bio);
    BIO_get_mem_ptr(bio, &bufferPtr);

    ret = (char *) palloc(bufferPtr->length + 1);
    memcpy(ret, bufferPtr->data, bufferPtr->length);
    ret[bufferPtr->length] = 0;

    (void) BIO_set_close(bio, BIO_NOCLOSE);
    BIO_free_all(bio);
    return ret;  // s
}

bool
sha256(const char *string, char outputBuffer[65])
{
    if (!string) return false;

    unsigned char hash[SHA256_DIGEST_LENGTH];  // 32
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, string, strlen(string));
    SHA256_Final(hash, &sha256);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        sprintf(outputBuffer + (i * 2), "%02x", hash[i]);
    }
    outputBuffer[64] = 0;

    return true;
}

// not returning the normal hex result, might have '\0'
void
sha1hmac(const char *str, char hash[20], const char *secret)
{
    unsigned int len = SHA_DIGEST_LENGTH;  // 20
    HMAC_CTX hmac;

    HMAC_CTX_init(&hmac);
    HMAC_Init_ex(&hmac, secret, strlen(secret), EVP_sha1(), NULL);
    HMAC_Update(&hmac, (unsigned char *)str, strlen(str));
    HMAC_Final(&hmac, (unsigned char *)hash, &len);

    HMAC_CTX_cleanup(&hmac);
}

bool
sha256hmac(const char *str, char out[65], const char *secret)
{
    unsigned char hash[32];  // must be unsigned here
    unsigned int len = 32;
    HMAC_CTX	hmac;
	int			i;

    if (!str)
		return false;

    HMAC_CTX_init(&hmac);
    HMAC_Init_ex(&hmac, secret, strlen(secret), EVP_sha256(), NULL);
    HMAC_Update(&hmac, (unsigned char *)str, strlen(str));

    HMAC_Final(&hmac, hash, &len);
    for (i = 0; i < SHA256_DIGEST_LENGTH; i++)
        sprintf(out + (i * 2), "%02x", hash[i]);

    HMAC_CTX_cleanup(&hmac);
    out[64] = 0;

    return true;
}

// return malloced memory because Base64Encode() does so
char *
SignatureV2(const char *date, const char *path, const char *key)
{
    int			maxlen;
    char	   *tmpbuf;
    char		outbuf[20];  // SHA_DIGEST_LENGTH is 20

    if (!date || !path || !key)
        return NULL;

    maxlen = strlen(date) + strlen(path) + 20;
    tmpbuf = (char *) palloc(maxlen);
    snprintf(tmpbuf, maxlen, "GET\n\n\n%s\n%s", date, path);
    // printf("%s\n",tmpbuf);
    sha1hmac(tmpbuf, outbuf, key);
	pfree(tmpbuf);

    return Base64Encode(outbuf, 20);
}

char *
SignatureV4(const char *date, const char *path, const char *key)
{
    return NULL;
}
