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

void
sha256(const char *string, char outputBuffer[65])
{
	unsigned char hash[SHA256_DIGEST_LENGTH];	/* 32 bytes */
	SHA256_CTX sha256;
	int			i;

	SHA256_Init(&sha256);
	SHA256_Update(&sha256, string, strlen(string));
	SHA256_Final(hash, &sha256);

	for (i = 0; i < SHA256_DIGEST_LENGTH; i++)
		sprintf(outputBuffer + (i * 2), "%02x", hash[i]);

	outputBuffer[64] = 0;
}

void
sha256hmac(unsigned char out[S3_SHA256_DIGEST_LENGTH], const char *str, const char *secret, int secretlen)
{
    unsigned int len = 32;
    HMAC_CTX	hmac;

    HMAC_CTX_init(&hmac);
    HMAC_Init_ex(&hmac, secret, secretlen, EVP_sha256(), NULL);
    HMAC_Update(&hmac, (unsigned char *) str, strlen(str));

    HMAC_Final(&hmac, out, &len);

    HMAC_CTX_cleanup(&hmac);
}
