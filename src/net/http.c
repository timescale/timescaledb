#include <postgres.h>

#include "http.h"

static const char *http_version_strings[] = {
	[HTTP_VERSION_10] = "HTTP/1.0",
	[HTTP_VERSION_11] = "HTTP/1.1",
	[HTTP_VERSION_INVALID] = "invalid HTTP version",
};

HttpVersion
http_version_from_string(const char *version)
{
	int			i;

	for (i = 0; i < HTTP_VERSION_INVALID; i++)
		if (pg_strcasecmp(http_version_strings[i], version) == 0)
			return i;

	return HTTP_VERSION_INVALID;
}

const char *
http_version_string(HttpVersion version)
{
	return http_version_strings[version];
}
