#include <avro.h>

#define AVRO_DATA_FILE "data.avro"
#define AVRO_FILE_CODEC "null"

int avro_read_data_from_file();
int avro_write_data_to_file(avro_schema_t *schema, avro_value_t *val);
char *avro_read_schema_json_file(const char *path);

#define try(func, msg) \
do { \
	if (func) { \
		fprintf(stderr, msg ":\n  %s\n", avro_strerror()); \
			return EXIT_FAILURE; \
	} \
} while (0)

#define CHECK_TEST_RESULT() \
do { \
	if (avro_read_data_from_file()) { \
		fprintf(stderr, "Test: %s - FAILED\n", __FUNCTION__); \
	} else { \
		printf("Test: %s - PASSED\n", __FUNCTION__); \
	} \
} while (0)

