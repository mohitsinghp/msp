#include <stdio.h>
#include <stdlib.h>
#include <avro.h>

#define AVRO_DATA_FILE "data.avro"
#define AVRO_FILE_CODEC "null"

#define try(func, msg) \
do { \
	if (func) { \
		fprintf(stderr, msg ":\n  %s\n", avro_strerror()); \
			return EXIT_FAILURE; \
	} \
} while (0)

#define CHECK_TEST_RESULT() \
do { \
	if (avro_read_data_file()) { \
		fprintf(stderr, "Test: %s - FAILED\n", __FUNCTION__); \
	} else { \
		printf("Test: %s - PASSED\n", __FUNCTION__); \
	} \
} while (0)

void avro_test_complex_types();
int avro_read_data_file();
int avro_test_record_schema();
int avro_test_enum_schema();
int avro_test_fixed_schema();
int avro_test_array_schema();
int avro_test_union_schema();
int avro_test_map_schema();

int main()
{
	avro_test_primitive_types();
	return 0;
}

void avro_test_primitive_types()
{
	 avro_test_record_schema();
	 avro_test_enum_schema();
	 avro_test_fixed_schema();
	 avro_test_array_schema();
	 avro_test_union_schema();
	 avro_test_map_schema();
}

int avro_read_data_file()
{
	char *cmd_avrocat = "avrocat ./"AVRO_DATA_FILE;
	char *cmd_hexdump = "hexdump -C ./"AVRO_DATA_FILE;
	int ret;
	printf("### avrocat output ###\n");
	ret = system(cmd_avrocat);
	printf("### hexdump output ###\n");
	(void) system(cmd_hexdump);
	return ret;
}

int avro_test_record_schema()
{
	return 0;
}

int avro_test_enum_schema()
{
	return 0;
}
int avro_test_fixed_schema()
{
	return 0;
}

int avro_test_array_schema()
{
	return 0;
}

int avro_test_union_schema()
{
	return 0;
}

int avro_test_map_schema()
{
	return 0;
}
