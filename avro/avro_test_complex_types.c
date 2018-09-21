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
char *avro_read_schema_json_file(const char *path);
int avro_test_record_schema();
int avro_test_enum_schema();
int avro_test_fixed_schema();
int avro_test_array_schema();
int avro_test_union_schema();
int avro_test_map_schema();

int main()
{
	avro_test_complex_types();
	return 0;
}

void avro_test_complex_types()
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

char *avro_read_schema_json_file(const char *path)
{
#define SZ_64K (64*1024)
	FILE *fp;
	static char buf[SZ_64K];
	int bytes; 

	fp = fopen(path, "r");
	if (fp == NULL) {
		fprintf(stderr, "Error in fopen\n");
		goto err;
	}
	bytes = fread(buf, sizeof(*buf), SZ_64K, fp);
	if (bytes >= SZ_64K) {
		fprintf(stderr, "Error: json schema size > 64KB\n");
		fclose(fp);
		goto err;
	}
	buf[bytes] = '\0';
	return buf;
err:
	return NULL;
#undef SZ_64K
}

int avro_test_record_schema()
{
	const char *path = "./schema/record_test_simple.json";
	char *json;
	avro_schema_t schema;
	avro_value_iface_t *record_class;
	avro_value_t val;
	avro_value_t field;
	size_t field_cnt;
	avro_file_writer_t writer;

	json = avro_read_schema_json_file(path);
	if (json == NULL) {
		fprintf(stderr, "Error in avro_read_schema_json_file\n");
		goto err;
	}

	try(avro_schema_from_json_length(json, strlen(json), &schema),
						"Error in getting schema from json");
	record_class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(record_class, &val), 
							"Error in getting record\n");
	try(avro_value_get_size(&val, &field_cnt), "Error in getting field count");
	if (field_cnt != 6) {
		fprintf(stderr, "record field cnt mismatch\n");
		goto err;
	}
	try(avro_value_get_by_index(&val, 0, &field, NULL), "cannot get field 0");
	try(avro_value_set_boolean(&field, 1), "cannot set field 0");

	try(avro_value_get_by_index(&val, 1, &field, NULL), "cannot get field 1");
	try(avro_value_set_null(&field), "cannot set field 1");

	try(avro_value_get_by_index(&val, 2, &field, NULL), "cannot get field 2");
	try(avro_value_set_int(&field, 1234), "cannot set field 2");

	try(avro_value_get_by_index(&val, 3, &field, NULL), "cannot get field 3");
	try(avro_value_set_float(&field, 22.714), "cannot set field 3");
	
	char bytes[] = {0xab, 0xcd, 0xef, 0x91};
	try(avro_value_get_by_index(&val, 4, &field, NULL), "cannot get field 4");
	try(avro_value_set_bytes(&field, bytes, sizeof(bytes)),
											"cannot set field 4");

	char *str = "mohit singh";
	try(avro_value_get_by_index(&val, 5, &field, NULL), "cannot get field 5");
	try(avro_value_set_string(&field, str), "cannot set field 5");

	
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in create file writer");
	try(avro_file_writer_append_value(writer, &val),
											"Error in append val\n");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_decref(&field);
	avro_value_iface_decref(record_class);
	avro_schema_decref(schema);

	return 0;
err:
	return 1;
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
