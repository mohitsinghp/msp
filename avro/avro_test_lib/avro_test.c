#include "avro_test.h"

int avro_read_data_from_file()
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

int avro_write_data_to_file(avro_schema_t *schema, avro_value_t *val)
{
	avro_file_writer_t writer;
	
	try(avro_file_writer_create(AVRO_DATA_FILE, *schema, &writer),
											"Error in create file writer");
	try(avro_file_writer_append_value(writer, val),
											"Error in append val\n");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	return EXIT_SUCCESS;
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

