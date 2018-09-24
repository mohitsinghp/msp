#include <stdio.h>
#include <stdlib.h>
#include <avro_test.h>


void avro_test_complex_types();
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

int avro_test_record_schema()
{
	const char *path = "./schema/record_test_simple.json";
	avro_schema_t schema;
	avro_value_iface_t *class;
	avro_value_t val;
	avro_value_t field;
	size_t field_cnt;

	try(avro_schema_from_json_file(path, &schema),
						"Error in getting schema from json");
	class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(class, &val), 
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

	try(avro_write_data_to_file(&schema, &val), "cannot write data to file");	

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_decref(&field);
	avro_value_iface_decref(class);
	avro_schema_decref(schema);

	return 0;
err:
	return 1;
}

int avro_test_enum_schema()
{
	const char *path = "./schema/enum_test.json";
	avro_schema_t schema;
	avro_file_writer_t writer;
	avro_value_iface_t *class;
	avro_value_t val;
	int i;

	try(avro_schema_from_json_file(path, &schema),
						"Error in getting schema from json");
	class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(class, &val), 
							"Error in getting record\n");
	
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in create file writer");
	for(i = 0; i < 4; i++) {
		try(avro_value_set_enum(&val, i), "cannot set enum value");
		try(avro_file_writer_append_value(writer, &val),
				"Error in append val\n");
		avro_file_writer_flush(writer);
		try(avro_value_reset(&val), "cannot reset value");
	}
	avro_file_writer_close(writer);

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_iface_decref(class);
	avro_schema_decref(schema);

	return 0;
}

int avro_test_fixed_schema()
{
	const char *path = "./schema/fixed_test.json";
	avro_schema_t schema;
	avro_value_iface_t *class;
	avro_value_t val;
	unsigned char bytes[] = {0xab, 0xcd, 0xef, 0x91};

	try(avro_schema_from_json_file(path, &schema),
						"Error in getting schema from json");
	class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(class, &val), 
							"Error in getting record\n");
	try(avro_value_set_fixed(&val, bytes, sizeof(bytes)), 
								"cannot set fixed value");

	try(avro_write_data_to_file(&schema, &val), "cannot write data to file");	

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_iface_decref(class);
	avro_schema_decref(schema);

	return 0;
}

int avro_test_array_schema()
{
#define ARRAY_SZ 8
	const char *path = "./schema/array_int_test.json";
	avro_schema_t schema;
	avro_value_iface_t *class;
	avro_value_t val;
	avro_value_t elem;
	unsigned int i;
	size_t cnt;

	try(avro_schema_from_json_file(path, &schema),
						"Error in getting schema from json");
	class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(class, &val), 
							"Error in getting record\n");
	try(avro_generic_int_new(&elem, 0), "Error in getting new int");
	for (i = 0; i < ARRAY_SZ; i++) {
		try(avro_value_append(&val, &elem, &cnt),
								"cannot append value to array");
		try(avro_value_set_int(&elem, i),
								"cannot set int value");
		try((cnt != i), "unexpected index");
	}
	try(avro_value_get_size(&val, &cnt), "cannot get array size");
	try((cnt != ARRAY_SZ), "array size mismatch");
	try(avro_write_data_to_file(&schema, &val), "cannot write data to file");	

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_decref(&elem);
	avro_value_iface_decref(class);
	avro_schema_decref(schema);

#undef ARRAY_SZ
	return 0;
}

int avro_test_union_schema()
{
	const char *path = "./schema/union_test.json";
	avro_schema_t schema;
	avro_value_iface_t *class;
	avro_file_writer_t writer;
	avro_value_t val;
	avro_value_t branch;

	try(avro_schema_from_json_file(path, &schema),
						"Error in getting schema from json");
	class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(class, &val), 
							"Error in getting record\n");
	
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in create file writer");
	
	try(avro_value_set_branch(&val, 0, &branch), "cannot set branch");
	try(avro_value_set_null(&branch), "cannot set null branch value");
	try(avro_file_writer_append_value(writer, &val),
											"Error in append val\n");
	
	try(avro_value_set_branch(&val, 1, &branch), "cannot set branch");
	try(avro_value_set_int(&branch, 1234), "cannot set int branch value");
	try(avro_file_writer_append_value(writer, &val),
											"Error in append val\n");
	
	try(avro_value_set_branch(&val, 2, &branch), "cannot set branch");
	try(avro_value_set_float(&branch, 22.7), "cannot set float branch value");
	try(avro_file_writer_append_value(writer, &val),
											"Error in append val\n");
	
	try(avro_value_set_branch(&val, 3, &branch), "cannot set branch");
	try(avro_value_set_string(&branch, "mohitsingh"),
										"cannot set srting branch value");
	try(avro_file_writer_append_value(writer, &val),
											"Error in append val\n");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_iface_decref(class);
	avro_schema_decref(schema);

	return 0;
	return 0;
}

int avro_test_map_schema()
{
#define MAP_SZ 4
	const char *path = "./schema/map_test.json";
	avro_schema_t schema;
	avro_value_iface_t *class;
	avro_value_t val;
	avro_value_t element;
	char *fruits[] = {"apple", "orange", "mango", "banana"};
	int i;
	char key[16];

	try(avro_schema_from_json_file(path, &schema),
						"Error in getting schema from json");
	class = avro_generic_class_from_schema(schema);
	try(avro_generic_value_new(class, &val), 
							"Error in getting record\n");

	try(avro_generic_string_new(&element, "test_str"), "cannot get string value");
	for (i = 0; i < MAP_SZ; i++)  {
		snprintf(key, sizeof(key), "fruit_%d", i);
		try(avro_value_add(&val, key, &element, NULL, NULL),
										"cannot add value to map");
		try(avro_value_set_string(&element, fruits[i]), "cannot set string");
	}
	try(avro_write_data_to_file(&schema, &val), "cannot write data to file");	

	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_value_iface_decref(class);
	avro_schema_decref(schema);

	return 0;
#undef MAP_SZ
}
