#include <stdio.h>
#include <stdlib.h>
#include <avro_test.h>

void avro_test_primitive_types();
int avro_test_boolean_schema();
int avro_test_int_schema();
int avro_test_long_schema();
int avro_test_float_schema();
int avro_test_double_schema();
int avro_test_bytes_schema();
int avro_test_string_schema();

int main()
{
	avro_test_primitive_types();
	return 0;
}

void avro_test_primitive_types()
{
	 avro_test_boolean_schema();
	 avro_test_int_schema();
	 avro_test_long_schema();
	 avro_test_float_schema();
	 avro_test_double_schema();
	 avro_test_bytes_schema();
	 avro_test_string_schema();
}

int avro_test_boolean_schema()
{
	avro_schema_t bool_schema = avro_schema_boolean();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_boolean_new(&val, 0), "Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, bool_schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_boolean(&val, 1), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(bool_schema);
	return 0;
}

int avro_test_int_schema() 
{
	avro_schema_t schema = avro_schema_int();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_int_new(&val, 1048576), "Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_int(&val, 222333), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}

int avro_test_long_schema() 
{
	avro_schema_t schema = avro_schema_long();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_long_new(&val, 11112222), "Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_long(&val, 33334444), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}

int avro_test_float_schema() 
{
	avro_schema_t schema = avro_schema_float();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_float_new(&val, 22.71), "Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_float(&val, 33.22), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}

int avro_test_double_schema() 
{
	avro_schema_t schema = avro_schema_double();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_double_new(&val, 1111.2222), "Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_double(&val, 3333.4444), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}

int avro_test_bytes_schema() 
{
	unsigned char bytes1[] = {0xdc, 0x99, 0x98, 0xab};
	unsigned char bytes2[] = {0xab, 0xcd, 0xef, 0x91};
	avro_schema_t schema = avro_schema_bytes();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_bytes_new(&val, bytes1, sizeof(bytes1)),
										"Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_bytes(&val, bytes2, sizeof(bytes2)), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}

int avro_test_string_schema() 
{
	char *str1 = "mohit";
	char *str2 = "singh";
	avro_schema_t schema = avro_schema_string();
	avro_value_t val;
	avro_file_writer_t writer;

	remove(AVRO_DATA_FILE);

	try(avro_generic_string_new(&val, str1), "Error in create boolean\n");
	try(avro_file_writer_create(AVRO_DATA_FILE, schema, &writer),
											"Error in getting file writer");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);

	try(avro_value_reset(&val), "Error in reset val");
	try(avro_value_set_string(&val, str2), "Error in set val");
	try(avro_file_writer_append_value(writer, &val), "Error in append val");
	avro_file_writer_flush(writer);
	avro_file_writer_close(writer);
	
	CHECK_TEST_RESULT();

	avro_value_decref(&val);
	avro_schema_decref(schema);
	return 0;
}
