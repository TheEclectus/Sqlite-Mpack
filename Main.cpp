#include <algorithm>
#include <cstdio>
#include <vector>

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "base64.h"
#include "mpack.h"

void DebugPrintFree(void* Mem)
{
	sqlite3_api->free(Mem);
	//printf_s("Freed sqlite mpack memory.\n");
}

void DebugPrintFreeMpack(void* Mem)
{
	//sqlite3_api->free(Mem);
	free(Mem);
	//printf_s("Freed mpack memory.\n");
}

//#ifdef _DEBUG
//	#undef sqlite3_free
//	#define sqlite3_free DebugPrintFree
//
//	#undef MPACK_FREE
//	#define MPACK_FREE DebugPrintFreeMpack
//#endif // DEBUG



void PrintEachByte(const char* Data, size_t Length)
{
	printf_s("%u [", Length);
	for (size_t i = 0u; i < Length; i++)
	{
		if(i > 0u)
			printf_s(" ");
		printf_s("%hhx", Data[i]);
	}
	printf_s("]\n");
}

#define MpackTypeMismatch(tree, expectedtypestr, node) \
{ \
	char ErrorBuf[256]; \
	memset(ErrorBuf, 0, sizeof(ErrorBuf)); \
	sprintf_s(ErrorBuf, "Expected msgpack type '" expectedtypestr "', got '%s'.", mpack_type_to_string(mpack_node_type(node))); \
	mpack_tree_destroy(& tree); \
	sqlite3_result_error(ctx, ErrorBuf, -1); \
	return; \
}

void StrToMsgPackBlob(sqlite3_context* ctx, int nArgs, sqlite3_value* values[])
{
	if (sqlite3_value_type(values[0]) != SQLITE_TEXT)
	{
		sqlite3_result_error(ctx, "Argument must be TEXT.", -1);
		return;
	}

	const char* Value = (const char*)sqlite3_value_text(values[0]);
	std::string MsgPackData = base64_decode(Value);

	//printf_s("BLOB std::string data: "); /*%s\n", MsgPackData.c_str());*/
	//PrintEachByte(MsgPackData.data(), MsgPackData.size());

	void *DataBuf = sqlite3_malloc(MsgPackData.size());
	memcpy_s(DataBuf, MsgPackData.size(), MsgPackData.data(), MsgPackData.size());

	//printf_s("BLOB data: "); /*%s\n", MsgPackData.c_str());*/
	//PrintEachByte((const char*)DataBuf, MsgPackData.size());

	sqlite3_result_blob(ctx, DataBuf, MsgPackData.size(), DebugPrintFree);
}

void ListFunc(sqlite3_context *ctx, int nArgs, sqlite3_value *values[])
{
	if (sqlite3_value_type(values[0]) != SQLITE_BLOB)
	{
		sqlite3_result_error(ctx, "Argument must be BLOB.", -1);
		return;
	}

	//printf_s("Data format: %d\n", ValueType);

	const void* Value = sqlite3_value_blob(values[0]);
	size_t ValueSize = sqlite3_value_bytes(values[0]);

	//printf_s("Retrieved data: "); // % s\n", Value);
	//PrintEachByte((const char*)Value, ValueSize);

	mpack_tree_t Tree;
	mpack_tree_init_data(&Tree, (const char*)Value, ValueSize);
	mpack_tree_parse(&Tree);
	if (mpack_tree_error(&Tree) != mpack_ok)
	{
		char ErrorBuf[256];
		memset(ErrorBuf, 0, sizeof(ErrorBuf));

		sprintf_s(ErrorBuf, "MPack tree error: %s", mpack_error_to_string(mpack_tree_error(&Tree)));
		mpack_tree_destroy(&Tree);

		sqlite3_result_error(ctx, ErrorBuf, -1);
		return;
	}

	mpack_node_print_to_stdout(mpack_tree_root(&Tree));

	mpack_tree_destroy(&Tree);
}

void SortAndRemoveDuplicates(std::vector<int>& Values)
{
	std::sort(Values.begin(), Values.end());
	Values.erase(std::unique(Values.begin(), Values.end()), Values.end());
}

// Only accept integers for now
void MakeArray(sqlite3_context* ctx, int nArgs, sqlite3_value* values[])
{
	std::vector<int> Values;
	for (int i = 0; i < nArgs; i++)
	{
		if (sqlite3_value_type(values[i]) != SQLITE_INTEGER)
		{
			sqlite3_result_error(ctx, "All arguments must be INTEGERs.", -1);
			return;
		}

		Values.push_back(sqlite3_value_int(values[i]));
	}
	SortAndRemoveDuplicates(Values);

	mpack_writer_t Writer;
	char* Buffer = nullptr;
	size_t BufferSize = 0u;
	mpack_writer_init_growable(&Writer, &Buffer, &BufferSize);
	mpack_start_array(&Writer, Values.size());

	for (size_t i = 0u; i < Values.size(); i++)
	{
		mpack_write_int(&Writer, Values[i]);
	}

	mpack_finish_array(&Writer);

	if (mpack_writer_destroy(&Writer) != mpack_ok)
	{
		sqlite3_result_error(ctx, "Error encoding mpack data.", -1);
		return;
	}

	sqlite3_result_blob(ctx, Buffer, BufferSize, DebugPrintFreeMpack);
}

void MpackArrayContains(sqlite3_context* ctx, int nArgs, sqlite3_value* values[])
{
	if (nArgs < 2)
	{
		sqlite3_result_error(ctx, "Must have at least 2 arguments.", -1);
		return;
	}

	if (sqlite3_value_type(values[0]) != SQLITE_BLOB)
	{
		sqlite3_result_error(ctx, "First argument must be BLOB.", -1);
		return;
	}

	const void* Value = sqlite3_value_blob(values[0]);
	size_t ValueSize = sqlite3_value_bytes(values[0]);

	//printf_s("Retrieved data: "); // % s\n", Value);
	//PrintEachByte((const char*)Value, ValueSize);

	std::vector<int> Values;
	for (int i = 1; i < nArgs; i++)
	{
		if (sqlite3_value_type(values[i]) != SQLITE_INTEGER)
		{
			sqlite3_result_error(ctx, "All arguments beyond the first must be INTEGERs.", -1);
			return;
		}

		Values.push_back(sqlite3_value_int(values[i]));
	}

	SortAndRemoveDuplicates(Values);

	mpack_tree_t Tree;
	mpack_tree_init_data(&Tree, (const char*)Value, ValueSize);
	mpack_tree_parse(&Tree);
	
	auto RootNode = mpack_tree_root(&Tree);
	mpack_type_t RootNodeType = mpack_node_type(RootNode);
	//printf_s("Root type: %s\n", mpack_type_to_string(RootNodeType));
	if (RootNodeType != mpack_type_array)
	{
		char ErrorBuf[256];
		memset(ErrorBuf, 0, sizeof(ErrorBuf));

		sprintf_s(ErrorBuf, "Invalid root node type: Expected msgpack type 'mpack_type_array', got '%s'.", mpack_type_to_string(RootNodeType));
		mpack_tree_destroy(&Tree);

		sqlite3_result_error(ctx, ErrorBuf, -1);
		return;
	}

	size_t ArrayLen = mpack_node_array_length(RootNode);
	size_t SearchArrayIndex = 0u;
	bool bFoundAll = false;

	// OPTIMIZATION: If there are more values to look for than the array has elements, immediately return false.
	//if(ArrayLen < Values.size())

	for (size_t i = 0u; i < ArrayLen; i++)
	{
		auto CurNode = mpack_node_array_at(RootNode, i);
		
		mpack_type_t CurNodeType = mpack_node_type(CurNode);
		//printf_s("Element type: %s\n", mpack_type_to_string(mpack_node_type(CurNode)));
		if (CurNodeType != mpack_type_int && CurNodeType != mpack_type_uint)
		{
			char ErrorBuf[256];
			memset(ErrorBuf, 0, sizeof(ErrorBuf));

			sprintf_s(ErrorBuf, "Invalid array value node type: Expected msgpack type 'mpack_type_int' or 'mpack_type_uint', got '%s'.", mpack_type_to_string(RootNodeType));
			printf_s("%d\n", mpack_tree_destroy(&Tree));

			sqlite3_result_error(ctx, ErrorBuf, -1);
			return;
		}
		
		int CurNodeVal = mpack_node_int(CurNode);
		// Because it's a sorted list, if the value we're looking for is less than the current value being searched for, then it isn't there.
		if (CurNodeVal > Values[SearchArrayIndex])
		{
			/*mpack_tree_destroy(&Tree);

			sqlite3_result_int(ctx, 0);
			return;*/

			break;
		}

		if (CurNodeVal == Values[SearchArrayIndex])
		{
			SearchArrayIndex++;
			if (SearchArrayIndex == Values.size())
			{
				bFoundAll = true;
				break;
			}
		}
	}

	if(mpack_tree_destroy(&Tree) != mpack_ok)
	{
		char ErrorBuf[256];
		memset(ErrorBuf, 0, sizeof(ErrorBuf));

		sprintf_s(ErrorBuf, "MPack tree error: %s", mpack_error_to_string(mpack_tree_error(&Tree)));
		mpack_tree_destroy(&Tree);

		sqlite3_result_error(ctx, ErrorBuf, -1);
		return;
	}

	sqlite3_result_int(ctx, (int)bFoundAll);
}

void MpackArrayHasOneOf(sqlite3_context* ctx, int nArgs, sqlite3_value* values[])
{
	if (nArgs < 2)
	{
		sqlite3_result_error(ctx, "Must have at least 2 arguments.", -1);
		return;
	}

	if (sqlite3_value_type(values[0]) != SQLITE_BLOB)
	{
		sqlite3_result_error(ctx, "First argument must be BLOB.", -1);
		return;
	}

	const void* Value = sqlite3_value_blob(values[0]);
	size_t ValueSize = sqlite3_value_bytes(values[0]);

	//printf_s("Retrieved data: "); // % s\n", Value);
	//PrintEachByte((const char*)Value, ValueSize);

	std::vector<int> Values;
	for (int i = 1; i < nArgs; i++)
	{
		if (sqlite3_value_type(values[i]) != SQLITE_INTEGER)
		{
			sqlite3_result_error(ctx, "All arguments beyond the first must be INTEGERs.", -1);
			return;
		}

		Values.push_back(sqlite3_value_int(values[i]));
	}

	SortAndRemoveDuplicates(Values);

	mpack_tree_t Tree;
	mpack_tree_init_data(&Tree, (const char*)Value, ValueSize);
	mpack_tree_parse(&Tree);

	auto RootNode = mpack_tree_root(&Tree);
	mpack_type_t RootNodeType = mpack_node_type(RootNode);
	//printf_s("Root type: %s\n", mpack_type_to_string(RootNodeType));
	if (RootNodeType != mpack_type_array)
	{
		char ErrorBuf[256];
		memset(ErrorBuf, 0, sizeof(ErrorBuf));

		sprintf_s(ErrorBuf, "Invalid root node type: Expected msgpack type 'mpack_type_array', got '%s'.", mpack_type_to_string(RootNodeType));
		mpack_tree_destroy(&Tree);

		sqlite3_result_error(ctx, ErrorBuf, -1);
		return;
	}

	size_t ArrayLen = mpack_node_array_length(RootNode);
	size_t SearchArrayIndex = 0u;
	bool bFound = false;

	// OPTIMIZATION: If there are more values to look for than the array has elements, immediately return false.
	//if(ArrayLen < Values.size())

	for (size_t i = 0u; i < ArrayLen; i++)
	{
		auto CurNode = mpack_node_array_at(RootNode, i);

		mpack_type_t CurNodeType = mpack_node_type(CurNode);
		//printf_s("Element type: %s\n", mpack_type_to_string(mpack_node_type(CurNode)));
		if (CurNodeType != mpack_type_int && CurNodeType != mpack_type_uint)
		{
			char ErrorBuf[256];
			memset(ErrorBuf, 0, sizeof(ErrorBuf));

			sprintf_s(ErrorBuf, "Invalid array value node type: Expected msgpack type 'mpack_type_int' or 'mpack_type_uint', got '%s'.", mpack_type_to_string(RootNodeType));
			printf_s("%d\n", mpack_tree_destroy(&Tree));

			sqlite3_result_error(ctx, ErrorBuf, -1);
			return;
		}

		int CurNodeVal = mpack_node_int(CurNode);
		// Because it's a sorted list, if the value we're looking for is less than the current value being searched for, then it isn't there.

		while (CurNodeVal > Values[SearchArrayIndex])
		{
			SearchArrayIndex++;
		}

		// If there's a single equivalency, we can return true.
		if (CurNodeVal == Values[SearchArrayIndex])
		{
			bFound = true;
			break;
		}
	}

	if (mpack_tree_destroy(&Tree) != mpack_ok)
	{
		char ErrorBuf[256];
		memset(ErrorBuf, 0, sizeof(ErrorBuf));

		sprintf_s(ErrorBuf, "MPack tree error: %s", mpack_error_to_string(mpack_tree_error(&Tree)));
		mpack_tree_destroy(&Tree);

		sqlite3_result_error(ctx, ErrorBuf, -1);
		return;
	}

	sqlite3_result_int(ctx, (int)bFound);
}

extern "C"
{
	/*
		All functions related to msgpack arrays, stored as a BLOB.

		FUNCTIONS:
		- mpack_b64(text base64_str)
			Decodes msgpack data encoded as a base64 string.

		- mpack_array(int val1, int val2, ..., int valN)
			Returns a msgpack-formatted BLOB containing a sorted array. Duplicate values will be removed.

		- mpack_contains(blob mpack, int val1, int val2, ..., int valN)
			Returns 1 if the msgpack array contains all specified values

		- mpack_contains_some(blob mpack, int val1, int val2, ..., int valN)
			Returns 1 if the msgpack array contains at least one of the specified values
			

		DEBUG:
		- mpack_dbg_list(mpack_blob)
			Prints the contents of mpack_blob to stdout formatted as JSON-like.
	*/
	__declspec(dllexport)
		int sqlite3_extension_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi)
	{
		SQLITE_EXTENSION_INIT2(pApi);

		sqlite3_create_function_v2(db, "mpack_b64", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, StrToMsgPackBlob, nullptr, nullptr, nullptr);
		sqlite3_create_function_v2(db, "mpack_array", -1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, MakeArray, nullptr, nullptr, nullptr);
		sqlite3_create_function_v2(db, "mpack_contains", -1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, MpackArrayContains, nullptr, nullptr, nullptr);
		sqlite3_create_function_v2(db, "mpack_contains_one", -1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, MpackArrayHasOneOf, nullptr, nullptr, nullptr);

		sqlite3_create_function_v2(db, "mpack_dbg_list", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, nullptr, ListFunc, nullptr, nullptr, nullptr);

		return SQLITE_OK;
	}
}