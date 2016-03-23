#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

#include "huawei_lib.h"
#include "supersonic/supersonic.h"

namespace supersonic
{
    int get_time(struct timespec& begin, struct timespec& end)
    {
        return 1000 * (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000;
    }

    const TupleSchema GetSchemaByName(const char* table_name)
    {
        if (strcmp(table_name, "lineitem") == 0)
            return GetLineitemSchema();
        else if (strcmp(table_name, "orders") == 0)
            return GetOrdersSchema();
        else if (strcmp(table_name, "part") == 0)
            return GetPartSchema();
        else if (strcmp(table_name, "customer") == 0)
            return GetCustomerSchema();

        return TupleSchema();
    }

    const TupleSchema GetOrdersSchema()
    {
        TupleSchema orders_schema;

        orders_schema.add_attribute(Attribute("o_orderkey",      INT32,  NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_custkey",       INT32,  NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_orderstatus",   STRING, NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_totalprice",    DOUBLE, NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_orderdate",     DATE,   NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_orderpriority", STRING, NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_clerk",         STRING, NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_shippriority",  INT32,  NOT_NULLABLE));
        orders_schema.add_attribute(Attribute("o_comment",       STRING, NOT_NULLABLE));

        return orders_schema;
    }

    const TupleSchema GetLineitemSchema()
    {
        TupleSchema lineitem_schema;

        lineitem_schema.add_attribute(Attribute("l_orderkey",       INT32,   NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_partkey",        INT32,   NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_suppkey",        INT32,   NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_linenumber",     INT32,   NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_quantity",       DOUBLE,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_extendedprice",  DOUBLE,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_discount",       DOUBLE,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_tax",            DOUBLE,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_returnflag",     STRING,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_linestatus",     STRING,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_shipdate",       DATE,    NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_commitdate",     DATE,    NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_receiptdate",    DATE,    NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_shipinstruct",   STRING,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_shipmode",       STRING,  NOT_NULLABLE));
        lineitem_schema.add_attribute(Attribute("l_comment",        STRING,  NOT_NULLABLE));

        return lineitem_schema;
    }

    const TupleSchema GetPartSchema()
    {
        TupleSchema part_schema;

        part_schema.add_attribute(Attribute("p_partkey",        INT32,  NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_name",           STRING, NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_mfgr",           STRING, NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_brand",          STRING, NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_type",           STRING, NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_size",           INT32,  NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_container",      STRING, NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_retailprice",    DOUBLE, NOT_NULLABLE));
        part_schema.add_attribute(Attribute("p_comment",        STRING, NOT_NULLABLE));

        return part_schema;
    }

    const TupleSchema GetCustomerSchema()
    {
        TupleSchema customer_schema;

        customer_schema.add_attribute(Attribute("c_custkey",    INT32,  NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_name",       STRING, NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_address",    STRING, NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_nationkey",  INT32,  NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_phone",      STRING, NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_acctbal",    DOUBLE, NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_mktsegment", STRING, NOT_NULLABLE));
        customer_schema.add_attribute(Attribute("c_comment",    STRING, NOT_NULLABLE));

        return customer_schema;
    }

    FailureOrVoid LoadFromTableFile(Table* table, const char* table_file)
    {
        const TupleSchema& schema = table->schema();

        FILE* file = fopen(table_file, "rb");

        char buffer[8192] = "";
        rowid_t row_index = 0;

        while (fgets(buffer, 8192, file) != NULL)
        {
            table->AddRow();

            char* pos = strtok(buffer, "|");

            for (int i = 0; i < schema.attribute_count(); i ++)
            {
                switch (schema.attribute(i).type())
                {
                    case INT32:
                        table->Set<INT32>(i, row_index, atoi(pos));
                        break;
                    case DOUBLE:
                        table->Set<DOUBLE>(i, row_index, atof(pos));
                        break;
                    case STRING:
                        table->Set<STRING>(i, row_index, pos);
                        break;
                    case DATE:
                    {
                        pos[7] = pos[4] = '/';
                        scoped_ptr<const Expression> date_or_null(
                                ParseStringNulling(DATE, ConstString(pos)));
                        bool date_is_null = false;
                        FailureOr<int32> date_as_int32 = GetConstantExpressionValue<DATE>(
                                *date_or_null,
                                &date_is_null);
                        PROPAGATE_ON_FAILURE(date_as_int32);
                        table->Set<DATE>(i, row_index, date_as_int32.get());
                        break;
                    }
                    default:
                        THROW(new Exception(ERROR_NOT_IMPLEMENTED,
                                "Unresolvable type in load from table file"));
                }

                pos = strtok(NULL, "|");
            }

            row_index ++;
        }

        fclose(file);

        std::cout << "load table complete: " << row_index << " lines." << std::endl;

        return Success();
    }

    void OffloadToDataFile(const View& view, const char* data_file)
    {
        FILE* file = fopen(data_file, "wb");

        rowcount_t row_count = view.row_count();
        fwrite(&row_count, sizeof(row_count), 1, file);

        scoped_array<int> data_length_array;
        rowcount_t data_length_size = 0;

        scoped_array<char> data(new char[kMaxArenaBufferSize]);

        for (int i = 0; i < view.column_count(); i ++)
        {
            if (!view.column(i).type_info().is_variable_length())
                fwrite(view.column(i).data().raw(),
                        view.column(i).type_info().size(),
                        row_count,
                        file);
            else
            {
                const StringPiece* string_piece_array = view.column(i).variable_length_data();

                for (rowcount_t row_begin = 0; row_begin < row_count;)
                {
                    int data_length_sum = string_piece_array[row_begin].length();
                    rowcount_t row_end = row_begin + 1;
                    for (; row_end < row_count; row_end ++)
                    {
                        if (data_length_sum + string_piece_array[row_end].length() >
                                kMaxArenaBufferSize)
                            break;
                        data_length_sum += string_piece_array[row_end].length();
                    }

                    rowcount_t current_row_count = row_end - row_begin;

                    if (data_length_size < current_row_count)
                    {
                        data_length_size = current_row_count;
                        data_length_array.reset(new int[data_length_size]);
                    }

                    data_length_sum = 0;
                    for (rowcount_t i = 0; i < current_row_count; i ++)
                    {
                        data_length_array[i] = string_piece_array[row_begin + i].length();
                        memcpy(data.get() + data_length_sum,
                                string_piece_array[row_begin + i].data(),
                                data_length_array[i]);
                        data_length_sum += data_length_array[i];
                    }

                    fwrite(&current_row_count, sizeof(current_row_count), 1, file);
                    fwrite(data_length_array.get(), sizeof(int), current_row_count, file);
                    fwrite(&data_length_sum, sizeof(data_length_sum), 1, file);
                    fwrite(data.get(), 1, data_length_sum, file);

                    row_begin = row_end;
                }
            }

            if (view.column(i).attribute().is_nullable())
            {
#if USE_BITS_FOR_IS_NULL_REPRESENTATION == true
                fwrite(view.column(i).is_null(), 1, ((row_count + 511) / 512) * 64, file);
#endif
#if USE_BITS_FOR_IS_NULL_REPRESENTATION == false
                fwrite(view.column(i).is_null(), 1, row_count, file);
#endif
            }
        }

        fclose(file);

        std::cout << "unload table complete: " << row_count << " lines." << std::endl;
    }

    FailureOrVoid LoadFromDataFile(Table *table, const char* data_file)
    {
        Block& block = *table->block();

        FILE* file = fopen(data_file, "rb");

        rowcount_t row_count = 0;
        fread(&row_count, sizeof(row_count), 1, file);
        table->AddRows(row_count);

        scoped_array<int> data_length_array;
        rowcount_t data_length_size = 0;

        for (int i = 0; i < block.column_count(); i ++)
        {
            if (!block.column(i).type_info().is_variable_length())
                fread(block.mutable_column(i)->mutable_data(),
                        block.column(i).type_info().size(),
                        row_count,
                        file);
            else
            {
                StringPiece* string_piece_array =
                        block.mutable_column(i)->mutable_variable_length_data();

                for (rowcount_t row_begin = 0; row_begin < row_count;)
                {
                    rowcount_t current_row_count = 0;
                    fread(&current_row_count, sizeof(current_row_count), 1, file);

                    if (data_length_size < current_row_count)
                    {
                        data_length_size = current_row_count;
                        data_length_array.reset(new int[data_length_size]);
                    }

                    fread(data_length_array.get(), sizeof(int), current_row_count, file);

                    int data_length_sum = 0;
                    fread(&data_length_sum, sizeof(int), 1, file);

                    char *data = (char *) block.mutable_column(i)->arena()->
                            AllocateBytes(data_length_sum);
                    fread(data, 1, data_length_sum, file);

                    string_piece_array[row_begin].set(data, data_length_array[0]);
                    for (rowcount_t j = 1; j < current_row_count; j ++)
                        string_piece_array[row_begin + j].set(
                                string_piece_array[row_begin + j - 1].end(),
                                data_length_array[j]);

                    row_begin += current_row_count;
                }
            }

            if (block.column(i).attribute().is_nullable())
            {
#if USE_BITS_FOR_IS_NULL_REPRESENTATION == true
                fread(block.mutable_column(i)->mutable_is_null(),
                        1,
                        ((row_count + 511) / 512) * 64,
                        file);
#endif
#if USE_BITS_FOR_IS_NULL_REPRESENTATION == false
                fread(block.mutable_column(i)->mutable_is_null(), 1, row_count, file);
#endif
            }
        }

        fclose(file);

        std::cout << "load table complete: " << row_count << " lines." << std::endl;

        return Success();
    }
}
