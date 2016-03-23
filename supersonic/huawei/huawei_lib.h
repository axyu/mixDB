#ifndef HUAWEI_LIB_H_
#define HUAWEI_LIB_H_

#define LISTEN_PORT         11280
#define CPU_TCP_ADDRESS     "10.77.110.244"
#define MIC0_TCP_ADDRESS    "172.31.1.1"
#define MIC1_TCP_ADDRESS    "172.31.2.1"
#define CPU_SCIF_ADDRESS    0
#define MIC0_SCIF_ADDRESS   1
#define MIC1_SCIF_ADDRESS   2

namespace supersonic
{
    class FailureOrVoid;
    class Table;
    class TupleSchema;
    class View;

    int get_time(struct timespec& begin, struct timespec& end);

    const TupleSchema GetSchemaByName(const char* table_name);

    const TupleSchema GetOrdersSchema();
    const TupleSchema GetLineitemSchema();
    const TupleSchema GetPartSchema();
    const TupleSchema GetCustomerSchema();

    FailureOrVoid LoadFromTableFile(Table* table, const char* table_file);
    void OffloadToDataFile(const View& view, const char* data_file);
    FailureOrVoid LoadFromDataFile(Table *table, const char* data_file);
}

#endif
