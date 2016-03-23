#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

#define RIGHTROWCOUNT   (1024 * 1024 * 128)
#define LEFTROWCOUNT    (1024 * 1024 * 128)

scoped_ptr<Table> left_table;
scoped_ptr<Table> right_table;

int LoadDataFromDirectory(const char* directory)
{
    scoped_array<int> temp_array(new int[LEFTROWCOUNT]);
    for (int i = 0; i < LEFTROWCOUNT; i ++)
        temp_array[i] = i % RIGHTROWCOUNT;

    for (int i = 0; i < LEFTROWCOUNT; i ++)
    {
        int a = rand() % LEFTROWCOUNT;
        int b = rand() % LEFTROWCOUNT;
        int temp = temp_array[a];
        temp_array[a] = temp_array[b];
        temp_array[b] = temp;
    }

    TupleSchema left_schema;
    left_schema.add_attribute(Attribute("left_key", INT32, NOT_NULLABLE));

    left_table.reset(new Table(left_schema, HeapBufferAllocator::Get()));

    left_table->AddRows(LEFTROWCOUNT);
    for (int i = 0; i < LEFTROWCOUNT; i ++)
        left_table->Set<INT32>(0, i, temp_array[i]);

    temp_array.reset(new int[RIGHTROWCOUNT]);
    for (int i = 0; i < RIGHTROWCOUNT; i ++)
        temp_array[i] = i;

    for (int i = 0; i < RIGHTROWCOUNT; i ++)
    {
        int a = rand() % RIGHTROWCOUNT;
        int b = rand() % RIGHTROWCOUNT;
        int temp = temp_array[a];
        temp_array[a] = temp_array[b];
        temp_array[b] = temp;
    }

    TupleSchema right_schema;
    right_schema.add_attribute(Attribute("right_key", INT32, NOT_NULLABLE));

    right_table.reset(new Table(right_schema, HeapBufferAllocator::Get()));

    right_table->AddRows(RIGHTROWCOUNT);
    for (int i = 0; i < RIGHTROWCOUNT; i ++)
        right_table->Set<INT32>(0, i, temp_array[i]);

    temp_array.reset();

    return 0;
}

Operation* single_thread_operation()
{
    CompoundMultiSourceProjector* result_projector = new CompoundMultiSourceProjector();
    result_projector->add(0, ProjectNamedAttribute("left_key"));
    result_projector->add(1, ProjectNamedAttribute("right_key"));

    scoped_ptr<Operation> hash_joiner(HashJoin(INNER,
            ProjectNamedAttribute("left_key"),
            ProjectNamedAttribute("right_key"),
            result_projector,
            UNIQUE,
            ScanView(left_table->view()),
            ScanView(right_table->view())));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(COUNT, "", "count");

    scoped_ptr<Operation> aggregator(ScalarAggregate(
            aggregation_specification,
            hash_joiner.release()));

    return aggregator.release();
}

Operation* multi_thread_operation(const int thread_count)
{
    scoped_ptr<Operation> distributer(Distribute(thread_count, 0));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM, "count", "count");

    scoped_ptr<Operation> aggregator(ScalarAggregate(
            aggregation_specification,
            distributer.release()));

    return aggregator.release();
}

Operation* supersonic::distribute_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    CompoundMultiSourceProjector* result_projector = new CompoundMultiSourceProjector();
    result_projector->add(0, ProjectNamedAttribute("left_key"));
    result_projector->add(1, ProjectNamedAttribute("right_key"));

    scoped_ptr<Operation> hash_joiner(ParallelHashJoin(INNER,
            ProjectNamedAttribute("left_key"),
            ProjectNamedAttribute("right_key"),
            result_projector,
            UNIQUE,
            ScanPartOfView(left_table->view(), thread_id, thread_count),
            ScanPartOfView(right_table->view(), thread_id, thread_count),
            1,
            thread_id,
            thread_count));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(COUNT, "", "count");

    scoped_ptr<Operation> aggregator(ScalarAggregate(
            aggregation_specification,
            hash_joiner.release()));

    return aggregator.release();
}

Operation* supersonic::sender_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    return NULL;
}
