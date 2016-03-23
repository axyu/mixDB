#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

#define ROWCOUNT   (1024 * 1024 * 480)

scoped_ptr<Table> table;

int LoadDataFromDirectory(const char* directory)
{
    TupleSchema schema;
    schema.add_attribute(Attribute("value", DOUBLE, NOT_NULLABLE));

    table.reset(new Table(schema, HeapBufferAllocator::Get()));

    table->AddRows(ROWCOUNT);
    for (int i = 0; i < ROWCOUNT; i ++)
        table->Set<DOUBLE>(0, i, i);

    return 0;
}

Operation* single_thread_operation()
{
    CompoundExpression* compute_expression = new CompoundExpression();
    compute_expression->AddAs("value", Plus(
            Multiply(NamedAttribute("value"), ConstDouble(2)),
            ConstDouble(1)));

    scoped_ptr<Operation> computer(Compute(compute_expression, ScanView(table->view())));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(COUNT, "", "count");

    scoped_ptr<Operation> aggregator(ScalarAggregate(
            aggregation_specification,
            computer.release()));

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
    CompoundExpression* compute_expression = new CompoundExpression();
    compute_expression->AddAs("value", Plus(
            Multiply(NamedAttribute("value"), ConstDouble(1)),
            ConstDouble(1)));

    scoped_ptr<Operation> computer(Compute(compute_expression,
            ScanPartOfView(table->view(), thread_id, thread_count)));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(COUNT, "", "count");

    scoped_ptr<Operation> aggregator(ScalarAggregate(
            aggregation_specification,
            computer.release()));

    return aggregator.release();
}

Operation* supersonic::sender_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    return NULL;
}
