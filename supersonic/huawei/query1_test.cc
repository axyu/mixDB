#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

scoped_ptr<Table> lineitem_table;

int LoadDataFromDirectory(const char* directory)
{
    char file_name[1024];

    lineitem_table.reset(new Table(GetLineitemSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(lineitem_table.get(), strcat(file_name, "lineitem.dat"));

    return 0;
}

Operation* single_thread_operation()
{
    const Expression *filter_expression =
            LessOrEqual(NamedAttribute("l_shipdate"), ConstDate(10471));

    CompoundSingleSourceProjector *filter_projector = new CompoundSingleSourceProjector();
    filter_projector->add(ProjectNamedAttribute("l_quantity"));
    filter_projector->add(ProjectNamedAttribute("l_extendedprice"));
    filter_projector->add(ProjectNamedAttribute("l_discount"));
    filter_projector->add(ProjectNamedAttribute("l_tax"));
    filter_projector->add(ProjectNamedAttribute("l_returnflag"));
    filter_projector->add(ProjectNamedAttribute("l_linestatus"));

    scoped_ptr<Operation> filter(Filter(filter_expression,
            filter_projector,
            ScanView(lineitem_table->view())));

    CompoundExpression *compute_expression1 = new CompoundExpression();
    compute_expression1->Add(NamedAttribute("l_returnflag"));
    compute_expression1->Add(NamedAttribute("l_linestatus"));
    compute_expression1->Add(NamedAttribute("l_quantity"));
    compute_expression1->Add(NamedAttribute("l_extendedprice"));
    compute_expression1->AddAs("disc_price",
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))));
    compute_expression1->AddAs("charge", Multiply(
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))),
            Plus(ConstDouble(1), NamedAttribute("l_tax"))));
    compute_expression1->Add(NamedAttribute("l_discount"));

    scoped_ptr<Operation> computer1(Compute(compute_expression1, filter.release()));

    AggregationSpecification *aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM,      "l_quantity",       "sum_qty");
    aggregation_specification->AddAggregation(SUM,      "l_extendedprice",  "sum_base_price");
    aggregation_specification->AddAggregation(SUM,      "disc_price",       "sum_disc_price");
    aggregation_specification->AddAggregation(SUM,      "charge",           "sum_charge");
    aggregation_specification->AddAggregation(SUM,      "l_discount",       "sum_disc");
    aggregation_specification->AddAggregation(COUNT,    "",                 "count_order");

    CompoundSingleSourceProjector *aggregation_group_projector =
            new CompoundSingleSourceProjector();
    aggregation_group_projector->add(ProjectNamedAttribute("l_returnflag"));
    aggregation_group_projector->add(ProjectNamedAttribute("l_linestatus"));

    scoped_ptr<Operation> aggregator(GroupAggregate(
            aggregation_group_projector,
            aggregation_specification,
            NULL,
            computer1.release()));

    CompoundExpression *compute_expression2 = new CompoundExpression();
    compute_expression2->Add(NamedAttribute("l_returnflag"));
    compute_expression2->Add(NamedAttribute("l_linestatus"));
    compute_expression2->Add(NamedAttribute("sum_qty"));
    compute_expression2->Add(NamedAttribute("sum_base_price"));
    compute_expression2->Add(NamedAttribute("sum_disc_price"));
    compute_expression2->Add(NamedAttribute("sum_charge"));
    compute_expression2->AddAs("avg_qty",
            Divide(NamedAttribute("sum_qty"),           NamedAttribute("count_order")));
    compute_expression2->AddAs("avg_price",
            Divide(NamedAttribute("sum_base_price"),    NamedAttribute("count_order")));
    compute_expression2->AddAs("avg_disc",
            Divide(NamedAttribute("sum_disc"),          NamedAttribute("count_order")));
    compute_expression2->Add(NamedAttribute("count_order"));

    scoped_ptr<Operation> computer2(Compute(compute_expression2, aggregator.release()));

    SortOrder *sort_order = new SortOrder();
    sort_order->add(ProjectNamedAttribute("l_returnflag"), ASCENDING);
    sort_order->add(ProjectNamedAttribute("l_linestatus"), ASCENDING);

    scoped_ptr<Operation> sorter(Sort(sort_order,
            ProjectAllAttributes(),
            128,
            computer2.release()));

    return sorter.release();
}

Operation* multi_thread_operation(const int thread_count)
{
    scoped_ptr<Operation> distributer(Distribute(thread_count, 0));

    AggregationSpecification *aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM, "sum_qty",        "sum_qty");
    aggregation_specification->AddAggregation(SUM, "sum_base_price", "sum_base_price");
    aggregation_specification->AddAggregation(SUM, "sum_disc_price", "sum_disc_price");
    aggregation_specification->AddAggregation(SUM, "sum_charge",     "sum_charge");
    aggregation_specification->AddAggregation(SUM, "sum_disc",       "sum_disc");
    aggregation_specification->AddAggregation(SUM, "count_order",    "count_order");

    CompoundSingleSourceProjector *aggregation_group_projector =
            new CompoundSingleSourceProjector();
    aggregation_group_projector->add(ProjectNamedAttribute("l_returnflag"));
    aggregation_group_projector->add(ProjectNamedAttribute("l_linestatus"));

    scoped_ptr<Operation> aggregator(GroupAggregate(
            aggregation_group_projector,
            aggregation_specification,
            NULL,
            distributer.release()));

    CompoundExpression *compute_expression = new CompoundExpression();
    compute_expression->Add(NamedAttribute("l_returnflag"));
    compute_expression->Add(NamedAttribute("l_linestatus"));
    compute_expression->Add(NamedAttribute("sum_qty"));
    compute_expression->Add(NamedAttribute("sum_base_price"));
    compute_expression->Add(NamedAttribute("sum_disc_price"));
    compute_expression->Add(NamedAttribute("sum_charge"));
    compute_expression->AddAs("avg_qty",
            Divide(NamedAttribute("sum_qty"),           NamedAttribute("count_order")));
    compute_expression->AddAs("avg_price",
            Divide(NamedAttribute("sum_base_price"),    NamedAttribute("count_order")));
    compute_expression->AddAs("avg_disc",
            Divide(NamedAttribute("sum_disc"),          NamedAttribute("count_order")));
    compute_expression->Add(NamedAttribute("count_order"));

    scoped_ptr<Operation> computer(Compute(compute_expression, aggregator.release()));

    SortOrder *sort_order = new SortOrder();
    sort_order->add(ProjectNamedAttribute("l_returnflag"), ASCENDING);
    sort_order->add(ProjectNamedAttribute("l_linestatus"), ASCENDING);

    scoped_ptr<Operation> sorter(Sort(sort_order,
            ProjectAllAttributes(),
            128,
            computer.release()));

    return sorter.release();
}

Operation* supersonic::distribute_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    const Expression *filter_expression =
            LessOrEqual(NamedAttribute("l_shipdate"), ConstDate(10471));

    CompoundSingleSourceProjector *filter_projector = new CompoundSingleSourceProjector();
    filter_projector->add(ProjectNamedAttribute("l_quantity"));
    filter_projector->add(ProjectNamedAttribute("l_extendedprice"));
    filter_projector->add(ProjectNamedAttribute("l_discount"));
    filter_projector->add(ProjectNamedAttribute("l_tax"));
    filter_projector->add(ProjectNamedAttribute("l_returnflag"));
    filter_projector->add(ProjectNamedAttribute("l_linestatus"));

    scoped_ptr<Operation> filter(Filter(filter_expression,
            filter_projector,
            ScanPartOfView(lineitem_table->view(), thread_id, thread_count)));

    CompoundExpression *compute_expression = new CompoundExpression();
    compute_expression->Add(NamedAttribute("l_returnflag"));
    compute_expression->Add(NamedAttribute("l_linestatus"));
    compute_expression->Add(NamedAttribute("l_quantity"));
    compute_expression->Add(NamedAttribute("l_extendedprice"));
    compute_expression->AddAs("disc_price",
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))));
    compute_expression->AddAs("charge", Multiply(
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))),
            Plus(ConstDouble(1), NamedAttribute("l_tax"))));
    compute_expression->Add(NamedAttribute("l_discount"));

    scoped_ptr<Operation> computer(Compute(compute_expression, filter.release()));

    AggregationSpecification *aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM,      "l_quantity",       "sum_qty");
    aggregation_specification->AddAggregation(SUM,      "l_extendedprice",  "sum_base_price");
    aggregation_specification->AddAggregation(SUM,      "disc_price",       "sum_disc_price");
    aggregation_specification->AddAggregation(SUM,      "charge",           "sum_charge");
    aggregation_specification->AddAggregation(SUM,      "l_discount",       "sum_disc");
    aggregation_specification->AddAggregation(COUNT,    "",                 "count_order");

    CompoundSingleSourceProjector *aggregation_group_projector =
            new CompoundSingleSourceProjector();
    aggregation_group_projector->add(ProjectNamedAttribute("l_returnflag"));
    aggregation_group_projector->add(ProjectNamedAttribute("l_linestatus"));

    scoped_ptr<Operation> aggregator(GroupAggregate(
            aggregation_group_projector,
            aggregation_specification,
            NULL,
            computer.release()));

    return aggregator.release();
}

Operation* supersonic::sender_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    return NULL;
}
