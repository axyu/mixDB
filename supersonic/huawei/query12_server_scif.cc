#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

scoped_ptr<Table> lineitem_table;
scoped_ptr<Table> orders_table;

int LoadDataFromDirectory(const char* directory)
{
    char file_name[1024];

    lineitem_table.reset(new Table(GetLineitemSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(lineitem_table.get(), strcat(file_name, "lineitem.dat"));

    orders_table.reset(new Table(GetOrdersSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(orders_table.get(), strcat(file_name, "orders.dat"));

    return 0;
}

Operation* supersonic::sender_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    if ((info >> 16) == 0)
    {
        scoped_ptr<Operation> fetcher(FetchSCIF(MIC0_SCIF_ADDRESS,
                    LISTEN_PORT,
                    0,
                    1,
                    (1 << 16) | (info & 0xffff)));

        AggregationSpecification* aggregation_specification = new AggregationSpecification();
        aggregation_specification->AddAggregation(SUM, "high_line_count", "high_line_count");
        aggregation_specification->AddAggregation(SUM, "low_line_count",  "low_line_count");

        scoped_ptr<Operation> aggregator(GroupAggregate(ProjectNamedAttribute("l_shipmode"),
                aggregation_specification,
                NULL,
                fetcher.release()));

        SortOrder* sort_order = new SortOrder();
        sort_order->add(ProjectNamedAttribute("l_shipmode"), ASCENDING);

        scoped_ptr<Operation> sorter(Sort(sort_order,
                ProjectAllAttributes(),
                128,
                aggregator.release()));

        return sorter.release();
    }

    if ((info >> 16) == 1)
        return Distribute((info & 0xffff), 0);

    if ((info >> 16) == 2)
    {
        CompoundSingleSourceProjector* lineitem_projector = new CompoundSingleSourceProjector();
        lineitem_projector->add(ProjectNamedAttribute("l_orderkey"));
        lineitem_projector->add(ProjectNamedAttribute("l_shipdate"));
        lineitem_projector->add(ProjectNamedAttribute("l_commitdate"));
        lineitem_projector->add(ProjectNamedAttribute("l_receiptdate"));
        lineitem_projector->add(ProjectNamedAttribute("l_shipmode"));

        scoped_ptr<Operation> projector(Project(lineitem_projector,
                ScanPartOfView(lineitem_table->view(), thread_id, thread_count)));

        return projector.release();
    }

    if ((info >> 16) == 3)
    {
        CompoundSingleSourceProjector* orders_projector = new CompoundSingleSourceProjector();
        orders_projector->add(ProjectNamedAttribute("o_orderkey"));
        orders_projector->add(ProjectNamedAttribute("o_orderpriority"));

        scoped_ptr<Operation> projector(Project(orders_projector,
                ScanPartOfView(orders_table->view(), thread_id, thread_count)));

        return projector.release();
    }

    return NULL;
}

Operation* supersonic::distribute_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    scoped_ptr<Operation> lineitem_fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
                LISTEN_PORT,
                thread_id,
                thread_count,
                (2 << 16)));

    scoped_ptr<Operation> orders_fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
                LISTEN_PORT,
                thread_id,
                thread_count,
                (3 << 16)));

    ExpressionList* lineitem_filter_in_expression_list = new ExpressionList();
    lineitem_filter_in_expression_list->add(ConstString("MAIL"));
    lineitem_filter_in_expression_list->add(ConstString("SHIP"));

    const Expression* lineitem_filter_expression1 =
            In(NamedAttribute("l_shipmode"), lineitem_filter_in_expression_list);
    const Expression* lineitem_filter_expression2 =
            Less(NamedAttribute("l_commitdate"), NamedAttribute("l_receiptdate"));
    const Expression* lineitem_filter_expression3 =
            Less(NamedAttribute("l_shipdate"), NamedAttribute("l_commitdate"));
    const Expression* lineitem_filter_expression4 =
            GreaterOrEqual(NamedAttribute("l_receiptdate"), ConstDate(8766));
    const Expression* lineitem_filter_expression5 =
            Less(NamedAttribute("l_receiptdate"), ConstDate(9131));
    const Expression* lineitem_filter_expression = And(
            And(And(lineitem_filter_expression1, lineitem_filter_expression2),
                    And(lineitem_filter_expression3, lineitem_filter_expression4)),
            lineitem_filter_expression5);

    CompoundSingleSourceProjector* lineitem_filter_projector =
            new CompoundSingleSourceProjector();
    lineitem_filter_projector->add(ProjectNamedAttribute("l_orderkey"));
    lineitem_filter_projector->add(ProjectNamedAttribute("l_shipmode"));

    scoped_ptr<Operation> lineitem_filter(Filter(lineitem_filter_expression,
            lineitem_filter_projector,
            lineitem_fetcher.release()));

    ExpressionList* compute_case_expression_list1 = new ExpressionList();
    compute_case_expression_list1->add(Or(
            Equal(NamedAttribute("o_orderpriority"), ConstString("1-URGENT")),
            Equal(NamedAttribute("o_orderpriority"), ConstString("2-HIGH"))));
    compute_case_expression_list1->add(ConstInt32(0));
    compute_case_expression_list1->add(ConstBool(true));
    compute_case_expression_list1->add(ConstInt32(1));
    compute_case_expression_list1->add(ConstBool(false));
    compute_case_expression_list1->add(ConstInt32(0));

    ExpressionList* compute_case_expression_list2 = new ExpressionList();
    compute_case_expression_list2->add(And(
            NotEqual(NamedAttribute("o_orderpriority"), ConstString("1-URGENT")),
            NotEqual(NamedAttribute("o_orderpriority"), ConstString("2-HIGH"))));
    compute_case_expression_list2->add(ConstInt32(0));
    compute_case_expression_list2->add(ConstBool(true));
    compute_case_expression_list2->add(ConstInt32(1));
    compute_case_expression_list2->add(ConstBool(false));
    compute_case_expression_list2->add(ConstInt32(0));

    CompoundExpression* compute_expression = new CompoundExpression();
    compute_expression->Add(NamedAttribute("o_orderkey"));
    compute_expression->AddAs("high_line", Case(compute_case_expression_list1));
    compute_expression->AddAs("low_line",  Case(compute_case_expression_list2));

    scoped_ptr<Operation> computer(Compute(compute_expression, orders_fetcher.release()));

    const SingleSourceProjector* lhs_key_selector = ProjectNamedAttribute("l_orderkey");
    const SingleSourceProjector* rhs_key_selector = ProjectNamedAttribute("o_orderkey");

    CompoundSingleSourceProjector* rhs_projector = new CompoundSingleSourceProjector();
    rhs_projector->add(ProjectNamedAttribute("high_line"));
    rhs_projector->add(ProjectNamedAttribute("low_line"));

    CompoundMultiSourceProjector* result_projector = new CompoundMultiSourceProjector();
    result_projector->add(0, ProjectNamedAttribute("l_shipmode"));
    result_projector->add(1, rhs_projector);

    scoped_ptr<Operation> hash_joiner(ParallelHashJoin(INNER,
            lhs_key_selector,
            rhs_key_selector,
            result_projector,
            UNIQUE,
            lineitem_filter.release(),
            computer.release(),
            1,
            thread_id,
            thread_count));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM, "high_line", "high_line_count");
    aggregation_specification->AddAggregation(SUM, "low_line",  "low_line_count");

    scoped_ptr<Operation> aggregator(GroupAggregate(ProjectNamedAttribute("l_shipmode"),
            aggregation_specification,
            NULL,
            hash_joiner.release()));

    return aggregator.release();
}
