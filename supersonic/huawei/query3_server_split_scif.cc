#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

scoped_ptr<Table> lineitem_table;
scoped_ptr<Table> orders_table;
scoped_ptr<Table> customer_table;

int LoadDataFromDirectory(const char* directory)
{
    char file_name[1024];

    lineitem_table.reset(new Table(GetLineitemSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(lineitem_table.get(), strcat(file_name, "lineitem.dat"));

    orders_table.reset(new Table(GetOrdersSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(orders_table.get(), strcat(file_name, "orders.dat"));

    customer_table.reset(new Table(GetCustomerSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(customer_table.get(), strcat(file_name, "customer.dat"));

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
        aggregation_specification->AddAggregation(SUM, "revenue", "revenue");

        CompoundSingleSourceProjector* group_projector = new CompoundSingleSourceProjector();
        group_projector->add(ProjectNamedAttribute("l_orderkey"));
        group_projector->add(ProjectNamedAttribute("o_orderdate"));
        group_projector->add(ProjectNamedAttribute("o_shippriority"));

        scoped_ptr<Operation> aggregator(GroupAggregate(group_projector,
                aggregation_specification,
                NULL,
                fetcher.release()));

        SortOrder* sort_order = new SortOrder();
        sort_order->add(ProjectNamedAttribute("revenue"), DESCENDING);
        sort_order->add(ProjectNamedAttribute("o_orderdate"), ASCENDING);

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
        CompoundSingleSourceProjector* orders_projector = new CompoundSingleSourceProjector();
        orders_projector->add(ProjectNamedAttribute("o_orderkey"));
        orders_projector->add(ProjectNamedAttribute("o_custkey"));
        orders_projector->add(ProjectNamedAttribute("o_orderdate"));
        orders_projector->add(ProjectNamedAttribute("o_shippriority"));

        scoped_ptr<Operation> projector(Project(orders_projector,
                ScanPartOfView(orders_table->view(), thread_id, thread_count)));

        scoped_ptr<Operation> split_batcher(SplitBatch(
                    ProjectNamedAttribute("o_orderkey"),
                    thread_id * 2,
                    2,
                    projector.release()));

        return split_batcher.release();
    }

    if ((info >> 16) == 3)
    {
        CompoundSingleSourceProjector* customer_projector = new CompoundSingleSourceProjector();
        customer_projector->add(ProjectNamedAttribute("c_custkey"));
        customer_projector->add(ProjectNamedAttribute("c_mktsegment"));

        scoped_ptr<Operation> projector(Project(customer_projector,
                ScanPartOfView(customer_table->view(), thread_id, thread_count)));

        return projector.release();
    }

    if ((info >> 16) == 4)
    {
        CompoundSingleSourceProjector* lineitem_projector = new CompoundSingleSourceProjector();
        lineitem_projector->add(ProjectNamedAttribute("l_orderkey"));
        lineitem_projector->add(ProjectNamedAttribute("l_extendedprice"));
        lineitem_projector->add(ProjectNamedAttribute("l_discount"));
        lineitem_projector->add(ProjectNamedAttribute("l_shipdate"));

        scoped_ptr<Operation> projector(Project(lineitem_projector,
                    ScanPartOfView(lineitem_table->view(), thread_id, thread_count)));

        scoped_ptr<Operation> split_batcher(SplitBatch(
                    ProjectNamedAttribute("l_orderkey"),
                    thread_id * 2 + 1,
                    2,
                    projector.release()));

        return split_batcher.release();
    }

    return NULL;
}

Operation* supersonic::distribute_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    scoped_ptr<Operation> orders_fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
                LISTEN_PORT,
                thread_id,
                thread_count,
                (2 << 16)));

    scoped_ptr<Operation> customer_fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
                LISTEN_PORT,
                thread_id,
                thread_count,
                (3 << 16)));

    scoped_ptr<Operation> lineitem_fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
                LISTEN_PORT,
                thread_id,
                thread_count,
                (4 << 16)));

    const Expression* orders_filter_expression =
            LessOrEqual(NamedAttribute("o_orderdate"), ConstDate(9204));

    CompoundSingleSourceProjector* orders_filter_projector =
            new CompoundSingleSourceProjector();
    orders_filter_projector->add(ProjectNamedAttribute("o_orderkey"));
    orders_filter_projector->add(ProjectNamedAttribute("o_custkey"));
    orders_filter_projector->add(ProjectNamedAttribute("o_orderdate"));
    orders_filter_projector->add(ProjectNamedAttribute("o_shippriority"));

    scoped_ptr<Operation> orders_filter(Filter(orders_filter_expression,
            orders_filter_projector,
            orders_fetcher.release()));

    const Expression* customer_filter_expression =
            Equal(NamedAttribute("c_mktsegment"), ConstString("BUILDING"));

    scoped_ptr<Operation> customer_filter(Filter(customer_filter_expression,
            ProjectNamedAttribute("c_custkey"),
            customer_fetcher.release()));

    const SingleSourceProjector* lhs_key_selector1 = ProjectNamedAttribute("o_custkey");
    const SingleSourceProjector* rhs_key_selector1 = ProjectNamedAttribute("c_custkey");

    CompoundSingleSourceProjector* lhs_projector1 = new CompoundSingleSourceProjector();
    lhs_projector1->add(ProjectNamedAttribute("o_orderkey"));
    lhs_projector1->add(ProjectNamedAttribute("o_orderdate"));
    lhs_projector1->add(ProjectNamedAttribute("o_shippriority"));

    CompoundMultiSourceProjector* result_projector1 = new CompoundMultiSourceProjector();
    result_projector1->add(0, lhs_projector1);

    scoped_ptr<Operation> hash_joiner1(ParallelHashJoin(INNER,
            lhs_key_selector1,
            rhs_key_selector1,
            result_projector1,
            UNIQUE,
            orders_filter.release(),
            customer_filter.release(),
            1,
            thread_id,
            thread_count));

    const Expression* lineitem_filter_expression =
            GreaterOrEqual(NamedAttribute("l_shipdate"), ConstDate(9204));

    CompoundSingleSourceProjector* lineitem_projector = new CompoundSingleSourceProjector();
    lineitem_projector->add(ProjectNamedAttribute("l_orderkey"));
    lineitem_projector->add(ProjectNamedAttribute("l_extendedprice"));
    lineitem_projector->add(ProjectNamedAttribute("l_discount"));

    scoped_ptr<Operation> lineitem_filter(Filter(lineitem_filter_expression,
            lineitem_projector,
            lineitem_fetcher.release()));

    const SingleSourceProjector* lhs_key_selector2 = ProjectNamedAttribute("l_orderkey");
    const SingleSourceProjector* rhs_key_selector2 = ProjectNamedAttribute("o_orderkey");

    CompoundSingleSourceProjector* lhs_projector2 = new CompoundSingleSourceProjector();
    lhs_projector2->add(ProjectNamedAttribute("l_orderkey"));
    lhs_projector2->add(ProjectNamedAttribute("l_extendedprice"));
    lhs_projector2->add(ProjectNamedAttribute("l_discount"));

    CompoundSingleSourceProjector* rhs_projector2 = new CompoundSingleSourceProjector();
    rhs_projector2->add(ProjectNamedAttribute("o_orderdate"));
    rhs_projector2->add(ProjectNamedAttribute("o_shippriority"));

    CompoundMultiSourceProjector* result_projector2 = new CompoundMultiSourceProjector();
    result_projector2->add(0, lhs_projector2);
    result_projector2->add(1, rhs_projector2);

    scoped_ptr<Operation> hash_joiner2(ParallelHashJoin(INNER,
            lhs_key_selector2,
            rhs_key_selector2,
            result_projector2,
            UNIQUE,
            lineitem_filter.release(),
            hash_joiner1.release(),
            2,
            thread_id,
            thread_count));

    scoped_ptr<Operation> merge_batcher(MergeBatch(2, hash_joiner2.release()));

    CompoundExpression* compute_expression = new CompoundExpression();
    compute_expression->Add(NamedAttribute("l_orderkey"));
    compute_expression->Add(NamedAttribute("o_orderdate"));
    compute_expression->Add(NamedAttribute("o_shippriority"));
    compute_expression->AddAs("revenue",
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))));

    scoped_ptr<Operation> computer(Compute(compute_expression, merge_batcher.release()));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM, "revenue", "revenue");

    CompoundSingleSourceProjector* group_projector = new CompoundSingleSourceProjector();
    group_projector->add(ProjectNamedAttribute("l_orderkey"));
    group_projector->add(ProjectNamedAttribute("o_orderdate"));
    group_projector->add(ProjectNamedAttribute("o_shippriority"));

    scoped_ptr<Operation> aggregator(GroupAggregate(group_projector,
            aggregation_specification,
            NULL,
            computer.release()));

    return aggregator.release();
}
