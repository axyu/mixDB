#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

scoped_ptr<Table> lineitem_table;
scoped_ptr<Table> part_table;

int LoadDataFromDirectory(const char* directory)
{
    char file_name[1024];

    lineitem_table.reset(new Table(GetLineitemSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(lineitem_table.get(), strcat(file_name, "lineitem.dat"));

    part_table.reset(new Table(GetPartSchema(), HeapBufferAllocator::Get()));
    strcpy(file_name, directory);
    LoadFromDataFile(part_table.get(), strcat(file_name, "part.dat"));

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
        aggregation_specification->AddAggregation(SUM, "sum1", "sum1");
        aggregation_specification->AddAggregation(SUM, "sum2", "sum2");

        scoped_ptr<Operation> aggregator(ScalarAggregate(
                aggregation_specification,
                fetcher.release()));

        CompoundExpression *compute_expression = new CompoundExpression();
        compute_expression->AddAs("promo_revenue", Multiply(
                Divide(NamedAttribute("sum1"), NamedAttribute("sum2")),
                ConstDouble(100.00)));

        scoped_ptr<Operation> computer(Compute(compute_expression, aggregator.release()));

        return computer.release();
    }

    if ((info >> 16) == 1)
        return Distribute((info & 0xffff), 0);

    if ((info >> 16) == 2)
    {
        CompoundSingleSourceProjector* lineitem_projector = new CompoundSingleSourceProjector();
        lineitem_projector->add(ProjectNamedAttribute("l_partkey"));
        lineitem_projector->add(ProjectNamedAttribute("l_extendedprice"));
        lineitem_projector->add(ProjectNamedAttribute("l_discount"));
        lineitem_projector->add(ProjectNamedAttribute("l_shipdate"));

        scoped_ptr<Operation> projector(Project(lineitem_projector,
                ScanPartOfView(lineitem_table->view(), thread_id, thread_count)));

        return projector.release();
    }

    if ((info >> 16) == 3)
    {
        CompoundSingleSourceProjector* part_projector = new CompoundSingleSourceProjector();
        part_projector->add(ProjectNamedAttribute("p_partkey"));
        part_projector->add(ProjectNamedAttribute("p_type"));

        scoped_ptr<Operation> projector(Project(part_projector,
                ScanPartOfView(part_table->view(), thread_id, thread_count)));

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

    scoped_ptr<Operation> part_fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
                LISTEN_PORT,
                thread_id,
                thread_count,
                (3 << 16)));

    const Expression* lineitem_filter_expression1 =
            GreaterOrEqual(NamedAttribute("l_shipdate"), ConstDate(9374));
    const Expression* lineitem_filter_expression2 =
            Less(NamedAttribute("l_shipdate"), ConstDate(9404));
    const Expression* lineitem_filter_expression =
            And(lineitem_filter_expression1, lineitem_filter_expression2);

    CompoundSingleSourceProjector* lineitem_filter_projector =
            new CompoundSingleSourceProjector();
    lineitem_filter_projector->add(ProjectNamedAttribute("l_partkey"));
    lineitem_filter_projector->add(ProjectNamedAttribute("l_extendedprice"));
    lineitem_filter_projector->add(ProjectNamedAttribute("l_discount"));

    scoped_ptr<Operation> lineitem_filter(Filter(lineitem_filter_expression,
            lineitem_filter_projector,
            lineitem_fetcher.release()));

    const Expression* part_filter_expression =
            StringContains(NamedAttribute("p_type"), ConstString("PROMO"));

    scoped_ptr<Operation> part_filter(Filter(part_filter_expression,
            ProjectNamedAttribute("p_partkey"),
            part_fetcher.release()));

    const SingleSourceProjector* lhs_key_selector = ProjectNamedAttribute("l_partkey");
    const SingleSourceProjector* rhs_key_selector = ProjectNamedAttribute("p_partkey");

    CompoundSingleSourceProjector* lhs_projector = new CompoundSingleSourceProjector();
    lhs_projector->add(ProjectNamedAttribute("l_extendedprice"));
    lhs_projector->add(ProjectNamedAttribute("l_discount"));

    CompoundMultiSourceProjector* result_projector = new CompoundMultiSourceProjector();
    result_projector->add(0, lhs_projector);
    result_projector->add(1, ProjectNamedAttribute("p_partkey"));

    scoped_ptr<Operation> hash_joiner(ParallelHashJoin(LEFT_OUTER,
            lhs_key_selector,
            rhs_key_selector,
            result_projector,
            UNIQUE,
            lineitem_filter.release(),
            part_filter.release(),
            1,
            thread_id,
            thread_count));

    ExpressionList* compute_case_expression_list = new ExpressionList();
    compute_case_expression_list->add(IsNull(NamedAttribute("p_partkey")));
    compute_case_expression_list->add(ConstDouble(0));
    compute_case_expression_list->add(ConstBool(true));
    compute_case_expression_list->add(ConstDouble(0));
    compute_case_expression_list->add(ConstBool(false));
    compute_case_expression_list->add(
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))));

    CompoundExpression* compute_expression = new CompoundExpression();
    compute_expression->AddAs("part1", Case(compute_case_expression_list));
    compute_expression->AddAs("part2",
            Multiply(NamedAttribute("l_extendedprice"),
                    Minus(ConstDouble(1), NamedAttribute("l_discount"))));

    scoped_ptr<Operation> computer(Compute(compute_expression, hash_joiner.release()));

    AggregationSpecification* aggregation_specification = new AggregationSpecification();
    aggregation_specification->AddAggregation(SUM, "part1", "sum1");
    aggregation_specification->AddAggregation(SUM, "part2", "sum2");

    scoped_ptr<Operation> aggregator(ScalarAggregate(
            aggregation_specification,
            computer.release()));

    return aggregator.release();
}
