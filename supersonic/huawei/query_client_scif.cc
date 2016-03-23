#include <iostream>

#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cerr << "please specify the thread count." << std::endl;
        return 0;
    }

    scoped_ptr<Operation> fetcher(FetchSCIF(CPU_SCIF_ADDRESS,
            LISTEN_PORT,
            0,
            1,
            atoi(argv[1])));

    struct timespec begin, end;
    clock_gettime(CLOCK_REALTIME, &begin);

    scoped_ptr<Cursor> result_cursor(SucceedOrDie(fetcher->CreateCursor()));

    scoped_ptr<ViewPrinter> view_printer(new ViewPrinter(false, true));
    view_printer->AppendSchemaToStream(result_cursor->schema(), &std::cout);
    std::cout << std::endl;

    rowcount_t result_count = 0;
    scoped_ptr<ResultView> result_view(new ResultView(result_cursor->Next(-1)));

    while (!result_view->is_done())
    {
        const View &view = result_view->view();
        result_count += view.row_count();

        view_printer->AppendViewToStream(view, &std::cout);

        result_view.reset(new ResultView(result_cursor->Next(-1)));
    }

    CHECK(!result_view->is_failure()) << "Exception in query_client.\n";

    std::cout << "total: " << result_count << " lines." << std::endl;

    clock_gettime(CLOCK_REALTIME, &end);
    std::cout << "time: " << get_time(begin, end) << " ms." << std::endl << std::endl;

    return 0;
}

Operation* supersonic::sender_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    return NULL;
}

Operation* supersonic::distribute_operation(const int thread_id,
        const int thread_count,
        const int info)
{
    return NULL;
}
