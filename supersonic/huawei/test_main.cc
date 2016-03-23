#include <iostream>

#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

int LoadDataFromDirectory(const char* directory);
Operation* single_thread_operation();
Operation* multi_thread_operation(const int thread_count);

void query(const int thread_count)
{
    if (thread_count == 0)
        std::cout << "single_thread." << std::endl;
    else
        std::cout << "multi_thread: " << thread_count << " thread(s)." << std::endl;

    scoped_ptr<Operation> result_operation(
            thread_count == 0 ?
                    single_thread_operation() :
                    multi_thread_operation(thread_count));

    struct timespec begin, end;
    clock_gettime(CLOCK_REALTIME, &begin);

    scoped_ptr<Cursor> result_cursor(SucceedOrDie(result_operation->CreateCursor()));

    scoped_ptr<ViewPrinter> view_printer(new ViewPrinter(false, true));
    view_printer->AppendSchemaToStream(result_cursor->schema(), &std::cout);
    std::cout << std::endl;

    rowcount_t result_count = 0;
    scoped_ptr<ResultView> result_view(new ResultView(result_cursor->Next(-1)));

    while (!result_view->is_done())
    {
        const View& view = result_view->view();
        result_count += view.row_count();

        view_printer->AppendViewToStream(view, &std::cout);

        result_view.reset(new ResultView(result_cursor->Next(-1)));
    }

    CHECK(!result_view->is_failure()) << "Exception in test_main.\n";

    std::cout << "total: " << result_count << " lines." << std::endl;

    clock_gettime(CLOCK_REALTIME, &end);
    std::cout << "time: " << get_time(begin, end) << " ms." << std::endl << std::endl;
}

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "please specify the data file directory." << std::endl;
        return 0;
    }

    LoadDataFromDirectory(argv[1]);

#if defined(__MIC__)
    int thread_counts[] = {0, 1, 2, 5, 10, 20, 30, 40, 60, 120, 180, 240, 300, 360, 420, 480};
    int thread_counts_size = 16;
#else
    int thread_counts[] = {0, 1, 2, 5, 10, 20, 40, 60, 80};
    int thread_counts_size = 9;
#endif

    for (int i = 0; i < thread_counts_size; i ++)
    {
        query(thread_counts[i]);
        query(thread_counts[i]);
        query(thread_counts[i]);
    }

    return 0;
}
