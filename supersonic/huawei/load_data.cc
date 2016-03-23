#include <iostream>

#include "huawei_lib.h"
#include "supersonic/supersonic.h"

using namespace supersonic;

int main(int argc, char* argv[])
{
    if (argc != 3)
    {   
        std::cerr << "please specify the table name and data file" << std::endl;
        return 0;
    }   

    scoped_ptr<Table> table(new Table(GetSchemaByName(argv[1]), HeapBufferAllocator::Get()));

    struct timespec begin, end;
    clock_gettime(CLOCK_REALTIME, &begin);

    LoadFromDataFile(table.get(), argv[2]);

    clock_gettime(CLOCK_REALTIME, &end);
    std::cerr << "load time: " << get_time(begin, end) << " ms." << std::endl << std::endl;

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
