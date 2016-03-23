#include <errno.h>

#include <scif.h>

#include <iostream>

#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

int LoadDataFromDirectory(const char* directory);

int main(int argc, char* argv[])
{
    if (argc == 2)
        LoadDataFromDirectory(argv[1]);

    scif_epd_t listen_end_point = scif_open();
    CHECK(listen_end_point != SCIF_OPEN_FAILED)
            << "scif_open() failed for " << strerror(errno) << ".\n";

    int result = scif_bind(listen_end_point, LISTEN_PORT);
    CHECK(result != -1) << "scif_bind() failed for " << strerror(errno) << ".\n";

    result = scif_listen(listen_end_point, 480);
    CHECK(result == 0) << "scif_listen() failed for " << strerror(errno) << ".\n";

    while (true)
    {
        struct scif_portID sender_address;
        scif_epd_t sender_end_point;
        result = scif_accept(listen_end_point,
                &sender_address,
                &sender_end_point,
                SCIF_ACCEPT_SYNC);
        CHECK(result == 0) << "scif_accept() failed for " << strerror(errno) << ".\n";

        pthread_t tid;
        result = pthread_create(&tid,
                NULL,
                sender_thread_scif,
                (void *) ((long) sender_end_point));
        CHECK(result == 0) << "pthread_create() failed for " << strerror(result) << ".\n";
    }

    return 0;
}
