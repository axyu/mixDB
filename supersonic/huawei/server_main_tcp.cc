#include <errno.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "huawei_lib.h"

#include "supersonic/supersonic.h"

using namespace supersonic;

int LoadDataFromDirectory(const char* directory);

int main(int argc, char* argv[])
{
    if (argc == 2)
        LoadDataFromDirectory(argv[1]);

    int listen_socket = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(listen_socket != -1) << "socket() failed for " << strerror(errno) << ".\n";

    struct sockaddr_in listen_address;
    listen_address.sin_family = AF_INET;
    listen_address.sin_port = htons(LISTEN_PORT);
    listen_address.sin_addr.s_addr = INADDR_ANY;

    int result = bind(listen_socket,
            (struct sockaddr*) &listen_address,
            sizeof(listen_address));
    CHECK(result == 0) << "bind() failed for " << strerror(errno) << ".\n";

    result = listen(listen_socket, 480);
    CHECK(result == 0) << "listen() failed for " << strerror(errno) << ".\n";

    while (true)
    {
        int sender_socket = accept(listen_socket, NULL, NULL);
        CHECK(sender_socket != -1) << "accept() failed for " << strerror(errno) << ".\n";

        pthread_t tid;
        result = pthread_create(&tid,
                NULL,
                sender_thread_tcp,
                (void *) ((long) sender_socket));
        CHECK(result == 0) << "pthread_create() failed for " << strerror(result) << ".\n";
    }

    return 0;
}
