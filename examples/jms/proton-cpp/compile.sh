# This requires g++ and qpid-cpp-client-devel
g++ src/main/cpp/hello.cpp  -o hello -l qpidmessaging -l qpidtypes
