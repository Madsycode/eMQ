#pragma once
#include <cstdint>

// Load generator for scalability runs: `EmpyKlient bench PREFIX ...` drives P
// publishers and S subscribers in one process and prints one JSON summary line.
int32_t RunBench(int32_t argc, char** argv);
