#include "Checks.h"
#include <iostream>

int main(int argc, char** argv)
{
    try
    {
        std::string suite = argc > 1 ? argv[1] : "all";
        if (suite == "protocol" || suite == "all") TestProtocol();
        if (suite == "runtime" || suite == "all") TestRuntime();
        if (suite == "security" || suite == "all") TestSecurity();
        if (suite == "integration" || suite == "all") TestIntegration();
        if (suite == "cluster" || suite == "all") TestCluster();
        if (suite == "adversarial" || suite == "all") TestAdversarial();
        if (suite == "quic") TestQuic();
        if (suite == "recovery" || suite == "all") TestRecovery();
        if (suite == "fabric_quic") TestFabricQuic();
        if (suite == "correlation" || suite == "all") TestCorrelation();
        if (suite == "monitoring" || suite == "all") TestMonitoring();
        if (suite != "protocol" && suite != "runtime" && suite != "security" && suite != "integration"
            && suite != "cluster" && suite != "quic" && suite != "adversarial" && suite != "all"
            && suite != "recovery" && suite != "fabric_quic" && suite != "correlation" && suite != "monitoring")
            throw std::invalid_argument("Unknown test suite");
        std::cout << suite << " passed\n";
        return EXIT_SUCCESS;
    }
    catch (const std::exception& error)
    {
        std::cerr << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
