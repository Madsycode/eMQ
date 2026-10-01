#pragma once
#include <eMQ/Common/State.h>
#include <stdexcept>

#define EMPY_CHECK(...) do { if (!(__VA_ARGS__)) throw std::runtime_error( \
    std::string(__FILE__) + ":" + std::to_string(__LINE__) + ": " + #__VA_ARGS__); } while (0)

template <typename _Function>
void Throws(_Function&& function)
{
    bool thrown = false;
    try { function(); }
    catch (const std::exception&) { thrown = true; }
    EMPY_CHECK(thrown);
}

void TestProtocol();
void TestRuntime();
void TestSecurity();
void TestIntegration();
void TestCluster();
void TestQuic();
void TestAdversarial();
void TestRecovery();
void TestFabricQuic();
void TestCorrelation();
void TestMonitoring();
