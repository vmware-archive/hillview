#include "histogram.hpp"
#include "data.hpp"
#include <iostream>
#include <chrono>
#include <iomanip>
#include <locale>

using namespace std::chrono;

static high_resolution_clock::time_point last;

void log(std::string message) {
    std::cout << message << std::endl;
}

int main() {
    const int dataSize = 100;  // in mega
    const int mega = 1024 * 1024;

    std::cout.imbue(std::locale(""));

    log("Allocating column");
    auto data = new DoubleColumn(dataSize * mega);
    log("Filling column");
    data->fillRandom(0, 100);
    log("Computing histogram");

    auto histo = new Histogram(40, 0, 100);
    high_resolution_clock:: time_point before = high_resolution_clock::now();
    for (int i = 0; i < 100; i++) {
       data->histogram(histo);
    }
    high_resolution_clock:: time_point after = high_resolution_clock::now();

    auto duration = duration_cast<microseconds>(after - before).count();
    std::cout << "Histogram took " << duration << " us; " << (((double)dataSize * mega) / (double)duration) << "Melems/sec" << std::endl;
    log("done");
    histo->print(std::cout);
    return 0;
}
