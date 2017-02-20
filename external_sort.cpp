#include "external_sort.h"

using namespace std;

void generateTestData() {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, std::numeric_limits<int>::max());

    ofstream ofile("input.dat", std::ios::out | std::ios::binary);
    if (!ofile) {
        cout << "Can't create test file" << endl;
        return;
    }

    const size_t size = 1000000000;
    const size_t partSize = size / 100;
    const size_t partNumber = size / partSize;
    const size_t remSize = size - partSize * partNumber;

    std::vector<int> data(static_cast<unsigned int>(partSize), 0);
    cout << "Generating test file" << endl;
    auto startTime = std::chrono::steady_clock::now();
    for (size_t i = 0; i < partNumber; i++) {
        cout << static_cast<double>(i * 100.0 / partNumber) << "% ";
        for (size_t j = 0; j < data.size(); j++) {
            data[j] = dist(mt);
        }
        ofile.write(reinterpret_cast<char*>(data.data()), data.size() * sizeof(int));
    }
    cout << endl;
    for (size_t j = 0; j < remSize; j++) {
        data[j] = dist(mt);
    }
    ofile.write(reinterpret_cast<char*>(data.data()), remSize * sizeof(int));
    ofile.close();
    auto finishTime = std::chrono::steady_clock::now();
    cout << "Generation complete. Elapsed time: " <<
        std::chrono::duration_cast<std::chrono::milliseconds>(finishTime - startTime).count() << "ms" << endl;
}

int main()
{
    //generateTestData();
    ext_sort::ExternalSortEngine<int> e("input.dat");
    auto start = std::chrono::steady_clock::now();
    e.sort();
    auto finish = std::chrono::steady_clock::now();
    cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << "ms" << endl;
    return 0;
}

