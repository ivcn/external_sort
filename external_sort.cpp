#include "stdafx.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <random>
#include <thread>
#include <vector>

using std::cout;
using std::endl;
using std::vector;


template<typename T>
class ExternalSortEngine {
public:
    using FilePos = std::streampos;

    ExternalSortEngine(const std::string& fn,
                       FilePos am = 100 * 1024 * 1024) 
        :   fileName(fn),
            availableMemory(am),
            fileSize(0),
            arraySize(0),
            partSize(0),
            partNumber(0),
            remSize(0),
            nextPartIndex(0),
            ifile(std::fstream(fileName, std::ios::in | std::ios::binary)) {
        if (!ifile) {
            throw std::runtime_error("Can't open test file");
        }
    }

    ExternalSortEngine(ExternalSortEngine& other) = delete;
    ExternalSortEngine operator=(ExternalSortEngine& other) = delete;

    ExternalSortEngine(ExternalSortEngine&& other) = default;
    ExternalSortEngine& operator=(ExternalSortEngine&& other) = default;

    ~ExternalSortEngine() {
        ifile.close();
    }

    void divideAndSort(std::fstream& ifile) {
        vector<T> data(partSize, 0);
        while (true) {
            std::unique_lock<std::mutex> lk(m);
            size_t idx = nextPartIndex;
            nextPartIndex++;
            if (idx < partNumber) {
                cout << "Thread " << std::this_thread::get_id() << "is working on " << idx << "part" << endl;
                ifile.seekg(idx * partSize, ifile.beg);
                ifile.read(reinterpret_cast<char*>(data.data()), partSize * sizeof(T));
                lk.unlock();
                std::sort(data.begin(), data.end());
                std::fstream ofile(getTmpFileName(idx), std::ios::out | std::ios::binary);
                ofile.write(reinterpret_cast<char*>(data.data()), partSize * sizeof(T));
                ofile.close();
            }
            else if (idx == partNumber) {
                cout << "Thread " << std::this_thread::get_id() << "is working on last part" << endl;
                ifile.seekg(nextPartIndex * partSize, ifile.beg);
                nextPartIndex++;
                ifile.read(reinterpret_cast<char*>(data.data()), remSize * sizeof(T));
                lk.unlock();
                std::sort(data.begin(), data.begin() + remSize);
                std::fstream ofile(getTmpFileName(idx), std::ios::out | std::ios::binary);
                ofile.write(reinterpret_cast<char*>(data.data()), remSize * sizeof(T));
                ofile.close();
            }
            else {
                cout << "No work left" << endl;
                lk.unlock();
                break;
            }
        }
    }

    void mergeFiles() {
        // We need to store the source file of each value
        // to be able to retrieve next value from that file.
        struct QueueRecord {
            T data;
            size_t from;
            QueueRecord(T d, size_t f) : data(d), from(f) {}
        };

        vector<std::ifstream> files(partNumber + 1);
        for (size_t i = 0; i <= partNumber;i++) {
            files[i].open(
                getTmpFileName(i), 
                std::ios::in | std::ios::binary);
            if (!files[i]) {
                throw std::runtime_error("Can't open tmp file");
            }
        }

        std::ofstream ofile("output.dat",
            std::ios::out | std::ios::binary);
        if (!ofile) {
            throw std::runtime_error("Can't create output file");
        }

        cout << "Merging temporary files" << endl;
        // Using heap to find the next element that should go to 
        // output file.

        // lowest element should be on top of the queue
        auto cmp = [](QueueRecord left, QueueRecord right) { 
            return left.data > right.data;
        };
        std::priority_queue < 
            QueueRecord, 
            vector<QueueRecord>, 
            decltype(cmp)   > queue(cmp);
        auto startTime = std::chrono::steady_clock::now();
        // fill the queue with first element from each file
        for (size_t i = 0; i < partNumber + 1; i++) {
            T tmp;
            if (files[i].read(reinterpret_cast<char*>(&tmp), sizeof(T)))
                queue.push(QueueRecord(tmp, i));
        }

        int count = 0;
        while (!queue.empty()) {
            QueueRecord next = queue.top();
            queue.pop();
            T tmp;
            // replace popped value by the next value from corresponding file
            if (files[next.from].read(reinterpret_cast<char*>(&tmp), sizeof(T))) {
                queue.push(QueueRecord(tmp, next.from));
            }
            else
            {
                count++;
                auto finishTime = std::chrono::steady_clock::now();
                cout << "File " << next.from << "is ended after " << 
                    std::chrono::duration_cast<std::chrono::milliseconds>(finishTime-startTime).count() << "ms" << endl;
                cout << 100 * count / partNumber << "% complete" << endl;
            }
            ofile.write(reinterpret_cast<char*>(&next.data), sizeof(T));
        }

        auto finishTime = std::chrono::steady_clock::now();
        cout << "Merging complete. Elapsed time: " <<
            std::chrono::duration_cast<std::chrono::milliseconds>(finishTime - startTime).count() << "ms" << endl;

        for (auto& f : files)
            f.close();

         ofile.close();
    }

    void sort() {

        discoverInputFile();

        size_t numberOfThreads = 4;
        vector<std::thread> workers(numberOfThreads);
        for (auto& w : workers) {
            w = std::thread(&ExternalSortEngine<T>::divideAndSort, this, std::ref(ifile));
        }
        for (auto& w : workers) {
            w.join();
        }

        mergeFiles();
    }

private: //methods
        void discoverInputFile() {
            ifile.seekg(0, ifile.end);
            fileSize = ifile.tellg();
            ifile.seekg(0, ifile.beg);

            arraySize = fileSize / sizeof(T);
            partSize = availableMemory / sizeof(T);
            partNumber = arraySize / partSize;
            remSize = arraySize - partSize * partNumber;
        }

        std::string getTmpFileName(size_t idx) {
            return std::string("part_file ") + std::to_string(idx) + ".dat";
        }

private: //members
    std::string fileName;
    FilePos availableMemory;

    FilePos fileSize;
    FilePos arraySize;
    FilePos partSize;
    size_t partNumber;
    size_t remSize;

    // synchronized resources
    std::mutex m;
    std::fstream ifile;
    size_t nextPartIndex;
};

void generateTestData() {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<int> dist(0, std::numeric_limits<int>::max());

    std::fstream ofile("input.dat", std::ios::out | std::ios::binary);
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
    ExternalSortEngine<int> e("input.dat");
    auto start = std::chrono::steady_clock::now();
    e.sort();
    auto finish = std::chrono::steady_clock::now();
    cout << "Total time: " << std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count() << "ms" << endl;
    return 0;
}

