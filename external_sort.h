#ifndef __EXT_SORT__
#define __EXT_SORT__

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

namespace ext_sort {

    using std::cout;
    using std::endl;
    using std::vector;
    using std::ifstream;
    using std::ofstream;

    template<typename T>
    class ExternalSortEngine {
    public:
        using FilePos = std::streampos;

        ExternalSortEngine(const std::string& fn,
            FilePos am = 100 * 1024 * 1024)
            : fileName(fn),
            availableMemory(am),
            fileSize(0),
            arraySize(0),
            partSize(0),
            partNumber(0),
            remSize(0),
            nextPartIndex(0),
            ifile(ifstream(fileName, std::ios::in | std::ios::binary)) {
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
            cout << "Size of input file is " << fileSize << " bytes." << endl;
            arraySize = fileSize / sizeof(T);
            cout << "Size of input array is " << arraySize << " elements." << endl;
            partSize = availableMemory / sizeof(T);
            partNumber = arraySize / partSize;
            cout << "Input array will be divided into " << partNumber << " parts with a size of " << partSize << " elements." << endl;
            remSize = arraySize - partSize * partNumber;
            cout << "Remainder is " << remSize << " elements." << endl;
        }

        void divideAndSort(ifstream& ifile) {
            vector<T> data(partSize, 0);
            while (true) {
                std::unique_lock<std::mutex> lk(m);
                size_t idx = nextPartIndex;
                nextPartIndex++;
                if (idx < partNumber) {
                    cout << "Thread " << std::this_thread::get_id() << " is working on " << idx << " part" << endl;
                    ifile.seekg(idx * partSize, ifile.beg);
                    ifile.read(reinterpret_cast<char*>(data.data()), partSize * sizeof(T));
                    lk.unlock();
                    std::sort(data.begin(), data.end());
                    ofstream ofile(getTmpFileName(idx), std::ios::out | std::ios::binary);
                    ofile.write(reinterpret_cast<char*>(data.data()), partSize * sizeof(T));
                    ofile.close();
                }
                else if (idx == partNumber) {
                    cout << "Thread " << std::this_thread::get_id() << " is working on last part" << endl;
                    ifile.seekg(nextPartIndex * partSize, ifile.beg);
                    nextPartIndex++;
                    ifile.read(reinterpret_cast<char*>(data.data()), remSize * sizeof(T));
                    lk.unlock();
                    std::sort(data.begin(), data.begin() + remSize);
                    ofstream ofile(getTmpFileName(idx), std::ios::out | std::ios::binary);
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
            struct QueueEntry {
                T data;
                size_t from;
                QueueEntry(T d, size_t f) : data(d), from(f) {}
            };

            vector<ifstream> files(partNumber + 1);
            for (size_t i = 0; i <= partNumber; i++) {
                files[i].open(
                    getTmpFileName(i),
                    std::ios::in | std::ios::binary);
                if (!files[i]) {
                    throw std::runtime_error("Can't open tmp file");
                }
            }

            ofstream ofile("output.dat",
                std::ios::out | std::ios::binary);
            if (!ofile) {
                throw std::runtime_error("Can't create output file");
            }

            cout << "Merging temporary files" << endl;
            // Using heap to find the next element that should go to 
            // output file.

            // lowest element should be on top of the queue
            auto cmp = [](QueueEntry left, QueueEntry right) {
                return left.data > right.data;
            };
            std::priority_queue <
                QueueEntry,
                vector<QueueEntry>,
                decltype(cmp)   > queue(cmp);
            auto startTime = std::chrono::steady_clock::now();
            // fill the queue with first element from each file
            for (size_t i = 0; i < partNumber + 1; i++) {
                T tmp;
                if (files[i].read(reinterpret_cast<char*>(&tmp), sizeof(T)))
                    queue.push(QueueEntry(tmp, i));
            }

            int count = 0;
            while (!queue.empty()) {
                QueueEntry next = queue.top();
                queue.pop();
                T tmp;
                // replace popped value by the next value from corresponding file
                if (files[next.from].read(reinterpret_cast<char*>(&tmp), sizeof(T))) {
                    queue.push(QueueEntry(tmp, next.from));
                }
                else
                {
                    cout << "File " << next.from << " is ended" << endl;
                }
                ofile.write(reinterpret_cast<char*>(&next.data), sizeof(T));
                count++;
                if (count % 1000000 == 0)
                    cout << 100.0 * count / arraySize << "% complete" << endl;
            }

            auto finishTime = std::chrono::steady_clock::now();
            cout << "Merging complete. Elapsed time: " <<
                std::chrono::duration_cast<std::chrono::milliseconds>(finishTime - startTime).count() << "ms" << endl;

            for (auto& f : files)
                f.close();

            ofile.close();
        }

        std::string getTmpFileName(size_t idx) {
            return std::string("part_file") + std::to_string(idx) + ".dat";
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
        ifstream ifile;
        size_t nextPartIndex;
    };

} // namespace ext_sort

#endif // __EXT_SORT__