#ifndef SAVIME_QUERY_TABLE_H
#define SAVIME_QUERY_TABLE_H

#include <iostream>
#include <mutex>
#include <memory>
#include <utility>
#include <unordered_map>
#include <condition_variable>

struct query_data {
    query_data(int f, size_t sz)
        : fd(f), fsize(sz), rdma_error(false), rdma_op_done(false)
    {
        std::cerr << "query_data ctor: " << fd << ':' << fsize << ':' << rdma_op_done << '\n';
    }

    ~query_data()
    {
        std::cerr << "query_data dtor: " << fd << ':' << fsize << ':' << rdma_op_done << '\n';
    }

    void notify(bool error=false)
    {
        rdma_error = error;
        rdma_op_done = true;
        cond.notify_one();
    }

    bool wait()
    {
        std::unique_lock<std::mutex> lk{mtx};
        while (!rdma_op_done) {
            cond.wait(lk);
        }
        return rdma_error;
    }

    int fd;
    size_t fsize;

    std::mutex mtx;
    bool rdma_error;
    bool rdma_op_done;
    std::condition_variable cond;
};

struct query_table_hash {
    // Follow the example at
    // http://en.cppreference.com/w/cpp/container/unordered_map/unordered_map
    std::size_t operator()(const std::pair<int, int>& k) const
    {
        return std::hash<int>{}(k.first) ^ (std::hash<int>{}(k.second) << 1);
    }
};

struct query_table {
    std::shared_ptr<query_data>& operator[](std::pair<int, int>& key)
    {
        return m_map[key];
    }

    void insert(std::pair<int, int>& key, std::shared_ptr<query_data> data)
    {
        std::unique_lock<std::mutex> lk{m_mtx};
        m_map[key] = data;
        m_cond.notify_all();
    }

    std::shared_ptr<query_data>& at(std::pair<int, int>& key)
    {
        std::unique_lock<std::mutex> lk{m_mtx};
        while (true) {
            try {
                return m_map.at(key);
            } catch (std::out_of_range& e) {
                m_cond.wait(lk);
            }
        }
    }

    void erase(std::pair<int, int>& key)
    {
        std::unique_lock<std::mutex> lk{m_mtx};
        m_map.erase(key);
    }

    std::mutex m_mtx;
    std::condition_variable m_cond;

    std::unordered_map<std::pair<int, int>, std::shared_ptr<query_data>,
        query_table_hash> m_map;
};

#endif /* SAVIME_QUERY_TABLE_H */
