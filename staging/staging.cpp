#include <cstring>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include "rdma_utils.h"
#include "staging.h"

inline char *to_char_ptr(const std::string& str)
{
    // FIXME: Avoid error prone castings (const char *) -> (char *).
    return (char *)str.data();
}

inline void init_dataset_request(staging::request& req, staging::operation op,
        const std::string& dataset, const std::string& type, size_t n)
{
    if (dataset.size() >= staging::MAX_DATASET_NAME_LEN
            && type.size() >= staging::MAX_TYPE_NAME_LEN) {
        throw std::length_error("cannot make a create_dataset request");
    }

    memset(&req, 0, sizeof(req));

    req.op = op;
    req.data.dataset.size = n;
    memcpy(req.data.dataset.name, dataset.c_str(), dataset.size());
    memcpy(req.data.dataset.type, type.c_str(), type.size());
}

staging::server::server(const std::string& address, std::size_t num_threads)
    : _comm{std::make_shared<communicator>(address, num_threads)}
{ }

staging::server::~server()
{ }

std::string staging::server::run_savime(const std::string& query)
{
    return _comm->run_savime(query);
}

void staging::server::sync()
{
    _comm->sync();
}

staging::dataset::dataset(const std::string& name, const std::string& type,
        staging::server& st)
    : _created{false}, _name{name}, _type{type}, _comm{st._comm}
{
    //std::cerr << "dataset(" << _name << ") ctor\n";
}

staging::dataset::~dataset()
{
    //std::cerr << "dataset(" << _name << ") dtor\n";
}

void staging::dataset::write(char *buf, std::size_t len)
{
    //std::cerr << "dataset(" << _name << ") write\n";
#if 0
    auto c = _get_comm();

    if (_created) {
        throw std::runtime_error("dataset already exists");

    } else {
        std::thread t{&staging::communicator::create_dataset, &(*c), _name,
            _type, buf, len};
        c->_thrds.push_back(std::move(t));
        _created = true;
    }
#else
    auto c = _get_comm();
    c->create_dataset(_name, _type, buf, len);
#endif
}

std::string staging::dataset::savime_response()
{
    throw std::logic_error("Not Implemented\n");
}

std::shared_ptr<staging::communicator> staging::dataset::_get_comm()
{
    if (auto c = _comm.lock()) {
        return c;
    }
    throw std::runtime_error("missing communicator");
}

staging::communicator::communicator(const std::string& address,
        std::size_t num_threads)
{
    const auto c = address.find(":");
    _host = address.substr(0, c);
    _service = address.substr(c + 1);
    //_keep_working = true;
    //std::cerr << "communicator ctor '" << _host << ':' << _service << "'\n";
    for (std::size_t i = 0; i < num_threads; ++i) {
        //std::cerr << "[lib] creating worker\n";
        std::thread t{&staging::communicator::worker, this};
        _thrds.push_back(std::move(t));
    }
}

staging::communicator::~communicator()
{
    //std::cerr << "communicator dtor\n";
    //_keep_working = false;
    _writers.close();
    _writers.flush();
    for (auto t = begin(_thrds); t != end(_thrds); ++t) {
        t->join();
    }
}

void staging::communicator::create_dataset(const std::string& name,
        const std::string& type, char *buf, std::size_t len)
{
    //std::cerr << "communicator create " << name << '\n';
#if 0
    staging::request req;
    constexpr auto op = staging::operation::create_dataset;
    init_dataset_request(req, op, name, type, len);
    write_rdma(to_char_ptr(_host), to_char_ptr(_service), req, buf, len);
#else
    dataset_writer w{name, type, buf, len};

    //{
    //    //std::cerr << "[lib|worker|writer] running " << name << ' ' << type << ' ' << len << '\n';
    //    {
    //        std::lock_guard<std::mutex> g{_writers_mtx};
    //        _writers.push(std::move(w));
    //    }
    //    _writers_cv.notify_one();
    //}

    _writers.push(std::move(w));
#endif
}

std::string staging::communicator::run_savime(const std::string& query)
{
    //std::cerr << "communicator run_savime '" << query << "'\n";
    staging::request req;
    memset(&req, 0, sizeof(req));
    req.op = staging::operation::run_savime;
    req.data.query.size = query.size();
    return send_query(to_char_ptr(_host), to_char_ptr(_service), req, query);
}

void staging::communicator::sync()
{
    _writers.close();
    _writers.flush();

    for (auto t = begin(_thrds); t != end(_thrds); ++t) {
        t->join();
    }
    auto num_threads = _thrds.size();
    _thrds.clear();

    _writers.reset();

    for (std::size_t i = 0; i < num_threads; ++i) {
        //std::cerr << "[lib] creating worker\n";
        std::thread t{&staging::communicator::worker, this};
        _thrds.push_back(std::move(t));
    }

    //std::cerr << "communicator sync\n";
    //std::unique_lock<std::mutex> lk{_writers_mtx};
    //_writers_cv.wait(lk, [this] { return _writers.empty(); });

    //lk.unlock();
    //_writers_cv.notify_one();
}

void staging::communicator::worker()
{
    auto host = to_char_ptr(_host);
    auto service = to_char_ptr(_service);

#if 0
    bool keep_working = _keep_working;

    while (keep_working) {
        bool has_job = false;
        staging::dataset_writer s;

        {
            //std::cerr << "[lib|worker] looking for some work\n";
            std::unique_lock<std::mutex> lk{_writers_mtx};
            _writers_cv.wait(lk, [this] { return !_writers.empty(); });

            s = _writers.front();
            _writers.pop();

            lk.unlock();
            _writers_cv.notify_one();
        }

        s.run(host, service);

        {
            std::lock_guard<std::mutex> g{_writers_mtx};
            keep_working = _keep_working || !_writers.empty();
        }
    }
#else
    dataset_writer s;
    while (_writers.pop(s)) {
        s.run(host, service);
    }
#endif
}

staging::dataset_writer::dataset_writer()
{ }

staging::dataset_writer::dataset_writer(const std::string& _name,
        const std::string& _type, char *_buf, std::size_t _len)
    : name{_name}, type{_type}, buf{_buf}, len{_len}
{ }

void staging::dataset_writer::run(char *host, char *service)
{
    //std::cerr << "[lib|worker|writer] running '" << host << ':' << service << "' " << name << ' ' << type << ' ' << len << '\n';
    staging::request req;
    constexpr auto op = staging::operation::create_dataset;
    init_dataset_request(req, op, name, type, len);
    //std::cerr << "=== start req: " << name << std::endl;
    write_rdma(host, service, req, buf, len);
    //std::cerr << "=== finished req: " << name << std::endl;
}

std::ostream& operator<<(std::ostream& o, const staging::response& r)
{
    switch (r.status) {
    case staging::result::ok:
        o << "OK";
        break;

    case staging::result::err:
        o << "ERR";
        break;
    }
}
