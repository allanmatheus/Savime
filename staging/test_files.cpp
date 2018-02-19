#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <list>
#include "../mapped_memory/mapped_memory.h"
#include "staging.h"

std::string get_name(const std::string& path)
{
    const auto last_slash = path.find_last_of('/');
    return path.substr(last_slash + 1);
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " ADDR:PORT FILE...\n";
        return 1;
    }

    staging::server st{argv[1], 1};

    std::list<mapped_memory> buffers;

    for (std::size_t i = 2; i < argc; ++i) {
        auto path = argv[i];
        auto name = get_name(path);

        staging::dataset dataset{name, "double", st};

        //std::cerr << "reading " << path << '\n';
        mapped_memory buffer{path};

        //std::cerr << "sending " << name << '\n';
        dataset.write(buffer.get(), buffer.size());

        buffers.push_back(std::move(buffer));
    }

    //std::cerr << "syncing...\n";
    st.sync();

    return 0;
}
