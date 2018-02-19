#include <array>
#include "staging.h"

void load_subtars(staging::server& st);

int main(int argc, char **argv)
{
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " ADDR:PORT\n";
        return 1;
    }

    constexpr std::size_t N = 5;
    std::array<double, N> v;

    for (std::size_t i = 0; i < N; ++i) {
        v[i] = i + 1;
    }

    staging::server st{argv[1]};

/*
    st.run_savime("create_tar(\"testar\", \"*\", \"implicit,x,int,0,1000,1 | implicit,y,int,0,1000,1 | implicit,z,int,0,1000,1 \", \"a,double|b,double\");");
*/

    staging::dataset dataset{"base", "double", st};

    auto buf = reinterpret_cast<char *>(v.data());
    auto len = v.size() * sizeof(double);

    dataset.write(buf, len); // to stating and then savime
    st.sync(); // with stating, not savime

/*
    load_subtars(st); // describe data
*/

    return 0;
}

void load_subtars(staging::server& st)
{
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,0,99 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,100,199 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,200,299 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,300,399 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,400,499 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,500,599 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,600,699 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,700,799 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,800,899 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");

    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,0,99 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,100,199 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,200,299 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,300,399 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,400,499 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,500,599 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,600,699 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,700,799 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,800,899 | ordered,z,0,999\", \"a,base | b,base\");");
    st.run_savime("load_subtar(\"testar\", \"ordered,x,900,999 | ordered,y,900,999 | ordered,z,0,999\", \"a,base | b,base\");");
}
