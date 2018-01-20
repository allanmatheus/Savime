#ifndef SAVIME_BITMASK_H
#define SAVIME_BITMASK_H

using namespace std;

class SavimeBitset;
typedef shared_ptr<SavimeBitset> SavimeBitsetPtr;

class SavimeBitset
{
    bool * _bits;
    int64_t _length;
    
public:
    
    SavimeBitset(int64_t length)
    {
        _length = length;
        _bits = (bool*)malloc(sizeof(length)*sizeof(bool));
    }
    
    inline bool operator[](int64_t pos)
    { 
        return _bits[pos];
    }
    
    inline int64_t size()
    {
        return _length;
    }
    
    static inline void and_parallel(SavimeBitsetPtr d, const SavimeBitsetPtr x,
      SavimeBitsetPtr y, int32_t num_cores, int32_t work_per_thread)
    {
        int64_t start_position_per_core[num_cores];
        int64_t final_position_per_core[num_cores];
        int64_t size = std::min(x->size(), y->size());

        SetWorkloadPerThread(size, work_per_thread, start_position_per_core, 
                             final_position_per_core, num_cores);

        #pragma omp parallel
        {
            for (int64_t i = start_position_per_core[omp_get_thread_num()]; i < final_position_per_core[omp_get_thread_num()]; ++i)
            {
                d->_bits[i] = x->_bits[i] & y->_bits[i];
            }
        }
    }
    

    static inline void or_parallel(SavimeBitsetPtr d, const SavimeBitsetPtr x,
      SavimeBitsetPtr y, int32_t num_cores, int32_t work_per_thread)
    {
        int64_t start_position_per_core[num_cores];
        int64_t final_position_per_core[num_cores];
        int64_t size = std::min(x->size(), y->size());

        SetWorkloadPerThread(size, work_per_thread, start_position_per_core, 
                             final_position_per_core, num_cores);
        
        #pragma omp parallel
        {
            for (int64_t i = start_position_per_core[omp_get_thread_num()]; i < final_position_per_core[omp_get_thread_num()]; ++i)
            {
                d->_bits[i] = x->_bits[i] | y->_bits[i];
            }       
        }
    }

    
    static inline void xor_parallel(SavimeBitsetPtr d, const SavimeBitsetPtr x,
        SavimeBitsetPtr y, int32_t num_cores, int32_t work_per_thread)
    {
        int64_t start_position_per_core[num_cores];
        int64_t final_position_per_core[num_cores];
        int64_t size = std::min(x->size(), y->size());

        SetWorkloadPerThread(size, work_per_thread, start_position_per_core, 
                             final_position_per_core, num_cores);
        
        #pragma omp parallel
        {
            for (int64_t i = start_position_per_core[omp_get_thread_num()]; i < final_position_per_core[omp_get_thread_num()]; ++i)
            {
                d->_bits[i] = x->_bits[i] ^ y->_bits[i];
            }       
        }

    }
    
    
    static inline void not_parallel(SavimeBitsetPtr d, const SavimeBitsetPtr x, 
                                    int32_t num_cores, int32_t work_per_thread)
    {
        int64_t start_position_per_core[num_cores];
        int64_t final_position_per_core[num_cores];

        SetWorkloadPerThread(x->size(), work_per_thread, start_position_per_core, 
                             final_position_per_core, num_cores);

        #pragma omp parallel
        {
            for (int64_t i = start_position_per_core[omp_get_thread_num()]; i < final_position_per_core[omp_get_thread_num()]; ++i)
            {
                d->_bits[i] = ~x->_bits[i];
            }
        }
    }    
    
    bool all_parallel(int32_t num_cores, int32_t work_per_thread)
    {
        int64_t start_position_per_core[num_cores], final_position_per_core[num_cores];
        int32_t num_threads = SetWorkloadPerThread(_length, work_per_thread, start_position_per_core, final_position_per_core, num_cores);
        bool found_zero = false;

        if (extra_bits == 0) {

            #pragma omp parallel
            {
                for (size_type i = start_position_per_core[omp_get_thread_num()], e = final_position_per_core[omp_get_thread_num()]; i < e; ++i) {
                    if (m_bits[i] != all_ones || found_zero) {
                        found_zero = true;
                        break;
                    }
                }
            }

            return !found_zero;

        } else {

            final_position_per_core[num_threads-1]--;

            #pragma omp parallel
            {
                for (size_type i = start_position_per_core[omp_get_thread_num()], e = final_position_per_core[omp_get_thread_num()]; i < e; ++i) {
                    if (m_bits[i] != all_ones || found_zero) {
                        found_zero = true;
                        break;
                    }
                }
            }


            block_type const mask = ~(~static_cast<Block>(0) << extra_bits);
            if (m_highest_block() != mask || found_zero) {
                found_zero = true;
            }
        }

        return !found_zero;
    }

    
    bool any() const;
    bool any_parallel(int32_t num_cores, int32_t work_per_thread) const;
    bool none() const;
    bool none_parallel(int32_t num_cores, int32_t work_per_thread) const;
    

    size_type count_parallel(int32_t num_cores, int32_t work_per_thread) const BOOST_NOEXCEPT;
    size_type count_parallel(int64_t lower_bound, int64_t upper_bound, int32_t num_cores, int32_t work_per_thread) const BOOST_NOEXCEPT;
    
    
}


#endif /* SAVIME_BITMASK_H */

