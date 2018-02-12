#ifndef UTIL_H
#define UTIL_H
//Auxiliary functions

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <algorithm>
#include <vector>
#include <string>
#include <omp.h>
#include <chrono>

using namespace std;
using namespace chrono;

//------------------------------------------------------------------------------
//TIME MEASUREMENTES MACROS
#define GET_T1() auto t1 = high_resolution_clock::now()
#define GET_T2() auto t2 = high_resolution_clock::now()
#define GET_DURATION() duration_cast<microseconds>(t2-t1).count()


//------------------------------------------------------------------------------
//FILES FUNCTIONS
inline std::ifstream::pos_type FILE_SIZE(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}

inline bool EXIST_FILE(const std::string& name)
{
  struct stat buffer;   
  return (stat (name.c_str(), &buffer) == 0); 
}

inline std::string generateUniqueFileName(std::string path)
{
    char number[50];
    std::string uniqueFile;
    srand (time(NULL));
    
    do
    {
       snprintf(number, 50, "%x", rand());
       uniqueFile = path +"/"+number;
        
    }while(EXIST_FILE(uniqueFile));
 
    return uniqueFile;
}

inline int fd_set_blocking(int fd, int blocking) {
    /* Save the current flags */
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
        return 0;

    if (blocking)
        flags &= ~O_NONBLOCK;
    else
        flags |= O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags) != -1;
}


inline void ReadFileAt(char * file, int64_t offset, int64_t lenght, void * ret)
{
    return;
}

//------------------------------------------------------------------------------
//STRINGS FUNCTIONS
inline std::string between(std::string source, std::string delimiter1, std::string delimiter2)
{
    unsigned first = source.find(delimiter1);
    unsigned last = source.find(delimiter2);
    
    if(first == std::string::npos || last == std::string::npos)
    {
        return "";
    }
    
    return source.substr(first+1,last-first-1);
}

inline std::vector<std::string> split(const std::string& str, const char& ch) 
{
    std::string next; std::vector<std::string> result;
 
    for (std::string::const_iterator it = str.begin(); it != str.end(); it++) 
    {
        if (*it == ch) 
        {
            if (!next.empty()) 
            {
                result.push_back(next);
                next.clear();
            }
        } 
        else 
        {
            next += *it;
        }
    }
    
    if (!next.empty())
         result.push_back(next);
    
    return result;
}

inline std::string & ltrim(std::string & str)
{
  auto it2 =  std::find_if( str.begin() , str.end() , [](char ch){ return !std::isspace<char>(ch , std::locale::classic() ) ; } );
  str.erase( str.begin() , it2);
  return str;   
}

inline std::string & rtrim(std::string & str)
{
  auto it1 =  std::find_if( str.rbegin() , str.rend() , [](char ch){ return !std::isspace<char>(ch , std::locale::classic() ) ; } );
  str.erase( it1.base() , str.end() );
  return str;   
}

inline std::string & trim(std::string & str)
{
   return ltrim(rtrim(str));
}

inline bool validadeIdentifier(string identifier)
{
    const char * cIdentifier = identifier.c_str();
    for(int32_t i = 0; i < identifier.length(); ++i)
    { 
        if(!isalpha(cIdentifier[i]) && !isdigit(cIdentifier[i]) 
              && cIdentifier[i] != '_')
            return false;
        
        if(isdigit(cIdentifier[i]) && i == 0)
            return false;    
    }
    
    return true;
}

////MISC FUNCTIONS
//------------------------------------------------------------------------------
//Function original source: https://www.codeproject.com/Articles/17480/Optimizing-integer-divisions-with-Multiply-Shift-i
// val/div == (val*mul) >> shift;
inline void fast_division(int64_t max, int64_t div, int64_t& mul, int64_t& shift)
{
    bool found = false;
    mul = -1;
    shift = -1;

    // zero divider error
    if (div == 0) return;

    // this division would always return 0 from 0..max
    if (max < div) 
    {
        mul = 0;
        shift = 0;
        return;
    }

    // catch powers of 2
    for (int s = 0; s <= 63; s++)
    {
        if (div == (1L << s))
        {
            mul = 1;
            shift = s;
            return;
        }
    }

    // start searching for a valid mul/shift pair
    for (shift = 1; shift <= 62; shift++)
    {
        // shift factor is at least 2log(div), skip others
        if ((1L << shift) <= div) continue;

        // we calculate a candidate for mul
        mul = (1L << shift) / div + 1;

        // assume it is a good one
        found = true;

        // test if it works for the range 0 .. max
        // Note: takes too much time for large values of max. 
        if (max < 1000000)
        {
            for (int64_t i = max; i >=1; i--)		// testing large values first fails faster 
            {
                if ((i / div) != ((i * mul) >> shift))
                {
                    found = false;
                    break;
                }
            }
        }
        else
        {
            // very fast test, no mathematical proof yet but it seems to work well
            // test highest number-pair for which the division must 'jump' correctly
            // test max, to be sure;
            int64_t t = (max/div +1) * div;
            if ((((t-1) / div) != (((t-1) * mul) >> shift)) ||
                ((t / div) != ((t * mul) >> shift)) ||
                ((max / div) != ((max * mul) >> shift))
               )
                {
                        found = false;
                }
        }

        // are we ready?
        if (found)
        {
            break;
        }
    }
}


inline int SetWorkloadPerThread(int64_t workloadSize,  
                                int32_t minWorkPerThread, 
                                int64_t startPositionPerCore[], 
                                int64_t finalPositionPerCore[], 
                                int32_t numCores)
{
    
    int64_t chunk = workloadSize/numCores;
    #define BYTE_ALIGNMENT 8
    //int64_t chunk = ((workloadSize/BYTE_ALIGNMENT)/numCores)*BYTE_ALIGNMENT;

    
    if(chunk > minWorkPerThread)
    {
        for(int64_t i = 0; i < numCores-1; i++)
        {
            finalPositionPerCore[i] = startPositionPerCore[i+1] = (i+1)*chunk;
        }

        startPositionPerCore[0] = 0;
        finalPositionPerCore[numCores-1]= workloadSize;
    }
    else
    {
        numCores = 1;
        startPositionPerCore[0] = 0;
        finalPositionPerCore[0] = workloadSize;
    }
    omp_set_num_threads(numCores);
    return numCores;
}

inline int SetWorkloadPerThread(int64_t workloadSize,  
                                int32_t minWorkPerThread, 
                                int64_t startPositionPerCore[], 
                                int64_t finalPositionPerCore[], 
                                int32_t numCores,
                                int32_t alignment)
{
    
    int64_t chunk = ((workloadSize/alignment)/numCores)*alignment;

    
    if(chunk > minWorkPerThread)
    {
        for(int64_t i = 0; i < numCores-1; i++)
        {
            finalPositionPerCore[i] = startPositionPerCore[i+1] = (i+1)*chunk;
        }

        startPositionPerCore[0] = 0;
        finalPositionPerCore[numCores-1]= workloadSize;
    }
    else
    {
        numCores = 1;
        startPositionPerCore[0] = 0;
        finalPositionPerCore[0] = workloadSize;
    }
    omp_set_num_threads(numCores);
    return numCores;
}

#endif /* UTIL_H */

