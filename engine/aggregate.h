/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#ifndef AGGREGATE_H
#define AGGREGATE_H

#include <../core/include/util.h>
using namespace std;

#define NUM_LOCK 1000

struct AggregateFunction
{
    string function;
    string paramName;
    string attribName;

    AggregateFunction(string f, string p, string a)
    {
        function = f;
        paramName = p;
        attribName = a;
    }

    double GetStartValue()
    {
        if(!function.compare("avg"))
        {
            return 0.0;
        }
        else if(!function.compare("sum"))
        {
            return 0.0;
        }
        else if(!function.compare("min"))
        {
            return std::numeric_limits<double>::max();
        }
        else if(!function.compare("max"))
        {
            return std::numeric_limits<double>::min();
        }
        else if (!function.compare("count"))
        {
            return 0.0;
        }
    }
    
    bool RequiresAuxDataset()
    {
        return !function.compare("avg");
    }

};typedef shared_ptr<AggregateFunction> AggregateFunctionPtr;

struct AggregateConfiguration
{
    unordered_map<string, int64_t> _multipliers;
    unordered_map<string, int64_t> _adj;
    unordered_map<string, int64_t> _skew;
    vector<DimensionPtr> _dimensions;
    vector<AggregateFunctionPtr> _functions;
    unordered_map<string, DatasetPtr> _datasets;
    unordered_map<string, DatasetHandlerPtr> _handlers;
    unordered_map<string, DatasetHandlerPtr> _auxHandlers;
    unordered_map<string, DatasetHandlerPtr> _inputHandlers;
    unordered_map<string, DatasetHandlerPtr> _indexesHandlers;
    unordered_map<string, int64_t*> _indexesHandlersBuffers;

    void Configure()
    {
        int32_t numDims = _dimensions.size();
        for(int32_t i = 0; i < numDims; i++)
        {
            auto dimName = _dimensions[i]->name;
            
            _multipliers[dimName] = 1;
            _skew[dimName] = _dimensions[i]->GetLength();
            _adj[dimName]  = 1;
            
            for(int32_t j = i+1; j < numDims; j++)
            {
                int64_t preamble = _multipliers[dimName];
                _multipliers[dimName] = preamble*_dimensions[j]->GetLength();
                _skew[dimName] *= _dimensions[j]->GetLength();
                _adj[dimName]  *= _dimensions[j]->GetLength();
            }
        }
        
        if(numDims == 0)
        {
            _skew["i"] = 1.0;
            _adj["i"] = 1.0;
        }
    }

    int64_t GetTotalLength()
    {
        int64_t totalLen = 1;

        for(DimensionPtr dim : _dimensions)
            totalLen *= dim->GetLength();

        return totalLen;
    }

    int64_t GetLinearPosition(unordered_map<string, int64_t> pos)
    {
        int64_t linearPos = 0;
        for(auto entry: pos)
        {
            string dim = entry.first;
            int64_t pos = entry.second;
            linearPos+=  _multipliers[dim]*pos;
        }

        return linearPos;
    }

};typedef shared_ptr<AggregateConfiguration> AggregateConfigurationPtr;

template <class T>
class AggregateEngine
{
    AggregateConfigurationPtr _aggConfig;
    AggregateFunctionPtr _function;
    int64_t _subtarLen;
    int64_t _numCores;
    int64_t _minWork;
    mutex _mutexes[NUM_LOCK];
    
public:
    
    AggregateEngine(AggregateConfigurationPtr aggConfig, 
                    AggregateFunctionPtr function, 
                    int64_t subtarLen, 
                    int64_t numCores, 
                    int64_t minWork)
    {
        _aggConfig = aggConfig;
        _function = function;
        _subtarLen = subtarLen;
        _numCores = numCores;
        _minWork = minWork;
    }

    void CalcAvg()
    {
        T* buffer = (T*) _aggConfig->_inputHandlers[_function->paramName]->GetBuffer();
        double * outputBuffer = (double*) _aggConfig->_handlers[_function->attribName]->GetBuffer();
        double * outputAuxBuffer = (double*) _aggConfig->_auxHandlers[_function->attribName]->GetBuffer();
                
        int64_t startPositionPerCore[_numCores], finalPositionPerCore[_numCores];
        SetWorkloadPerThread(_subtarLen, _minWork, startPositionPerCore, finalPositionPerCore, _numCores);
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
        {
            unordered_map<string, int64_t> pos;
            for(auto dim : _aggConfig->_dimensions)
            {
                pos[dim->name] =  _aggConfig->_indexesHandlersBuffers[dim->name][i];
            }
            int64_t linearPos = _aggConfig->GetLinearPosition(pos);
            
            int64_t mutexId = linearPos%NUM_LOCK;
            
            _mutexes[mutexId].lock();
            outputBuffer[linearPos]+= buffer[i];
            outputAuxBuffer[linearPos]++;
            _mutexes[mutexId].unlock();
        }
    }

    void CalcSum()
    {
        T* buffer = (T*) _aggConfig->_inputHandlers[_function->paramName]->GetBuffer();
        double * outputBuffer = (double*) _aggConfig->_handlers[_function->attribName]->GetBuffer();
        int64_t startPositionPerCore[_numCores], finalPositionPerCore[_numCores];
        SetWorkloadPerThread(_subtarLen, _minWork, startPositionPerCore, finalPositionPerCore, _numCores);
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
        {
            unordered_map<string, int64_t> pos;
            for(auto dim : _aggConfig->_dimensions)
            {
                pos[dim->name] =  _aggConfig->_indexesHandlersBuffers[dim->name][i];
            }
            int64_t linearPos = _aggConfig->GetLinearPosition(pos);
            
            int64_t mutexId = linearPos%NUM_LOCK;
            
            _mutexes[mutexId].lock();
            outputBuffer[linearPos] += buffer[i];
            _mutexes[mutexId].unlock();
        }
    }

    void CalcMin()
    {
        T* buffer = (T*) _aggConfig->_inputHandlers[_function->paramName]->GetBuffer();
        double * outputBuffer = (double*) _aggConfig->_handlers[_function->attribName]->GetBuffer();
        int64_t startPositionPerCore[_numCores], finalPositionPerCore[_numCores];
        SetWorkloadPerThread(_subtarLen, _minWork, startPositionPerCore, finalPositionPerCore, _numCores);
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
        {
            unordered_map<string, int64_t> pos;
            for(auto dim : _aggConfig->_dimensions)
            {
                pos[dim->name] =  _aggConfig->_indexesHandlersBuffers[dim->name][i];
            }
            int64_t linearPos = _aggConfig->GetLinearPosition(pos);
            
            int64_t mutexId = linearPos%NUM_LOCK;
            
            _mutexes[mutexId].lock();
            if(buffer[i] < outputBuffer[linearPos])
                outputBuffer[linearPos] = buffer[i];
            _mutexes[mutexId].unlock();
        }    
    }

    void CalcMax()
    {
        T* buffer = (T*) _aggConfig->_inputHandlers[_function->paramName]->GetBuffer();
        double * outputBuffer = (double*) _aggConfig->_handlers[_function->attribName]->GetBuffer();
        int64_t startPositionPerCore[_numCores], finalPositionPerCore[_numCores];
        SetWorkloadPerThread(_subtarLen, _minWork, startPositionPerCore, finalPositionPerCore, _numCores);
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
        {
            unordered_map<string, int64_t> pos;
            for(auto dim : _aggConfig->_dimensions)
            {
                pos[dim->name] =  _aggConfig->_indexesHandlersBuffers[dim->name][i];
            }
            int64_t linearPos = _aggConfig->GetLinearPosition(pos);
            
            int64_t mutexId = linearPos%NUM_LOCK;
            
            _mutexes[mutexId].lock();
            if(buffer[i] > outputBuffer[linearPos])
                outputBuffer[linearPos] = buffer[i];
            _mutexes[mutexId].unlock();
        }
    }

    void CalcCount()
    {
        T* buffer = (T*) _aggConfig->_inputHandlers[_function->paramName]->GetBuffer();
        double * outputBuffer = (double*) _aggConfig->_handlers[_function->attribName]->GetBuffer();
        int64_t startPositionPerCore[_numCores], finalPositionPerCore[_numCores];
        SetWorkloadPerThread(_subtarLen, _minWork, startPositionPerCore, finalPositionPerCore, _numCores);
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
        {
            unordered_map<string, int64_t> pos;
            for(auto dim : _aggConfig->_dimensions)
            {
                pos[dim->name] =  _aggConfig->_indexesHandlersBuffers[dim->name][i];
            }
            int64_t linearPos = _aggConfig->GetLinearPosition(pos);
            
            int64_t mutexId = linearPos%NUM_LOCK;
            
            _mutexes[mutexId].lock();
            outputBuffer[linearPos]++;
            _mutexes[mutexId].unlock();
        }
    }

    void Run()
    {
        if(!_function->function.compare("avg"))
        {
            CalcAvg();
        }
        else  if(!_function->function.compare("sum"))
        {
            CalcSum();
        }
        else  if(!_function->function.compare("min"))
        {
            CalcMin();
        }
        else  if(!_function->function.compare("max"))
        {
            CalcMax();
        }
        else  if(!_function->function.compare("count"))
        {
            CalcCount();
        }
    }
    
    void Finalize()
    {
        if(!_function->function.compare("avg"))
        {
            double * outputBuffer = (double*) _aggConfig->_handlers[_function->attribName]->GetBuffer();
            double * outputAuxBuffer = (double*) _aggConfig->_auxHandlers[_function->attribName]->GetBuffer();

            int64_t startPositionPerCore[_numCores], finalPositionPerCore[_numCores];
            SetWorkloadPerThread(_subtarLen, _minWork, startPositionPerCore, finalPositionPerCore, _numCores);
            
            #pragma omp parallel for
            for(int64_t i = 0; i < _aggConfig->_datasets[_function->attribName]->entry_count; i++)
            {
                if(outputAuxBuffer[i] > 0.0)
                    outputBuffer[i] /= outputAuxBuffer[i];
            }
        }
    }
};
        
        
#endif /* AGGREGATE_H */

