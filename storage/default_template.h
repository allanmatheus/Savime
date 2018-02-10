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
#ifndef DEFAULT_TEMPLATE_H
#define DEFAULT_TEMPLATE_H

#include "include/util.h"
#include "include/query_data_manager.h"
#include "include/storage_manager.h"
#include "default_storage_manager.h"
#include <cmath>
#include <omp.h>

template <class T1, class T2, class T3>
class TemplateStorageManager 
{
    StorageManagerPtr _storageManager;
    ConfigurationManagerPtr _configurationManager;
    SystemLoggerPtr _systemLogger;
    
public:
       
    TemplateStorageManager(StorageManagerPtr storageManager, ConfigurationManagerPtr configurationManager, SystemLoggerPtr systemLogger)
    {
        _storageManager = storageManager;
        _configurationManager = configurationManager;
        _systemLogger = systemLogger;
    }
      
    RealIndex Logical2Real(DimensionPtr dimension, T1 logicalIndex)
    {
        double DIFF = 0.000001;
        double intpart;
        RealIndex realIndex = INVALID_EXACT_REAL_INDEX;
        
        if(dimension->dimension_type == IMPLICIT)
        {
            //If it is in the range
            if(dimension->lower_bound <= logicalIndex &&
                    dimension->upper_bound >= logicalIndex)
            {
                double fRealIndex = 0.0;
                double preamble = 1/dimension->spacing;
                
                fRealIndex = (logicalIndex*preamble - dimension->lower_bound*preamble);
                double mod = std::modf(fRealIndex, &intpart);
                
                if(mod < DIFF)
                {
                    //returns lowest index closest to the logical value
                    realIndex = (RealIndex) intpart;
                }
            }
        }
        else if(dimension->dimension_type == EXPLICIT)
        {
            int numCores = _configurationManager->GetIntValue(MAX_THREADS);
            omp_set_num_threads(numCores);
            
            auto handler = _storageManager->GetHandler(dimension->dataset);
            T1 * buffer = (T1*) handler->GetBuffer();
            
            if(dimension->dataset->sorted)
            {
                int64_t first=0, last=dimension->dataset->entry_count-1;
                int64_t middle=(last+first)/2;
                
                if(buffer[first] <= logicalIndex && buffer[last] >= logicalIndex)
                {
                    while(first <= last)
                    {
                        if (buffer[middle] < logicalIndex)
                        {
                           first=middle+1;
                        }
                        else if (buffer[middle] == logicalIndex) 
                        {
                           realIndex=middle;
                           break;
                        }
                        else
                        {
                           last=middle-1;
                        }

                        middle=(first+last)/2;
                    }
                }
            }
            else
            {
                #pragma omp parallel for
                for(int64_t i = 0; i < dimension->dataset->entry_count; ++i)
                {    
                    if(buffer[i] == logicalIndex)
                    {
                        realIndex = i;                    
                    }
                }
            }
           
            handler->Close();
        }

        return realIndex;
    }
    
    RealIndex Logical2ApproxReal(DimensionPtr dimension, T1 logicalIndex)
    {
        double intpart;
        RealIndex realIndex = INVALID_EXACT_REAL_INDEX;
        
        if(dimension->dimension_type == IMPLICIT)
        {       
            if(dimension->lower_bound > logicalIndex)
                return BELOW_OFFBOUNDS_REAL_INDEX;
                    
            if(dimension->upper_bound < logicalIndex)
                return ABOVE_OFFBOUNDS_REAL_INDEX;
            
                
            double fRealIndex = (logicalIndex - dimension->lower_bound)/dimension->spacing;
            std::modf(fRealIndex, &intpart);
            realIndex = (RealIndex) intpart;
            
        }
        else if(dimension->dimension_type == EXPLICIT)
        {
            int numCores = _configurationManager->GetIntValue(MAX_THREADS);
            omp_set_num_threads(numCores);
            
            auto handler = _storageManager->GetHandler(dimension->dataset);
            T1 * buffer = (T1*) handler->GetBuffer();
            
            if(dimension->dataset->sorted)
            {
                int64_t first=0, last=dimension->dataset->entry_count-1;
                int64_t middle=(last+first)/2;
                
                if(buffer[first] > logicalIndex)
                    return BELOW_OFFBOUNDS_REAL_INDEX;
                    
                if(buffer[last] < logicalIndex)
                    return ABOVE_OFFBOUNDS_REAL_INDEX;    
                
                while(first <= last)
                {
                    if (buffer[middle] < logicalIndex)
                    {
                       first=middle+1;
                    }
                    else if (buffer[middle] == logicalIndex) 
                    {
                       realIndex=middle;
                       break;
                    }
                    else
                    {
                       last=middle-1;
                    }

                    middle=(first+last)/2;
                }

                if(realIndex == INVALID_EXACT_REAL_INDEX)
                {
                    realIndex=middle;
                }
                
            }
            else
            {
                #pragma omp parallel for
                for(int64_t i = 0; i < dimension->dataset->entry_count; ++i)
                {    
                    if(buffer[i] == logicalIndex)
                    {
                        realIndex = i;                    
                    }
                }
            }
           
            handler->Close();
        }

        return realIndex;
    }
      
    SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr logicalIndexes, DatasetPtr& destinyDataset)
    {
        bool invalidMapping = false;
        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];
       
        SetWorkloadPerThread(logicalIndexes->entry_count, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
                
        destinyDataset = _storageManager->Create(LONG_TYPE, logicalIndexes->entry_count);
        if(destinyDataset == NULL)
                throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr logicalIndexesHandler = _storageManager->GetHandler(logicalIndexes);
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);
        T1 * logicalBuffer = (T1 *)logicalIndexesHandler->GetBuffer();
        int64_t * destinyBuffer = (int64_t *)destinyHandler->GetBuffer();
        
        if(dimension->dimension_type == IMPLICIT)
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                destinyBuffer[i] = (RealIndex) (logicalBuffer[i]-dimension->lower_bound)/dimension->spacing;
                if(destinyBuffer[i] < dimSpecs->lower_bound || destinyBuffer[i] > dimSpecs->upper_bound)
                {
                    invalidMapping = true;
                    break;
                }
            }
        }
        else if(dimension->dimension_type == EXPLICIT)
        {
            
            auto dimensionHandler = _storageManager->GetHandler(dimension->dataset);
            T1 * dimensionBuffer = (T1*) dimensionHandler->GetBuffer();
            std::map<T1, RealIndex> indexMap;
            
            for(int64_t i = 0; i < dimension->dataset->entry_count; ++i)
            {
                indexMap[dimensionBuffer[i]] = i;
            }
            
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                if(indexMap.find(logicalBuffer[i]) != indexMap.end())
                {
                    destinyBuffer[i] = indexMap[logicalBuffer[i]];
                    if(destinyBuffer[i] < dimSpecs->lower_bound || destinyBuffer[i] > dimSpecs->upper_bound)
                    {
                        invalidMapping = true;
                        break;
                    }
                }
                else
                {
                    invalidMapping = true;
                    break;
                }
            }
            
            dimensionHandler->Close();
        }
        
        logicalIndexesHandler->Close();
        destinyHandler->Close();
        
        if(!invalidMapping)
            return SAVIME_SUCCESS;
        else
            return SAVIME_FAILURE;
    }
    
    T1 Real2Logical(DimensionPtr dimension, RealIndex realIndex)
    {
        T1 logicalIndex = 0;

        if(dimension->dimension_type == IMPLICIT)
        {
           logicalIndex = (T1)(realIndex*dimension->spacing+dimension->lower_bound);
        }
        else if(dimension->dimension_type == EXPLICIT)
        {
            auto handler = _storageManager->GetHandler(dimension->dataset);
            T1 * buffer = (T1*) handler->GetBuffer();
            
            if(realIndex < dimension->dataset->entry_count)
                logicalIndex = buffer[realIndex];
            
            handler->Close();
        }

        return logicalIndex;
    }
        
    SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr realIndexes,  DatasetPtr& destinyDataset)
    {
        bool invalidMapping = false;
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];
        SetWorkloadPerThread(realIndexes->entry_count, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
 
        destinyDataset = _storageManager->Create(LONG_TYPE, realIndexes->entry_count);
        if(destinyDataset == NULL)
            throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr realIndexesHandler = _storageManager->GetHandler(realIndexes);
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);
        int64_t * realBuffer = (int64_t *)realIndexesHandler->GetBuffer();
        T1 * destinyBuffer = (T1 *)destinyHandler->GetBuffer();
        
        if(dimension->dimension_type == IMPLICIT)
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                if(realBuffer[i] < dimSpecs->lower_bound || realBuffer[i] > dimSpecs->upper_bound)
                {
                    invalidMapping = true;
                    break;
                }
                
                destinyBuffer[i] = (T1)(realBuffer[i]*dimension->spacing+dimension->lower_bound);
            }
        }
        else if(dimension->dimension_type == EXPLICIT)
        {
            auto dimensionHandler = _storageManager->GetHandler(dimension->dataset);
            T1 * dimensionBuffer = (T1*) dimensionHandler->GetBuffer();
                    
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                if(realBuffer[i] < dimSpecs->lower_bound || realBuffer[i] > dimSpecs->upper_bound)
                {
                    invalidMapping = true;
                    break;
                }
                
                if(realBuffer[i] < dimension->dataset->entry_count)
                {
                    destinyBuffer[i] = dimensionBuffer[realBuffer[i]];
                }
                else
                {
                    invalidMapping = false;
                    break;
                }
                    
            }
            
            dimensionHandler->Close();
        }
        
        realIndexesHandler->Close();
        destinyHandler->Close();
        
        if(!invalidMapping)
            return SAVIME_SUCCESS;
        else
            return SAVIME_FAILURE;
    }
        
    SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2, DimensionPtr& destinyDim)
    {
        #define IN_RANGE(X, Y, Z) (X >= Y) && (X <= Z)

        int numCores = _configurationManager->GetIntValue(MAX_THREADS); 
        int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];
        DimensionPtr dims[2] = {dim1, dim2};
        DimSpecPtr dummyDims[2];
        DatasetPtr materializedDimensions[2];
        
        for(int32_t i = 0; i < 2; i++)
        {
            dummyDims[i] = DimSpecPtr(new DimensionSpecification());
            dummyDims[i]->dimension = DataElementPtr(new DataElement(dims[i]));
            dummyDims[i]->type = ORDERED;
            dummyDims[i]->lower_bound = 0;
            dummyDims[i]->upper_bound = dims[i]->GetLength()-1;
            dummyDims[i]->adjacency = 1;
            dummyDims[i]->skew = dims[i]->GetLength()-1;
            _storageManager->MaterializeDim(dummyDims[i], dims[i]->GetLength(), materializedDimensions[i]);
        }

        DatasetHandlerPtr handler1 = _storageManager->GetHandler(materializedDimensions[0]);
        T1 * buffer1 = (T1*) handler1->GetBuffer(); 
        DatasetHandlerPtr handler2 = _storageManager->GetHandler(materializedDimensions[1]);
        T2 * buffer2 = (T2*) handler2->GetBuffer(); 

        
        DatasetPtr filterDs = DatasetPtr(new Dataset());
        filterDs->Addlistener(std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
        filterDs->has_indexes = false;
        filterDs->sorted = false;
        filterDs->bitMask = std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(dim1->GetLength()));
        
        if(filterDs->bitMask == NULL)
            throw std::runtime_error("Could not allocate memory for the bitmask index.");
        
        if(CheckSorted(materializedDimensions[1]))
        {
            SetWorkloadPerThread(dim1->GetLength(), workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
                
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                T1 value = buffer1[i];

                int64_t first=0, last=dim2->GetLength();
                int64_t middle=(last+first)/2;
                
                while(first <= last)
                {
                    if (buffer2[middle] < value)
                    {
                       first=middle+1;
                    }
                    else if (buffer2[middle] == value) 
                    {
                       filterDs->bitMask->set(i, 1);
                       break;
                    }
                    else
                    {
                       last=middle-1;
                    }

                    middle=(first+last)/2;
                }
            }
        }
        else if(CheckSorted(materializedDimensions[0]))
        {
            SetWorkloadPerThread(dim2->GetLength(), workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
            
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                T2 value = buffer2[i];

                int64_t first=0, last=dim2->GetLength();
                int64_t middle=(last+first)/2;
                
                while(first <= last)
                {
                    if (buffer2[middle] < value)
                    {
                       first=middle+1;
                    }
                    else if (buffer2[middle] == value) 
                    {
                      filterDs->bitMask->set(middle, 1);
                       break;
                    }
                    else
                    {
                       last=middle-1;
                    }

                    middle=(first+last)/2;
                }
            }
           
        }
        else
        {
            SetWorkloadPerThread(dim1->GetLength(), workPerThread, startPositionPerCore, finalPositionPerCore, numCores, filterDs->bitMask->bits_per_block);
                
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                T1 value = buffer1[i];

                int64_t last = dim2->GetLength();

                for(int64_t j = 0; j < last; j++)
                {
                    if (buffer2[j] == value) 
                    {
                       filterDs->bitMask->set(i, 1);
                       break;
                    }
                }
            }
            
        }
        
        DatasetPtr intersectedDimDs;
        _storageManager->Filter(materializedDimensions[0], filterDs, intersectedDimDs);
        destinyDim = DimensionPtr(new Dimension());
        destinyDim->dataset = intersectedDimDs;
        destinyDim->lower_bound = 0;
        destinyDim->upper_bound = intersectedDimDs->entry_count-1;
        destinyDim->spacing = 1;
        destinyDim->type = dim1->type;
        destinyDim->dimension_type = EXPLICIT;
        
        return SAVIME_SUCCESS;
    }
    
    bool CheckSorted(DatasetPtr dataset)
    {
        bool isSorted = true;
        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];
        SetWorkloadPerThread(dataset->entry_count, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        startPositionPerCore[0]=1;
        
        dataset->sorted = true;
        DatasetHandlerPtr dsHandler = _storageManager->GetHandler(dataset);
        T1 * buffer = (T1*) dsHandler->GetBuffer();
        
        #pragma omp parallel
        {
            for(int64_t i = startPositionPerCore[omp_get_thread_num()]; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
            {
                if(buffer[i-1] > buffer[i] || !isSorted)
                {
                    dataset->sorted = isSorted = false;
                    break;
                }
            }
        }
        
        dsHandler->Close();
        return dataset->sorted;
    }
    
    SavimeResult Copy(DatasetPtr originDataset, int64_t lowerBound, int64_t upperBound, int64_t offsetInDestiny, int64_t spacingInDestiny, DatasetPtr destinyDataset)
    {
        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores], finalPositionPerCore[numCores];
        SetWorkloadPerThread(upperBound-lowerBound, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        
        DatasetHandlerPtr originHandler = _storageManager->GetHandler(originDataset);
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);
        T1 * originBuffer = (T1*) originHandler->GetBuffer();
        T2 * destinyBuffer = (T2*) destinyHandler->GetBuffer();
        
        #pragma omp parallel for
        for(int64_t i = lowerBound; i <= upperBound; i++)
        {
            int64_t pos = (i-lowerBound)*spacingInDestiny+offsetInDestiny;
            destinyBuffer[pos] = (T2) originBuffer[i];
        }
        
        originHandler->Close();
        destinyHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping, DatasetPtr destinyDataset, int64_t& copied)
    {
        #define INVALID -1

        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores], finalPositionPerCore[numCores];
        SetWorkloadPerThread(originDataset->entry_count, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        
        DatasetHandlerPtr originHandler = _storageManager->GetHandler(originDataset);
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);
        DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mapping);
        
        T1 * originBuffer = (T1*) originHandler->GetBuffer();
        T2 * destinyBuffer = (T2*) destinyHandler->GetBuffer();
        int64_t * mappingBuffer = (int64_t*)mappingHandler->GetBuffer();
        
        #pragma omp parallel for reduction(+:copied)
        for(int64_t i = 0; i < originDataset->entry_count; i++)
        {
            int64_t pos = mappingBuffer[i];
            if(pos != INVALID)
            {
                destinyBuffer[pos] = (T2) originBuffer[i];
                copied++;
            }
        }
        
        originHandler->Close();
        destinyHandler->Close();
        mappingHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet, DataType type,  DatasetPtr& destinyDataset)
    {
        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];

        int64_t counters[numCores], partialCounters[numCores], generalIndex=0;
        memset((char*)counters, 0, sizeof(int64_t)*numCores);
        memset((char*)partialCounters, 0, sizeof(int64_t)*numCores);

        if(!filterDataSet->has_indexes)
            _storageManager->FromBitMaskToIndex(filterDataSet, true);
               
        SetWorkloadPerThread(filterDataSet->entry_count, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        DatasetHandlerPtr originHandler = _storageManager->GetHandler(originDataset);
        DatasetHandlerPtr filterHandler = _storageManager->GetHandler(filterDataSet);
        int64_t * filterBuffer = (int64_t *)filterHandler->GetBuffer();
        
        destinyDataset = _storageManager->Create(type, filterDataSet->entry_count);
        if(destinyDataset == NULL)
            throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);    
        T3 * destinyBuffer = (T3*) destinyHandler->GetBuffer();
        T3 * originBuffer = (T3*) originHandler->GetBuffer();
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
        {
            destinyBuffer[i] = originBuffer[filterBuffer[i]];
        }
       
        originHandler->Close();
        filterHandler->Close();
        destinyHandler->Close(); 
        return SAVIME_SUCCESS;
    }
    
    SavimeResult Comparison(std::string op, DatasetPtr  operand1,  DatasetPtr  operand2,  DatasetPtr& destinyDataset)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores]; int64_t finalPositionPerCore[numCores];
        int64_t entryCount = operand1->entry_count <= operand2->entry_count? operand1->entry_count : operand2->entry_count;

        DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
        DatasetHandlerPtr op2Handler = _storageManager->GetHandler(operand2);        
        T1 * op1Buffer = (T1*) op1Handler->GetBuffer();
        T2 * op2Buffer = (T2*) op2Handler->GetBuffer();
       
        destinyDataset = DatasetPtr(new Dataset());
        destinyDataset->Addlistener(std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
        destinyDataset->has_indexes = false;
        destinyDataset->sorted = false;
        destinyDataset->bitMask = std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(entryCount));
        
        if(destinyDataset->bitMask == NULL)
            throw std::runtime_error("Could not allocate memory for the bitmask index.");
        
        
        SetWorkloadPerThread(entryCount, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores, destinyDataset->bitMask->bits_per_block);
        
        if(!op.compare("="))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] == op2Buffer[i];
                
        }
        else if(!op.compare("<>"))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] != op2Buffer[i];
         
        }
        else if(!op.compare("<"))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i]< op2Buffer[i];
        }
        else if(!op.compare(">"))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i]> op2Buffer[i];
        }
        else if(!op.compare("<="))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i]<=op2Buffer[i];
        }
        else if(!op.compare(">="))
        {
           #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i]>= op2Buffer[i];
        }
        else
        {
            throw std::runtime_error("Invalid comparison operation.");
        }
        
        op1Handler->Close();
        op2Handler->Close();
                
        return SAVIME_SUCCESS;
    }
    
    SavimeResult Comparison(std::string op,  DatasetPtr operand1, T2 operand2,  DatasetPtr& destinyDataset)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores]; int64_t finalPositionPerCore[numCores];
        int64_t entryCount = operand1->entry_count;
        
        DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
        T1 * op1Buffer = (T1*) op1Handler->GetBuffer();
       
        destinyDataset = DatasetPtr(new Dataset());
        destinyDataset->Addlistener(std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
        destinyDataset->has_indexes = false;
        destinyDataset->sorted = false;
        destinyDataset->bitMask = std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(entryCount));
        
        if(destinyDataset->bitMask == NULL)
            throw std::runtime_error("Could not allocate memory for the bitmask index.");
        
        SetWorkloadPerThread(entryCount, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores, destinyDataset->bitMask->bits_per_block);
        
        if(!op.compare("="))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] == operand2;
                
        }
        else if(!op.compare("<>"))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] != operand2;
         
        }
        else if(!op.compare("<"))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] < operand2;
        }
        else if(!op.compare(">"))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] > operand2;
        }
        else if(!op.compare("<="))
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] <= operand2;
        }
        else if(!op.compare(">="))
        {
           #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                (*destinyDataset->bitMask)[i] = op1Buffer[i] >= operand2;
        }
        else
        {
            throw std::runtime_error("Invalid comparison operation.");
        }
        
        op1Handler->Close();        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs, vector<int64_t> lowerBounds, vector<int64_t> upperBounds, DatasetPtr& destinyDataset)
    {
        vector<DimSpecPtr> subsetSpecs; int64_t offset = 0, subsetLen = 1;
        
        for(int32_t i=0; i < dimSpecs.size(); i++)
        {
            lowerBounds[i] =std::max(lowerBounds[i], dimSpecs[i]->lower_bound);
            upperBounds[i] =std::min(upperBounds[i], dimSpecs[i]->upper_bound);
        }
        
        for(int32_t i=0; i < dimSpecs.size(); i++)
        {
            offset +=   (lowerBounds[i]-dimSpecs[i]->lower_bound)*dimSpecs[i]->adjacency;
            subsetLen *= (upperBounds[i]-lowerBounds[i]+1);
        }
        
        for(int32_t i=0; i < dimSpecs.size(); i++)
        {
            DimSpecPtr subSpecs = DimSpecPtr(new DimensionSpecification());
            subSpecs->lower_bound = lowerBounds[i];
            subSpecs->upper_bound = upperBounds[i];
            subSpecs->dimension = dimSpecs[i]->dimension;
            subSpecs->adjacency = dimSpecs[i]->adjacency;
            subsetSpecs.push_back(subSpecs);
        }
        
        std::sort(subsetSpecs.begin(), subsetSpecs.end(), compareAdj);
        std::sort(dimSpecs.begin(), dimSpecs.end(), compareAdj);
        
        for(DimSpecPtr spec : subsetSpecs)
        {
            bool isPosterior = false;
            spec->skew = 1;
            spec->adjacency = 1;

            for(DimSpecPtr innerSpec : subsetSpecs)
            {
                if(isPosterior)
                    spec->adjacency *= innerSpec->GetLength();

                if(!spec->dimension->GetName()
                   .compare(innerSpec->dimension->GetName()))
                {
                    isPosterior = true;
                }

                if(isPosterior)
                    spec->skew *= innerSpec->GetLength();
            }
        }
        
        vector<int64_t> realIndexes;
        realIndexes.resize(subsetSpecs.size());
        
        destinyDataset = _storageManager->Create(LONG_TYPE, subsetLen);
        destinyDataset->has_indexes = true;
        if(destinyDataset == NULL)
              throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr handler = _storageManager->GetHandler(destinyDataset);
        int64_t * buffer = (int64_t*) handler->GetBuffer();

        for(int64_t i=0; i < subsetLen; i++)
        {
            int64_t index = 0;
            
            for(int64_t dim = 0; dim < subsetSpecs.size(); dim++)
            {
                realIndexes[dim] = (i%subsetSpecs[dim]->skew)/subsetSpecs[dim]->adjacency;
            }
            
            for(int64_t dim = 0; dim < dimSpecs.size(); dim++)
            {
                index += realIndexes[dim]*dimSpecs[dim]->adjacency;
            }
            
            buffer[i] = index+offset;
        }
        
        handler->Close();
        
        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult ComparisonOrderedDim(std::string op, DimSpecPtr dimSpecs, T1 operand2, int64_t totalLength, DatasetPtr& destinyDataset)
    {
      
     
        #define MIN(X, Y) (X < Y) ? X : Y 
        map<string, string> invertedOp = {{">", "<="}, {"<", ">="}, {">=", "<"}, {"<=", ">"}};
        
        int numCores = _configurationManager->GetIntValue(MAX_THREADS); bool isInRange = false;
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        
        bool inSubtarRange = false, isInverted = false;
        int64_t entriesInBlock = dimSpecs->GetLength()*dimSpecs->adjacency, range;
        int64_t copies = totalLength/entriesInBlock;
        int64_t lowerInitialBound = 0, upperInitialBound = dimSpecs->GetLength();
                
        destinyDataset = DatasetPtr(new Dataset());
        destinyDataset->Addlistener(std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
        destinyDataset->has_indexes = false;
        destinyDataset->sorted = false;
        destinyDataset->bitMask = std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(totalLength));
        
        if(destinyDataset->bitMask == NULL)
            throw std::runtime_error("Could not allocate memory for the bitmask index.");
        
       
        //Exact real index is != -1 when there is a perfect match
        int64_t exactRealIndex = Logical2Real(dimSpecs->dimension->GetDimension(), operand2);
        
        //Approx real index is != -1 when logical index is in the range
        int64_t approxRealIndex = Logical2ApproxReal(dimSpecs->dimension->GetDimension(), operand2);  
       
        
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(approxRealIndex > INVALID_EXACT_REAL_INDEX)
        {
            if(approxRealIndex < dimSpecs->lower_bound)
                approxRealIndex = BELOW_OFFBOUNDS_REAL_INDEX;
            else if(approxRealIndex > dimSpecs->upper_bound)
                approxRealIndex = ABOVE_OFFBOUNDS_REAL_INDEX;
            else
                inSubtarRange = true;
        }
        
        if(approxRealIndex != BELOW_OFFBOUNDS_REAL_INDEX && 
              approxRealIndex !=  ABOVE_OFFBOUNDS_REAL_INDEX &&
              op != "=" &&  op != "<>")
        {
            int64_t midPoint = (dimSpecs->upper_bound - dimSpecs->lower_bound)/2;
            
            if(!op.compare("<") || !op.compare("<="))
            {
                if(approxRealIndex > midPoint)
                {
                    op = invertedOp[op];
                    isInverted = true;
                }
            }
            else
            {
                if(approxRealIndex < midPoint)
                {
                    op = invertedOp[op];
                    isInverted = true;
                }
            }
        }
        
        if(!op.compare("=") || !op.compare("<>"))
        {   
            bool val;
            if(!op.compare("="))
            {
                val = true;
            }
            else
            {
                val = false;
                destinyDataset->bitMask->set_parallel(numCores, minWorkPerThread);
            }
            
            if(exactRealIndex != INVALID_EXACT_REAL_INDEX && inSubtarRange)
            {
                lowerInitialBound = (exactRealIndex-dimSpecs->lower_bound)*dimSpecs->adjacency;
                upperInitialBound = lowerInitialBound+dimSpecs->adjacency;
                range = upperInitialBound - lowerInitialBound ;
                
                //Avoid race conditions in bitmask access. 
                //Positions accessed by different threads must be spaced.
                int64_t space = (dimSpecs->skew+lowerInitialBound) - (lowerInitialBound+range);
                
                if(space > (*destinyDataset->bitMask).bits_per_block)
                    omp_set_num_threads(MIN(numCores, copies));
                else
                    omp_set_num_threads(1);
                        
                #pragma omp parallel for
                for(int64_t i = 0; i < copies; i++)
                {
                    int64_t startPos = i*dimSpecs->skew+lowerInitialBound;
                    int64_t endPos = startPos+range;
                    for(int64_t pos = startPos; pos < endPos; pos++)
                    {
                        (*destinyDataset->bitMask)[pos] = val;
                        //(*destinyDataset->bitMask).fast_assign(pos, val);
                    }
                }
            }
        }
        else if(!op.compare("<") || !op.compare("<="))
        {
            if(approxRealIndex == BELOW_OFFBOUNDS_REAL_INDEX)
            {
                //Maintains bitmask zeroed
                return SAVIME_SUCCESS;
            }
            else if(approxRealIndex == ABOVE_OFFBOUNDS_REAL_INDEX)
            {
                destinyDataset->bitMask->set_parallel(numCores, minWorkPerThread);
            }
            else
            {
                bool val = (isInverted) ? 0 : 1;
                
                if(isInverted)
                    destinyDataset->bitMask->set_parallel(numCores, minWorkPerThread);
                
                lowerInitialBound = 0;
                upperInitialBound = (approxRealIndex-dimSpecs->lower_bound)*dimSpecs->adjacency+dimSpecs->adjacency;
              
                if(!op.compare("<") && (approxRealIndex == exactRealIndex))
                    upperInitialBound-=dimSpecs->adjacency;
                               
                range = upperInitialBound - lowerInitialBound;
               
                //Avoid race conditions in bitmask access. 
                //Positions accessed by different threads must be spaced.
                int64_t space = (dimSpecs->skew+lowerInitialBound) - (lowerInitialBound+range);  
                if(space > (*destinyDataset->bitMask).bits_per_block)
                    omp_set_num_threads(MIN(numCores, copies));
                else
                    omp_set_num_threads(1);
                
                #pragma omp parallel for
                for(int64_t i = 0; i < copies; i++)
                {
                    int64_t startPos = i*dimSpecs->skew+lowerInitialBound;
                    int64_t endPos = startPos+range;
                    for(int64_t pos = startPos; pos < endPos; pos++)
                    {
                        (*destinyDataset->bitMask)[pos] = val;
                        //(*destinyDataset->bitMask).fast_assign(pos, val);
                    }
                }
            }
        }
        else if(!op.compare(">") || !op.compare(">="))
        {
            if(approxRealIndex == BELOW_OFFBOUNDS_REAL_INDEX)
            {
               destinyDataset->bitMask->set_parallel(numCores, minWorkPerThread);
            }
            else if(approxRealIndex == ABOVE_OFFBOUNDS_REAL_INDEX)
            {
                //Maintains bitmask zeroed
                return SAVIME_SUCCESS;
            }
            else
            {
                bool val = (isInverted) ? 0 : 1;
                
                if(isInverted)
                    destinyDataset->bitMask->set_parallel(numCores, minWorkPerThread);
                
                lowerInitialBound = (approxRealIndex-dimSpecs->lower_bound)*dimSpecs->adjacency;
                if(!op.compare(">") || (approxRealIndex != exactRealIndex))
                    lowerInitialBound+=dimSpecs->adjacency;
                
                upperInitialBound = dimSpecs->GetLength()*dimSpecs->adjacency;
                
                range = upperInitialBound - lowerInitialBound;
                
                //Avoid race conditions in bitmask access. 
                //Positions accessed by different threads must be spaced.
                int64_t space = (dimSpecs->skew+lowerInitialBound) - (lowerInitialBound+range);    
                if(space > (*destinyDataset->bitMask).bits_per_block)
                    omp_set_num_threads(MIN(numCores, copies));
                else
                    omp_set_num_threads(1);
                
                #pragma omp parallel for
                for(int64_t i = 0; i < copies; i++)
                {
                    int64_t startPos = i*dimSpecs->skew+lowerInitialBound;
                    int64_t endPos = startPos+range;
                    for(int64_t pos = startPos; pos < endPos; pos++)
                    {
                        (*destinyDataset->bitMask)[pos] = val;
                        //(*destinyDataset->bitMask).fast_assign(pos, val);
                    }
                }
                
            }
        }
        else
        {
            throw std::runtime_error("Invalid comparison operation.");
        }
        
        #ifdef TIME 
           GET_T2();
           _systemLogger->LogEvent("TemplateStorage", "OrderedComparison took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return SAVIME_SUCCESS;
    }
    

    SavimeResult ComparisonDim(std::string op, DimSpecPtr dimSpecs, T1 operand2, int64_t totalLength, DatasetPtr& destinyDataset)
    {
        bool fastDimComparsionPossible = false;
        auto dimension = dimSpecs->dimension->GetDimension();
        auto dataset = dimension->dataset;
        bool sortedDataset = (dataset == NULL) ? false : dataset->sorted;
        
        fastDimComparsionPossible |= dimSpecs->type == ORDERED && dimension->dimension_type == IMPLICIT;
        fastDimComparsionPossible |= dimSpecs->type == ORDERED && sortedDataset;
        
        if(fastDimComparsionPossible)
        {
            return ComparisonOrderedDim(op, dimSpecs, operand2, totalLength, destinyDataset);
        }
        else
        {
            DatasetPtr  materializeDimDataset;
            if(_storageManager->MaterializeDim(dimSpecs, totalLength, materializeDimDataset) != SAVIME_SUCCESS)
                return SAVIME_FAILURE;
             
            return _storageManager->Comparison(op, materializeDimDataset, operand2, destinyDataset);
        }
    }
    
    SavimeResult Aritmethic(std::string op,  DatasetPtr operand1,  DatasetPtr operand2,  DatasetPtr& destinyDataset)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores], finalPositionPerCore[numCores];
        int64_t entryCount = operand1->entry_count <= operand2->entry_count? operand1->entry_count : operand2->entry_count;      
        SetWorkloadPerThread(entryCount, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);

        DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
        DatasetHandlerPtr op2Handler = _storageManager->GetHandler(operand2);

        destinyDataset = _storageManager->Create(SelectType(operand1->type, operand2->type, op), operand1->entry_count);
        if(destinyDataset == NULL)
                throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);

        T1* op1Buffer = (T1*) op1Handler->GetBuffer();
        T2* op2Buffer = (T2*) op2Handler->GetBuffer();
        T3* destinyBuffer = (T3*) destinyHandler->GetBuffer();

        if(op.c_str()[0] == '+')
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()]; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]+op2Buffer[i];     
        }
        else if(op.c_str()[0] == '-')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]-op2Buffer[i];
        }
        else if(op.c_str()[0] == '*')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]*op2Buffer[i];
        }
        else if(op.c_str()[0] == '/')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]/op2Buffer[i];
        }
        else if(op.c_str()[0] == '%')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = fmod(op1Buffer[i],op2Buffer[i]);
        }
        else if(!op.compare("pow"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = pow(op1Buffer[i],op2Buffer[i]);
        }
        else
        {
            throw std::runtime_error("Invalid arithmetic operation.");
        }
        
        op1Handler->Close();
        op2Handler->Close();
        destinyHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult Aritmethic(std::string op,  DatasetPtr  operand1, T2 operand2, DataType type,  DatasetPtr& destinyDataset)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores], finalPositionPerCore[numCores]; 
        SetWorkloadPerThread(operand1->entry_count, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
        
        destinyDataset = _storageManager->Create(SelectType(operand1->type, type, op), operand1->entry_count);
        if(destinyDataset == NULL)
                throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);

        T1* op1Buffer = (T1*) op1Handler->GetBuffer();
        T3* destinyBuffer = (T3*) destinyHandler->GetBuffer();

        if(op.c_str()[0] == '+')
        {
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()]; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]+operand2;     
        }
        else if(op.c_str()[0] == '-')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]-operand2;
        }
        else if(op.c_str()[0] == '*')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]*operand2;
        }
        else if(op.c_str()[0] == '/')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = op1Buffer[i]/operand2;
        }
        else if(op.c_str()[0] == '%')
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = fmod(op1Buffer[i], operand2);
        }
        else if(!op.compare("cos"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = cos(op1Buffer[i]);
        }
        else if(!op.compare("sin"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = sin(op1Buffer[i]);
        }
        else if(!op.compare("tan"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = tan(op1Buffer[i]);
        }
        else if(!op.compare("acos"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = acos(op1Buffer[i]);
        }
        else if(!op.compare("asin"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = asin(op1Buffer[i]);
        }
        else if(!op.compare("atan"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = atan(op1Buffer[i]);
        }
          else if(!op.compare("cosh"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = cosh(op1Buffer[i]);
        }
        else if(!op.compare("sinh"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = sinh(op1Buffer[i]);
        }
        else if(!op.compare("tanh"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = tanh(op1Buffer[i]);
        }
        else if(!op.compare("acosh"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = acosh(op1Buffer[i]);
        }
        else if(!op.compare("asinh"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = asinh(op1Buffer[i]);
        }
        else if(!op.compare("atanh"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = atanh(op1Buffer[i]);
        }
        else if(!op.compare("exp"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = exp(op1Buffer[i]);
        }
        else if(!op.compare("log"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = log(op1Buffer[i]);
        }
        else if(!op.compare("log10"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = log10(op1Buffer[i]);
        }
        else if(!op.compare("pow"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = pow(op1Buffer[i], operand2);
        }
        else if(!op.compare("sqrt"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = sqrt(op1Buffer[i]);
        }
        else if(!op.compare("ceil"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = ceil(op1Buffer[i]);
        }
        else if(!op.compare("floor"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = floor(op1Buffer[i]);
        }
        else if(!op.compare("round"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = round(op1Buffer[i]);
        }
        else if(!op.compare("abs"))
        {
            #pragma omp parallel 
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                destinyBuffer[i] = fabs(op1Buffer[i]);
        }
        else
        {
            throw std::runtime_error("Invalid arithmetic operation.");
        }
        
        op1Handler->Close();
        destinyHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength, DataType type,  DatasetPtr& destinyDataset)
    {
        if(dimSpecs->materialized != NULL)
        {
            destinyDataset=dimSpecs->materialized;
            return SAVIME_SUCCESS;
        }
        
        int numCores = _configurationManager->GetIntValue(MAX_THREADS); 
        int64_t dimLength = dimSpecs->GetLength(); //((dimSpecs->upper_bound - dimSpecs->lower_bound))+1;
        int64_t chunk = dimLength/numCores;
        if(chunk*dimSpecs->adjacency < _configurationManager->GetIntValue(WORK_PER_THREAD))
        {
            numCores = 1;
        }
        omp_set_num_threads(numCores);

        destinyDataset = _storageManager->Create(type, totalLength);
        if(destinyDataset == NULL)
            throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyDataset);
        T3 * destinyBuffer = (T3 *) destinyHandler->GetBuffer();
        
        if(dimSpecs->type == ORDERED)
        {
            int64_t entriesInBlock = dimLength*dimSpecs->adjacency;
            int64_t copies = totalLength/entriesInBlock;
           
            if(dimSpecs->dimension->GetDimension()->dimension_type == IMPLICIT)
            { 
                double dimspecs_lower_bound = dimSpecs->lower_bound;
                double dimension_lower_bound = dimSpecs->dimension->GetDimension()->lower_bound;
                double spacing = dimSpecs->dimension->GetDimension()->spacing;
                int64_t adjacency = dimSpecs->adjacency;
                double preamble1 = dimspecs_lower_bound*spacing+dimension_lower_bound;
                
                #pragma omp parallel for
                for(int64_t i = 0; i < dimLength; ++i)
                {
                    int64_t offset = i*adjacency;
                    double rangeMark = preamble1+i*spacing;
                    
                    #pragma omp parallel for
                    for(int64_t adjMark = 0; adjMark < adjacency; ++adjMark)
                    {
                        destinyBuffer[offset+adjMark] = rangeMark;
                    }
                }
            }
            else
            {
                DatasetPtr mappingDataset = dimSpecs->dimension->GetDimension()->dataset;
                DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mappingDataset);
                T3 * mappingBuffer = (T3 *) mappingHandler->GetBuffer();
                
                int64_t dimspecs_lower_bound = dimSpecs->lower_bound;
                int64_t adjacency = dimSpecs->adjacency;
                        
                #pragma omp parallel for
                for(int64_t i = dimSpecs->lower_bound; i <= dimSpecs->upper_bound; ++i)
                {
                    double rangeMark = mappingBuffer[i];

                    for(int64_t adjMark = 0; adjMark < adjacency; ++adjMark)
                    {
                        destinyBuffer[(i-dimspecs_lower_bound)*adjacency+adjMark] = rangeMark;
                    }
                }
                mappingHandler->Close();
            }
             
            #pragma omp parallel for
            for(int64_t i = 1; i < copies; ++i)
            {
                mempcpy((char*) &(destinyBuffer[entriesInBlock*i]), (char*) destinyBuffer, sizeof(T3)*entriesInBlock);
            }
            
            //destinyHandler->Close();
        }
        else if(dimSpecs->type == PARTIAL)
        {
            
            if(dimSpecs->dimension->GetDimension()->dimension_type == IMPLICIT)
            {
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                T1 * dimDatasetBuffer = (T1*) dimDataSetHandler->GetBuffer();
                int64_t adjacency = dimSpecs->adjacency;
                
                #pragma omp parallel for
                for(int64_t i = 0; i < dimLength; ++i)
                {
                    for(int64_t adjMark = 0; adjMark < adjacency; ++adjMark)
                    {
                        destinyBuffer[i*adjacency+adjMark] = dimDatasetBuffer[i];
                    }
                }
                
                dimDataSetHandler->Close();
            }
            else
            {
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                int64_t * dimDatasetBuffer = (int64_t *) dimDataSetHandler->GetBuffer();
                
                DatasetPtr mappingDataset = dimSpecs->dimension->GetDimension()->dataset;
                DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mappingDataset);
                T3 * mappingBuffer = (T3 *)mappingHandler->GetBuffer();
                
                int64_t adjacency = dimSpecs->adjacency;
                
                #pragma omp parallel for
                for(int64_t i = 0; i < dimLength; ++i)
                {
                    for(int64_t adjMark = 0; adjMark < adjacency; ++adjMark)
                    {
                        destinyBuffer[i*adjacency+adjMark] = mappingBuffer[dimDatasetBuffer[i]];
                    }
                }
                
                dimDataSetHandler->Close();
                mappingHandler->Close();
            }    
                
            
            int64_t entriesInBlock = dimLength*dimSpecs->adjacency;
            int64_t copies = totalLength/entriesInBlock;
            
            #pragma omp parallel for
            for(int64_t i = 1; i < copies; ++i)
            {
                mempcpy((char*) &(destinyBuffer[entriesInBlock*i]), (char*) destinyBuffer, sizeof(T3)*entriesInBlock);
            }
            
            //destinyHandler->Close();
        }
        else if(dimSpecs->type == TOTAL)
        {  
            if(dimSpecs->dimension->GetDimension()->dimension_type == IMPLICIT)
            {
                destinyDataset = dimSpecs->dataset;
            }
            else
            {
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                int64_t * dimDatasetBuffer = (int64_t *) dimDataSetHandler->GetBuffer();
                
                DatasetPtr mappingDataset = dimSpecs->dimension->GetDimension()->dataset;
                DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mappingDataset);
                T3 * mappingBuffer = (T3 *) mappingHandler->GetBuffer();
                        
                #pragma omp parallel for
                for(int64_t i = 0; i < totalLength; ++i)
                {
                    destinyBuffer[i] = mappingBuffer[dimDatasetBuffer[i]];
                }
                
                dimDataSetHandler->Close();
                mappingHandler->Close();
            }      
        }
        destinyHandler->Close();
        
        //destinyDataset=dimSpecs->materialized=destinyDataset;
        return SAVIME_SUCCESS;
    }
    
    SavimeResult PartiatMaterializeDim(DatasetPtr filter, DimSpecPtr dimSpecs, 
                                       int64_t totalLength, DataType type, 
                                       DatasetPtr& destinyLogicalDataset, 
                                       DatasetPtr& destinyRealDataset)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS); 
        int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];
        
        if(!filter->has_indexes)
            _storageManager->FromBitMaskToIndex(filter, true);
        
        int64_t dimLength = dimSpecs->GetLength();
        destinyLogicalDataset = _storageManager->Create(type, filter->entry_count);
        if(destinyLogicalDataset == NULL)
            throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr destinyHandler = _storageManager->GetHandler(destinyLogicalDataset);
        T3 * destinyBuffer = (T3 *) destinyHandler->GetBuffer();
        
        SetWorkloadPerThread(filter->entry_count, workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        DatasetHandlerPtr filTerHandler = _storageManager->GetHandler(filter);
        int64_t * filterBuffer = (int64_t *) filTerHandler->GetBuffer();
            
        if(dimSpecs->type == ORDERED)
        {
            destinyRealDataset = _storageManager->Create(LONG_TYPE, filter->entry_count);
            DatasetHandlerPtr realHandler = _storageManager->GetHandler(destinyRealDataset);
            int64_t * realBuffer = (int64_t*)realHandler->GetBuffer();
            
            if(dimSpecs->dimension->GetDimension()->dimension_type == IMPLICIT)
            {
                double preamble0 = dimSpecs->lower_bound;
                double preamble1 = dimSpecs->lower_bound*dimSpecs->dimension->GetDimension()->spacing 
                                  + dimSpecs->dimension->GetDimension()->lower_bound;
                
                int64_t preamble2 = dimSpecs->adjacency*dimLength;
                int64_t adjacency = dimSpecs->adjacency;
                double spacing = dimSpecs->dimension->GetDimension()->spacing;
                
                #pragma omp parallel
                for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                {
                    int64_t preamble4 = ((filterBuffer[i]%(preamble2))/adjacency);
                    realBuffer[i] = preamble0+preamble4;
                    destinyBuffer[i] = preamble1+preamble4*spacing;
                }
            }
            else
            {
                DatasetPtr mappingDataset = dimSpecs->dimension->GetDimension()->dataset;
                DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mappingDataset);
                T3 * mappingBuffer = (T3 *) mappingHandler->GetBuffer();

                int64_t preamble1 = dimSpecs->adjacency*dimLength;
                int64_t adjacency = dimSpecs->adjacency;
                int64_t lower_bound = dimSpecs->lower_bound;
                
                #pragma omp parallel
                for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                {
                    realBuffer[i] = (((filterBuffer[i])%(preamble1))/adjacency)+lower_bound;
                    destinyBuffer[i] = mappingBuffer[realBuffer[i]];
                }
               
                mappingHandler->Close();
            }
            realHandler->Close();

        }
        else if(dimSpecs->type == PARTIAL)
        {
            if(dimSpecs->dimension->GetDimension()->dimension_type == IMPLICIT)
            {
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                T1 * dimDatasetBuffer = (T1*) dimDataSetHandler->GetBuffer();
                
                int64_t preamble1 = dimSpecs->adjacency*dimLength;
                int64_t adjacency = dimSpecs->adjacency;
               
                #pragma omp parallel
                for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                {
                    destinyBuffer[i] = dimDatasetBuffer[((filterBuffer[i]%(preamble1))/adjacency)];
                }
                
                dimDataSetHandler->Close();
            }
            else
            {
                destinyRealDataset = _storageManager->Create(LONG_TYPE, filter->entry_count);
                DatasetHandlerPtr realHandler = _storageManager->GetHandler(destinyRealDataset);
                int64_t * realBuffer = (int64_t*)realHandler->GetBuffer();
                
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                int64_t * dimDatasetBuffer = (int64_t*) dimDataSetHandler->GetBuffer();

                DatasetPtr mappingDataset = dimSpecs->dimension->GetDimension()->dataset;
                DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mappingDataset);
                T3 * mappingBuffer = (T3 *) mappingHandler->GetBuffer();

                int64_t preamble1 = dimSpecs->adjacency*dimLength;
                int64_t adjacency = dimSpecs->adjacency;
                
                #pragma omp parallel
                for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                {
                    realBuffer[i] = dimDatasetBuffer[((filterBuffer[i]%(preamble1))/adjacency)];
                    destinyBuffer[i] = mappingBuffer[realBuffer[i]];
                }
                
                realHandler->Close();
                dimDataSetHandler->Close();
                mappingHandler->Close();
            }
        }
        else if(dimSpecs->type == TOTAL)
        {   
            if(dimSpecs->dimension->GetDimension()->dimension_type == IMPLICIT)
            {
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                T1 * dimDatasetBuffer = (T1*) dimDataSetHandler->GetBuffer();

                #pragma omp parallel
                for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
                {
                    destinyBuffer[i] = dimDatasetBuffer[filterBuffer[i]];
                }

                //destinyLogicalDataset = dimSpecs->dataset;
                dimDataSetHandler->Close();
            }
            else
            {
                destinyRealDataset = _storageManager->Create(LONG_TYPE, filter->entry_count);
                DatasetHandlerPtr realHandler = _storageManager->GetHandler(destinyRealDataset);
                int64_t * realBuffer = (int64_t*)realHandler->GetBuffer();
                
                DatasetHandlerPtr dimDataSetHandler = _storageManager->GetHandler(dimSpecs->dataset);
                int64_t * dimDatasetBuffer = (int64_t *) dimDataSetHandler->GetBuffer();

                DatasetPtr mappingDataset = dimSpecs->dimension->GetDimension()->dataset;
                DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mappingDataset);
                T3 * mappingBuffer = (T3 *) mappingHandler->GetBuffer();

                #pragma omp parallel for
                for(int64_t i = 0; i < totalLength; ++i)
                {
                    realBuffer[i] = dimDatasetBuffer[filterBuffer[i]];
                    destinyBuffer[i] = mappingBuffer[realBuffer[i]];
                }

                dimDataSetHandler->Close();
                mappingHandler->Close();
                realHandler->Close();
            }        
        }
            
        destinyHandler->Close();    
        filTerHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
    SavimeResult Stretch(DatasetPtr origin, int64_t entryCount, int64_t recordsRepetitions, int64_t datasetRepetitions, DataType type, DatasetPtr& destinyDataset)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS); 
        int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];        
        int64_t totalDestinySize = entryCount * recordsRepetitions * datasetRepetitions;
        int64_t singleDatasetSize = entryCount * recordsRepetitions;
        
        DatasetHandlerPtr originHandler = _storageManager->GetHandler(origin);
        T1 * originBuffer = (T1*)originHandler->GetBuffer();
        
        destinyDataset = _storageManager->Create(type, totalDestinySize);
        if(destinyDataset == NULL)
            throw std::runtime_error("Could not create dataset.");
        
        DatasetHandlerPtr handler = _storageManager->GetHandler(destinyDataset);
        T1 * buffer = (T1*)handler->GetBuffer();
        
        SetWorkloadPerThread(totalDestinySize, workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
        {
            int64_t originIndex = i%singleDatasetSize;
            originIndex = originIndex/recordsRepetitions;
            buffer[i] = originBuffer[originIndex];
        }
        
        handler->Close();
        originHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
    
    SavimeResult Split(DatasetPtr origin, int64_t totalLength, int64_t parts, vector<DatasetPtr>& brokenDatasets)
    {
        int numCores = _configurationManager->GetIntValue(MAX_THREADS); 
        int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];  
        int64_t partitionSize = totalLength/parts;
        vector<T1*> buffers; vector<DatasetHandlerPtr> handlers;
        
        if((partitionSize*parts) != totalLength)
            throw std::runtime_error("Invalid number of parts.");
        
        DatasetHandlerPtr originHandler = _storageManager->GetHandler(origin);
        T1 * originBuffer = (T1*)originHandler->GetBuffer();
        brokenDatasets.resize(parts);
        
        handlers.resize(parts);
        buffers.resize(parts);
        SetWorkloadPerThread(totalLength, workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        
        //#pragma omp parallel for
        for(int64_t i = 0; i < parts; i++)
        {
            brokenDatasets[i] = _storageManager->Create(origin->type, partitionSize);
            
            if(brokenDatasets[i] == NULL)
                throw std::runtime_error("Could not create dataset.");
            
            handlers[i] = _storageManager->GetHandler(brokenDatasets[i]);
            buffers[i] = (T1*)handlers[i]->GetBuffer();
        }
        
        #pragma omp parallel
        for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()]; ++i)
        {
            int64_t bufferIndex = i/partitionSize;
            int64_t internIndex = i%partitionSize;
            buffers[bufferIndex][internIndex] = originBuffer[i];
        }
        
        //#pragma omp parallel for
        for(int64_t i = 0; i < parts; i++)
        {
            handlers[i]->Close();
        }
        
        originHandler->Close();
        
        return SAVIME_SUCCESS;
    }
    
};



#endif /* DEFAULT_TEMPLATE_H */

