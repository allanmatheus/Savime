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
#ifndef DIMJOIN_H
#define DIMJOIN_H

using namespace std;

struct JoinedRange
{
    int64_t lower_bound;
    int64_t upper_bound;
    
    int64_t GetEstimatedLength()
    {
        return upper_bound - lower_bound + 1;
    }
};
typedef shared_ptr<JoinedRange> JoinedRangePtr;

bool compareDimSpecsByAdj(DimSpecPtr a, DimSpecPtr b)
{
    return a->adjacency < b->adjacency;
}

bool checkIntersection(TARPtr tar, SubtarPtr subtar1, SubtarPtr subtar2, map<string, string> joinedDims, map<string, JoinedRangePtr>& intersection, StorageManagerPtr storageManager)
{
    #define IN_RANGE(X, Y, Z) ((X >= Y) && (X <= Z))
    
    for(auto entry : joinedDims)
    {
        DimSpecPtr dimSpecs1 = subtar1->GetDimensionSpecificationFor(entry.first);
        DimSpecPtr dimSpecs2 = subtar2->GetDimensionSpecificationFor(entry.second);
        DimensionPtr dim = tar->GetDataElement(LEFT_DATAELEMENT_PREFIX+entry.first)->GetDimension();
        DimSpecPtr dimSpecs[2] = {dimSpecs1, dimSpecs2};
        LogicalIndex lower[2], upper[2];
        int64_t realLower[2], realUpper[2];
        
        for(int32_t i = 0; i < 2; i ++)
        {
            lower[i] = storageManager->Real2Logical(dimSpecs[i]->dimension->GetDimension(), dimSpecs[i]->lower_bound);
            upper[i] = storageManager->Real2Logical(dimSpecs[i]->dimension->GetDimension(), dimSpecs[i]->upper_bound);
            realLower[i] = storageManager->Logical2Real(dim, lower[i]);
            realUpper[i] = storageManager->Logical2Real(dim, upper[i]);
        }
        
        
        if(IN_RANGE(realLower[0], realLower[1], realUpper[1]) 
           || IN_RANGE(realLower[1], realLower[0], realUpper[0]) )
        {
            JoinedRangePtr joinedRange = JoinedRangePtr(new JoinedRange());
            joinedRange->lower_bound = (realLower[0] > realLower[1]) ? realLower[0] : realLower[1];
            joinedRange->upper_bound = (realUpper[0] < realUpper[1]) ? realUpper[0] : realUpper[1];
            intersection[dim->name] = joinedRange;
        }
        else
        {
            return false;
        }
    }
    
    return true;
}


bool compareIndexes(char * leftBuffer, DataType leftType, char * rightBuffer, DataType rightType)
{
    #define THRESHOLD 0.0000001;

    if(leftType == INTEGER_TYPE && rightType == INTEGER_TYPE)
    {
        return *((int32_t*)leftBuffer) == *((int32_t*)rightBuffer);
    }
    else if(leftType ==  INTEGER_TYPE && rightType == LONG_TYPE)
    {
        return *((int32_t*)leftBuffer) == *((int64_t*)rightBuffer);
    }
    else if(leftType ==  INTEGER_TYPE && rightType == FLOAT_TYPE)
    {
        return fabs( *((int32_t*)leftBuffer)- *((float*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  INTEGER_TYPE && rightType == DOUBLE_TYPE)
    {
        return fabs( *((int32_t*)leftBuffer)- *((double*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  LONG_TYPE && rightType == INTEGER_TYPE)
    {
        return (*(int64_t*)leftBuffer) == *((int32_t*)rightBuffer);
    }
    else if(leftType ==  LONG_TYPE && rightType == LONG_TYPE)
    {
        return (*(int64_t*)leftBuffer) == *((int64_t*)rightBuffer);
    }
    else if(leftType ==  LONG_TYPE && rightType == FLOAT_TYPE)
    {
        return fabs(*((int64_t*)leftBuffer)- *((float*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  LONG_TYPE && rightType == DOUBLE_TYPE)
    {
        return fabs(*((int64_t*)leftBuffer)- *((float*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  FLOAT_TYPE && rightType == INTEGER_TYPE)
    {
        return fabs(*((float*)leftBuffer)- *((int32_t*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  FLOAT_TYPE && rightType == LONG_TYPE)
    {
        return fabs(*((float*)leftBuffer)- *((int64_t*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  FLOAT_TYPE && rightType == FLOAT_TYPE)
    {
        return fabs(*((float*)leftBuffer)- *((float*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  FLOAT_TYPE && rightType == DOUBLE_TYPE)
    {
        return fabs(*((float*)leftBuffer)- *((double*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  DOUBLE_TYPE && rightType == INTEGER_TYPE)
    {
        return fabs(*((double*)leftBuffer)- *((int32_t*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  DOUBLE_TYPE && rightType == LONG_TYPE)
    {
        return fabs(*((double*)leftBuffer)- *((int64_t*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  DOUBLE_TYPE && rightType == FLOAT_TYPE)
    {
        return fabs(*((double*)leftBuffer)- *((float*)rightBuffer)) < THRESHOLD;
    }
    else if(leftType ==  DOUBLE_TYPE && rightType == DOUBLE_TYPE)
    {
        return fabs(*((double*)leftBuffer)- *((double*)rightBuffer)) < THRESHOLD;
    }
    
    return false;
}

void join_dimensions(TARPtr outputTar, SubtarPtr left, SubtarPtr right, map<string, string> dimsMapping, map<string, JoinedRangePtr> ranges, 
                DatasetPtr& leftProjectionDs, DatasetPtr& rightProjectionDs, StorageManagerPtr storageManager, ConfigurationManagerPtr configurationManager)
{
    std::mutex mutex;
    int32_t numCores = configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread = configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t startPositionPerCore[numCores];
    int64_t finalPositionPerCore[numCores];
    int64_t estimatedIntersectionLen = 1; int64_t projectionCount = 0;
    int64_t leftSubtarLen = left->GetTotalLength();
    int64_t rightSubtarLen = right->GetTotalLength();
    
    for(auto entry : ranges)
    {
        estimatedIntersectionLen *= entry.second->GetEstimatedLength();
    }
    
    int64_t projectionLen = (leftSubtarLen*rightSubtarLen)/estimatedIntersectionLen;
    SetWorkloadPerThread(leftSubtarLen*rightSubtarLen, minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
    
    leftProjectionDs = storageManager->Create(LONG_TYPE, projectionLen);
    if(leftProjectionDs == NULL)
        throw std::runtime_error("Could not create dataset.");
    DatasetHandlerPtr leftProjHandler = storageManager->GetHandler(leftProjectionDs);
    int64_t * leftProjBuffer = (int64_t*) leftProjHandler->GetBuffer();
    
    rightProjectionDs = storageManager->Create(LONG_TYPE, projectionLen);
    if(rightProjectionDs == NULL)
        throw std::runtime_error("Could not create dataset.");
    DatasetHandlerPtr rightProjHandler = storageManager->GetHandler(rightProjectionDs);
    int64_t * rightProjBuffer = (int64_t*) rightProjHandler->GetBuffer();
    
    map<string, DatasetHandlerPtr> leftDims;
    map<string, DatasetHandlerPtr> rightDims;
    

    for(auto entry : dimsMapping)
    {
        DatasetPtr leftMatds, rightMatDs;
        DimSpecPtr leftDimSpecs = left->GetDimensionSpecificationFor(entry.first);
        
        if(storageManager->MaterializeDim(leftDimSpecs, leftSubtarLen, leftMatds) != SAVIME_SUCCESS)
             throw std::runtime_error("Error while joining dimensions.");
        
        leftDims[entry.first] = storageManager->GetHandler(leftMatds);
        
        DimSpecPtr rightDimSpecs = right->GetDimensionSpecificationFor(entry.second);
        
        if(storageManager->MaterializeDim(rightDimSpecs, rightSubtarLen, rightMatDs) != SAVIME_SUCCESS)
             throw std::runtime_error("Error while joining dimensions.");
        
        rightDims[entry.second] = storageManager->GetHandler(rightMatDs);
    }

    int64_t s = 0;
    //#pragma omp parallel for collapse(2) 
    for(int64_t i = 0; i < leftSubtarLen; i++)
    {
        for(int64_t j = 0; j < rightSubtarLen; j++)
        {
           /* bool match = true;
            
            for(auto entry : dimsMapping)
            {
                char * leftBuffer = leftDims[entry.first]->GetBufferAt(i);
                char * rightBuffer = rightDims[entry.second]->GetBufferAt(j);
                
                match =  match & compareIndexes(leftBuffer, leftDims[entry.first]->GetDataSet()->type,
                                                     rightBuffer, rightDims[entry.second]->GetDataSet()->type);
                
                if(!match) break;
            }
            
            if(match)
            {
                mutex.lock();
                leftProjBuffer[projectionCount] = i;
                rightProjBuffer[projectionCount] = j;
                projectionCount++;
                mutex.unlock();
            }*/
            s++;
        }
    }
    printf("%ld\n", s);
    
    if(projectionCount < leftProjectionDs->entry_count)
    {
        leftProjectionDs->entry_count = projectionCount;
        rightProjectionDs->entry_count = projectionCount;
    }
    
    for(auto entry : dimsMapping)
    {
        leftDims[entry.first]->Close();
        rightDims[entry.second]->Close();
    }
    
    leftProjHandler->Close();
    rightProjHandler->Close();
}

#endif /* DIMJOIN_H */

