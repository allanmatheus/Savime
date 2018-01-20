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
#include <stdio.h>
#include <algorithm>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>
#include <time.h>
#include <omp.h>
#include <chrono>
#include "include/util.h"
#include "include/dynamic_bitset.h"
#include "default_storage_manager.h"
#include "default_template.h"

using namespace std;
using namespace std::chrono;

DefaultDatasetHandler::DefaultDatasetHandler(DatasetPtr  ds, 
                                             StorageManagerPtr storageManager,
                                             int64_t hugeTblThreshold, 
                                             int64_t hugeTblSize):
    DatasetHandler(ds){
        
    _storageManager = storageManager;
    _huge_pages_size = hugeTblSize;
    _buffer_offset = 0;
    _buffer = NULL;
    
    if (ds == NULL) 
    {
        throw std::runtime_error("Invalid dataset for handler creation.");
    }
    
    _entry_length = TYPE_SIZE(ds->type);

    _fd = open(ds->location.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
    if (_fd == -1) {
        throw std::runtime_error("Could not open dataset file: "+ds->location+" Error: "+std::string(strerror(errno)));
    }

    if(_buffer == NULL)
    {
        _mapping_length = ((ds->length/_huge_pages_size)+1)*_huge_pages_size;
        _buffer = (char*)mmap(0, _mapping_length, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
        _huge_pages = 0;
    }

    if (_buffer == MAP_FAILED || _buffer == NULL) 
    {
        close(_fd);
         throw std::runtime_error("Could not map dataset file: "+ds->location+" Error: "+std::string(strerror(errno)));
    }
    
  }

int32_t DefaultDatasetHandler::GetValueLength()
{
    return _entry_length;
}

void DefaultDatasetHandler::Remap()
{
    munmap(_buffer, _mapping_length);
    _mapping_length = ((_ds->length/_huge_pages_size)+1)*_huge_pages_size;
    _buffer = (char*)mmap(0, _mapping_length, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
    
    if (_buffer == MAP_FAILED) {
         close(_fd);
         throw std::runtime_error("Could not map dataset file: "+_ds->location+" Error: "+std::string(strerror(errno)));
    }
}

void DefaultDatasetHandler::Append(char * value)
{
    if(_buffer_offset > _ds->length)
    {
        int written = write(_fd, value, _entry_length);
        
        if(written == -1)
        {
            throw std::runtime_error("Could not write to dataset file: "+_ds->location+" Error: "+std::string(strerror(errno)));
        }

        _ds->length += _entry_length;
        _ds->entry_count++;
        _buffer_offset+= _entry_length;
        
        if(_storageManager->RegisterDatasetExpasion(_entry_length) == SAVIME_FAILURE)
        {
             throw std::runtime_error("Could not append to dataset file, max storage size reached, consider increasing max storage size.");
        }
    }
    else
    {
        memcpy(&(_buffer[_buffer_offset]), value,  _entry_length);
        _buffer_offset+=_entry_length;
    }
}

void * DefaultDatasetHandler::Next()
{
   if(_buffer_offset < _mapping_length)
   {
       char * v = &_buffer[_buffer_offset];
       _buffer_offset+=_entry_length;
       return (void*)v;
   }
   else if(_buffer_offset < _ds->length)
   {
       Remap();
       char * v = &_buffer[_buffer_offset];
       _buffer_offset+=_entry_length;
       return (void*)v;
   }
  
   return NULL;
}

bool DefaultDatasetHandler::HasNext()
{
    return _buffer_offset < _ds->length;
}

void DefaultDatasetHandler::InsertAt(char * value, int64_t offset)
{
    offset = offset*_entry_length;
    memcpy(&_buffer[offset], value,  _entry_length);
}

void DefaultDatasetHandler::CursorAt(int64_t index)
{    
    _buffer_offset = index*_entry_length;
}

char * DefaultDatasetHandler::GetBuffer()
{
    if(_ds->length > _mapping_length)
        Remap();
  
    return _buffer;
}

char * DefaultDatasetHandler::GetBufferAt(int64_t index)
{
    int64_t buffer_offset = index*_entry_length;
    if(_ds->length > buffer_offset )
    {
        if(_ds->length > _mapping_length)
            Remap();
        
        return &_buffer[index*_entry_length];
    }
    else
    {
        return NULL;
    }
}

void DefaultDatasetHandler::TruncateAt(int64_t index)
{
    int64_t reduction;
    
    if(_ds->length > index)
    {
        if (ftruncate(_fd, index) == -1) {
            throw std::runtime_error("Could not truncate dataset file: "
                    +_ds->location+" Error: "+std::string(strerror(errno)));
        }
                
        reduction = _ds->length - index;
        _ds->length = index;
        _ds->entry_count = _ds->length/_entry_length;
        _storageManager->RegisterDatasetTruncation(reduction);
        Remap();
    }
}


void DefaultDatasetHandler::Close()
{ 
    munmap(_buffer, _mapping_length);        
    close(_fd);
}

DatasetPtr  DefaultDatasetHandler::GetDataSet()
{
    return _ds;
}

 //-----------------------------------------------------------------------------
 //Storage Manager Members
std::string DefaultStorageManager::GenerateUniqueFileName()
{
    std::string path = _configurationManager->GetStringValue(SHM_STORAGE_DIR);
    return generateUniqueFileName(path);
}

DatasetPtr DefaultStorageManager::Create(DataType type, int64_t size)
{
    try
    {
        if(size <= 0) return NULL;
       
        int error, typeSize;
        DatasetPtr ds = DatasetPtr(new Dataset());
        typeSize = TYPE_SIZE(type);

        //Checking size
        int64_t max = _configurationManager->GetLongValue(MAX_STORAGE_SIZE);
        if((_usedStorageSize+size)> max)
            throw std::runtime_error("Could not create dataset: size would exceed max storage size.");

        ds->entry_count = size;
        ds->length = size*typeSize;
        ds->type = type;
        ds->sorted = false;
        ds->location = GenerateUniqueFileName();

        int fd = open(ds->location.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
        if (fd == -1) 
        {
            throw std::runtime_error("Could not open dataset file: "+ds->location+" Error: "+std::string(strerror(errno)));
        }

        if((error = ftruncate(fd, ds->length))!= 0)
        {
            //if(error != EINTR)
            //    throw std::runtime_error("Could not allocate dataset file: "+ds->location+" Error: "+std::string(strerror(error)));
        }

        _mutex.lock();
        _usedStorageSize+=size;
        _mutex.unlock();

        ds->Addlistener(_this);
        close(fd);
        return ds;
       
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return NULL;
    }
}

DatasetPtr DefaultStorageManager::Create(DataType type, double init, double spacing, double end)
{
    DatasetPtr newDataset;
    
    DimensionPtr dummyDimension = DimensionPtr(new Dimension());
    dummyDimension->lower_bound = init;
    dummyDimension->upper_bound = end;
    dummyDimension->spacing = spacing;
    dummyDimension->real_lower_bound = 0;
    dummyDimension->real_upper_bound = dummyDimension->GetLength()-1;
    dummyDimension->type = type;
    dummyDimension->dimension_type = IMPLICIT;
    
    DimSpecPtr dummySpecs = DimSpecPtr(new DimensionSpecification());
    dummySpecs->adjacency = 1;
    dummySpecs->dimension = DataElementPtr(new DataElement(dummyDimension));
    dummySpecs->lower_bound = 0;
    dummySpecs->upper_bound = dummyDimension->GetLength()-1;
    dummySpecs->type = ORDERED;
    
    MaterializeDim(dummySpecs, dummySpecs->GetLength(), newDataset);
            
    return newDataset;
}
 
SavimeResult DefaultStorageManager::Save(DatasetPtr dataset)
{
    int64_t max = _configurationManager->GetLongValue(MAX_STORAGE_SIZE);
    
    _mutex.lock();
    if((_usedStorageSize+dataset->length)> max)
    {
        _mutex.unlock();
        return SAVIME_FAILURE;
    }
    
    _usedStorageSize+=dataset->length;
    _mutex.unlock();
    dataset->Addlistener(_this);
    
    return SAVIME_SUCCESS;
}

DatasetHandlerPtr DefaultStorageManager::GetHandler(DatasetPtr dataset)
{
    int64_t hugeTblThreshold = _configurationManager->GetLongValue(HUGE_TBL_THRESHOLD);
    int64_t hugeTblSize = _configurationManager->GetLongValue(HUGE_TBL_SIZE);
    DatasetHandlerPtr handler = DatasetHandlerPtr(new DefaultDatasetHandler(dataset, _this, hugeTblThreshold, hugeTblSize));
    return handler;
}

SavimeResult DefaultStorageManager::Drop(DatasetPtr dataset)
{
    return SAVIME_SUCCESS;
}

SavimeResult DefaultStorageManager::RegisterDatasetExpasion(int64_t size)
{
    int64_t max = _configurationManager->GetLongValue(MAX_STORAGE_SIZE);
    _mutex.lock();
    if(_usedStorageSize+size > max)
    {
        _mutex.unlock();
        return SAVIME_FAILURE;
    }
    
    _usedStorageSize+=size;
    _mutex.unlock();
    return SAVIME_SUCCESS;
}

SavimeResult DefaultStorageManager::RegisterDatasetTruncation(int64_t size)
{
    _mutex.lock();
    if(_usedStorageSize<size)
    {
        _mutex.unlock();
        return SAVIME_FAILURE;
    }
    
    _usedStorageSize-=size;
    _mutex.unlock();
    return SAVIME_SUCCESS;
}

bool DefaultStorageManager::CheckSorted(DatasetPtr dataset)
{
    bool ret;
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 
        

        if(dataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            ret = tsm.CheckSorted(dataset);
        }
        else if(dataset->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, int64_t> tsm(_this, _configurationManager, _systemLogger);
            ret = tsm.CheckSorted(dataset);
        }
        else if(dataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, float> tsm(_this, _configurationManager, _systemLogger);
            ret = tsm.CheckSorted(dataset);
        }
        else if(dataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, double> tsm(_this, _configurationManager, _systemLogger);
            ret = tsm.CheckSorted(dataset);
        }

        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Logical2Real took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return ret;
    }
    catch(std::exception& e)
    {
         _systemLogger->LogEvent(this->_moduleName, e.what());
         throw e;
    }
}

RealIndex DefaultStorageManager::Logical2Real(DimensionPtr dimension, LogicalIndex logicalIndex)
{
    RealIndex realIndex = 0;
    try
    {                
        if(dimension->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2Real(dimension, logicalIndex.intIndex);
        }
        else if(dimension->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2Real(dimension, logicalIndex.longIndex);
        }
        else if(dimension->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2Real(dimension, logicalIndex.floatIndex);
        }
        else if(dimension->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2Real(dimension, logicalIndex.doubleIndex);
        }

        return realIndex;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        throw e;
    }
}

RealIndex DefaultStorageManager::Logical2ApproxReal(DimensionPtr dimension, LogicalIndex logicalIndex)
{
    RealIndex realIndex = 0;
    try
    {        
        if(dimension->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2ApproxReal(dimension, logicalIndex.intIndex);
        }
        else if(dimension->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2ApproxReal(dimension, logicalIndex.longIndex);
        }
        else if(dimension->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2ApproxReal(dimension, logicalIndex.floatIndex);
        }
        else if(dimension->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            realIndex = tsm.Logical2ApproxReal(dimension, logicalIndex.doubleIndex);
        }

        return realIndex;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        throw e;
    }
}

SavimeResult DefaultStorageManager::Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr logicalIndexes, DatasetPtr& destinyDataset)
{
    try
    {
        SavimeResult result;
        
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(dimension->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Logical2Real(dimension, dimSpecs, logicalIndexes, destinyDataset);
        }
        else if(dimension->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Logical2Real(dimension, dimSpecs, logicalIndexes, destinyDataset);
        }
        else if(dimension->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Logical2Real(dimension, dimSpecs, logicalIndexes, destinyDataset);
        }
        else if(dimension->type == DOUBLE_TYPE)
        {
           TemplateStorageManager<double, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
           result = tsm.Logical2Real(dimension, dimSpecs, logicalIndexes, destinyDataset);
        }
        
       #ifdef TIME 
         GET_T2();
         _systemLogger->LogEvent(_moduleName, "Logical2Real took "+std::to_string(GET_DURATION())+" ms.");
       #endif
      
       return result;
    }
    catch(std::exception& e)
    {
       _systemLogger->LogEvent(this->_moduleName, e.what());
       return SAVIME_FAILURE;
    }
}

LogicalIndex DefaultStorageManager::Real2Logical(DimensionPtr dimension, RealIndex realIndex)
{
    LogicalIndex logicalIndex;
    logicalIndex.type = dimension->type;
    
    try
    {
        if(dimension->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            logicalIndex.intIndex = tsm.Real2Logical(dimension, realIndex);
        }
        else if(dimension->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            logicalIndex.longIndex = tsm.Real2Logical(dimension, realIndex);
        }
        else if(dimension->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            logicalIndex.floatIndex = tsm.Real2Logical(dimension, realIndex);
        }
        else if(dimension->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            logicalIndex.doubleIndex = tsm.Real2Logical(dimension, realIndex);
        }
        
        return logicalIndex;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        throw e;
    }
}

SavimeResult DefaultStorageManager::Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr realIndexes, DatasetPtr& destinyDataset)
{
    try
    {
        SavimeResult result;
        
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(dimension->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Real2Logical(dimension, dimSpecs, realIndexes, destinyDataset);
        }
        else if(dimension->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Real2Logical(dimension, dimSpecs, realIndexes, destinyDataset);
        }
        else if(dimension->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Real2Logical(dimension, dimSpecs, realIndexes, destinyDataset);
        }
        else if(dimension->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Real2Logical(dimension, dimSpecs, realIndexes, destinyDataset);
        }
        
        #ifdef TIME 
           GET_T2();
           _systemLogger->LogEvent(_moduleName, "Real2logical took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2, DimensionPtr& destinyDim)
{
    SavimeResult result;
    
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(dim1->type == INTEGER_TYPE && dim2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, bool> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == INTEGER_TYPE && dim2->type == LONG_TYPE)
        {
            TemplateStorageManager<int32_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == INTEGER_TYPE && dim2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int32_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == INTEGER_TYPE && dim2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int32_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == LONG_TYPE && dim2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == LONG_TYPE && dim2->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == LONG_TYPE && dim2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int64_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == LONG_TYPE && dim2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int64_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == FLOAT_TYPE && dim2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<float, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == FLOAT_TYPE && dim2->type == LONG_TYPE)
        {
            TemplateStorageManager<float, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == FLOAT_TYPE && dim2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == FLOAT_TYPE && dim2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<float, double, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == DOUBLE_TYPE && dim2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<double, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == DOUBLE_TYPE && dim2->type == LONG_TYPE)
        {
            TemplateStorageManager<double, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == DOUBLE_TYPE && dim2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<double, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        else if(dim1->type == DOUBLE_TYPE && dim2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.IntersectDimensions(dim1, dim2, destinyDim);
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Intersect took "+std::to_string(GET_DURATION())+" ms.");
        #endif
       
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Copy(DatasetPtr originDataset, int64_t lowerBound, int64_t upperBound, int64_t offsetInDestiny, int64_t spacingInDestiny, DatasetPtr destinyDataset)
{
    SavimeResult result;
    
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(originDataset->type == INTEGER_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, bool> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == INTEGER_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<int32_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == INTEGER_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int32_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == INTEGER_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int32_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int64_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int64_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<float, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<float, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<float, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<double, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<double, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<double, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, lowerBound, upperBound, offsetInDestiny, spacingInDestiny, destinyDataset);
        }
        else
        {
            throw std::runtime_error("Dataset types are invalid for copy operations.");
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Copy took "+std::to_string(GET_DURATION())+" ms.");
        #endif
       
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Copy(DatasetPtr originDataset, DatasetPtr mapping, DatasetPtr destinyDataset, int64_t& copied)
{
    SavimeResult result;
     
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(originDataset->type == INTEGER_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, bool> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == INTEGER_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<int32_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == INTEGER_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int32_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == INTEGER_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int32_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int64_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == LONG_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int64_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<float, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<float, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == FLOAT_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<float, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<double, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<double, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<double, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else if(originDataset->type == DOUBLE_TYPE && destinyDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Copy(originDataset, mapping, destinyDataset, copied);
        }
        else
        {
            throw std::runtime_error("Dataset types are invalid for copy operations.");
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Copy took "+std::to_string(GET_DURATION())+" ms.");
        #endif
       
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Filter(DatasetPtr originDataset, DatasetPtr filterDataSet, DatasetPtr& destinyDataset)
{
    SavimeResult result;
    
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 

        if(originDataset->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Filter(originDataset, filterDataSet, INTEGER_TYPE, destinyDataset);
        }
        else if(originDataset->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Filter(originDataset, filterDataSet, LONG_TYPE, destinyDataset);
        }
        else if(originDataset->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Filter(originDataset, filterDataSet, FLOAT_TYPE, destinyDataset);
        }
        else if(originDataset->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Filter(originDataset, filterDataSet, DOUBLE_TYPE, destinyDataset);
        }
    
       #ifdef TIME 
         GET_T2();
         _systemLogger->LogEvent(_moduleName, "Filter took "+std::to_string(GET_DURATION())+" ms.");
       #endif
       
       return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::And(DatasetPtr operand1, DatasetPtr operand2, DatasetPtr& destinyDataset)
{
    try
    {   
        #ifdef TIME 
            GET_T1();
        #endif 
        
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];

        if(operand1->bitMask != NULL  && operand2->bitMask != NULL)
        {
            SetWorkloadPerThread(operand1->bitMask->size(), minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
            destinyDataset = DatasetPtr(new Dataset());
            destinyDataset->Addlistener(_this);
            destinyDataset->has_indexes = false;
            destinyDataset->sorted = false;
            destinyDataset->bitMask =std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(operand1->bitMask->size()));
            if(destinyDataset->bitMask == NULL)
                throw std::runtime_error("Could not allocate bitmask index.");
            
            boost::dynamic_bitset<>::and_parallel(*(destinyDataset->bitMask),
                                                    *(operand1->bitMask),
                                                    *(operand2->bitMask), 
                                                    numCores, 
                                                    minWorkPerThread);

        }
        else
        {
            throw std::runtime_error("Invalid dataset type for logic operation.");
        }
      
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "And operator took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Or(DatasetPtr  operand1, DatasetPtr  operand2, DatasetPtr& destinyDataset)
{
    try
    {  
        #ifdef TIME 
            GET_T1();
        #endif

        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];

        if(operand1->bitMask != NULL  && operand2->bitMask != NULL)
        {
            SetWorkloadPerThread(operand1->bitMask->size(), minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
            destinyDataset = DatasetPtr(new Dataset());
            destinyDataset->has_indexes = false;
            destinyDataset->sorted = false;
            destinyDataset->Addlistener(_this);
            destinyDataset->bitMask =std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(operand1->bitMask->size()));
            if(destinyDataset->bitMask == NULL)
                throw std::runtime_error("Could not allocate bitmask index.");
            
            boost::dynamic_bitset<>::or_parallel(*(destinyDataset->bitMask),
                                                 *(operand1->bitMask),
                                                 *(operand2->bitMask), 
                                                  numCores, 
                                                  minWorkPerThread);

        }
        else
        {
            throw std::runtime_error("Invalid dataset type for logic operation.");
        }
      
       #ifdef TIME 
          GET_T2();
         _systemLogger->LogEvent(_moduleName, "Or operator took "+std::to_string(GET_DURATION())+" ms.");
       #endif

       return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
       _systemLogger->LogEvent(this->_moduleName, e.what());
       return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Not(DatasetPtr operand1, DatasetPtr& destinyDataset)
{
    try
    {   
        #ifdef TIME 
            GET_T1();
        #endif
    
        int numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];

        if(operand1->bitMask != NULL)
        {
            SetWorkloadPerThread(operand1->bitMask->size(), minWorkPerThread, startPositionPerCore, finalPositionPerCore, numCores);
            destinyDataset = DatasetPtr(new Dataset());
            destinyDataset->has_indexes = false;
            destinyDataset->sorted = false;
            destinyDataset->Addlistener(_this);
            destinyDataset->bitMask =std::shared_ptr<boost::dynamic_bitset<>>(new boost::dynamic_bitset<>(operand1->bitMask->size()));
            
            if(destinyDataset->bitMask == NULL)
                throw std::runtime_error("Could not allocate bitmask index.");
            
            boost::dynamic_bitset<>::not_parallel(*(destinyDataset->bitMask),
                                                  *(operand1->bitMask), 
                                                  numCores, 
                                                  minWorkPerThread);
            
        }
        else
        {
            throw std::runtime_error("Invalid dataset type for logical operation.");
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Not operator took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Comparison(std::string op, DatasetPtr  operand1, DatasetPtr  operand2, DatasetPtr& destinyDataset)
{
    SavimeResult result;
    
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 
        
        if(operand1->type == INTEGER_TYPE && operand2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, bool> tsm(_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset);
        }
        else if(operand1->type == INTEGER_TYPE && operand2->type == LONG_TYPE)
        {
            TemplateStorageManager<int32_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == INTEGER_TYPE && operand2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int32_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == INTEGER_TYPE && operand2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int32_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE && operand2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE && operand2->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE && operand2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<int64_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE && operand2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<int64_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<float, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == LONG_TYPE)
        {
            TemplateStorageManager<float, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<float, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == INTEGER_TYPE)
        {
            TemplateStorageManager<double, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == LONG_TYPE)
        {
            TemplateStorageManager<double, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == FLOAT_TYPE)
        {
            TemplateStorageManager<double, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else
        {
            throw std::runtime_error("Dataset types are invalid for arithmetic operations.");
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
        #endif
       
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::ComparisonDim(std::string op, DimSpecPtr dimSpecs, int64_t totalLength, DatasetPtr operand2,  DatasetPtr& destinyDataset)
{
    DatasetPtr materializeDimDataset;
    
    if(MaterializeDim(dimSpecs, totalLength, materializeDimDataset) == SAVIME_SUCCESS)
    {
        #ifdef TIME 
            GET_T1();
        #endif 
        
        SavimeResult result = Comparison(op, materializeDimDataset, operand2, destinyDataset);
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "ComparisonDim took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return result;
    }
    else
    {
       return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Comparison(std::string op,  DatasetPtr  operand1, double operand2, DatasetPtr& destinyDataset)
{
    SavimeResult result;
    try
    {
        #ifdef TIME 
            GET_T1();
        #endif 

        if(operand1->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, double, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        
        #ifdef TIME 
            GET_T2();
           _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
        #endif
      
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::ComparisonDim(std::string op,  DimSpecPtr dimSpecs, int64_t totalLength, double operand2, DatasetPtr& destinyDataset)
{
    SavimeResult result;
    auto operand1 = dimSpecs->dimension->GetDimension();
    
    #ifdef TIME 
        GET_T1();
    #endif 
    
    if(operand1->type == INTEGER_TYPE)
    {
        TemplateStorageManager<int32_t, double, int32_t> tsm(_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset);
    }
    else if(operand1->type == LONG_TYPE)
    {
        TemplateStorageManager<int64_t, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == FLOAT_TYPE)
    {
        TemplateStorageManager<float, double, float> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == DOUBLE_TYPE)
    {
        TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }    

    #ifdef TIME 
       GET_T2();
       _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
    #endif

    return result;
}

SavimeResult DefaultStorageManager::Comparison(std::string op, DatasetPtr operand1, float operand2, DatasetPtr& destinyDataset)
{
    SavimeResult result;
    
    try
    {
        #ifdef TIME 
           GET_T1();
        #endif 

        if(operand1->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, float, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        
       #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
       #endif

       return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::ComparisonDim(std::string op, DimSpecPtr dimSpecs, int64_t totalLength, float operand2,  DatasetPtr& destinyDataset)
{
    SavimeResult result;
    auto operand1 = dimSpecs->dimension->GetDimension();    
    
    #ifdef TIME 
        GET_T1();
    #endif 
    
    if(operand1->type == INTEGER_TYPE)
    {
        TemplateStorageManager<int32_t, float, int32_t> tsm(_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs, operand2, totalLength, destinyDataset);
    }
    else if(operand1->type == LONG_TYPE)
    {
        TemplateStorageManager<int64_t, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs, operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == FLOAT_TYPE)
    {
        TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs, operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == DOUBLE_TYPE)
    {
        TemplateStorageManager<double, float, double> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs, operand2, totalLength, destinyDataset); 
    }    

    #ifdef TIME 
       GET_T2();
       _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
    #endif

    return result;
}

SavimeResult DefaultStorageManager::Comparison(std::string op,  DatasetPtr operand1, int32_t operand2,  DatasetPtr& destinyDataset)
{
    SavimeResult result;
    try
    {
        #ifdef TIME 
           GET_T1();
        #endif

        if(operand1->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, int32_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::ComparisonDim(std::string op,  DimSpecPtr dimSpecs, int64_t totalLength, int32_t operand2,  DatasetPtr &   destinyDataset)
{
    SavimeResult result;
    auto operand1 = dimSpecs->dimension->GetDimension();    
    
    #ifdef TIME 
        GET_T1();
    #endif 

    if(operand1->type == INTEGER_TYPE)
    {
        TemplateStorageManager<int32_t, int32_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset);
    }
    else if(operand1->type == LONG_TYPE)
    {
        TemplateStorageManager<int64_t, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == FLOAT_TYPE)
    {
        TemplateStorageManager<float, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == DOUBLE_TYPE)
    {
        TemplateStorageManager<double, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }    

    #ifdef TIME 
       GET_T2();
       _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
    #endif

    return result;
}

SavimeResult DefaultStorageManager::Comparison(std::string op,  DatasetPtr  operand1, int64_t operand2,  DatasetPtr &   destinyDataset)
{
    SavimeResult result;
    
    try
    {  
        #ifdef TIME 
           GET_T1();
        #endif

        if(operand1->type == INTEGER_TYPE)
        {
            TemplateStorageManager<int32_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == LONG_TYPE)
        {
            TemplateStorageManager<int64_t, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == FLOAT_TYPE)
        {
            TemplateStorageManager<float, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
            TemplateStorageManager<double, int64_t, bool> tsm (_this, _configurationManager, _systemLogger);
            result = tsm.Comparison(op, operand1, operand2, destinyDataset); 
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::ComparisonDim(std::string op, DimSpecPtr dimSpecs, int64_t totalLength, int64_t operand2,  DatasetPtr&  destinyDataset)
{
    SavimeResult result;
    auto operand1 = dimSpecs->dimension->GetDimension();
    
    #ifdef TIME 
        GET_T1();
    #endif

    if(operand1->type == INTEGER_TYPE)
    {
        TemplateStorageManager<int32_t, int64_t, int32_t> tsm(_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset);
    }
    else if(operand1->type == LONG_TYPE)
    {
        TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == FLOAT_TYPE)
    {
        TemplateStorageManager<float, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }
    else if(operand1->type == DOUBLE_TYPE)
    {
        TemplateStorageManager<double, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
        result = tsm.ComparisonDim(op, dimSpecs,  operand2, totalLength, destinyDataset); 
    }    

    #ifdef TIME 
       GET_T2();
       _systemLogger->LogEvent(_moduleName, "Comparison took "+std::to_string(GET_DURATION())+" ms.");
    #endif

    return result;
}

SavimeResult DefaultStorageManager::Comparison(std::string op, DatasetPtr operand1, std::string operand2, DatasetPtr& destinyDataset)
{
    return SAVIME_FAILURE;
}

SavimeResult DefaultStorageManager::ComparisonDim(std::string op,  DimSpecPtr dimSpecs, int64_t totalLength, std::string operand2, DatasetPtr& destinyDataset)
{
    return SAVIME_FAILURE;
}

SavimeResult DefaultStorageManager::Aritmethic(std::string op, DatasetPtr operand1, DatasetPtr operand2, DatasetPtr& destinyDataset)
{
    try
    {
        #ifdef TIME 
           GET_T1();
        #endif
        DataType resultType = SelectType(operand1->type, operand2->type, op);
                   
        if(operand1->type == INTEGER_TYPE && operand2->type == INTEGER_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int32_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int32_t, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int32_t, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int32_t, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == INTEGER_TYPE && operand2->type == LONG_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int32_t, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int32_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int32_t, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int32_t, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == INTEGER_TYPE && operand2->type == FLOAT_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int32_t, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int32_t, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int32_t, float, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int32_t, float, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == INTEGER_TYPE && operand2->type == DOUBLE_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int32_t, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int32_t, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int32_t, double, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int32_t, double, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == LONG_TYPE && operand2->type == INTEGER_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int64_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int64_t, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int64_t, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int64_t, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == LONG_TYPE && operand2->type == LONG_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int64_t, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int64_t, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int64_t, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == LONG_TYPE && operand2->type == FLOAT_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int64_t, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int64_t, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int64_t, float, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int64_t, float, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == LONG_TYPE && operand2->type == DOUBLE_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<int64_t, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<int64_t, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<int64_t, double, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<int64_t, double, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == INTEGER_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<float, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<float, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<float, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<float, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == LONG_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<float, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<float, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<float, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<float, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == FLOAT_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<float, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<float, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<float, float, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == FLOAT_TYPE && operand2->type == DOUBLE_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<float, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<float, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<float, double, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<float, double, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == INTEGER_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<double, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<double, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<double, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<double, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == LONG_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<double, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<double, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<double, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<double, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == FLOAT_TYPE)
        {
            if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<double, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<double, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<double, float, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<double, float, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else if(operand1->type == DOUBLE_TYPE && operand2->type == DOUBLE_TYPE)
        {
           if(resultType == INTEGER_TYPE)
            {
                TemplateStorageManager<double, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == LONG_TYPE)
            {
                TemplateStorageManager<double, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else if(resultType == FLOAT_TYPE)
            {
                TemplateStorageManager<double, double, float> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
            else
            {
                TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
                return tsm.Aritmethic(op, operand1, operand2, destinyDataset);
            }
        }
        else
        {
            throw std::runtime_error("Dataset types are invalid for arithmetic operations.");
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Arithmetic took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Aritmethic(std::string op, DatasetPtr operand1, double operand2, DatasetPtr& destinyDataset)
{   
    try
    {
        #ifdef TIME 
           GET_T1();
        #endif

        DataType resultType = SelectType(operand1->type, DOUBLE_TYPE, op);
            
        if(operand1->type == INTEGER_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int32_t, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int32_t, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int32_t, double, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int32_t, double, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           } 
        }
        else if(operand1->type == LONG_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int64_t, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int64_t, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int64_t, double, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int64_t, double, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           } 
        }
        else if(operand1->type == FLOAT_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<float, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<float, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<float, double, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<float, double, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           } 
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<double, double, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<double, double, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<double, double, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, DOUBLE_TYPE, destinyDataset);
           } 
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Aritmethic took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }  
}

SavimeResult DefaultStorageManager::Aritmethic(std::string op, DatasetPtr operand1, float operand2, DatasetPtr& destinyDataset)
{ 
    try
    {       
        #ifdef TIME 
          GET_T1();
        #endif

        DataType resultType = SelectType(operand1->type, FLOAT_TYPE, op);
        
        if(operand1->type == INTEGER_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int32_t, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int32_t, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int32_t, float, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int32_t, float, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           } 
        }
        else if(operand1->type == LONG_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int64_t, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int64_t, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int64_t, float, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int64_t, float, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           } 
        }
        else if(operand1->type == FLOAT_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<float, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<float, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<float, float, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<double, float, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<double, float, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<double, float, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<double, float, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, FLOAT_TYPE, destinyDataset);
           }
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Aritmethic took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Aritmethic(std::string op, DatasetPtr operand1, int32_t operand2, DatasetPtr& destinyDataset)
{
    try
    {
        #ifdef TIME 
          GET_T1();
        #endif
        
        DataType resultType = SelectType(operand1->type, INTEGER_TYPE, op);
          
        if(operand1->type == INTEGER_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int32_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int32_t, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int32_t, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int32_t, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == LONG_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int64_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int64_t, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int64_t, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int64_t, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == FLOAT_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<float, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<float, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<float, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<float, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<double, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<double, int32_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<double, int32_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<double, int32_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, INTEGER_TYPE, destinyDataset);
           }
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Aritmethic took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Aritmethic(std::string op,  DatasetPtr  operand1, int64_t operand2, DatasetPtr& destinyDataset)
{
    try
    {
        #ifdef TIME 
          GET_T1();
        #endif

        DataType resultType = SelectType(operand1->type, LONG_TYPE, op);
    
        if(operand1->type == INTEGER_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int32_t, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int32_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int32_t, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int32_t, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == LONG_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<int64_t, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<int64_t, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<int64_t, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == FLOAT_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<float, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<float, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<float, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<float, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
        }
        else if(operand1->type == DOUBLE_TYPE)
        {
           if(resultType == INTEGER_TYPE)
           {
               TemplateStorageManager<double, int64_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == LONG_TYPE)
           {
               TemplateStorageManager<double, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else if(resultType == FLOAT_TYPE)
           {
               TemplateStorageManager<double, int64_t, float> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
           else
           {
               TemplateStorageManager<double, int64_t, double> tsm (_this, _configurationManager, _systemLogger);
               return tsm.Aritmethic(op, operand1, operand2, LONG_TYPE, destinyDataset);
           }
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Aritmethic took "+std::to_string(GET_DURATION())+" ms.");
        #endif
            
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Aritmethic(std::string op,  DatasetPtr  operand1, std::string operand2,  DatasetPtr &   destinyDataset)
{  
    return SAVIME_FAILURE;
}

 SavimeResult DefaultStorageManager::MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength,  DatasetPtr& destinyDataset)
 {
    try
    {  
        #ifdef TIME 
          GET_T1();
        #endif
        
        if(dimSpecs->dimension->GetDimension()->type == INTEGER_TYPE)
        {
           TemplateStorageManager<int32_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.MaterializeDim(dimSpecs, totalLength, INTEGER_TYPE, destinyDataset);
        }
        else if (dimSpecs->dimension->GetDimension()->type == LONG_TYPE)
        {
           TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.MaterializeDim(dimSpecs, totalLength, LONG_TYPE, destinyDataset);
        }
        else if (dimSpecs->dimension->GetDimension()->type == FLOAT_TYPE)
        {
           TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
           return tsm.MaterializeDim(dimSpecs, totalLength, FLOAT_TYPE, destinyDataset);
        }
        else if (dimSpecs->dimension->GetDimension()->type == DOUBLE_TYPE)
        {
           TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
           return tsm.MaterializeDim(dimSpecs, totalLength, DOUBLE_TYPE, destinyDataset);
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "MaterializeDim took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
 }
 
 SavimeResult DefaultStorageManager::PartiatMaterializeDim(DatasetPtr filter,  DimSpecPtr dimSpecs, 
                    int64_t totalLength, DatasetPtr& destinyDataset, DatasetPtr&  destinyRealDataset)
 {
    try
    {   
        #ifdef TIME 
            GET_T1();
        #endif
        
        if(dimSpecs->dimension->GetDimension()->type == INTEGER_TYPE)
        {
           TemplateStorageManager<int32_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.PartiatMaterializeDim(filter, dimSpecs, totalLength, INTEGER_TYPE, destinyDataset, destinyRealDataset);
        }
        else if (dimSpecs->dimension->GetDimension()->type == LONG_TYPE)
        {
           TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.PartiatMaterializeDim(filter, dimSpecs, totalLength, LONG_TYPE, destinyDataset, destinyRealDataset);
        }
        else if (dimSpecs->dimension->GetDimension()->type == FLOAT_TYPE)
        {
           TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
           return tsm.PartiatMaterializeDim(filter, dimSpecs, totalLength, FLOAT_TYPE, destinyDataset, destinyRealDataset);
        }
        else if (dimSpecs->dimension->GetDimension()->type == DOUBLE_TYPE)
        {
           TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
           return tsm.PartiatMaterializeDim(filter, dimSpecs, totalLength, DOUBLE_TYPE, destinyDataset, destinyRealDataset);
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "PartiatMaterializeDim took "+std::to_string(GET_DURATION())+" ms.");
        #endif

        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
 }
 
SavimeResult DefaultStorageManager::Stretch(DatasetPtr origin, int64_t entryCount, int64_t recordsRepetitions, int64_t datasetRepetitions, DatasetPtr& destinyDataset)
{
    try
    {  
        #ifdef TIME 
          GET_T1();
        #endif
        
        if(origin->type == INTEGER_TYPE)
        {
           TemplateStorageManager<int32_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Stretch(origin, entryCount, recordsRepetitions, datasetRepetitions, INTEGER_TYPE, destinyDataset);
        }
        else if(origin->type == LONG_TYPE)
        {
           TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Stretch(origin, entryCount, recordsRepetitions, datasetRepetitions, LONG_TYPE, destinyDataset);
        }
        else if(origin->type == FLOAT_TYPE)
        {
           TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Stretch(origin, entryCount, recordsRepetitions, datasetRepetitions, FLOAT_TYPE, destinyDataset);
        }
        else if(origin->type == DOUBLE_TYPE)
        {
           TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Stretch(origin, entryCount, recordsRepetitions, datasetRepetitions, DOUBLE_TYPE, destinyDataset);
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Stretch took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}

SavimeResult DefaultStorageManager::Split(DatasetPtr origin, int64_t totalLength, int64_t parts, vector<DatasetPtr>& brokenDatasets)
{
    try
    {  
        #ifdef TIME 
          GET_T1();
        #endif
        SavimeResult result;
        
        if(origin->type == INTEGER_TYPE)
        {
           TemplateStorageManager<int32_t, int32_t, int32_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Split(origin, totalLength, parts, brokenDatasets);
        }
        else if(origin->type == LONG_TYPE)
        {
           TemplateStorageManager<int64_t, int64_t, int64_t> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Split(origin, totalLength, parts, brokenDatasets);
        }
        else if(origin->type == FLOAT_TYPE)
        {
           TemplateStorageManager<float, float, float> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Split(origin, totalLength, parts, brokenDatasets);
        }
        else if(origin->type == DOUBLE_TYPE)
        {
           TemplateStorageManager<double, double, double> tsm (_this, _configurationManager, _systemLogger);
           return tsm.Split(origin, totalLength, parts, brokenDatasets);
        }
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Stretch took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        return result;
    }
    catch(std::exception& e)
    {
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}
 
void DefaultStorageManager::FromBitMaskToIndex(DatasetPtr& dataset, bool keepBitmask)
{
    #ifdef TIME 
        GET_T1();
    #endif
    
    int numCores = _configurationManager->GetIntValue(MAX_THREADS); 
    int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t startPositionPerCore[numCores];
    int64_t finalPositionPerCore[numCores];
    
    if(dataset->bitMask == NULL)
        throw std::runtime_error("Dataset does not contain a bitmask.");
    
    int32_t numThreads = SetWorkloadPerThread(dataset->bitMask->num_blocks(), workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
    int64_t sizePerChunk[numCores];
    int64_t totalLen=0, offsetPerChunk[numCores];
      
    for(int32_t i = 0;i < numThreads; i++)
    {
        sizePerChunk[i] = dataset->bitMask->count_parallel(startPositionPerCore[i], finalPositionPerCore[i], numCores, workPerThread);
        startPositionPerCore[i] *= dataset->bitMask->bits_per_block; 
        finalPositionPerCore[i] *= dataset->bitMask->bits_per_block;
        finalPositionPerCore[i] = std::max(finalPositionPerCore[i], (int64_t)dataset->bitMask->size()); 
    }
    
    //calculating offset per chunk
    for(int32_t i = 0; i < numThreads; i++)
    {
        offsetPerChunk[i] = totalLen;
        totalLen+=sizePerChunk[i];
    }
    
    //if no bit is set return empty dataset
    if(totalLen == 0)
    {
        dataset->length = dataset->entry_count = 0;
        return;
    }
        
    //Dataset must be created
    auto auxDataSet = Create(LONG_TYPE, totalLen);
    if(auxDataSet == NULL)
        throw std::runtime_error("Could not create a dataset.");
    
    dataset->location = auxDataSet->location;
    dataset->length = auxDataSet->length;
    dataset->entry_count = auxDataSet->entry_count;
    dataset->type = auxDataSet->type;
    
    //To avoid file removal by destructor
    auxDataSet->location = "";
    
    DatasetHandlerPtr handler = GetHandler(dataset);
    int64_t * buffer = (int64_t*)handler->GetBuffer();
    
    //creating index set from bitmap
    #pragma omp parallel
    {
        int64_t i=0, index;
        if(startPositionPerCore[omp_get_thread_num()] == 0)
           index = dataset->bitMask->find_first();
        else
           index = dataset->bitMask->find_next(startPositionPerCore[omp_get_thread_num()]);
        
        while(index <= finalPositionPerCore[omp_get_thread_num()] && index != dataset->bitMask->npos)
        {
            buffer[offsetPerChunk[omp_get_thread_num()]+i] = index;
            index = dataset->bitMask->find_next(index);
            i++;
        }  
    }
    
    dataset->has_indexes = true;
    dataset->sorted = true;
    handler->Close();
    
    if(!keepBitmask)
        dataset->bitMask->clear();
    
    #ifdef TIME 
        GET_T2();
        _systemLogger->LogEvent(_moduleName, "FromBitMaskToIndex took "+std::to_string(GET_DURATION())+" ms.");
    #endif
}
 
 
void DefaultStorageManager::DisposeObject(MetadataObject * object)
{
     if(Dataset * dataset = dynamic_cast<Dataset*>(object))
     {
         if(dataset->location.empty())
             return;
            
        #ifdef TIME 
            GET_T1();
        #endif
         
         int64_t fileSize = FILE_SIZE(dataset->location.c_str());
         if(remove(dataset->location.c_str()) == 0)
         {   
             
            #ifdef TIME 
                GET_T2();
                _systemLogger->LogEvent(_moduleName, "Remove dataset took "+std::to_string(GET_DURATION())+" ms.");
            #endif

             
            #ifdef DEBUG
             _systemLogger->LogEvent(this->_moduleName, "  Removing dataset "+dataset->name+"  "
                                     +dataset->location+" "+std::to_string(dataset->length));
            #endif
            
             _usedStorageSize -= fileSize;   
         }
         else
         {
             _systemLogger->LogEvent(this->_moduleName, "Could not remove dataset "+dataset->location+": "+std::string(strerror(errno)));
         }
     }
 }