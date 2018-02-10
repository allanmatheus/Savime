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
#ifndef DEFAULT_STORAGE_MANAGER_H
#define DEFAULT_STORAGE_MANAGER_H

#include "../core/include/storage_manager.h"
#define END_OF_REGISTERS -1


class DefaultDatasetHandler : public DatasetHandler
{
    protected:
        
    int32_t _fd;
    int32_t _entry_length;
    int64_t _buffer_offset;
    int64_t _mapping_length;
    int64_t _huge_pages;
    int64_t _huge_pages_size;
    StorageManagerPtr _storageManager;
    char * _buffer;
    
    void Remap();
    
    public :
        
    DefaultDatasetHandler(DatasetPtr ds, StorageManagerPtr storageManager, 
                         int64_t hugeTblThreshold, int64_t hugeTblSize);
    
    int32_t GetValueLength();
    DatasetPtr GetDataSet();
    void Append(char * value);
    void * Next();
    bool HasNext();
    void InsertAt(char * value, int64_t offset);
    void CursorAt(int64_t index);
    char* GetBuffer();
    char* GetBufferAt(int64_t offset);
    void TruncateAt(int64_t offset);
    void Close();
};


class DefaultStorageManager : public StorageManager, public MetadataObjectListener
{
    mutex  _mutex;
    int64_t _usedStorageSize;
    std::shared_ptr<DefaultStorageManager> _this;
    std::string GenerateUniqueFileName();
    
public:
    
    DefaultStorageManager(ConfigurationManagerPtr configurationManager, SystemLoggerPtr systemLogger) :
        StorageManager(configurationManager, systemLogger) {}
    
    void SetThisPtr(std::shared_ptr<DefaultStorageManager> thisPtr)
    {
        _usedStorageSize = 0;
        _this = thisPtr;
    }
    
    std::shared_ptr<DefaultStorageManager> GetOwn()
    {
        return _this;
    }
    
    DatasetPtr Create(DataType type, int64_t size);
    DatasetPtr Create(DataType type, double init, double spacing, double end);
    SavimeResult Save(DatasetPtr dataset) ;
    DatasetHandlerPtr GetHandler( DatasetPtr dataset);
    SavimeResult Drop( DatasetPtr dataset);
    bool CheckSorted(DatasetPtr dataset);
    SavimeResult RegisterDatasetExpasion(int64_t size);
    SavimeResult RegisterDatasetTruncation(int64_t size);
    
    RealIndex Logical2Real(DimensionPtr dimension, LogicalIndex logicalIndex);
    RealIndex Logical2ApproxReal(DimensionPtr dimension, LogicalIndex logicalIndex);
    SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr logicalIndexes, DatasetPtr& destinyDataset);
    LogicalIndex Real2Logical(DimensionPtr dimension, RealIndex realIndex);
    SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr realIndexes, DatasetPtr& destinyDataset);
    SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2, DimensionPtr& destinyDim);
    
    SavimeResult Copy(DatasetPtr originDataset, int64_t lowerBound, int64_t upperBound, int64_t offsetInDestiny, int64_t spacingInDestiny, DatasetPtr destinyDataset);
    SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping, DatasetPtr destinyDataset, int64_t& copied);
     
    SavimeResult Filter(DatasetPtr originDataset,  DatasetPtr filterDataSet,  DatasetPtr& destinyDataset);
    
    SavimeResult And(DatasetPtr operand1,  DatasetPtr operand2,  DatasetPtr& destinyDataset);
    SavimeResult Or(DatasetPtr operand1,  DatasetPtr operand2,  DatasetPtr& destinyDataset);
    SavimeResult Not(DatasetPtr operand1,  DatasetPtr& destinyDataset);
    
    SavimeResult Comparison(std::string op,  DatasetPtr operand1,  DatasetPtr operand2,  DatasetPtr& destinyDataset);
    SavimeResult ComparisonDim(std::string op,  DimSpecPtr  dimSpecs, int64_t totalLength,  DatasetPtr operand2,  DatasetPtr& destinyDataset);
    SavimeResult Comparison(std::string op,  DatasetPtr operand1, double operand2,  DatasetPtr& destinyDataset);
    SavimeResult ComparisonDim(std::string op,  DimSpecPtr  dimSpecs, int64_t totalLength, double operand2,  DatasetPtr& destinyDataset);
    SavimeResult Comparison(std::string op,  DatasetPtr operand1, float operand2,  DatasetPtr& destinyDataset);
    SavimeResult ComparisonDim(std::string op,  DimSpecPtr  dimSpecs, int64_t totalLength, float operand2,  DatasetPtr& destinyDataset);
    SavimeResult Comparison(std::string op,  DatasetPtr operand1, int32_t operand2,  DatasetPtr& destinyDataset);
    SavimeResult ComparisonDim(std::string op,  DimSpecPtr  dimSpecs, int64_t totalLength, int32_t operand2,  DatasetPtr& destinyDataset);
    SavimeResult Comparison(std::string op,  DatasetPtr operand1, int64_t operand2,  DatasetPtr& destinyDataset);
    SavimeResult ComparisonDim(std::string op,  DimSpecPtr  dimSpecs, int64_t totalLength, int64_t operand2,  DatasetPtr& destinyDataset);
    SavimeResult Comparison(std::string op,  DatasetPtr operand1, std::string operand2,  DatasetPtr& destinyDataset);
    SavimeResult ComparisonDim(std::string op,  DimSpecPtr  dimSpecs, int64_t totalLength, std::string operand2,  DatasetPtr& destinyDataset);
    
    SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs, vector<int64_t> lowerBounds, vector<int64_t> upperBounds, DatasetPtr& destinyDataset);
    
    SavimeResult Aritmethic(std::string op, DatasetPtr operand1,  DatasetPtr operand2,  DatasetPtr& destinyDataset);
    SavimeResult Aritmethic(std::string op, DatasetPtr operand1, double operand2,  DatasetPtr& destinyDataset);
    SavimeResult Aritmethic(std::string op, DatasetPtr operand1, float operand2,  DatasetPtr& destinyDataset);
    SavimeResult Aritmethic(std::string op, DatasetPtr operand1, int32_t operand2,  DatasetPtr& destinyDataset);
    SavimeResult Aritmethic(std::string op, DatasetPtr operand1, int64_t operand2,  DatasetPtr& destinyDataset);
    SavimeResult Aritmethic(std::string op, DatasetPtr operand1, std::string operand2,  DatasetPtr& destinyDataset);
    
    SavimeResult MaterializeDim( DimSpecPtr  dimSpecs, int64_t totalLength,  DatasetPtr& destinyDataset);
    SavimeResult PartiatMaterializeDim( DatasetPtr filter,  DimSpecPtr dimSpecs, int64_t totalLength,  DatasetPtr& destinyDataset, DatasetPtr& destinyRealDataset);
    SavimeResult Stretch(DatasetPtr origin, int64_t entryCount, int64_t recordsRepetitions, int64_t datasetRepetitions, DatasetPtr& destinyDataset);
    SavimeResult Split(DatasetPtr origin, int64_t totalLength, int64_t parts, vector<DatasetPtr>& brokenDatasets);
    void FromBitMaskToIndex(DatasetPtr& dataset, bool keepBitmask);
    
    void DisposeObject(MetadataObject * object);
};



#endif /* DEFAULT_STORAGE_MANAGER_H */

