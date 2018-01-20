#ifndef STORAGE_MANAGER_H
#define STORAGE_MANAGER_H
/*! \file */
#include "metadata.h"
#include "query_data_manager.h"

using namespace std;

#define SET_LOGICAL_INDEX(l, x) l.intIndex = x; l.longIndex = x; l.floatIndex = x; l.doubleIndex = x
#define INVALID_EXACT_REAL_INDEX -1
#define BELOW_OFFBOUNDS_REAL_INDEX -2
#define ABOVE_OFFBOUNDS_REAL_INDEX -3

typedef int64_t RealIndex;

struct LogicalIndex
{
    DataType type;
    int32_t intIndex;
    int64_t longIndex;
    float floatIndex;
    double doubleIndex;
    string stdIndex;
};

#define GET_LOGICAL_INDEX(X, T, Z)  switch(T.type) {\
                                        case INTEGER_TYPE: X = (Z) T.intIndex; break;\
                                        case FLOAT_TYPE: X = (Z) T.floatIndex; break;\
                                        case LONG_TYPE: X = (Z) T.longIndex; break;\
                                        case DOUBLE_TYPE: X = (Z) T.doubleIndex; break;\
                                    }\
  
#define _SET_LOGICAL_INDEX(X, T, V)  switch(T) {\
                                        case INTEGER_TYPE: X.intIndex = (int32_t)V; break;\
                                        case FLOAT_TYPE: X.floatIndex = (float)V; break;\
                                        case LONG_TYPE: X.longIndex = (int64_t)V; break;\
                                        case DOUBLE_TYPE: X.doubleIndex = (double)V; break;\
                                    }\
                                    X.type = T;\ 


/**A DatasetHandler encapsulates a Dataset reference and allows 
 * iteration over dataset values, data appending, insertions and trucations.*/
class DatasetHandler
{
    
protected:
    DatasetPtr _ds; /*!<The Dataset reference to be handled.*/

public :
        
    /**
     * Constructor. It maps the Dataset file in memory
     * and sets a inner entry cursor at position 0.
     * @param ds is a Dataset reference to be handled.
     */
    DatasetHandler(DatasetPtr ds)   {_ds = ds;}
    
    /**
     * Returns the size of a single Dataset entry/value.
     * @return 
     */
    virtual int32_t GetValueLength() = 0;
    
    /**
     * Gets the encapsulated Dataset reference.
     * @return A Dataset reference being handled.
     */
    virtual DatasetPtr GetDataSet() = 0;
    
    /**
     * Appends a new value at the end of the dataset. 
     * @param value is a pointer to a value to be appended. The length of
     * the memory chunk pointed by value must be equal to the value
     * returned by GetValueLength();
     */
    virtual void Append(char * value)= 0;
    
    /**
     * Gets a reference to the next value in the Dataset file and increments
     * the inner cursor.
     * @return A pointer to memory region of the sized returned by
     * GetValueLength() containing the next value in the Dataset.
     */
    virtual void * Next() = 0;
    
    /**
     * Checks whether the inner cursor has reached the end of the Dataset
     * or if there are still values to be iterated over.
     * @return True if there are reamining values to be iterated over, false otherwise.
     */
    virtual bool HasNext()  = 0;
    
    virtual void InsertAt(char * value, int64_t offset) = 0;
    
    /**
     * Sets the inner cursor at specific value index in the dataset.
     * @param index the index for which to set the inner cursor to.
     */
    virtual void CursorAt(int64_t index) = 0;
    
    /**
     * Gets a pointer to the memory region mapped for the Dataset file.
     * @return A pointer the memory region mapped for the Dataset file.
     */
    virtual char* GetBuffer() = 0;
    
    /**
     * Gets a point to the memory region mapped for the Dataset file at a given
     * dataset index.
     * @param index is the value index for which the pointer must be retrieved.
     * @return A pointer to the memory region mapped for the Dataset file at a given
     * dataset index.
     */
    virtual char* GetBufferAt(int64_t index) = 0;
    
    /**
    * Truncates the Dataset file at a given indexing, removing posterior entries.
    * @param index is the value index defining the position where the file must
    * be truncated.
    */
    virtual void TruncateAt(int64_t index) = 0;
   
    /**
     * Closes the Dataset file and calls unmap to free the used memory region.
     * The dataset file is maintained.
     */
    virtual void Close() = 0;
};
typedef std::shared_ptr<DatasetHandler> DatasetHandlerPtr;

/**The storage manager is the module responsible for creating and removing
 * Datasets files, keeping tracking of storage usage, and for carrying out
 * basic Dataset operations.*/
class StorageManager : public SavimeModule
{
    
public:
    /**
     * Constructor
     * @param configurationManager is the standard system ConfigurationManager.
     * @param systemLogger is the standard SystemLogger.
     */
    StorageManager(ConfigurationManagerPtr configurationManager, 
                   SystemLoggerPtr systemLogger) :
        SavimeModule("StorageManager", configurationManager, systemLogger) {}
    
    /**
     * Creates a new Dataset.
     * @param type is the DataType for the Dataset entries.
     * @param size is the number of entries in the Dataset.
     * @return A reference to the newly created Dataset, or NULL in case
     * of failure.
     */
    virtual DatasetPtr Create(DataType type, int64_t size) = 0;
    
    /**
     * Creates a new Dataset and fills it with a sequence of numerical values.
     * @param type is the DataType for the Dataset entries.
     * @param init is the initial value of the numerical sequence.
     * @param spacing is the difference between two adjacent values in the sequence.
     * @param end is the final entry in the sequence.
     * @return A reference to the newly created Dataset, or NULL in case
     * of failure.
     */
    virtual DatasetPtr Create(DataType type, double init, double spacing, double end) = 0;
    
    /**
     * Moves a Dataset file to the storage manager dir and registers 
     * the Dataset memory usage.
     * @param dataset is a Dataset reference to be saved.
     * @return SAVIME_SUCCESS on sucess or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Save(DatasetPtr dataset) = 0;
    
    /**
     * Creates a DatasetHandler for a Dataset.
     * @param dataset is a Dataset for which the DatasetHandler must be created.
     * @return The DatasetHandler for the informed Dataset.
     */
    virtual DatasetHandlerPtr GetHandler(DatasetPtr dataset) = 0;
    
    /**
     * Removes the Dataset file and accounts for the freed memory.
     * @param dataset is a Dataset reference to be deleted.
     * @return SAVIME_SUCCESS on sucess or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Drop(DatasetPtr dataset) = 0;
    
    /**
     * Checks whether the underlying Datset values are sorted or not and
     * sets the sorted dataset flag accordingly.
     * @param dataset is a Dataset reference to the dataset to be checked.
     * @return True if the dataset is sorted and false otherwise.
     */
    virtual bool CheckSorted(DatasetPtr dataset) = 0;
    
    /**
     * Register the new size occupied by a recently expanded Dataset.
     * @param size is a 64-bit integer containing the size by which
     * the dataset was increased.
     * @return SAVIME_SUCCESS on sucess or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult RegisterDatasetExpasion(int64_t size) = 0;
    
    /**
     * Register the new size occupied by a recently truncated Dataset.
     * @param size is a 64-bit integer containing the size by which
     * the dataset was decreased.
     * @return SAVIME_SUCCESS on sucess or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult RegisterDatasetTruncation(int64_t size) = 0;
    
    /**
     * Converts a Logical TAR index into a Real index.
     * @param dimension is the Dimension reference for which the indexes must be converted.
     * @return A RealIndex reference containing the real index.
     */
    virtual RealIndex Logical2Real(DimensionPtr dimension, LogicalIndex logicalIndex) = 0;
    
    /**
     * Gets the closest TAR real index for a logical index.
     * @param dimension is the Dimension reference for which the indexes must be converted.
     * @return A RealIndex reference containing the real index.
     */
    virtual RealIndex Logical2ApproxReal(DimensionPtr dimension, LogicalIndex logicalIndex) = 0;
    
    /**
    * Converts a Dataset containing Logical TAR indexex into a Dataset containing Real indexes.
    * @param dimension is the Dimension reference for which the indexes must be converted.
    * @param dimSpecs is the DimensionSpecification of the subtar that contains the Dataset with
    * Logical Indexes.
    * @param  logicalIndexes is a Dataset containing the logical indexes.
    * @param  destinyDataset is the reference where the newly created Dataset
    * with Real indexes will be saved.
    * @return SAVIME_SUCCESS on sucess or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr logicalIndexes, DatasetPtr& destinyDataset) = 0;
    
    /**
    * Converts a Real TAR index into a Logical index.
    * @param dimension is the Dimension reference for which the indexes must be converted.
    * @return A LogicalIndex reference containing the logical index.
    */
    virtual LogicalIndex Real2Logical(DimensionPtr dimension, RealIndex realIndex) = 0;
    
    /**
    * Converts a Dataset containing Real TAR indexes into a Dataset containing Logical indexes.
    * @param dimension is the Dimension reference for which the indexes must be converted.
    * @param dimSpecs is the DimensionSpecification of the subtar that contains the Dataset with
    * Logical Indexes.
    * @param  realIndexes is a Dataset containing the real indexes.
    * @param  destinyDataset is the reference where the newly created Dataset
    * with Logical indexes will be saved.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr realIndexes, DatasetPtr& destinyDataset) = 0;
   
    /**
    * Creates an new dimension as the intersection between the two input dimensions.
    * @param dim1 is the Dimension reference.
    * @param dim2 is the Dimension reference. 
    * @param destinyDim is the Dimension reference where the resulting dimension will be saved. 
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2, DimensionPtr& destinyDim) = 0;
    
    /**
    * Copies data from one Dataset into another.
    * @param originDataset is the Dataset reference where data is.
    * @param lowerBound.
    * @param upperBound.
    * @param offsetInDestiny.
    * @param spacingInDestiny.
    * @param destinyDataset.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Copy(DatasetPtr originDataset, int64_t lowerBound, int64_t upperBound, int64_t offsetInDestiny, int64_t spacingInDestiny, DatasetPtr destinyDataset)= 0;
    
    /**
    * Copies data from one Dataset into another.
    * @param originDataset is the Dataset reference where data is.
    * @param mapping is the Dataset containing the position in the destiny dataset.
    * @param  destinyDataset is a Dataset reference where the result is to be saved. It must exist.
    * @param  copied is an long int where the number of copied values is saved.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping, DatasetPtr destinyDataset, int64_t& copied)= 0;
    
    
    /**
    * Apply a bitmaks of the filterDataset into the originDataset and saves the result into
     *the desitinyDataset.
    * @param originDataset is the Dataset reference to be filtered.
    * @param filterDataSet is the Dataset containing the bitmask specifying the filter.
    * @param  destinyDataset is a Dataset reference where the result is to be saved.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet, DatasetPtr& destinyDataset)= 0;
    
    /**
    * Applies a logical conjuction in two Datasets bitmasks and saves the resulting bitmaks into the de
    * destinyDataset.
    * @param operand1 is the Dataset containing the bitmask for LHS of the conjuction.
    * @param operand2 is the Dataset containing the bitmask for RHS of the conjuction.
    * @param  destinyDataset is a Dataset reference where the result is to be saved.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult And(DatasetPtr operand1, DatasetPtr operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
    * Applies a logical disjunction in two Datasets bitmasks and saves the resulting bitmaks into the de
    * destinyDataset.
    * @param operand1 is the Dataset containing the bitmask for LHS of the disjunction.
    * @param operand2 is the Dataset containing the bitmask for RHS of the disjunction.
    * @param  destinyDataset is a Dataset reference where the result is to be saved.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Or(DatasetPtr operand1, DatasetPtr operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
    * Applies a logical inversion in the operand1 Datasets bitmask and saves the resulting bitmaks into the de
    * destinyDataset.
    * @param operand1 is the Dataset containing the bitmask to be inverted.
    * @param  destinyDataset is a Dataset reference where the result is to be saved.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
    */
    virtual SavimeResult Not(DatasetPtr operand1, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Comparison(string op, DatasetPtr operand1, DatasetPtr operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, int64_t totalLength, DatasetPtr operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Comparison(string op, DatasetPtr operand1, double operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, int64_t totalLength, double operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Comparison(string op, DatasetPtr operand1, float operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, int64_t totalLength, float operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Comparison(string op, DatasetPtr operand1, int32_t operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, int64_t totalLength, int32_t operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Comparison(string op, DatasetPtr operand1, int64_t operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, int64_t totalLength, int64_t operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Comparison(string op, DatasetPtr operand1, string operand2, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Executes a comparison operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "=", "<>", ">", "<", "<=", ">=".
     * @param operand1 is the LHS for the comparison operation.
     * @param operand2 is the RHS for the comparison operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, int64_t totalLength, string operand2, DatasetPtr& destinyDataset)= 0;
    
    
     /**
     * Executes an arithmetic operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "+", "-", "*", "/", "%" or a supported function name.
     * @param operand1 is the LHS for the arithmetic operation.
     * @param operand2 is the RHS for the arithmetic operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Aritmethic(string op, DatasetPtr operand1, DatasetPtr operand2, DatasetPtr& destinyDataset)= 0;
    
     /**
     * Executes an arithmetic operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "+", "-", "*", "/", "%" or a supported function name.
     * @param operand1 is the LHS for the arithmetic operation.
     * @param operand2 is the RHS for the arithmetic operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Aritmethic(string op, DatasetPtr operand1, double operand2, DatasetPtr& destinyDataset)= 0;
    
     /**
     * Executes an arithmetic operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "+", "-", "*", "/", "%" or a supported function name.
     * @param operand1 is the LHS for the arithmetic operation.
     * @param operand2 is the RHS for the arithmetic operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Aritmethic(string op, DatasetPtr operand1, float operand2, DatasetPtr& destinyDataset)= 0;
    
     /**
     * Executes an arithmetic operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "+", "-", "*", "/", "%" or a supported function name.
     * @param operand1 is the LHS for the arithmetic operation.
     * @param operand2 is the RHS for the arithmetic operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Aritmethic(string op, DatasetPtr operand1, int32_t operand2, DatasetPtr& destinyDataset)= 0;
    
     /**
     * Executes an arithmetic operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "+", "-", "*", "/", "%" or a supported function name.
     * @param operand1 is the LHS for the arithmetic operation.
     * @param operand2 is the RHS for the arithmetic operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Aritmethic(string op, DatasetPtr operand1, int64_t operand2, DatasetPtr& destinyDataset)= 0;
    
     /**
     * Executes an arithmetic operation between operand1 and operand2 and saves the result in the destinyDataset.
     * @param op is a string containing the comparison operation: "+", "-", "*", "/", "%" or a supported function name.
     * @param operand1 is the LHS for the arithmetic operation.
     * @param operand2 is the RHS for the arithmetic operation.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Aritmethic(string op, DatasetPtr operand1, string operand2, DatasetPtr& destinyDataset)= 0;
        
    /**
     * Materiazes a dimension withing the range and the parameters specified by a subtar's DimensionSpecification.
     * Materialization Rules:
     *      For Implicit Dimensions (The ones whose dimension datasets do not contain a dataset)
     *          Ordered Dimspecs contain null datasets, logical values are calculated on demand
     *          Partial Dimspecs contain datasets with logical indexes that do not need mapping, real indexes are implicit (Partial)
     *          Total Dimepsecs contain datasets with logical indexes that do not need mapping, real indexes are implicit (Total)
     *  
     *      For Explicit dimensions (The ones whose dimension datasets contain a dataset with explicti logical indexes)
     *          Ordered Dimspecs contain null datasets, real indexes are implicit and logical ones are stored on the dimension dataset
     *          Partial Dimspecs contains datasets with real indexes that are mapped for logical ones by using the dimension dataset
     *          Total Dimespecs contains datasets with real indexes that are mapped for logical ones by using the dimension dataset
     *  
     * Partial x Total = Partial means that the dimspecs contains a mapping for that dimension, and that mapping/ordering/scheme is well behaved, i. e.
     * it repeats itself for other dimensions. Total means that the dimension mapping is not well behaved, and the dimension follows a particular pattern
     * just like a regular attribute.
     * 
     * @param dimSpecs is the DimensionSpecification of the subtar for which the dimension indexes must be materialized.
     * @param totalLength is a 64-bit integer containing the total length of the subtar the DimensionSpecification belongs to.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength, DatasetPtr& destinyDataset)= 0;
    
    /**
     * Materializes a dimension taking into consideration a bitmask filter.
     * @param filter is a Dataset containing the bitmask filter.
     * @param dimSpecs is the DimensionSpecification of the subtar for which the dimension indexes must be materialized.
     * @param totalLength is a 64-bit integer containing the total length of the subtar the DimensionSpecification belongs to.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @param destinyRealDataset is a Dataset reference where the real indexes for explicit dimensions is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult PartiatMaterializeDim(DatasetPtr filter,  DimSpecPtr dimSpecs, int64_t totalLength,  DatasetPtr& destinyDataset, DatasetPtr& destinyRealDataset)=0;
    
    /**
     * Create a new dataset by replicate individual records or replicating the entire dataset.
     * @param origin is a Dataset to be stretched.
     * @param recordsRepetitions is a 64-bit integer indicating how many times each record should be replicated.
     * @param datasetRepetitions is a 64-bit integer indicating how many the entire dataset should be replicated.
     * @param destinyDataset is a Dataset reference where the result is to be saved.
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Stretch(DatasetPtr origin, int64_t entryCount, int64_t recordsRepetitions, int64_t datasetRepetitions, DatasetPtr& destinyDataset)=0;
    
    /**
     * Fills the Dataset file with indexes according to the bitmask.
     * @param dataset is a Dataset reference containing the bitmask.
     * @param keepBitmask is a flag specifying if the bitmask in the dataset 
     * should or not be freed after the conversion.
     */
    virtual void FromBitMaskToIndex(DatasetPtr& dataset, bool keepBitmask) = 0;
    
    /**
     * Divides a dataset into many datasets.
     * @param origin is a Dataset reference containing the dataset to be split.
     * @param parts is the number of parts the dataset must be split into; 
     * @param totalLength is the size of the origin dataset.
     * @param brokenDatasets is a dataset vector where the resulting split datasets are to be stored; 
     * @return SAVIME_SUCCESS on success or SAVIME_FAILURE otherwise.
     */
    virtual SavimeResult Split(DatasetPtr origin, int64_t totalLength, int64_t parts, vector<DatasetPtr>& brokenDatasets) = 0;
};
typedef std::shared_ptr<StorageManager> StorageManagerPtr;

 
/**
 * Function that prints a dataset for debugging purposes.
 */     
inline void dbg_print_dataset(DatasetHandler * handler)
{
    DatasetPtr ds = handler->GetDataSet();
    int64_t i = 0; handler->CursorAt(0);
    
    printf("%s - %s\n", ds->name, ds->location.c_str());
    printf("-------------------------------------------\n");
    
    while(handler->HasNext())
    {
        switch(handler->GetDataSet()->type)
        {
            case INTEGER_TYPE: printf("%ld - %d\n",    i++,  *((int32_t*)handler->Next())); break;
            case LONG_TYPE:    printf("%ld - %ld\n",   i++,  *((int64_t*)handler->Next())); break;  
            case FLOAT_TYPE:   printf("%ld - %.2f\n",  i++,  *((float*)handler->Next()));   break;  
            case DOUBLE_TYPE:  printf("%ld - %.2lf\n", i++,  *((double*)handler->Next()));  break;  
        }
    }
    printf("-------------------------------------------\n");
}

#endif /* STORAGE_MANAGER_H */

