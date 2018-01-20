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
#include <cassert>
#include <omp.h>
#include "dml_operators.h"
#include "default_engine.h"
#include "aggregate.h"
#include "dimjoin.h"
#include "viz.h"

#define ERROR_MSG(F, O) "Error during "+std::string(F)+" execution in "+std::string(O)+" operator. Check the log file for more info."

int scan(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);
        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        
        while(true)
        {
            auto subtar = generator->GetSubtar(subtarIndex);
            if(subtar == NULL) break;
            generator->TestAndDisposeSubtar(subtarIndex);
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what()+string("\n")+error);
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int select(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);

        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
        
        while(true)
        {
            auto subtar = generator->GetSubtar(subtarIndex);
            if(subtar == NULL) break;
            
            SubtarPtr newSubtar =  SubtarPtr(new Subtar);
                    
            if(outputTAR->GetDimensions().size() == 1 && !outputTAR->GetDimensions().front()->name.compare("i"))
            {
                for(auto entry : subtar->GetDataSets())
                {
                    if(outputTAR->GetDataElement(entry.first) != NULL)
                        newSubtar->AddDataSet(entry.first, entry.second);
                }

                int64_t offset = 0;
                
                if(subtarIndex > 0)
                    offset = outputGenerator->GetSubtarsIndexMap(subtarIndex-1);

                int64_t lowerBoundinI = offset;
                int64_t upperBoundinI = offset + subtar->GetTotalLength()-1;
                outputGenerator->SetSubtarsIndexMap(subtarIndex, upperBoundinI+1);
               
                int64_t skew = upperBoundinI-lowerBoundinI+1;

                DimSpecPtr newDimSpecs =  DimSpecPtr(new DimensionSpecification);
                newDimSpecs->lower_bound = lowerBoundinI;
                newDimSpecs->upper_bound = upperBoundinI;
                newDimSpecs->type = ORDERED;
                newDimSpecs->skew = skew;
                newDimSpecs->adjacency = 1;
                newDimSpecs->dimension = outputTAR->GetDataElement(DEFAULT_SYNTHETIC_DIMENSION);
                newSubtar->AddDimensionsSpecification(newDimSpecs);

                int64_t totalLength = subtar->GetTotalLength();

                for(auto entry : subtar->GetDimSpecs())
                {
                    if(outputTAR->GetDataElement(entry.first) != NULL)
                    {
                        auto dimSpec = entry.second;
                        DatasetPtr dataset;
                        if(storageManager->MaterializeDim(dimSpec, totalLength, dataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "SELECT"));
                        newSubtar->AddDataSet(dimSpec->dimension->GetName(), dataset);
                    }
                }
            }
            else
            {
                for(auto entry : subtar->GetDataSets())
                {
                    if(outputTAR->GetDataElement(entry.first) != NULL)
                        newSubtar->AddDataSet(entry.first, entry.second);
                }

                for(auto entry : subtar->GetDimSpecs())
                {
                    newSubtar->AddDimensionsSpecification(entry.second);
                }
            }
        
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            generator->TestAndDisposeSubtar(subtarIndex);
            subtarIndex++;
            
            if(iteratorModeEnabled) break;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what()+string("\n")+error);
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int filter(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        auto numThreads = configurationManager->GetIntValue(MAX_THREADS);
        auto workPerThread = configurationManager->GetIntValue(WORK_PER_THREAD);
        
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        auto params = operation->GetParameters();
        ParameterPtr filterParam = params.back();
        
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        auto filterGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[filterParam->tar->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
        
        while(true)
        {
            SubtarPtr newSubtar = SubtarPtr(new Subtar);
            DatasetPtr newDataset; DatasetPtr filterDs;
            int32_t currentSubtar = subtarIndex; SubtarPtr filterSubtar;
                 
            if(outputGenerator->GetSubtarsIndexMap(currentSubtar-1) != -1)
            {
                currentSubtar = outputGenerator->GetSubtarsIndexMap(currentSubtar-1)+1;
            }
            
            while(true)
            {
                filterSubtar = filterGenerator->GetSubtar(currentSubtar);
                if(filterSubtar == NULL) break;
                
                filterDs = filterSubtar->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
                if(filterDs->bitMask->any_parallel(numThreads, workPerThread)) break;
                
                filterGenerator->TestAndDisposeSubtar(currentSubtar);
                currentSubtar++;
            }
            
            auto subtar = generator->GetSubtar(currentSubtar);
            if(subtar == NULL) break;
            int64_t totalLength = subtar->GetTotalLength();
            newSubtar->SetTAR(outputTAR);

            if(!filterDs->bitMask->all_parallel(numThreads, workPerThread)) 
            {    
                for(auto entry : subtar->GetDimSpecs())
                {
                    
                    DatasetPtr matDim, realDim;
                    DimSpecPtr newDimSpec = DimSpecPtr(new DimensionSpecification());
                    newDimSpec->lower_bound = entry.second->lower_bound;
                    newDimSpec->upper_bound = entry.second->upper_bound;
                    newDimSpec->adjacency = entry.second->adjacency;
                    newDimSpec->skew = entry.second->skew;
                    newDimSpec->dimension = entry.second->dimension;
                    newDimSpec->type = TOTAL;
                   
                    if(storageManager->PartiatMaterializeDim(filterDs, entry.second, totalLength, matDim, realDim) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("PartiatMaterializeDim", "FILTER"));
                    
                   
                    
                    if(newDimSpec->dimension->GetDimension()->dimension_type == EXPLICIT)
                    {
                        newDimSpec->dataset = realDim;
                        newDimSpec->materialized = matDim;
                    }
                    else
                    {
                        newDimSpec->dataset = matDim;
                    }

                    newSubtar->AddDimensionsSpecification(newDimSpec);    
                }

                for(auto entry : subtar->GetDataSets())
                {
                    DatasetPtr dataset;
                    if(storageManager->Filter(entry.second, filterDs, dataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Filter", "FILTER"));
                        
                    newSubtar->AddDataSet(entry.first, dataset);
                }
            }
            else
            {
                for(auto entry : subtar->GetDataSets())
                {
                    if(outputTAR->GetDataElement(entry.first) != NULL)
                        newSubtar->AddDataSet(entry.first, entry.second);
                }

                for(auto entry : subtar->GetDimSpecs())
                {
                    newSubtar->AddDimensionsSpecification(entry.second);
                }
            }
                  
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            generator->TestAndDisposeSubtar(currentSubtar);
            filterGenerator->TestAndDisposeSubtar(currentSubtar);
            outputGenerator->SetSubtarsIndexMap(subtarIndex, currentSubtar);
            
            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what()+string("\n")+error);
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int subset(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    #define IN_RANGE(X, Y, Z)  ((X>=Y && X<=Z)?1:0)
    #define MAX(X, Y)  ((X>Y)?(X):(Y))
    #define MIN(X, Y)  ((X<Y)?(X):(Y))

    enum IntersectionType {NONE, PARTIAL, TOTAL};
    auto numThreads = configurationManager->GetIntValue(MAX_THREADS);
    auto workPerThread = configurationManager->GetIntValue(WORK_PER_THREAD);
        
    try
    {
        IntersectionType currentSubtarIntersection = NONE;
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
        
        std::unordered_map<string, string> filteredDim; int paramCount = 0;
        while(true)
        {
            auto param = operation->GetParametersByName(DIM(paramCount++));
            if(!param) break;
            filteredDim[param->literal_str] = param->literal_str;
        }
        
        while(true)
        {
            SubtarPtr subtar, newSubtar = SubtarPtr(new Subtar);
            int32_t currentSubtar = subtarIndex; 
            
            if(outputGenerator->GetSubtarsIndexMap(currentSubtar-1) != -1)
            {
                currentSubtar = outputGenerator->GetSubtarsIndexMap(currentSubtar-1)+1;
            }
            
            while(true)
            {
                subtar = generator->GetSubtar(currentSubtar);
                if(subtar == NULL) return SAVIME_SUCCESS;
                currentSubtarIntersection = TOTAL;
                        
                for(auto entry : subtar->GetDimSpecs())
                {
                    double specsLowerBound, specsUpperBound;
                    auto dimName = entry.first;
                    auto specs = entry.second;
                    auto dim = outputTAR->GetDataElement(dimName)->GetDimension();
                    auto originalDim = inputTAR->GetDataElement(dimName)->GetDimension();
                    
                    if(filteredDim.find(dimName) == filteredDim.end()) continue;
                    
                    auto lowerLogicalIndex = storageManager->Real2Logical(originalDim, specs->lower_bound);
                    auto upperLogicalIndex = storageManager->Real2Logical(originalDim, specs->upper_bound);
                    GET_LOGICAL_INDEX(specsLowerBound, lowerLogicalIndex, double);
                    GET_LOGICAL_INDEX(specsUpperBound, upperLogicalIndex, double); 

                    bool lbInRange = IN_RANGE(specsLowerBound, dim->lower_bound, dim->upper_bound);
                    bool upInRange = IN_RANGE(specsUpperBound, dim->lower_bound, dim->upper_bound);
                    bool dimlbInRange = IN_RANGE(dim->lower_bound, specsLowerBound, specsUpperBound);
                    bool dimupInRange = IN_RANGE(dim->upper_bound, specsLowerBound, specsUpperBound);

                    if(lbInRange && upInRange)
                    {
                        continue;
                    }
                    else if(lbInRange || upInRange || dimlbInRange || dimupInRange)
                    {
                        currentSubtarIntersection = PARTIAL;
                    }
                    else
                    {
                        currentSubtarIntersection = NONE;
                        break;
                    }

                }
                
                if(currentSubtarIntersection == NONE) 
                {
                    generator->TestAndDisposeSubtar(currentSubtar);
                    currentSubtar++;
                }
                else
                {
                    int64_t totalLength = subtar->GetTotalLength();
                    newSubtar->SetTAR(outputTAR);


                    if(currentSubtarIntersection == PARTIAL)
                    {    
                        DatasetPtr filter, comparisonResult, matDim, realDim;
                        vector<DimSpecPtr> dimensionsSpecs;

                        for(auto entry : subtar->GetDimSpecs())
                        {
                            LogicalIndex logLower, logUpper;
                            int64_t originalRealLowerBound, originalRealUpperBound;
                            double subsetLowerBound, subsetUpperBound;
                            auto dimName = entry.first;
                            auto specs = entry.second;
                            auto dim = outputTAR->GetDataElement(dimName)->GetDimension();
                            auto originalDim = inputTAR->GetDataElement(dimName)->GetDimension();

                            _SET_LOGICAL_INDEX(logLower, originalDim->type, dim->lower_bound);
                            _SET_LOGICAL_INDEX(logUpper, originalDim->type, dim->upper_bound); 
                            originalRealLowerBound = storageManager->Logical2Real(originalDim, logLower);
                            originalRealUpperBound = storageManager->Logical2Real(originalDim, logUpper);

                            int64_t offset = originalRealLowerBound- originalDim->real_lower_bound;
                            subsetLowerBound = dim->lower_bound;
                            subsetUpperBound = dim->upper_bound;

                            DimSpecPtr newDimSpec = DimSpecPtr(new DimensionSpecification());
                            newDimSpec->lower_bound = MAX(specs->lower_bound, originalRealLowerBound)-offset;
                            newDimSpec->upper_bound = MIN(specs->upper_bound, originalRealUpperBound)-offset;
                            newDimSpec->adjacency = specs->adjacency;
                            newDimSpec->skew = specs->skew;
                            newDimSpec->dimension = outputTAR->GetDataElement(dimName);
                            newDimSpec->type = specs->type;
                            newDimSpec->dataset = specs->dataset;

                            if(newDimSpec->lower_bound+offset != specs->lower_bound)
                            {
                                if(storageManager->ComparisonDim(string(">="), specs, totalLength, subsetLowerBound, comparisonResult) != SAVIME_SUCCESS)
                                    throw std::runtime_error(ERROR_MSG("ComparisonDim", "SUBSET"));

                                if(filter != NULL)
                                {
                                    if(storageManager->And(filter, comparisonResult, filter) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                                }
                                else
                                {
                                    filter = comparisonResult;
                                }
                            }

                            if(newDimSpec->upper_bound+offset != specs->upper_bound)
                            {
                                if(storageManager->ComparisonDim("<=", specs, totalLength, subsetUpperBound, comparisonResult) != SAVIME_SUCCESS)
                                    throw std::runtime_error(ERROR_MSG("ComparisonDim", "SUBSET"));

                                if(filter != NULL)
                                {
                                    if(storageManager->And(filter, comparisonResult, filter) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                                }
                                else
                                {
                                    filter = comparisonResult;
                                }
                            }

                            dimensionsSpecs.push_back(newDimSpec);
                            newSubtar->AddDimensionsSpecification(newDimSpec);   
                        }
                        
                        if(!filter->bitMask->any_parallel(numThreads, workPerThread))
                        {
                            generator->TestAndDisposeSubtar(currentSubtar);
                            currentSubtar++;
                            continue;
                        }
                        
                        for(auto entry : subtar->GetDimSpecs())
                        {
                            LogicalIndex logLower, logUpper;
                            int64_t originalRealLowerBound, originalRealUpperBound;
                            auto dimName = entry.first;
                            auto specs = entry.second;
                            auto dim = outputTAR->GetDataElement(dimName)->GetDimension();
                            auto originalDim = inputTAR->GetDataElement(dimName)->GetDimension();
                            auto newDimSpec = newSubtar->GetDimensionSpecificationFor(dimName);

                            _SET_LOGICAL_INDEX(logLower, originalDim->type, dim->lower_bound);
                            _SET_LOGICAL_INDEX(logUpper, originalDim->type, dim->upper_bound); 
                            originalRealLowerBound = storageManager->Logical2ApproxReal(originalDim, logLower);
                            originalRealUpperBound = storageManager->Logical2ApproxReal(originalDim, logUpper);

                            if(newDimSpec->type == PARTIAL)
                            {
                                DatasetPtr auxDs1, auxDs2, fitered, filterResult;
                                
                                if(originalDim->dimension_type == IMPLICIT)
                                {
                                    if(storageManager->Comparison(">=", newDimSpec->dataset, dim->lower_bound, auxDs1) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));
                                    
                                    if(storageManager->Comparison("<=", newDimSpec->dataset, dim->upper_bound, auxDs2)!= SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));
                                    
                                    if(storageManager->And(auxDs1, auxDs2, fitered) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                                }
                                else
                                {
                                    DatasetPtr logical;
                                    
                                    if(storageManager->Real2Logical(originalDim, specs, newDimSpec->dataset, logical) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("Real2Logical", "SUBSET"));
                                    
                                    if(storageManager->Comparison(">=", logical, dim->lower_bound, auxDs1) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));
                                    
                                    if(storageManager->Comparison("<=", logical, dim->upper_bound, auxDs2)!= SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));
                                    
                                    if(storageManager->And(auxDs1, auxDs2, fitered) != SAVIME_SUCCESS)
                                        throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                                }

                                if(storageManager->Filter(newDimSpec->dataset, fitered, filterResult)!= SAVIME_SUCCESS)
                                    throw std::runtime_error(ERROR_MSG("Filter", "SUBSET"));

                                newDimSpec->dataset = filterResult;
                            }
                            else if(newDimSpec->type == TOTAL)
                            {
                                if(storageManager->PartiatMaterializeDim(filter, specs, totalLength, matDim, realDim) != SAVIME_SUCCESS)
                                    throw std::runtime_error(ERROR_MSG("PartiatMaterializeDim", "SUBSET"));

                                newDimSpec->dataset = matDim;
                            }
                        }

                        for(auto entry : subtar->GetDataSets())
                        {
                            DatasetPtr dataset;
                            if(storageManager->Filter(entry.second, filter, dataset) != SAVIME_SUCCESS)
                                throw std::runtime_error(ERROR_MSG("Filter", "SUBSET"));

                            newSubtar->AddDataSet(entry.first, dataset);
                        }

                        std::sort(dimensionsSpecs.begin(), dimensionsSpecs.end(), compareAdj);
                        for(DimSpecPtr spec : dimensionsSpecs)
                        {
                            bool isPosterior = false;
                            spec->skew = 1;
                            spec->adjacency = 1;

                            for(DimSpecPtr innerSpec : dimensionsSpecs)
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

                    }
                    //if totally within the range
                    else
                    {
                        for(auto entry : subtar->GetDataSets())
                        {
                            if(outputTAR->GetDataElement(entry.first) != NULL)
                                newSubtar->AddDataSet(entry.first, entry.second);
                        }

                        for(auto entry : subtar->GetDimSpecs())
                        {
                            newSubtar->AddDimensionsSpecification(entry.second);
                        }
                    }
                    
                    break;
                }
            }
            
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            generator->TestAndDisposeSubtar(currentSubtar);
            outputGenerator->SetSubtarsIndexMap(subtarIndex, currentSubtar);
            
            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int logical(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        ParameterPtr logicalOperation = operation->GetParametersByName(OP);
        auto params = operation->GetParameters();
        ParameterPtr operand1=NULL, operand2=NULL;
        
        if(params.size() == 4)
        {
            operand2 = params.back();
            params.pop_back();
            operand1 = params.back();
        }
        else
        {
            operand1 = operation->GetParameters().back();
        }
            
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        //Obtaining subtar generator
        TARGeneratorPtr generator, generatorOp1=NULL, generatorOp2=NULL;
        generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        generatorOp1 = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[operand1->tar->GetName()];
        if(operand2 != NULL)
            generatorOp2 = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[operand2->tar->GetName()];
        
        while(true)
        {
            SubtarPtr subtar, subtarOp1, subtarOp2;
            
            subtar = generator->GetSubtar(subtarIndex);
            subtarOp1 = generatorOp1->GetSubtar(subtarIndex);
            
            if(generatorOp2 != NULL)
                subtarOp2 = generatorOp2->GetSubtar(subtarIndex);
            
            if(subtar == NULL) break;
            SubtarPtr newSubtar = SubtarPtr(new Subtar);
            
            newSubtar->SetTAR(outputTAR);
            
            int64_t totalLength = subtar->GetTotalLength();
            
            for(auto entry : subtar->GetDimSpecs())
            {
                newSubtar->AddDimensionsSpecification(entry.second);
            }
            
            for(auto entry : subtar->GetDataSets())
            {
                newSubtar->AddDataSet(entry.first, entry.second);
            }
            
            DatasetPtr filterDataset; 
            if(!logicalOperation->literal_str.compare("and"))
            {
                auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
                auto dsOp2 = subtarOp2->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
                
                if(storageManager->And(dsOp1, dsOp2, filterDataset) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("And", "LOGICAL"));
            }
            else if(!logicalOperation->literal_str.compare("or"))
            {
                auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
                auto dsOp2 = subtarOp2->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
                
                if(storageManager->Or(dsOp1, dsOp2, filterDataset) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Or", "LOGICAL"));
            }
            else if(!logicalOperation->literal_str.compare("not"))
            {
                auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
                
                if(storageManager->Not(dsOp1, filterDataset) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Not", "LOGICAL"));
            }
            
            newSubtar->AddDataSet(DEFAULT_MASK_ATTRIBUTE, filterDataset);
            
            auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            generator->TestAndDisposeSubtar(subtarIndex);
            generatorOp1->TestAndDisposeSubtar(subtarIndex);
            
            if(generatorOp2 != NULL)
                generatorOp2->TestAndDisposeSubtar(subtarIndex);
            
            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;    
}

int comparison(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        ParameterPtr comparisonOperation = operation->GetParametersByName(OP);
        auto params = operation->GetParameters();
        
        ParameterPtr operand2 = params.back();
        params.pop_back();
        ParameterPtr operand1 = params.back();
        
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        
        while(true)
        {
            auto subtar = generator->GetSubtar(subtarIndex);
            if(subtar == NULL) break;
            
            SubtarPtr newSubtar = SubtarPtr(new Subtar);
            DatasetPtr newDataset;
            
            newSubtar->SetTAR(outputTAR);
            
            int64_t totalLength = subtar->GetTotalLength();
            
            for(auto entry : subtar->GetDimSpecs())
            {
                newSubtar->AddDimensionsSpecification(entry.second);
            }
            
            for(auto entry : subtar->GetDataSets())
            {
                newSubtar->AddDataSet(entry.first, entry.second);
            }
            
            DatasetPtr filterDataset;
            if(operand1->type == LITERAL_DOUBLE_PARAM  && operand2->type == LITERAL_DOUBLE_PARAM)
            {
                throw std::runtime_error("Constant comparators not supported.");
            }
            else if(operand1->type == LITERAL_STRING_PARAM && operand2->type == LITERAL_DOUBLE_PARAM)
            {
                if(inputTAR->GetDataElement(operand1->literal_str)->GetType() == DIMENSION_SCHEMA_ELEMENT)
                {
                    auto dimSpecs = subtar->GetDimensionSpecificationFor(operand1->literal_str);
                    if(storageManager->ComparisonDim(comparisonOperation->literal_str, dimSpecs, totalLength,  operand2->literal_dbl, filterDataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
                }
                else
                {
                    auto dataset = subtar->GetDataSetFor(operand1->literal_str);
                    if(storageManager->Comparison(comparisonOperation->literal_str, dataset, operand2->literal_dbl, filterDataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
                }
                
            }
            else if(operand1->type == LITERAL_DOUBLE_PARAM && operand2->type == LITERAL_STRING_PARAM)
            {
                if(inputTAR->GetDataElement(operand2->literal_str)->GetType() == DIMENSION_SCHEMA_ELEMENT)
                {
                    auto dimSpecs = subtar->GetDimensionSpecificationFor(operand2->literal_str);
                    if(storageManager->ComparisonDim(comparisonOperation->literal_str, dimSpecs, totalLength,  operand1->literal_dbl, filterDataset) != SAVIME_SUCCESS) 
                        throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
                }
                else
                {
                    auto dataset = subtar->GetDataSetFor(operand2->literal_str);
                    if(storageManager->Comparison(comparisonOperation->literal_str, dataset, operand1->literal_dbl, filterDataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
                }
            }
            else if(operand1->type == LITERAL_STRING_PARAM && operand2->type == LITERAL_STRING_PARAM)
            {
                DatasetPtr dsOperand1, dsOperand2;
                if(inputTAR->GetDataElement(operand1->literal_str)->GetType() == DIMENSION_SCHEMA_ELEMENT)
                {
                    auto dimSpecs = subtar->GetDimensionSpecificationFor(operand1->literal_str);
                    if(storageManager->MaterializeDim(dimSpecs, totalLength, dsOperand1) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("MaterializeDim", "COMPARISON"));
                }
                else
                {
                    dsOperand1 = subtar->GetDataSetFor(operand1->literal_str);
                }
                
                if(inputTAR->GetDataElement(operand2->literal_str)->GetType() == DIMENSION_SCHEMA_ELEMENT)
                {
                    auto dimSpecs = subtar->GetDimensionSpecificationFor(operand2->literal_str);
                    if(storageManager->MaterializeDim(dimSpecs, totalLength, dsOperand2) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("MaterializeDim", "COMPARISON"));
                }
                else
                {
                    dsOperand2 = subtar->GetDataSetFor(operand2->literal_str);
                }
                
                if(storageManager->Comparison(comparisonOperation->literal_str, dsOperand1, dsOperand2, filterDataset)  != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "COMPARISON"));
            }
                
            filterDataset->bitMask->resize(totalLength);
            newSubtar->AddDataSet(DEFAULT_MASK_ATTRIBUTE, filterDataset);
            
            auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            generator->TestAndDisposeSubtar(subtarIndex);
            
            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;    
}

int arithmetic(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        ParameterPtr newMember = operation->GetParametersByName(NEW_MEMBER);
        ParameterPtr op = operation->GetParametersByName(OP);
        
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        
        while(true)
        {
            auto subtar = generator->GetSubtar(subtarIndex);
         
            
            if(subtar == NULL) break;
            
            SubtarPtr newSubtar = SubtarPtr(new Subtar);
            DatasetPtr newDataset;
            
            newSubtar->SetTAR(outputTAR);
            
            int64_t totalLength = subtar->GetTotalLength();
            
            for(auto entry : subtar->GetDimSpecs())
            {
                newSubtar->AddDimensionsSpecification(entry.second);
            }
            
            for(auto entry : subtar->GetDataSets())
            {
                newSubtar->AddDataSet(entry.first, entry.second);
            }
            
            ParameterPtr operand0 = operation->GetParametersByName(OPERAND(0));
            ParameterPtr operand1 = operation->GetParametersByName(OPERAND(1));
            
            if(operand0->type == LITERAL_DOUBLE_PARAM)
            {
                if(operand1 == NULL)
                {
                    omp_set_num_threads(configurationManager->GetIntValue(MAX_THREADS));
                    auto dataset = storageManager->Create(DOUBLE_TYPE, totalLength);
                    if(dataset == NULL)
                        throw std::runtime_error("Could not create dataset.");
                    
                    auto datasethandler = storageManager->GetHandler(dataset);
                    double * buffer = (double*)datasethandler->GetBuffer();
                    
                    #pragma omp parallel for
                    for(int64_t i = 0; i < totalLength; i++)
                    {
                        buffer[i] = operand0->literal_dbl;
                    }
                    datasethandler->Close();
                    
                    if(storageManager->Aritmethic(op->literal_str, dataset, 0, newDataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Arithmetic", "ARITHMETIC"));
                }
                else if(operand1->type == LITERAL_DOUBLE_PARAM)
                {
                    //omp_set_num_threads(configurationManager->GetIntValue(MAX_THREADS));
                    auto dataset = storageManager->Create(DOUBLE_TYPE, totalLength);
                    if(dataset == NULL)
                        throw std::runtime_error("Could not create dataset.");
                        
                    auto datasethandler = storageManager->GetHandler(dataset);
                    double * buffer = (double*)datasethandler->GetBuffer();
                    
                    #pragma omp parallel for
                    for(int64_t i = 0; i < totalLength; i++)
                    {
                        buffer[i] = operand0->literal_dbl;
                    }
                    datasethandler->Close();
                    if(storageManager->Aritmethic(op->literal_str, dataset, operand1->literal_dbl, newDataset)!= SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Arithmetic", "ARITHMETIC"));
                    
                }
                else if(operand1->type == LITERAL_STRING_PARAM)
                {
                    double operand = operand0->literal_dbl;
                    auto dataset = subtar->GetDataSetFor(operand1->literal_str);
                    
                    if(!dataset)
                    {
                        auto dimSpecs = subtar->GetDimensionSpecificationFor(operand1->literal_str);
                        if(storageManager->MaterializeDim(dimSpecs, totalLength, dataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
                    }
                    
                    if(operand == ((int32_t)operand))
                    {
                        if(storageManager->Aritmethic(op->literal_str, dataset, (int32_t)operand, newDataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Arithmetic", "ARITHMETIC"));
                    }
                    else if(operand == ((int64_t)operand))
                    {
                        if(storageManager->Aritmethic(op->literal_str, dataset, (int64_t)operand, newDataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Arithmetic", "ARITHMETIC"));
                    }
                    else
                    {
                        if(storageManager->Aritmethic(op->literal_str, dataset, operand, newDataset) != SAVIME_SUCCESS);
                            throw std::runtime_error(ERROR_MSG("AritHmetic", "ARITHMETIC"));
                    }
                }
            }
            
            if(operand0->type == LITERAL_STRING_PARAM)
            {
                if(operand1 == NULL)
                {
                    double operand = 0;
                    auto dataset = subtar->GetDataSetFor(operand0->literal_str);
                    
                    if(!dataset)
                    {
                        auto dimSpecs = subtar->GetDimensionSpecificationFor(operand0->literal_str);
                        if(storageManager->MaterializeDim(dimSpecs, totalLength, dataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
                    }
                    
                    if(storageManager->Aritmethic(op->literal_str, dataset, operand, newDataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("AritHmetic", "ARITHMETIC"));
                }
                else if(operand1->type == LITERAL_DOUBLE_PARAM)
                {
                    auto dataset = subtar->GetDataSetFor(operand0->literal_str);
                    
                    if(!dataset)
                    {
                        auto dimSpecs = subtar->GetDimensionSpecificationFor(operand0->literal_str);
                        if(storageManager->MaterializeDim(dimSpecs, totalLength, dataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
                    }
                    
                    double operand = operand1->literal_dbl;
                    
                    if(operand == ((int32_t)operand))
                    {
                        if(storageManager->Aritmethic(op->literal_str, dataset, (int32_t)operand, newDataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("AritHmetic", "ARITHMETIC"));
                    }
                    else if(operand == ((int64_t)operand))
                    {
                        if(storageManager->Aritmethic(op->literal_str, dataset, (int64_t)operand, newDataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("AritHmetic", "ARITHMETIC"));
                    }
                    else
                    {
                        if(storageManager->Aritmethic(op->literal_str, dataset, operand, newDataset) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("AritHmetic", "ARITHMETIC"));
                    }
                    
                }
                else if(operand1->type == LITERAL_STRING_PARAM)
                {
                    auto dataset0 = subtar->GetDataSetFor(operand0->literal_str);
                    if(!dataset0)
                    {
                        auto dimSpecs = subtar->GetDimensionSpecificationFor(operand0->literal_str);
                        if(storageManager->MaterializeDim(dimSpecs, totalLength, dataset0) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
                    }
                    
                    auto dataset1 = subtar->GetDataSetFor(operand1->literal_str);
                    if(!dataset1)
                    {
                        auto dimSpecs = subtar->GetDimensionSpecificationFor(operand1->literal_str);
                        if(storageManager->MaterializeDim(dimSpecs, totalLength, dataset1) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
                    }
                    
                    if(storageManager->Aritmethic(op->literal_str, dataset0, dataset1, newDataset) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
                }
            }
            
            newSubtar->AddDataSet(newMember->literal_str, newDataset);
            auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            generator->TestAndDisposeSubtar(subtarIndex);
            
            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int cross_join(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {      
        auto leftTAR = operation->GetParametersByName(OPERAND(0))->tar;
        auto rightTAR = operation->GetParametersByName(OPERAND(1))->tar;
        assert(leftTAR != NULL && rightTAR != NULL);
        TARPtr outputTAR = operation->GetResultingTAR();
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        bool freeBufferedSubtars = configurationManager->GetBooleanValue(FREE_BUFFERED_SUBTARS);
        
        //Obtaining subtar generator
        auto leftGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[leftTAR->GetName()];
        auto rightGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[rightTAR->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];

        while(true)
        {
            SubtarPtr newSubtar = SubtarPtr(new Subtar);
            int32_t leftSubtarIndex = 0; int32_t rightSubtarIndex = 0; 
            SubtarPtr rightSubtar, leftSubtar;

            rightSubtarIndex = outputGenerator->GetSubtarsIndexMap(1, subtarIndex-1)+1;
            rightSubtar = rightGenerator->GetSubtar(rightSubtarIndex);            

            if(rightSubtar == NULL)
            {
                leftSubtarIndex = outputGenerator->GetSubtarsIndexMap(0, subtarIndex-1)+1;
                leftGenerator->TestAndDisposeSubtar(leftSubtarIndex-1);
                rightSubtarIndex = 0;
                rightSubtar = rightGenerator->GetSubtar(rightSubtarIndex);
            }
            else if(subtarIndex > 0)
            {
                leftSubtarIndex = outputGenerator->GetSubtarsIndexMap(0, subtarIndex-1);
            }
            else
            {
                leftSubtarIndex = 0;
            }

            leftSubtar = leftGenerator->GetSubtar(leftSubtarIndex);
            if(leftSubtar == NULL)
            {
                int32_t lastSubtar = outputGenerator->GetSubtarsIndexMap(1, subtarIndex-2);
                if(!freeBufferedSubtars)
                {
                    for(int32_t i = 0; i <= lastSubtar; i++)
                    {
                        rightGenerator->TestAndDisposeSubtar(i);
                    }
                }
                break;
            }
            
            int64_t leftSubtarLen = leftSubtar->GetTotalLength();
            int64_t rightSubtarLen = rightSubtar->GetTotalLength();
             
            for(auto entry : leftSubtar->GetDimSpecs())
            {
                string dimName = entry.first;
                DimSpecPtr dimspec = entry.second;
                DimSpecPtr newDimspec = DimSpecPtr(new DimensionSpecification());
                
                newDimspec->dimension = outputTAR->GetDataElement(dimName);
                newDimspec->lower_bound = dimspec->lower_bound;
                newDimspec->upper_bound = dimspec->upper_bound;
                newDimspec->type = dimspec->type;
                newDimspec->dataset = dimspec->dataset;
                
                if(newDimspec->type == ORDERED || newDimspec->type == PARTIAL)
                {
                    newDimspec->skew = dimspec->skew*rightSubtarLen;
                    newDimspec->adjacency = dimspec->adjacency*rightSubtarLen;
                }
                else
                {
                    DatasetPtr matDim;
                    if(storageManager->Stretch(dimspec->dataset, leftSubtarLen, rightSubtarLen, 1, matDim) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));
                     
                    newDimspec->dataset = matDim;
                }
                
                newSubtar->AddDimensionsSpecification(newDimspec);
            }
            
            for(auto entry : rightSubtar->GetDimSpecs())
            {
                string dimName = RIGHT_DATAELEMENT_PREFIX+entry.first;
                DimSpecPtr dimspec = entry.second;
                DimSpecPtr newDimspec = DimSpecPtr(new DimensionSpecification());
                
                newDimspec->dimension = outputTAR->GetDataElement(dimName);
                newDimspec->lower_bound = dimspec->lower_bound;
                newDimspec->upper_bound = dimspec->upper_bound;
                newDimspec->type = dimspec->type;
                newDimspec->skew = dimspec->skew;
                newDimspec->adjacency = dimspec->adjacency;
                newDimspec->dataset = dimspec->dataset;
                
                if(newDimspec->type == TOTAL)
                {
                    DatasetPtr matDim;
                    if(storageManager->Stretch(dimspec->dataset, rightSubtarLen, 1, leftSubtarLen, matDim) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));
                    newDimspec->dataset = matDim;
                }
                
                newSubtar->AddDimensionsSpecification(newDimspec);
            }
            
            
            for(auto entry : leftSubtar->GetDataSets())
            {
                string attName = entry.first;
                DatasetPtr ds = entry.second;
                DatasetPtr joinedDs;
                
                if(storageManager->Stretch(ds, leftSubtarLen, rightSubtarLen, 1, joinedDs) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));
                
                newSubtar->AddDataSet(attName, joinedDs);
            }
            
            for(auto entry : rightSubtar->GetDataSets())
            {
                string attName = RIGHT_DATAELEMENT_PREFIX+entry.first;
                DatasetPtr ds = entry.second;
                DatasetPtr joinedDs;
                
                if(storageManager->Stretch(ds, rightSubtarLen, 1, leftSubtarLen, joinedDs) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));
                    
                newSubtar->AddDataSet(attName, joinedDs);
            }
            
            if(freeBufferedSubtars)
            {
                 rightGenerator->TestAndDisposeSubtar(rightSubtarIndex);
            }
            
            outputGenerator->AddSubtar(subtarIndex, newSubtar);
            outputGenerator->SetSubtarsIndexMap(0, subtarIndex, leftSubtarIndex);
            outputGenerator->SetSubtarsIndexMap(1, subtarIndex, rightSubtarIndex);

            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int equijoin(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    return SAVIME_FAILURE;
}

int dimjoin(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, std::shared_ptr<QueryDataManager>queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr  storageManager, EnginePtr engine)
{
    try
    {
        auto numThreads = configurationManager->GetIntValue(MAX_THREADS);
        auto workPerThread = configurationManager->GetIntValue(WORK_PER_THREAD);
        
        auto leftTAR = operation->GetParametersByName(OPERAND(0))->tar;
        auto rightTAR = operation->GetParametersByName(OPERAND(1))->tar;
        assert(leftTAR != NULL && rightTAR != NULL);
        TARPtr outputTAR = operation->GetResultingTAR();
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        bool freeBufferedSubtars = configurationManager->GetBooleanValue(FREE_BUFFERED_SUBTARS);
  
        //Creating dimensions mapping
        map<string, string> leftDims, rightDims; int32_t count = 0;
        map<string, JoinedRangePtr> ranges;
                
        while(true)
        {
            auto param1 = operation->GetParametersByName(DIM(count++));
            auto param2 = operation->GetParametersByName(DIM(count++));
            if(param1 == NULL) break;
            leftDims[param1->literal_str] = param2->literal_str;
            rightDims[param2->literal_str] = param1->literal_str;
        }
        
        //Obtaining subtar generator
        auto leftGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[leftTAR->GetName()];
        auto rightGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[rightTAR->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];

        while(true)
        {
            SubtarPtr newSubtar = SubtarPtr(new Subtar);
            int64_t leftSubtarLen, rightSubtarLen;
            int32_t leftSubtarIndex = 0; int32_t rightSubtarIndex = 0; DatasetPtr filterDs = NULL; 
            SubtarPtr rightSubtar, leftSubtar; int32_t currentSubtar = subtarIndex;
            bool hasTotalDims = false, hasPartialJoinDims = false, finalSubtar = false; 
            ranges.clear();
            
            subtarIndex = outputGenerator->GetSubtarsIndexMap(2, subtarIndex-1)+1;
            
            //check intersection 
            while(true)        
            {
                rightSubtarIndex = outputGenerator->GetSubtarsIndexMap(1, subtarIndex-1)+1;
                rightSubtar = rightGenerator->GetSubtar(rightSubtarIndex);            

                if(rightSubtar == NULL)
                {
                    leftSubtarIndex = outputGenerator->GetSubtarsIndexMap(0, subtarIndex-1)+1;
                    leftGenerator->TestAndDisposeSubtar(leftSubtarIndex-1);
                    rightSubtarIndex = 0;
                    rightSubtar = rightGenerator->GetSubtar(rightSubtarIndex);
                }
                else if(subtarIndex > 0)
                {
                    leftSubtarIndex = outputGenerator->GetSubtarsIndexMap(0, subtarIndex-1);
                }
                else
                {
                    leftSubtarIndex = 0;
                }

                leftSubtar = leftGenerator->GetSubtar(leftSubtarIndex);
                if(leftSubtar == NULL)
                {
                    int32_t lastSubtar = outputGenerator->GetSubtarsIndexMap(1, subtarIndex-2);
                    if(!freeBufferedSubtars)
                    {
                        for(int32_t i = 0; i <= lastSubtar; i++)
                        {
                            rightGenerator->TestAndDisposeSubtar(i);
                        }
                    }
                    finalSubtar = true;
                    break;
                }

                outputGenerator->SetSubtarsIndexMap(0, subtarIndex, leftSubtarIndex);
                outputGenerator->SetSubtarsIndexMap(1, subtarIndex, rightSubtarIndex);
                
                if(!checkIntersection(outputTAR, leftSubtar, rightSubtar, leftDims, ranges, storageManager))
                {
                    subtarIndex++;
                }
                else
                {
                    leftSubtarLen = leftSubtar->GetTotalLength();
                    rightSubtarLen = rightSubtar->GetTotalLength();


                    for(auto entry: leftDims)
                    {
                        DatasetPtr leftDimDs, rightDimDs;
                        DatasetPtr strechedLeftDimDs, strechedRightDimDs, resultDs;

                        auto leftDimSpecs =leftSubtar->GetDimensionSpecificationFor(entry.first);
                        auto rightDimSpecs = rightSubtar->GetDimensionSpecificationFor(entry.second);

                        if(storageManager->MaterializeDim(leftDimSpecs, leftSubtarLen, leftDimDs) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "DIMJOIN"));
                        
                        if(storageManager->MaterializeDim(rightDimSpecs, rightSubtarLen, rightDimDs) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("MaterializeDim", "DIMJOIN"));
                            
                        if(storageManager->Stretch(leftDimDs, leftSubtarLen, rightSubtarLen, 1, strechedLeftDimDs) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Stretch", "DIMJOIN"));
                            
                        if(storageManager->Stretch(rightDimDs, rightSubtarLen, 1, leftSubtarLen, strechedRightDimDs) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Stretch", "DIMJOIN"));
                            
                        if(storageManager->Comparison("=", strechedLeftDimDs, strechedRightDimDs, resultDs) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Comparison", "DIMJOIN"));
                        
                        if(filterDs != NULL)
                        {
                            if(storageManager->And(filterDs, resultDs, filterDs) != SAVIME_SUCCESS)
                                throw std::runtime_error(ERROR_MSG("And", "DIMJOIN"));
                        }
                        else
                        {
                            filterDs = resultDs;
                        }
                    }
                    
                    if(filterDs->bitMask->any_parallel(numThreads, workPerThread))
                    {
                        break;
                    }
                    else
                    {
                        subtarIndex++;
                    }
                }
            }
            
            if(finalSubtar) break;
            
            for(auto entry : leftSubtar->GetDataSets())
            {
                string attName = LEFT_DATAELEMENT_PREFIX+entry.first;
                DatasetPtr ds = entry.second;
                DatasetPtr joinedDs;
                
                if(storageManager->Stretch(ds, leftSubtarLen, rightSubtarLen, 1, joinedDs) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Stretch", "DIMJOIN"));
                
                if(storageManager->Filter(joinedDs, filterDs, ds) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                
                newSubtar->AddDataSet(attName, ds);
            }
            
            for(auto entry : rightSubtar->GetDataSets())
            {
                string attName = RIGHT_DATAELEMENT_PREFIX+entry.first;
                DatasetPtr ds = entry.second;
                DatasetPtr joinedDs;
                
                if(storageManager->Stretch(ds, rightSubtarLen, 1, leftSubtarLen, joinedDs) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Stretch", "DIMJOIN"));
                    
                if(storageManager->Filter(joinedDs, filterDs, ds) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    
                newSubtar->AddDataSet(attName, ds);
            }
            
            
            list<DimSpecPtr> leftDimSpecs, rightDimSpecs;
            for(auto entry : leftSubtar->GetDimSpecs())
            {
                string dimName = LEFT_DATAELEMENT_PREFIX+entry.first;
                DimSpecPtr dimspec = entry.second;
                DimSpecPtr newDimspec = DimSpecPtr(new DimensionSpecification());
                
                newDimspec->dimension = outputTAR->GetDataElement(dimName);
                newDimspec->type = dimspec->type;
                newDimspec->dataset = dimspec->dataset;
                
                if(leftDims.find(entry.first) != leftDims.end())
                {
                    newDimspec->lower_bound = ranges[dimName]->lower_bound;
                    newDimspec->upper_bound = ranges[dimName]->upper_bound;
                }
                else
                {
                    newDimspec->lower_bound = dimspec->lower_bound;
                    newDimspec->upper_bound = dimspec->upper_bound;
                }
                
                hasPartialJoinDims = hasPartialJoinDims || (dimspec->type == PARTIAL);
                hasTotalDims = hasTotalDims || (newDimspec->type == TOTAL);
                //newSubtar->AddDimensionsSpecification(newDimspec);
                leftDimSpecs.push_back(newDimspec);
            }
            
            for(auto entry : rightSubtar->GetDimSpecs())
            {
                string dimName = RIGHT_DATAELEMENT_PREFIX+entry.first;
                DimSpecPtr dimspec = entry.second;
                DimSpecPtr newDimspec = DimSpecPtr(new DimensionSpecification());
                
                hasPartialJoinDims = hasPartialJoinDims || (dimspec->type == PARTIAL);
                hasTotalDims = hasTotalDims || (dimspec->type == TOTAL);
                
                if(rightDims.find(entry.first) == rightDims.end())
                {
                    newDimspec->dimension = outputTAR->GetDataElement(dimName);
                    newDimspec->lower_bound = dimspec->lower_bound;
                    newDimspec->upper_bound = dimspec->upper_bound;
                    newDimspec->type = dimspec->type;
                    newDimspec->skew = dimspec->skew;
                    newDimspec->adjacency = dimspec->adjacency;
                    newDimspec->dataset = dimspec->dataset;

                    hasTotalDims = hasTotalDims || (dimspec->type == TOTAL);
                    //newSubtar->AddDimensionsSpecification(newDimspec);
                    rightDimSpecs.push_back(newDimspec);
                }
            }

            if(hasTotalDims || hasPartialJoinDims)
            {
                for(DimSpecPtr dimSpecs : leftDimSpecs)
                {
                    string originalDimName = dimSpecs->dimension->GetName().substr(5, string::npos);
                    DimensionType dimType = dimSpecs->dimension->GetDimension()->dimension_type;
                    DatasetPtr ds, strechedDimDs, joinedDs;
                    DimSpecPtr originalDimspecs = leftSubtar->GetDimensionSpecificationFor(originalDimName);
                    
                    if(storageManager->MaterializeDim(originalDimspecs, leftSubtarLen, ds) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    
                    if(storageManager->Stretch(ds, leftSubtarLen, rightSubtarLen, 1, strechedDimDs) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    
                    if(storageManager->Filter(strechedDimDs, filterDs, joinedDs) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    
                    if(dimType == EXPLICIT)
                    {
                        if(storageManager->Logical2Real(dimSpecs->dimension->GetDimension(),
                                                     originalDimspecs,
                                                     joinedDs, joinedDs) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    }
                    
                    dimSpecs->type = TOTAL;
                    dimSpecs->dataset = joinedDs;
                    newSubtar->AddDimensionsSpecification(dimSpecs);
                }
                
                for(DimSpecPtr dimSpecs : rightDimSpecs)
                {
                    string originalDimName = dimSpecs->dimension->GetName().substr(6, string::npos);
                    DimensionType dimType = dimSpecs->dimension->GetDimension()->dimension_type;
                    DatasetPtr ds, strechedDimDs, joinedDs;
                    DimSpecPtr originalDimspecs = rightSubtar->GetDimensionSpecificationFor(originalDimName);
                      
                    if(storageManager->MaterializeDim(originalDimspecs, rightSubtarLen, ds) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("MaterializeDim", "DIMJOIN"));
                    if(storageManager->Stretch(ds, rightSubtarLen, 1, leftSubtarLen, strechedDimDs) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Stretch", "DIMJOIN"));
                    if(storageManager->Filter(strechedDimDs, filterDs, joinedDs) != SAVIME_SUCCESS)
                        throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    
                    if(dimType == EXPLICIT)
                    {
                        if(storageManager->Logical2Real(originalDimspecs->dimension->GetDimension(),
                                                    originalDimspecs,
                                                    joinedDs, ds) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
                    }
                    
                    dimSpecs->type = TOTAL;
                    dimSpecs->dataset = ds;
                    newSubtar->AddDimensionsSpecification(dimSpecs);
                }
                
            }
            else
            {
                
                leftDimSpecs.sort(compareDimSpecsByAdj);
                rightDimSpecs.sort(compareDimSpecsByAdj);
                while(rightDimSpecs.size()) 
                {
                    leftDimSpecs.push_back(rightDimSpecs.front());
                    rightDimSpecs.pop_front();
                }
             
             
                for(DimSpecPtr spec : leftDimSpecs)
                {
                    
                    bool isPosterior = false;
                    spec->skew = 1;
                    spec->adjacency = 1;

                    for(DimSpecPtr innerSpec : leftDimSpecs)
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
                    
                    newSubtar->AddDimensionsSpecification(spec);          
                }
                //leftDimSpecs, rightDimSpecs;
            }
                
            if(freeBufferedSubtars)
            {
                 rightGenerator->TestAndDisposeSubtar(rightSubtarIndex);
            }
            
            outputGenerator->AddSubtar(currentSubtar, newSubtar);
            outputGenerator->SetSubtarsIndexMap(2, currentSubtar, subtarIndex);

            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what()+string("\n")+error);
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;    
}

int slice(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, std::shared_ptr<QueryDataManager>queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr  storageManager, EnginePtr engine)
{
    return SAVIME_FAILURE;
}

int aggregate(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, std::shared_ptr<QueryDataManager>queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr  storageManager, EnginePtr engine)
{
    try
    {
        auto numCores = configurationManager->GetIntValue(MAX_THREADS);
        auto workPerThread = configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t startPositionPerCore[numCores];
        int64_t finalPositionPerCore[numCores];
        
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        auto params = operation->GetParameters();
        
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);
        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
        SubtarPtr newSubtar = SubtarPtr(new Subtar);
        
        //Creating aggregation configuration
        AggregateConfigurationPtr aggConfig = AggregateConfigurationPtr(new AggregateConfiguration());
        
        //Get length of output 
        int32_t dimCount = 0;
        while(true)
        {
           auto param = operation->GetParametersByName(DIM(dimCount++));
           if(param == NULL) break;
           auto dataElement = inputTAR->GetDataElement(param->literal_str);
           aggConfig->_dimensions.push_back(dataElement->GetDimension());
        }
        
        //Get functions
        int32_t opCount = 0;
        while(true)
        {
           auto param1 = operation->GetParametersByName(OPERAND(opCount++));
           if(param1 == NULL) break;
           auto param2 = operation->GetParametersByName(OPERAND(opCount++));
           auto param3 = operation->GetParametersByName(OPERAND(opCount++));
           
           AggregateFunctionPtr func = AggregateFunctionPtr(new AggregateFunction(
                                                                param1->literal_str,
                                                                param2->literal_str,
                                                                param3->literal_str
                                                            ));
           
           aggConfig->_functions.push_back(func);  
        }
        
        aggConfig->Configure();
        int64_t totalLen = aggConfig->GetTotalLength();
        SetWorkloadPerThread(totalLen, workPerThread, startPositionPerCore, finalPositionPerCore, numCores);
        
        for(auto func : aggConfig->_functions)
        {
            DatasetPtr aggregateDs = storageManager->Create(DOUBLE_TYPE, totalLen);
            if(aggregateDs == NULL)
                throw std::runtime_error("Could not create dataset."); 
                    
            DatasetHandlerPtr aggregateHandler = storageManager->GetHandler(aggregateDs);
            
            aggConfig->_datasets[func->attribName] = aggregateDs;
            aggConfig->_handlers[func->attribName] = aggregateHandler;
            double * buffer = (double*) aggregateHandler->GetBuffer();
            double initVal = func->GetStartValue();
            
            #pragma omp parallel
            for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
            {
                buffer[i] = initVal;
            }
            
            if(func->RequiresAuxDataset())
            {
                DatasetPtr aggregateDsAux = storageManager->Create(DOUBLE_TYPE, totalLen);
                if(aggregateDsAux == NULL)
                    throw std::runtime_error("Could not create dataset."); 
                
                DatasetHandlerPtr auxAggregateHandler = storageManager->GetHandler(aggregateDsAux);
                aggConfig->_auxHandlers[func->attribName] = auxAggregateHandler;
                buffer = (double*) aggConfig->_auxHandlers[func->attribName]->GetBuffer();
                 
                #pragma omp parallel
                for(int64_t i = startPositionPerCore[omp_get_thread_num()] ; i < finalPositionPerCore[omp_get_thread_num()] ; ++i)
                {
                    buffer[i] = initVal;
                }
            }
        }
        
        while(true)
        {
            int32_t currentSubtar = subtarIndex;
            auto subtar = generator->GetSubtar(currentSubtar);
            if(subtar == NULL) break;
            int64_t subtarLen = subtar->GetTotalLength();
            
            for(DimensionPtr dim : aggConfig->_dimensions)
            {
                DatasetPtr auxDataset, indexes;
                auto dimSpecs = subtar->GetDimensionSpecificationFor(dim->name);
                
                if(storageManager->MaterializeDim(dimSpecs,subtarLen,auxDataset) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("MaterializeDim", "AGGREGATE"));
                     
                if(storageManager->Logical2Real(dim, dimSpecs, auxDataset, indexes) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Logical2Real", "AGGREGATE"));
                
                aggConfig->_indexesHandlers[dim->name] = storageManager->GetHandler(indexes);
                aggConfig->_indexesHandlersBuffers[dim->name] = (int64_t*) aggConfig->_indexesHandlers[dim->name]->GetBuffer();
            }
            
            aggConfig->_inputHandlers.clear();
            for(auto func : aggConfig->_functions)
            {
                DatasetPtr dataset = subtar->GetDataSetFor(func->paramName);
                
                if(dataset == NULL)
                    storageManager->MaterializeDim(subtar->GetDimensionSpecificationFor(func->paramName),
                                                   subtarLen, dataset);
                                                    
                
                aggConfig->_inputHandlers[func->paramName] = storageManager->GetHandler(dataset);
            }
            
            
            for(auto func : aggConfig->_functions)
            {
                
                auto type = inputTAR->GetDataElement(func->paramName)->GetDataType();
                
                if(type == INTEGER_TYPE)
                {
                    AggregateEngine<int32_t> aggEngine(aggConfig, func, subtarLen, numCores, workPerThread);
                    aggEngine.Run();
                }
                else if (type == LONG_TYPE)
                {
                    AggregateEngine<int64_t> aggEngine(aggConfig, func, subtarLen, numCores, workPerThread);
                    aggEngine.Run();
                }
                else if (type == FLOAT_TYPE)
                {
                    AggregateEngine<float> aggEngine(aggConfig, func, subtarLen, numCores, workPerThread);
                    aggEngine.Run();
                }
                else if(type == DOUBLE_TYPE)
                {
                    AggregateEngine<double> aggEngine(aggConfig, func, subtarLen, numCores, workPerThread);
                    aggEngine.Run();
                }
            }
            
            generator->TestAndDisposeSubtar(currentSubtar);
            subtarIndex++;
        }
        
        for(auto func : aggConfig->_functions)
        {

            auto type = inputTAR->GetDataElement(func->paramName)->GetDataType();

            if(type == INTEGER_TYPE)
            {
                AggregateEngine<int32_t> aggEngine(aggConfig, func, 0, numCores, workPerThread);
                aggEngine.Finalize();
            }
            else if (type == LONG_TYPE)
            {
                AggregateEngine<int64_t> aggEngine(aggConfig, func, 0, numCores, workPerThread);
                aggEngine.Finalize();
            }
            else if (type == FLOAT_TYPE)
            {
                AggregateEngine<float> aggEngine(aggConfig, func, 0, numCores, workPerThread);
                aggEngine.Finalize();
            }
            else if(type == DOUBLE_TYPE)
            {
                AggregateEngine<double> aggEngine(aggConfig, func, 0, numCores, workPerThread);
                aggEngine.Finalize();
            }
        }
        
        for(auto dim : outputTAR->GetDimensions())
        {
            DimSpecPtr newDimSpec = DimSpecPtr(new DimensionSpecification());
            newDimSpec->lower_bound = dim->real_lower_bound;
            newDimSpec->upper_bound = dim->real_upper_bound;
            newDimSpec->dimension = outputTAR->GetDataElement(dim->name);
            newDimSpec->type = ORDERED;
            newDimSpec->skew = aggConfig->_skew[dim->name];
            newDimSpec->adjacency = aggConfig->_adj[dim->name];
            newSubtar->AddDimensionsSpecification(newDimSpec);
        }
            
        for(auto entry : aggConfig->_datasets)
        {
            newSubtar->AddDataSet(entry.first, entry.second);
        }
        
        outputGenerator->AddSubtar(0, newSubtar);
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int split(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, std::shared_ptr<QueryDataManager>queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr  storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);
        
        TARPtr inputTAR = inputTarParam->tar;
        assert(inputTAR != NULL);

        TARPtr outputTAR = operation->GetResultingTAR();
        assert(outputTAR != NULL);
        
        //Checking if iterator mode is enabled
        bool iteratorModeEnabled = configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[outputTAR->GetName()];
        
        while(true)
        {
            SubtarPtr subtar; int32_t lastSubtar = 0, currentSubtar = 0; 
            currentSubtar = outputGenerator->GetSubtarsIndexMap(subtarIndex-1)+1;
           
            subtar = generator->GetSubtar(currentSubtar);
            if(subtar == NULL) return SAVIME_SUCCESS;
            int64_t totalLength = subtar->GetTotalLength();
            
            //outputGenerator->AddSubtar(subtarIndex, newSubtar);
            DimSpecPtr splitDimension = NULL; 
            int64_t greatestAdj = 0; bool hasTotalDimension = false;
            
            for(auto entry : subtar->GetDimSpecs())
            {
                DimSpecPtr dimSpecs = entry.second;
                if(dimSpecs->GetLength() > 1)
                {
                    if(dimSpecs->adjacency > greatestAdj)
                    {
                        splitDimension = dimSpecs;
                        greatestAdj = dimSpecs->adjacency;
                    }
                }
                
                if(dimSpecs->type == TOTAL)
                    hasTotalDimension = true;
            }
            
            if(splitDimension != NULL)
            {
                
                int64_t maxSplitLength = configurationManager->GetLongValue(MAX_SPLIT_LEN);
                
                if(splitDimension->GetLength() > maxSplitLength)
                    throw std::runtime_error("Leftmost dimension is too large for split operation. Consider"
                                             "loading smaller subtars.");
                    
                if(!hasTotalDimension)
                {
                    int64_t subtarsNo = splitDimension->GetLength();
                    vector<SubtarPtr> subtars;
                    map<string, vector<DatasetPtr>> datasets;
                            
                    for(auto entry : subtar->GetDataSets())
                    {
                        vector<DatasetPtr> _datasets;
                        if(storageManager->Split(entry.second, totalLength, subtarsNo, _datasets) != SAVIME_SUCCESS)
                            throw std::runtime_error(ERROR_MSG("split", "SPLIT"));
                        
                        datasets[entry.first] = _datasets;
                    }
                    
                    if(splitDimension->type == ORDERED)
                    {
                        for(int64_t index = splitDimension->lower_bound; 
                                index <= splitDimension->upper_bound; index++)
                        {
                            SubtarPtr newSubtar = SubtarPtr(new Subtar());
                            DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification()); 
                            dimSpecs->adjacency = splitDimension->adjacency;
                            dimSpecs->dataset = splitDimension->dataset;
                            dimSpecs->dimension = outputTAR->GetDataElement(
                                                    splitDimension->dimension->GetName());
                            
                            dimSpecs->lower_bound = index;
                            dimSpecs->upper_bound = index;
                            dimSpecs->skew = splitDimension->skew/dimSpecs->adjacency;
                            dimSpecs->type = splitDimension->type;
                            dimSpecs->materialized = NULL;
                            newSubtar->AddDimensionsSpecification(dimSpecs);
                            
                            for(auto entry : subtar->GetDimSpecs())
                            {
                                if(splitDimension->dimension->GetName() != entry.first)
                                {
                                    DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification()); 
                                    
                                    if(entry.second->GetLength() == 1 && entry.second->adjacency > splitDimension->adjacency)
                                    {
                                        dimSpecs->adjacency = entry.second->adjacency/splitDimension->GetLength();
                                    }
                                    else
                                    {
                                        dimSpecs->adjacency = entry.second->adjacency;
                                    }
                                    
                                    dimSpecs->dataset = entry.second->dataset;
                                    dimSpecs->dimension = outputTAR->GetDataElement(
                                                            entry.second->dimension->GetName());
                                    dimSpecs->lower_bound = entry.second->lower_bound;
                                    dimSpecs->upper_bound = entry.second->upper_bound;
                                    dimSpecs->skew = entry.second->skew;
                                    dimSpecs->type = entry.second->type;
                                    dimSpecs->materialized = NULL;
                                    newSubtar->AddDimensionsSpecification(dimSpecs);
                                }
                            }
                            
                            for(auto entry : subtar->GetDataSets())
                            {
                                DatasetPtr ds = datasets[entry.first][index-splitDimension->lower_bound];
                                newSubtar->AddDataSet(entry.first, ds);
                            }
                            
                            subtars.push_back(newSubtar);
                        }
                    }
                    else
                    {
                        vector<DatasetPtr> _partialDimDatasets;
                        if(storageManager->Split(splitDimension->dataset, totalLength, subtarsNo, _partialDimDatasets) != SAVIME_SUCCESS)
                             throw std::runtime_error(ERROR_MSG("split", "SPLIT"));
                             
                        for(int64_t index = splitDimension->lower_bound; 
                                index <= splitDimension->upper_bound; index++)
                        {
                            SubtarPtr newSubtar = SubtarPtr(new Subtar());
                            DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification()); 
                            dimSpecs->adjacency = splitDimension->adjacency;
                            dimSpecs->dataset = _partialDimDatasets[index-dimSpecs->lower_bound];
                            dimSpecs->dimension = outputTAR->GetDataElement(
                                                    splitDimension->dimension->GetName());
                            
                            dimSpecs->lower_bound = index;
                            dimSpecs->upper_bound = index;
                            dimSpecs->skew = splitDimension->skew/dimSpecs->adjacency;
                            dimSpecs->type = splitDimension->type;
                            dimSpecs->materialized = NULL;
                            newSubtar->AddDimensionsSpecification(dimSpecs);
                            
                            for(auto entry : subtar->GetDimSpecs())
                            {
                                if(splitDimension->dimension->GetName() != entry.first)
                                {
                                    DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification()); 
                                    if(entry.second->GetLength() == 1 && entry.second->adjacency > splitDimension->adjacency)
                                    {
                                        dimSpecs->adjacency = entry.second->adjacency/splitDimension->GetLength();
                                    }
                                    else
                                    {
                                        dimSpecs->adjacency = entry.second->adjacency;
                                    }
                                    dimSpecs->dataset = entry.second->dataset;
                                    dimSpecs->dimension = outputTAR->GetDataElement(
                                                            entry.second->dimension->GetName());
                                    dimSpecs->lower_bound = entry.second->lower_bound;
                                    dimSpecs->upper_bound = entry.second->upper_bound;
                                    dimSpecs->skew = entry.second->skew;
                                    dimSpecs->type = entry.second->type;
                                    dimSpecs->materialized = NULL;
                                    newSubtar->AddDimensionsSpecification(dimSpecs);
                                }
                            }
                            
                            for(auto entry : subtar->GetDataSets())
                            {
                                DatasetPtr ds = datasets[entry.first][index-splitDimension->lower_bound];
                                newSubtar->AddDataSet(entry.first, ds);
                            }
                            
                            subtars.push_back(newSubtar);
                        }
                    }
                    
                    for(auto s : subtars)
                    {
                        outputGenerator->AddSubtar(subtarIndex++, s);
                    }
                    
                    subtarIndex--;
                }
                else
                {
                    outputGenerator->AddSubtar(subtarIndex, subtar);
                }              
            }
            else
            {
                outputGenerator->AddSubtar(subtarIndex, subtar);
            }

            generator->TestAndDisposeSubtar(subtarIndex);
            outputGenerator->SetSubtarsIndexMap(subtarIndex, currentSubtar);
            
            if(iteratorModeEnabled) break;
            subtarIndex++;
        }
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int store(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, std::shared_ptr<QueryDataManager>queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr  storageManager, EnginePtr engine)
{
    char * error_store = "Invalid parameters for store operation. Expected STORE(tar, tar_name),";
    try
    {
        if(operation->GetParameters().size() !=  3)
            throw std::runtime_error(error_store);
        
        ParameterPtr inputTarParam = operation->GetParametersByName(OPERAND(0));
        ParameterPtr newNameParam = operation->GetParametersByName(OPERAND(1));
        TARSPtr defaultTARS = metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));
        
        if(inputTarParam == NULL || newNameParam == NULL)
            throw std::runtime_error(error_store);
        
        TARPtr inputTAR = inputTarParam->tar;
        TARPtr outputTAR = inputTAR->Clone(false, false, false);
        string newName = newNameParam->literal_str;
        newName.erase(std::remove(newName.begin(), newName.end(), '"'), newName.end());
        
        if(!metadataManager->ValidateIdentifier(newName, "tar"))
            throw std::runtime_error("Invalid identifier for TAR: "+ newName);
        
        if(metadataManager->GetTARByName(defaultTARS, newName) != NULL)
            throw std::runtime_error("TAR "+newName+" already exists.");
        
        outputTAR->AlterTAR(UNSAVED_ID, newName, false);
        
        if(metadataManager->SaveTAR(defaultTARS, outputTAR) != SAVIME_SUCCESS)
            throw std::runtime_error("Could not save TAR: "+ newName);
        
        
        //Obtaining subtar generator
        auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))->GetGenerators()[inputTAR->GetName()];
        SubtarPtr subtar; int32_t subtarCount = 0;
        
        while(true)
        {
            subtar = generator->GetSubtar(subtarCount);     
            if(subtar == NULL) break;
            
            if(metadataManager->SaveSubtar(outputTAR, subtar) != SAVIME_SUCCESS)
                 throw std::runtime_error("Could not save subtar.");
            
            subtarCount++;
        }
        
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what()+string("\n")+error);
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}

int user_defined(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, std::shared_ptr<QueryDataManager>queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr  storageManager, EnginePtr engine)
{
    #define _CATALYZE "catalyze"
    #define _STORE "store"
    
    if(operation->GetParametersByName(OPERATOR_NAME)->literal_str == _CATALYZE)
    {
        #ifdef CATALYST
        return catalyze(subtarIndex, operation, configurationManager, queryDataManager, metadataManager, storageManager, engine);
        #else
        return SAVIME_SUCCESS;
        #endif
    }
    else if(operation->GetParametersByName(OPERATOR_NAME)->literal_str == _STORE)
    {
        return store(subtarIndex, operation, configurationManager, queryDataManager, metadataManager, storageManager, engine);;
    }
    
    return SAVIME_SUCCESS;
}