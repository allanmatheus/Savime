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
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include "default_engine.h"
#include "../core/include/savime.h"
#include "../core/include/parser.h"
#include "ddl_operators.h"
#include "dml_operators.h"


#include <chrono>
#include <condition_variable>

using namespace std;
using namespace std::chrono;

OperatorFunction operatorFunctions[] = {create_tars, create_tar, create_type, create_dataset, drop_tars, drop_tar, drop_type, drop_dataset, insert_subtar, show,
                        scan, select, filter, subset, logical, comparison, arithmetic, cross_join, equijoin, dimjoin, slice, aggregate, split, user_defined};


SubtarPtr TARGenerator::GetSubtar(int64_t subtarIndex)
{
    if(_tar == NULL)
    {
        _mutex.lock();
        if(_subtarMap.find(subtarIndex) != _subtarMap.end())
        {
            auto subtarController = _subtarMap[subtarIndex];
            subtarController->accessCount--;
            _mutex.unlock();
            return subtarController->subtar;
        }
        _mutex.unlock();

        if(_producer(subtarIndex, _operation, _configurationManager, _queryDataManager
                     , _metadataManager, _storageManager, _engine) == SAVIME_FAILURE)
            throw std::runtime_error("Error in operation "+_operation->GetName()+": "
                                     + _queryDataManager->GetErrorResponse());

        _mutex.lock();
        if(_subtarMap.find(subtarIndex) == _subtarMap.end())
        {
            _mutex.unlock();
            return NULL;
        }
        else
        {
            auto subtarController = _subtarMap[subtarIndex];
            subtarController->accessCount--;
            _mutex.unlock();
            return subtarController->subtar;
        }
    }
    else
    {
        _mutex.lock();
        if(_subtarsVector.size() > subtarIndex)
        {
           SubtarPtr subtar = _subtarsVector[subtarIndex];
           _mutex.unlock();
           return subtar;
        }
        else
        {
            _mutex.unlock();
           return NULL;
        }
    }
 }

void TARGenerator::AddSubtar(int64_t subtarIndex, SubtarPtr subtar)
{
    SubtarControlerPtr controler = SubtarControlerPtr(new SubtarControler);
    controler->accessCount = _maxAccesses;
    controler->subtar = subtar;
    _mutex.lock(); 
    _subtarMap[subtarIndex] = controler;
    _mutex.unlock();
}

void TARGenerator::TestAndDisposeSubtar(int64_t subtarIndex)
{
    _mutex.lock(); 
    if(_tar == NULL && _subtarMap.find(subtarIndex) != _subtarMap.end())
    {
        if(_subtarMap[subtarIndex]->accessCount <= 0)
        {
            _subtarMap.erase(subtarIndex);
        }
    }
    _mutex.unlock();
}

int32_t TARGenerator::GetSubtarsIndexMap(int64_t index)
{
    int32_t subtarIndex = -1;
    _mutex.lock();
    if(_subtarIndexMap[DEFAULT_MAP].find(index) != _subtarIndexMap[DEFAULT_MAP].end())
        subtarIndex = _subtarIndexMap[DEFAULT_MAP][index];
    _mutex.unlock();
    return subtarIndex;
}

void TARGenerator::SetSubtarsIndexMap(int64_t index, int64_t value)
{
    _mutex.lock();
    _subtarIndexMap[DEFAULT_MAP][index] = value;
    _mutex.unlock();
}

int32_t TARGenerator::GetSubtarsIndexMap(int64_t mapIndex, int64_t index)
{
    int32_t subtarIndex = -1;
    _mutex.lock();
    if(_subtarIndexMap[mapIndex].find(index) != _subtarIndexMap[mapIndex].end())
        subtarIndex = _subtarIndexMap[mapIndex][index];
    _mutex.unlock();
    return subtarIndex;
}

void TARGenerator::SetSubtarsIndexMap(int32_t mapIndex, int64_t index, int64_t value)
{
    _mutex.lock();
    _subtarIndexMap[mapIndex][index] = value;
    _mutex.unlock();
}

SubtarControlerPtr TARGenerator::GetSubtarsMap(int64_t index)
{
    SubtarControlerPtr controller = NULL;
    _mutex.lock();
    if(_subtarMap.find(index) != _subtarMap.end())
        controller = _subtarMap[index];
    _mutex.unlock();
    return controller;
}

void TARGenerator::SetSubtarsMap(int64_t index, SubtarControlerPtr value)
{
    _mutex.lock();
    _subtarMap[index] = value;
    _mutex.unlock();
}

/*std::unordered_map<int32_t, int32_t>& TARGenerator::GetSubtarsIndexMap()
{
    return _subtarIndexMap;
}

std::unordered_map<int32_t, SubtarControlerPtr>& TARGenerator::GetSubtarsMap()
{
    return _subtarMap;
}*/

int32_t TARGenerator::getMaxAccesses()
{
    return _maxAccesses;
}

void TARGenerator::SetMaxAccesses(int32_t maxAccesses)
{
    _mutex.lock(); 
    _maxAccesses = maxAccesses;
    _mutex.unlock();
}

//DefaultEngine members definition
void DefaultEngine::SetMetadaManager(MetadataManagerPtr metadataManager)
{
    _metadataManager = metadataManager;
}

void DefaultEngine::CleanTempTARs()
{
    _gererators.clear();
    tempTARs.clear();
}

SavimeResult DefaultEngine::WaitSendBlocksCompletion()
{
   while(true)
   {
        _dispatchMutex.lock();
        if(_blocksToDispatch.size()==0)
            break;
        _dispatchMutex.unlock();
   }
   
   _dispatchMutex.unlock();
   return _sendResult;
}

void DefaultEngine::AddBlockToDispatchList(EngineListener * caller, 
                                           DatasetPtr dataset, 
                                           string paramName, 
                                           string fileLocation, 
                                           int64_t size,  
                                           bool isFirst, 
                                           bool isLast)
{
    BlockToDispatchPtr block = BlockToDispatchPtr(new BlockToDispatch());
    block->caller = caller;
    block->dataset = dataset;
    block->param_name = paramName;
    block->file_location = fileLocation;
    block->size = size;
    block->is_first = isFirst;
    block->is_last = isLast;
    
    _dispatchMutex.lock();
    _blocksToDispatch.push_back(block);
    _dispatchMutex.unlock();
}

void DefaultEngine::WakeDispatcher()
{
    _conditionVar.notify_one();
}

void DefaultEngine::DispatchBlocks()
{
    try
    {
        std::mutex lock;
        std::unique_lock<std::mutex> locker(lock);

        while(_runningDispatcher)
        {
            _sendResult = SAVIME_SUCCESS;
            _dispatchMutex.lock();

            while(_blocksToDispatch.size() > 0)
            {
                BlockToDispatchPtr block = _blocksToDispatch.front();

                 _systemLogger->LogEvent("Engine Dispatcher", 
                                    "Sending block "+block->param_name);

                int fileDescriptor = open(block->file_location.c_str(), O_RDONLY);

                if(fileDescriptor < 0)
                {
                    _systemLogger->LogEvent("Engine Dispatcher", 
                                            "Could not send block."
                                            +std::string(strerror(errno)));
                    
                    _blocksToDispatch.clear();
                    _sendResult = SAVIME_FAILURE;
                    break;
                }

                if(block->caller->NotifyNewBlockReady(block->param_name, 
                                                      fileDescriptor, 
                                                      block->size, 
                                                      block->is_first, 
                                                      block->is_last) != SAVIME_SUCCESS)
                {
                     _systemLogger->LogEvent("Engine Dispatcher", 
                                             "Could not send block.");

                    _blocksToDispatch.clear();
                    _sendResult = SAVIME_FAILURE;
                    break;
                }

                close(fileDescriptor);
                _blocksToDispatch.pop_front();
            }
           
            //_mutex.unlock();
            _dispatchMutex.unlock();
            _conditionVar.wait_for(locker, std::chrono::seconds(1));
        }
    }
    catch(std::exception& e)
    {
        _blocksToDispatch.clear();
        _mutex.unlock();
        _dispatchMutex.unlock();
        _sendResult = SAVIME_FAILURE;
        _systemLogger->LogEvent("Engine Dispatcher", e.what());
    }
     
}

void DefaultEngine::SendResultingTAR(EngineListener * caller, TARPtr tar)
{    
    DatasetPtr dataset;
    int32_t subtarCounter = 0;
    bool isFirst = true, isLast = false;
     
    auto generator = _gererators[tar->GetName()];
    
    while(true)
    {
        #ifdef TIME 
            GET_T1();
        #endif

        auto subtar = generator->GetSubtar(subtarCounter);
        
        if(subtar == NULL) break;
        int64_t totalLength = subtar->GetTotalLength();
        subtar->RemoveTempDataElements();
          
        if(_sendResult == SAVIME_FAILURE)
        {
            throw std::runtime_error("Problem while sending resulting TAR.");
        }
        
        for(auto entry : subtar->GetDimSpecs())
        { 
            _storageManager->MaterializeDim(entry.second, totalLength, dataset);
            AddBlockToDispatchList(caller, dataset, entry.first, dataset->location, dataset->length, isFirst, isLast);
        }
        
        for(auto entry : subtar->GetDataSets())
        {            
            AddBlockToDispatchList(caller, entry.second, entry.first, entry.second->location, entry.second->length, isFirst, isLast);
        }
        
        WakeDispatcher();
        
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Subtar #"+std::to_string(subtarCounter)+" production took "+std::to_string(GET_DURATION())+" ms.");
        #endif
       
        generator->TestAndDisposeSubtar(subtarCounter++);      
        isFirst = false;
    }
    
    if(WaitSendBlocksCompletion() != SAVIME_SUCCESS)
    {
        throw std::runtime_error("Problem while sending resulting TAR.");
    }
}

/*void DefaultEngine::SendResultingTAR(EngineListener * caller, TARPtr tar)
{    
    DatasetPtr dataset;
    int32_t subtarCounter = 0;
    bool isFirst = true, isLast = false;
     
    int32_t numThreads = _configurationManager->GetIntValue(MAX_THREADS_ENGINE);
    omp_set_num_threads(numThreads);
    SubtarPtr subtars[numThreads];
     
    auto generator = _gererators[tar->GetName()];
     
    while(true)
    {
        #ifdef TIME 
            GET_T1();
        #endif
 
        #pragma omp parallel
        {
            int32_t thread = omp_get_thread_num();
            subtars[thread] = generator->GetSubtar(subtarCounter+thread);
        }
 
        for(int32_t i = 0; i < numThreads; i++)
        {
            auto subtar = subtars[i];
            if(subtar == NULL)
            {
                isLast = true;
                break;
            }
 
            int64_t totalLength = subtar->GetTotalLength();
            subtar->RemoveTempDataElements();
 
            if(_sendResult == SAVIME_FAILURE)
            {
                throw std::runtime_error("Problem while sending resulting TAR.");
            }
 
            for(auto entry : subtar->GetDimSpecs())
            { 
                _storageManager->MaterializeDim(entry.second, totalLength, dataset);
                AddBlockToDispatchList(caller, dataset, entry.first, dataset->location, dataset->length, isFirst, isLast);
            }
 
            for(auto entry : subtar->GetDataSets())
            {            
                AddBlockToDispatchList(caller, entry.second, entry.first, entry.second->location, entry.second->length, isFirst, isLast);
            }
         
            WakeDispatcher();
             
            generator->TestAndDisposeSubtar(subtarCounter+i);
            isFirst = false;
        }
         
        #ifdef TIME 
            GET_T2();
            _systemLogger->LogEvent(_moduleName, "Subtar #"+std::to_string(subtarCounter)+" production took "+std::to_string(GET_DURATION())+" ms.");
        #endif
        
        subtarCounter+=numThreads;    
        if(isLast) break;
    }
     
    if(WaitSendBlocksCompletion() != SAVIME_SUCCESS)
    {
        throw std::runtime_error("Problem while sending resulting TAR.");
    }
}*/

unordered_map<std::string, TARGeneratorPtr>& DefaultEngine::GetGenerators()
{
    return _gererators;
}

SavimeResult DefaultEngine::run(QueryDataManagerPtr queryDataManager, EngineListenerPtr caller)
{
    try
    {
        _mutex.unlock();
        _dispatchMutex.unlock();
        
        if(!_runningDispatcher)
        {
            auto thisPtr =  std::dynamic_pointer_cast<DefaultEngine>(_this);
            if(_thread) _thread->detach();
            _runningDispatcher = true;
            _thread = std::shared_ptr<std::thread>(new std::thread(&DefaultEngine::DispatchBlocks, thisPtr));
        }
        
        _systemLogger->LogEvent(this->_moduleName, "Processing query "
                           +std::to_string(queryDataManager->GetQueryId())+".");
        
        OperationPtr lastOp = queryDataManager->GetQueryPlan()->GetOperations().back();
        
        
        //Checking if is a DDL query
        if(queryDataManager->GetQueryPlan()->GetType() == DDL)
        {
            for(auto operation : queryDataManager->GetQueryPlan()->GetOperations())
            {
                int result = operatorFunctions[operation->GetOperation()](0, operation, _configurationManager, queryDataManager, _metadataManager, _storageManager, _this);

                if(result != SAVIME_SUCCESS)
                {
                    queryDataManager->SetErrorResponseText("Error during operation execution: "+queryDataManager->GetErrorResponse());
                    throw std::runtime_error(queryDataManager->GetErrorResponse());
                }

                tempTARs.push_back(operation->GetResultingTAR());
            }
        }
        else 
        {
            /*
             * Operations in the query plan are evaluated from top to bottom
             */
            for(auto operation : queryDataManager->GetQueryPlan()->GetOperations())
            {
                auto resultingTAR = operation->GetResultingTAR();
                
                /*
                 * For every operation that returns a TAR, creates a generators
                 * that creates the TAR on demand as Subtars are required by 
                 * subsequent operations.
                 */
                if(resultingTAR != NULL)
                {
                    TARGeneratorPtr generator = 
                    TARGeneratorPtr(new TARGenerator((OperatorFunction)operatorFunctions[operation->GetOperation()], operation,
                                                 _configurationManager, queryDataManager, _metadataManager, _storageManager, _this));

                    //Set max accesses to subtar to 0, meaning subtars 
                    generator->SetMaxAccesses(0);
                    _gererators[resultingTAR->GetName()] = generator;
                }
                
                /*
                 * Checking operations parameters for input tars. An input TAR is either
                 * a stored TAR in Savime, or a a temp TAR created during query execution.
                 */
                for(auto param : operation->GetParameters())
                {
                    
                    /*
                     * If it is a TAR parameter
                     */
                    if(param->type == TAR_PARAM)
                    {
                        /*
                         * Check if a generator for the specified TAR has already been created.
                         * If not, since it is a top down search, the TAR must be a savime stored TAR
                         * instead of one created on demand by the query
                         */
                        if(_gererators.find(param->tar->GetName()) == _gererators.end())
                        {
                            TARGeneratorPtr generator = 
                            TARGeneratorPtr(new TARGenerator(param->tar));
                            _gererators[param->tar->GetName()] = generator;
                        }
                        /*
                         * If a TAR generator has been created, it means the TAR 
                         * can be accessed many times. Therefore we must increase
                         * the max access counter.
                         */
                        else
                        {
                            auto generator = _gererators[param->tar->GetName()];
                            generator->SetMaxAccesses(generator->getMaxAccesses()+1);
                        }
                    }
                }
            }
        }
      
        if(lastOp->GetResultingTAR() != NULL)
        {
            lastOp->GetResultingTAR()->RemoveTempDataElements();
            caller->NotifyTextResponse(lastOp->GetResultingTAR()->toSmallString());
            SendResultingTAR(caller, lastOp->GetResultingTAR());
        }
        else if (queryDataManager->GetQueryPlan()->GetType() == DML)
        {
            auto operation = queryDataManager->GetQueryPlan()->GetOperations().back();
            int result = operatorFunctions[operation->GetOperation()](0, operation, _configurationManager, queryDataManager, _metadataManager, _storageManager, _this);
            if(result != SAVIME_SUCCESS)
            {
                queryDataManager->SetErrorResponseText("Error during operation execution: "+queryDataManager->GetErrorResponse());
                throw std::runtime_error(queryDataManager->GetErrorResponse());
            }           
            caller->NotifyTextResponse("Query executed successfully");
        }
        else
        {
            if(queryDataManager->GetQueryResponseText().empty())
                caller->NotifyTextResponse("Query executed successfully");
            else
                caller->NotifyTextResponse(queryDataManager->GetQueryResponseText());
        }
         
        CleanTempTARs();
        caller->NotifyWorkDone();
        _systemLogger->LogEvent(this->_moduleName, "Finished processing query "
                           +std::to_string(queryDataManager->GetQueryId())+".");
        
        _runningDispatcher = false;
        WakeDispatcher();
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
         _runningDispatcher = false;
         WakeDispatcher();
        _mutex.unlock();
        _gererators.clear();
        _systemLogger->LogEvent(this->_moduleName, e.what());
        return SAVIME_FAILURE;
    }
}
