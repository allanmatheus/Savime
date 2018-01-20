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
#include <thread>
#include <mutex>

#include "../core/include/savime.h"
#include "default_job_manager.h"
#include "default_server_job.h"


ConnectionListenerPtr DefaultJobManager::NotifyNewConnection(ConnectionDetailsPtr connectionDetails)
{
    std::mutex mutex;
    std::unique_lock<std::mutex> locker(mutex);
    std::shared_ptr<DefaultServerJob> newJob;
    QueryDataManagerPtr queryDataManager = _queryDataManager->GetInstance();
    
    //Creating new job
    newJob = std::shared_ptr<DefaultServerJob>(new DefaultServerJob(_jobIdCounter++,
                                                    connectionDetails,
                                                    this,
                                                   _connectionManager, 
                                                   _configurationManager, 
                                                   _systemLogger,
                                                   _engine, 
                                                   _parser,
                                                   _optimizer, 
                                                   _metadaManager,
                                                   queryDataManager));
    
    newJob->SetThisPtr(newJob);
    
    //if(_thread != NULL)
    //    _conditionVar.wait(locker);
    _thread_mutex.lock();
    
    _systemLogger->LogEvent("Job Manager", "Creating job "+std::to_string(_jobIdCounter-1)+
                                           " with socket "+std::to_string(connectionDetails->socket));
    
    //Add new job to running jobs list
    //_runningJobs[newJob->GetId()] = newJob;
    
    //Starting thread for new job
    _thread = std::shared_ptr<std::thread>(new std::thread(&DefaultServerJob::Run, newJob));
    
    //Add new thread to jobs list
    //_threads[newJob->GetId()] = thread;
     
    
    
    return newJob.get();
}


void DefaultJobManager::NotifyMessageArrival(ConnectionDetailsPtr connectionDetails){};

void DefaultJobManager::SetEngine(EnginePtr engine)
{
    _engine = engine;
}

void DefaultJobManager::SetParser(ParserPtr parser)
{
    _parser = parser;
}

void DefaultJobManager::SetOptmizer(OptimizerPtr optimizer)
{
    _optimizer = optimizer;
}

void DefaultJobManager::SetMetadaManager(MetadataManagerPtr metadaManager)
{
    _metadaManager = metadaManager;
}

SavimeResult DefaultJobManager::Start() 
{ 
    _thread = NULL;
    _connectionManager->AddConnectionListener(this);
    return SAVIME_SUCCESS;
}

SavimeResult DefaultJobManager::StopJob(ServerJobPtr job)
{
    _thread->detach();
    _thread = NULL;
    _thread_mutex.unlock();
    _conditionVar.notify_all();
    return SAVIME_SUCCESS;
}

SavimeResult DefaultJobManager::StopAllJobs()
{
    return SAVIME_SUCCESS;
}

SavimeResult DefaultJobManager::Stop()
{
    return SAVIME_SUCCESS;
}