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
#include <stdexcept>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>

#include "include/builder.h"
#include "include/config_manager.h"
#include "include/system_logger.h"
#include "include/job_manager.h"
#include "include/engine.h"
#include "include/parser.h"
#include "include/optimizer.h"
#include "include/metadata.h"

#include "../job/default_job_manager.h"
#include "../parser/default_parser.h"
#include "../optimizer/default_optimizer.h"
#include "../metada/default_metada_manager.h"
#include "../connection/default_connection_manager.h"
#include "../configuration/default_config_manager.h"
#include "../engine/default_engine.h"
#include "../query/default_query_data_manager.h"
#include "../storage/default_storage_manager.h"

ModulesBuilder::ModulesBuilder(int argc, char ** args)
{
    #define BOOT_QUERY_FILE "boot_query_file"

    int32_t threads; char c;
    BuildConfigurationManager();
    
    struct option longopts[] = {
        {MAX_THREADS, required_argument,  NULL,  'm'},
        {SHM_STORAGE_DIR, required_argument, NULL, 's'},
        {SEC_STORAGE_DIR, required_argument, NULL, 'd'},
        {CONFIG_FILE, required_argument, NULL, 'f'},
        {BOOT_QUERY_FILE, required_argument, NULL, 'b'},
        { 0, 0, 0, 0 }
    };
    
    while ((c = getopt_long(argc, args, ":m:s:d:f:b:", longopts, NULL)) != -1) 
    {
        switch (c) 
        {
            case 'm':
                threads = atoi(optarg);
                _configurationManager->SetIntValue(MAX_THREADS, threads); 
                break;
            case 's':
                _configurationManager->SetStringValue(SHM_STORAGE_DIR, std::string(optarg)); 
                break;
            case 'd':
                _configurationManager->SetStringValue(SEC_STORAGE_DIR, std::string(optarg));
                break;
            case 'f':
                _configurationManager->LoadConfigFile(std::string(optarg));
                break;
            case 'b':
                RunBootQueryFile(std::string(optarg)); 
                break;   
       }
   }
   
   BuildSystemLogger();
   int32_t numThreads = _configurationManager->GetIntValue(MAX_THREADS);
   std::string shmPath = _configurationManager->GetStringValue(SHM_STORAGE_DIR);
   std::string secPath = _configurationManager->GetStringValue(SEC_STORAGE_DIR);
   std::string address = _configurationManager->GetStringValue(SERVER_ADDRESS(0));
   std::string address_unix = _configurationManager->GetStringValue(SERVER_UNIX_PATH(0));
   int32_t port = _configurationManager->GetIntValue(SERVER_PORT(0));
           
   if(!EXIST_FILE(shmPath))
        if (mkdir(shmPath.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) == -1)
            throw std::runtime_error("Could not create dir "+shmPath+". "+strerror(errno));
   
   if(!EXIST_FILE(secPath))
        if (mkdir(secPath.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH) == -1)
            throw std::runtime_error("Could not create dir "+secPath+". "+strerror(errno));

   
   _systemLogger->LogEvent("SAVIME", "Server started with "
                                      +std::to_string(numThreads)+" thread(s) "
                                      +"in "+address+":"+to_string(port)+" "
                                      +"shm_storage_dir: "+shmPath+" "
                                      +"sec_storage_dir: "+secPath+".");
}

ConfigurationManagerPtr ModulesBuilder::BuildConfigurationManager()
{
    if(_configurationManager == NULL)
    {
        _configurationManager = ConfigurationManagerPtr(new DefaultConfigurationManager());
    }
    
    return _configurationManager;
}

SystemLoggerPtr ModulesBuilder::BuildSystemLogger()
{
    if(_systemLogger == NULL)
    {
        _systemLogger = SystemLoggerPtr(new DefaultSystemLogger());
    }
    
    return _systemLogger;
}

EnginePtr ModulesBuilder::BuildEngine()
{
    if(_engine == NULL)
    {
        _engine = EnginePtr(new DefaultEngine(BuildConfigurationManager(), 
                                              BuildSystemLogger(), 
                                              BuildMetadaManager(), 
                                              BuildStorageManager()));
        
        ((DefaultEngine*)_engine.get())->SetThisPtr(_engine);
    }
    
    return _engine;
}

ParserPtr ModulesBuilder::BuildParser()
{
    if(_parser == NULL)
    {
        _parser = ParserPtr(new DefaultParser(BuildConfigurationManager(), BuildSystemLogger()));
        _parser->SetMetadaManager(BuildMetadaManager());
        _parser->SetStorageManager(BuildStorageManager());
    }
    
    return _parser;
}

OptimizerPtr ModulesBuilder::BuildOptimizer()
{
    if(_optmizier == NULL)
    {
        _optmizier = OptimizerPtr(new DefaultOptimizer(BuildConfigurationManager(), BuildSystemLogger()));
    }
    
    return _optmizier; 
}

MetadataManagerPtr ModulesBuilder::BuildMetadaManager()
{
    if(_metadataManager == NULL)
    {
        _metadataManager = MetadataManagerPtr (new DefaultMetadataManager(BuildConfigurationManager(), BuildSystemLogger()));
    }
    
    return _metadataManager; 
}

ConnectionManagerPtr ModulesBuilder::BuildConnectionManager()
{
    if(_connectionManager == NULL)
    {
        _connectionManager = ConnectionManagerPtr(new DefaultConnectionManager(BuildConfigurationManager(), BuildSystemLogger()));
    }
    
    return _connectionManager; 
}

StorageManagerPtr ModulesBuilder::BuildStorageManager()
{
    if(_storageManager == NULL)
    {
        _storageManager = StorageManagerPtr (new DefaultStorageManager(BuildConfigurationManager(), BuildSystemLogger()));
        ((DefaultStorageManager*)(_storageManager.get()))->SetThisPtr(std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
    }
    
    return _storageManager; 
}

QueryDataManagerPtr ModulesBuilder::BuildQueryDataManager()
{
    if(_queryDataManager == NULL)
    {
        _queryDataManager = QueryDataManagerPtr(new DefaultQueryDataManager(BuildConfigurationManager(), BuildSystemLogger()));
    }
    
    return _queryDataManager; 
}

JobManagerPtr ModulesBuilder::BuildJobManager()
{
    if(_jobManager == NULL)
    {
        _jobManager = JobManagerPtr(new DefaultJobManager(BuildConfigurationManager(), 
                                                    BuildSystemLogger(),
                                                    BuildConnectionManager(),    
                                                    BuildEngine(), 
                                                    BuildParser(), 
                                                    BuildOptimizer(), 
                                                    BuildMetadaManager(),
                                                    BuildQueryDataManager()));
        
        
    }
    
    return _jobManager;      
}

void ModulesBuilder::RunBootQueryFile(string queryFile)
{   
    std::string query; int queryId=0;
    auto configurationManager = BuildConfigurationManager();
    auto systemLogger = BuildSystemLogger();
    auto parser = BuildParser();
    auto engine = BuildEngine();

    std::ifstream input(queryFile);
    while (std::getline(input, query))
    {
        auto queryDataManager = QueryDataManagerPtr(new DefaultQueryDataManager(configurationManager, systemLogger));
        queryDataManager->SetQueryId(queryId++);
        queryDataManager->AddQueryTextPart(query);

        if(parser->Parse(queryDataManager) != SAVIME_SUCCESS)
        {
            continue;
        }

        if(engine->run(queryDataManager, this) != SAVIME_SUCCESS)
        {
            systemLogger->LogEvent("Builder", "Error during boot query execution: "+query+" "+queryDataManager->GetErrorResponse());
        }
    }
}
