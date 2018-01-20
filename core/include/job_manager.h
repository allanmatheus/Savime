#ifndef JOB_MANAGER_H
#define JOB_MANAGER_H
/*! \file */
#include <memory>
#include "optimizer.h"
#include "savime.h"
#include "include/engine.h"
#include "include/connection_manager.h"

using namespace std;

/**
 * A ServerJob is an instance of work run to respond to a user's request.
 */
class ServerJob
{
public:
    /**
    * Executes the protocol for communicating with a client and calls other modules,
    * such as the Parser, Optimizer and the Engine for answering to the user's request. 
    */
    virtual void Run() = 0;
};
typedef ServerJob* ServerJobPtr;


/**
 * A JobManager is responsible for subscribe the ConnectionManager for new 
 * connections, and then create and run a ServerJob to answer the connection request.
 */
class JobManager: public SavimeModule, public ConnectionListener{
    
public:
  
  /**
  * Constructor.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  */
  JobManager(ConfigurationManagerPtr configurationManager, SystemLoggerPtr systemLogger) :
    SavimeModule("Job Manager", configurationManager, systemLogger){}  
  
  /**
  * Sets the standard Engine.
  * @param engine is the standard Engine.
  */  
  virtual void SetEngine(EnginePtr engine) = 0;
  
  /**
  * Sets the standard Parser.
  * @param parser is the standard Parser.
  */  
  virtual void SetParser(ParserPtr parser) = 0;

  /**
  * Sets the standard Optimizer.
  * @param optimizer is the standard Optimizer.
  */  
  virtual void SetOptmizer(OptimizerPtr optimizer) = 0;
  
  /**
  * Sets the standard MetadataManager.
  * @param metadaManager is the standard MetadataManager.
  */ 
  virtual void SetMetadaManager(MetadataManagerPtr metadaManager) = 0;
  
  /**
  * Starts the JobManager instance.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Start() = 0;
  
  /**
  * Signalizes the JobManager that a job must be stopped.
  * @param job is ServerJob that must be stopped.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult StopJob(ServerJobPtr job) = 0;
  
  /**
  * Signalizes the JobManager that all jobs must be stopped.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult StopAllJobs() = 0;
  
  /**
  * Stops the ServerJob instance.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Stop() = 0;
};
typedef shared_ptr<JobManager> JobManagerPtr;

#endif /* JOB_MANAGER_H */

