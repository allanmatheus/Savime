#ifndef KERNEL_H
#define KERNEL_H
/*! \file */
#include <string>
#include <memory>
#include "system_logger.h"
#include "include/config_manager.h"
    
using namespace std;

/**Enum with codes for results of executions of main modules functions. */
enum SavimeResult {SAVIME_SUCCESS  /*!<Indicates sucessful execution. */
                  ,SAVIME_FAILURE  /*!<Indicates failure during execution.*/};

/**
 * Base class for all modules in Savime. It contains a reference to 
 * the standard SystemLogger and ConfigurationManager.
 */                  
class SavimeModule
{

protected:
    string _moduleName; /*!<String containing the modules name for logging purposes.*/
    ConfigurationManagerPtr _configurationManager; /*!<Instance of the standard ConfigurationManager*/
    SystemLoggerPtr _systemLogger; /*!<Instance of the standard SystemLogger*/

public:
    
    /**
    * Constructor.
    * @param moduleName is a string for the module's name.
    * @param configurationManager is the standard ConfigurationManager.
    * @param systemLogger is the standard SystemLogger.
    */
    SavimeModule(string moduleName, ConfigurationManagerPtr configurationManager, 
                 SystemLoggerPtr systemLogger)
    {
        _moduleName = moduleName;
        _configurationManager = configurationManager;
        _systemLogger = systemLogger;
    }
    
    /**
    * Sets the module's name. 
    * @param name is a string containing the module's name. 
    */
    void SetName(string name)
    {
        this->_moduleName = name;
    }
    
    /**
    * Sets the module's standard ConfigurationManager. 
    * @param configurationManager is a standard ConfigurationManager. 
    */
    void SetConfigurationManager(ConfigurationManagerPtr configurationManager)
    {
        this->_configurationManager = configurationManager;
    }
    
    /**
    * Sets the module's standard SystemLogger. 
    * @param systemLogger is a standard SystemLogger. 
    */
    void SetSystemLogger(SystemLoggerPtr systemLogger)
    {
        this->_systemLogger = systemLogger;
    }
    
};

#endif /* KERNEL_H */

