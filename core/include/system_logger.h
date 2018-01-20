#ifndef SYSTEM_LOGGER_H
#define SYSTEM_LOGGER_H
/*! \file */
#include <string>
#include <memory>

using namespace std;

/**The system logger is the module responsible for registering\logging system events.*/
class SystemLogger
{
public:
    
    /**
    * Register a event that happend in SAVIME.
    * @param module is a string containing the module who fired the event. 
    * @param message is a string containing the events info to be logged. 
    */ 
    virtual void LogEvent(string module, string message) = 0;
};
typedef std::shared_ptr<SystemLogger> SystemLoggerPtr;

class DefaultSystemLogger : public SystemLogger
{
    
    static void SystemLoggerHandler(int32_t signal);
    static string GatherStackTrace();
    static void WriteLog(string module, string message);
    
public:
    DefaultSystemLogger();
    void LogEvent(string module, string message);
};



#endif /* SYSTEM_LOGGER_H */

