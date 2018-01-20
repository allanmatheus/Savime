#ifndef PARSER_H
#define PARSER_H
/*! \file */
#include "savime.h"
#include "metadata.h"
#include "storage_manager.h"
#include "query_data_manager.h"

#define DEFAULT_TEMP_MEMBER "_aux"
#define DEFAULT_SYNTHETIC_DIMENSION "i"
#define DEFAULT_MASK_ATTRIBUTE "mask"
#define DEFAULT_OFFSET_ATTRIBUTE "offset"
#define LEFT_DATAELEMENT_PREFIX "left_"
#define RIGHT_DATAELEMENT_PREFIX "right_"
#define INPUT_TAR "input_tar"
#define OPERATOR_NAME "operator_name"
#define NEW_MEMBER "new_member"
#define AUX_TAR "aux_tar"
#define OP "op"
#define LITERAL "literal"
#define IDENTIFIER "identifier"
#define COMMAND "command_str"
#define OPERAND(x) "operand"+std::to_string(x)
#define DIM(x) "dim"+std::to_string(x)
#define LB(x) "lb"+std::to_string(x)
#define UP(x) "up"+std::to_string(x)

/**Parser is module responsible for parsing the query text and generating
 * a query plan.*/
class Parser : public SavimeModule
{

public:
    
    /**
    * Constructor.
    * @param configurationManager is the standard ConfigurationManager.
    * @param systemLogger is the standard SystemLogger.
    */   
    Parser(ConfigurationManagerPtr configurationManager, 
                           SystemLoggerPtr systemLogger) :
        SavimeModule("Parser", configurationManager, systemLogger) {}
    
    /**
    * Sets the standard Metadata Manager for the Parser.
    * @param metadaManager is the standard system MetadataManager.
    */
    virtual void SetMetadaManager(MetadataManagerPtr metadaManager) = 0;
    
    /**
    * Sets the standard Storage Manager for the Parser.
    * @param metadaManager is the standard system StorageManager.
    */
    virtual void SetStorageManager(StorageManagerPtr storageManager) = 0;
    
    /**
    * Parses the query string and creates a QueryPlan in the QueryDataManager.
    * @param queryDataManager is the QueryDataManager reference containg the query
    * to be parsed and where the parser is going to store the newly created QueryPlan.
    * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
    */
    virtual SavimeResult Parse(QueryDataManagerPtr queryDataManager) = 0;
};
typedef std::shared_ptr<Parser> ParserPtr;

#endif /* PARSER_H */

