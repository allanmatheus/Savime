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
#include "include/query_data_manager.h"
#include "include/storage_manager.h"
#include "ddl_operators.h"
#include "dml_operators.h"
#include <algorithm>
#include <vector>
#include <fstream>
#include <omp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* 
 * CREATE_TARS("[tar_name]");
 */
int create_tars(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{  
    try
    {
        ParameterPtr parameter = operation->GetParameters().front();
        std::string tarsName = parameter->literal_str;
        tarsName.erase(std::remove(tarsName.begin(), tarsName.end(), '"'), tarsName.end());
        
        if(metadataManager->ValidateIdentifier(tarsName, "tars"))
        {               
            TARSPtr newTars = TARSPtr(new TARS());
            newTars->id = UNSAVED_ID;
            newTars->name = tarsName;
            
            if(metadataManager->SaveTARS(newTars) == SAVIME_FAILURE)
            {
                 throw std::runtime_error("Could not save new TARS: "+ tarsName);
            }
        }
        else
        {
            throw std::runtime_error("Invalid or already existing identifier for TARS: "+ tarsName);
        }
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
    return SAVIME_SUCCESS;
}

/*Auxiliary functions for create TAR:
 * Dimensions can be either natural integer or long typed with and integer spacing equals to 1 (simplest case) or greater than
 * They can also be float or double typed, and also have a float/double spacing value
 * In addition, dimensions can be explicity, meaning the user must specify a dataset with values to be explicitly mapped from and integer
 * index into a value in the mapping.
 * Upper and lower bound have different meanings. If it is a spaced or implicitly dimensions, it must hold the actual values.
 * If it is an explicity, then the upper and lower bounds must be integer/long indexes that will be mapped to values in the dataset.
 */
std::list<DimensionPtr> create_dimensions(std::string dimBlock, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager)
{
    std::list<DimensionPtr> dimensions;
    std::vector<std::string> dimensionsSpecification = split(dimBlock, '|');
    
    for(std::string dimSpecs : dimensionsSpecification)
    {
        std::vector<std::string> params = split(dimSpecs, ',');
        DimensionPtr dimension = DimensionPtr(new Dimension);
        
        dimension->dimension_type = STR2DIMTYPE(trim(params[0]).c_str());
        if(dimension->dimension_type == NO_DIM_TYPE)
            throw std::runtime_error("Invalid type "+trim(params[0])+" in dimension specification.");
        
        if(dimension->dimension_type == IMPLICIT)
        {
            if(params.size() != 6)
               throw std::runtime_error("Invalid dimension definition. Wrong number of parameters.");
            
            //name validation done outside this function
            dimension->name = trim(params[1]);
            
            dimension->type = STR2TYPE(trim(params[2]).c_str());
            if(dimension->type == NO_TYPE)
                throw std::runtime_error("Invalid type in dimension specification: "+params[2]);
            
            dimension->lower_bound = atof(trim(params[3]).c_str());
            
            if(trim(params[4]).c_str()[0] == '*')
                dimension->upper_bound = std::numeric_limits<double>::infinity();
            else
                dimension->upper_bound = atof(trim(params[4]).c_str());
            
            if(dimension->upper_bound <= dimension->lower_bound)
                throw std::runtime_error("Dimension "+dimension->name+" upper bound must be greater than the lower bound.");
            
            dimension->spacing = strtod ((trim(params[5]).c_str()), NULL);
            
            if(dimension->type == INTEGER_TYPE)
            {
                dimension->lower_bound = (int32_t)dimension->lower_bound;
                dimension->upper_bound = (int32_t)dimension->upper_bound;
                dimension->spacing = (int32_t)dimension->spacing;
            }
            
            if(dimension->type == LONG_TYPE)
            {
                dimension->lower_bound = (int64_t)dimension->lower_bound;
                dimension->upper_bound = (int64_t)dimension->upper_bound;
                dimension->spacing = (int64_t)dimension->spacing;
            }
            
            dimension->real_lower_bound = 0;
            dimension->real_upper_bound = dimension->GetLength()-1;
            
            if(dimension->spacing <= 0 )
                throw std::runtime_error("Spacing for dimension "+dimension->name+" must be greater than 0.");
            
            if(dimension->spacing < 1 && (dimension->type == INTEGER_TYPE || dimension->type == LONG_TYPE))
                throw std::runtime_error("Spacing for dimension "+dimension->name+" must be equal or greater than 1.");
        }
        else if(dimension->dimension_type == EXPLICIT)
        {
            //name validation done outside this function
            dimension->name = trim(params[1]);
            dimension->lower_bound = 0;
            dimension->real_lower_bound = 0;
            dimension->spacing = 1;
      
            DatasetPtr ds = metadataManager->GetDataSetByName(trim(params[2]));
            
            if(ds == NULL)
                 throw std::runtime_error("Invalid dataset name: "+trim(params[2]));
            
            dimension->type = ds->type;
            dimension->upper_bound = dimension->real_upper_bound = ds->entry_count-1;
            dimension->dataset = ds;
            if(!dimension->dataset->sorted)
                storageManager->CheckSorted(dimension->dataset);
            
        }
       
        dimensions.push_back(dimension);
    }
    
    return dimensions;
}

std::list<AttributePtr> create_attributes(std::string attBlock)
{
    std::list<AttributePtr> attributes;
    std::vector<std::string> attributeSpecification = split(attBlock, '|');
    
    for(std::string attSpecs : attributeSpecification)
    {
        std::vector<std::string> params = split(attSpecs, ',');
        
        if(params.size() != 2)
             throw std::runtime_error("Invalid attribute definition.");
            
        AttributePtr att = AttributePtr(new Attribute());
        att->name = trim(params[0]);
        att->type = STR2TYPE(trim(params[1]).c_str());
        if(att->type == NO_TYPE)
            throw std::runtime_error("Invalid type in dimension specification: "+params[1]);
        
        attributes.push_back(att);
    }
    
    return attributes;
}

std::map<std::string, RolePtr> create_roles(TARPtr tar, TypePtr type, std::string rolesBlock)
{
    if(rolesBlock.empty())
        throw std::runtime_error("Invalid roles definition. Block is empty.");
    
    std::vector<std::string> params = split(rolesBlock, ',');
    std::map<std::string, std::string> roles2Element;
    std::map<std::string, std::string> element2Role;
    std::map<std::string, RolePtr> roleSpecification;
    
    if(params.size()%2!=0)
         throw std::runtime_error("Invalid roles block definition.");
    
    //Check for undefined or duplicated types or roles definitions
    for(int i = 0; i< params.size(); i+=2)
    {
        params[i] = trim(params[i]);
        params[i+1] = trim(params[i+1]);
        
        if(type->roles.find(params[i+1]) == type->roles.end())
             throw std::runtime_error("The role "+params[i+1]+" is not defined in the type: "+type->name+".");
        
        if(tar->GetDataElement(params[i]) == NULL)
            throw std::runtime_error("The data element "+params[i]+" has not been defined.");
        
        if(element2Role.find(params[i]) == element2Role.end())
            element2Role[params[i]] = params[i+1];
        else
            throw std::runtime_error("Duplicated definition for data element "+params[i]+" in roles definition.");
        
        if(roles2Element.find(params[i+1]) == roles2Element.end())
            roles2Element[params[i+1]] = params[i];
        else
           throw std::runtime_error("Duplicated definition for role "+params[i+1]+" in roles definition.");
    }
    
    //test if all mandatory roles are defined
    for(auto entry : type->roles)
    {
        if(entry.second->is_mandatory)
        {
            if(roles2Element.find(entry.first) == roles2Element.end())
                 throw std::runtime_error("Mandatory role "+entry.second->name+" is not assigned a data element.");
        }
    }
    
    //Creating roles specification
    for(auto entry: element2Role)
    {
        roleSpecification[entry.first] = type->roles[entry.second];
    }
   
    return roleSpecification;
}

/* CREATE_TAR("tar_name", "type_name", "(dimensions_block)", "(attributes_block)", ["(roles_block)"]);
 * dimensions_block: dim_block_implicit = dimtype, name, type, logical_lower, logical_upper, spacing |     
 *                   dim_block_explicit = dimtype, name, dataset |  //Type and size inferred by dataset in this case;
 *                                                                  //Logical_upper can be an "*", which means it is an unbounded dimensions.    
 * attributes_block = name, type |
 * roles_block = data_element_name, role1, data_element2, role2, .... | // This block is optional.
 */
int create_tar(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        std::map<std::string, RolePtr> roles;
        std::list<AttributePtr> atts;
        std::list<DimensionPtr> dims;
        TypePtr type = NULL;

        std::list<ParameterPtr>  parameters = operation->GetParameters();
        ParameterPtr parameter = parameters.front();
        std::string tarName = parameter->literal_str;
        tarName.erase(std::remove(tarName.begin(), tarName.end(), '"'), tarName.end());
        parameters.pop_front();
        
        parameter = parameters.front();
        std::string typeName = parameter->literal_str;
        typeName.erase(std::remove(typeName.begin(), typeName.end(), '"'), typeName.end());
        parameters.pop_front();
        
        parameter = parameters.front();
        std::string dimBlock = parameter->literal_str;
        dimBlock.erase(std::remove(dimBlock.begin(), dimBlock.end(), '"'), dimBlock.end());
        parameters.pop_front();
        
        parameter = parameters.front();
        std::string attBlock = parameter->literal_str;
        attBlock.erase(std::remove(attBlock.begin(), attBlock.end(), '"'), attBlock.end());
        parameters.pop_front();

        std::string rolesBlock = "";
        if(parameters.size() > 0)
        {
            parameter = parameters.front();
            rolesBlock = parameter->literal_str;
            rolesBlock.erase(std::remove(rolesBlock.begin(), rolesBlock.end(), '"'), rolesBlock.end());
        }
        
        if(!metadataManager->ValidateIdentifier(tarName, "tar"))
            throw std::runtime_error("Invalid identifier for TAR: "+ tarName);
        
        TARSPtr defaultTARS = metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));
        
        if(metadataManager->GetTARByName(defaultTARS, tarName) != NULL)
            throw std::runtime_error(tarName+" TAR already exists.");
        
        if(typeName.compare("*") != 0)
        {
            type = metadataManager->GetTypeByName(defaultTARS, typeName);
            if(type == NULL)
                 throw  std::runtime_error("Undefined type: "+ typeName);
        }
         
        dims = create_dimensions(dimBlock, metadataManager, storageManager);
        atts = create_attributes(attBlock);
          
        TARPtr newTar = TARPtr(new TAR());
        newTar->AlterTAR(UNSAVED_ID, tarName, false);
        newTar->AlterType(type);
        
        for(auto d : dims)
        {
            if(!metadataManager->ValidateIdentifier(d->name, "dimension"))
                throw std::runtime_error("Invalid dimension name: "+ d->name);
                
            if(newTar->GetDataElement(d->name) != NULL)
                throw std::runtime_error("Duplicated data element name: "+ d->name);

            newTar->AddDimension(d);
        }
        
        for(auto a : atts)
        {
            if(!metadataManager->ValidateIdentifier(a->name, "attribute"))
               throw std::runtime_error("Invalid attribute name: "+ a->name);
            
            if(newTar->GetDataElement(a->name) != NULL)
               throw std::runtime_error("Duplicated data element name: "+ a->name);
            
            newTar->AddAttribute(a);
        }
        
        if(type != NULL)
        {
            roles = create_roles(newTar, type, rolesBlock);
        }
        
        for(auto r : roles)
        {
            if(newTar->GetDataElement(r.first) == NULL)
                throw std::runtime_error("Invalid role specification, undefined data element: "+r.first);
                
            newTar->SetRole(r.first, r.second);
        }
          
        if(metadataManager->SaveTAR(defaultTARS, newTar) == SAVIME_FAILURE)
            throw std::runtime_error("Could not save new TAR: "+ tarName);
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
}

/*
 * CREATE_TYPE("[type_name](mandatory_role1, mandatory_role2, *non_mandatory_role3, ..., roleN)"); 
 * Role definitions starting with * are non-mandatory.
 */
int create_type(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        ParameterPtr parameter = operation->GetParameters().front();
        std::string commandString = parameter->literal_str;
        commandString.erase(std::remove(commandString.begin(), commandString.end(), '"'), commandString.end());
        std::string newTypeName = "";
        
        newTypeName = commandString.substr(0, commandString.find("("));
        std::string betweenParenthesis = between(commandString, "(", ")");
        std::vector<std::string> roles = split(betweenParenthesis, ',');
        
        if(betweenParenthesis.empty() || roles.size() == 0)
        {
            throw std::runtime_error("Invalid type string definition.");
        }
        
        if(metadataManager->ValidateIdentifier(newTypeName, "type"))
        {
            TARSPtr defaultTARS = metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));
            
            if(metadataManager->GetTypeByName(defaultTARS, newTypeName) != NULL)
                throw std::runtime_error("Type "+newTypeName+" already exists.");
                        
            TypePtr newType = TypePtr(new Type());
            newType->id = UNSAVED_ID;
            newType->name = newTypeName;
            std::map<std::string, std::string> roleMaps;
            
            for(auto role : roles)
            {
                std::string trimmedRole = trim(role);
                std::string trimmedRoleWithoutStar = trimmedRole;
                trimmedRoleWithoutStar.erase(std::remove(trimmedRoleWithoutStar.begin(), 
                                                       trimmedRoleWithoutStar.end(), '*'), 
                                                       trimmedRoleWithoutStar.end());
                
                if(roleMaps.find(trimmedRole) == roleMaps.end() 
                        && metadataManager->ValidateIdentifier(trimmedRoleWithoutStar, "role"))
                {
                    roleMaps[trimmedRole] = trimmedRoleWithoutStar;
                }
                else
                {
                    throw std::runtime_error("Invalid or duplicate definition of role: "+role);
                }
            }
            
            for(auto entry: roleMaps)
            {
                RolePtr role = RolePtr(new Role());
                role->id = UNSAVED_ID;
                
                if(entry.first.c_str()[0] == '*')
                {
                    role->name = entry.second;
                    role->is_mandatory = false;
                }
                else
                {
                    role->name = entry.second;
                    role->is_mandatory = true;
                }
                
                newType->roles[role->name]=role;
            }

            if(metadataManager->SaveType(defaultTARS, newType) == SAVIME_FAILURE)
            {
                throw std::runtime_error("Could not save new Type: "+ newTypeName);
            }
        }
        else
        {
            throw std::runtime_error("Invalid or already existing identifier for Type: "+ newTypeName);
        }
        
        return SAVIME_SUCCESS;
    }
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
}

/*
 * CREATE_DATASET("name:type", "data_file_path"); 
 * //Local file paths must starts with @ and remote server files don't.
 * CREATE_DATASET("name:type", "literal dataset"); 
 * //Literal dataset is a series of comma separated values between squared brackets.
 * CREATE_DATASET("name:type", "initial:spacing:final"); 
 * //Datasets can be filled with a sequential interval of numerical values specified by an initial and a final values along with a spacing.
 */
//
int create_dataset(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    #define RANGE_FILLER ":"
    #define LITERAL_FILLER_LEFT "["
    #define LITERAL_FILLER_RIGHT "]"
    #define LITERAL_FILLER_SEPARATOR ","

    try
    {  
        ParameterPtr parameter = operation->GetParameters().front();
        ParameterPtr parameter2 = operation->GetParameters().back();
        std::string commandString = parameter->literal_str;
        std::string inBetween = between(commandString, "\"", "\"");
        inBetween.erase(std::remove(inBetween.begin(), inBetween.end(), '"'), inBetween.end());
        std::vector<std::string> arguments = split(inBetween, ':');
        
        if(arguments.size() == 2)
        {
            std::string dsName = trim(arguments[0]);
            std::string type = trim(arguments[1]);
            std::string file, filler = parameter2->literal_str;
            bool fileFiller = false; file = filler; 
            
            file = between(file, "\"", "\"");
            file.erase(std::remove(file.begin(), file.end(), '"'), file.end());
            
            if(!metadataManager->ValidateIdentifier(dsName, "dataset"))
                throw std::runtime_error("Invalid dataset name: "+dsName+".");
            
            if(metadataManager->GetDataSetByName(dsName) != NULL)
                throw std::runtime_error("Dataset "+dsName+" already exists.");
                
            DataType dsType = STR2TYPE(type.c_str());
            if(dsType == NO_TYPE)
                throw std::runtime_error("Invalid type: "+ type+".");
            
            int typeSize = TYPE_SIZE(dsType);
            DatasetPtr ds = DatasetPtr(new Dataset());
          
            if(queryDataManager->GetParamsList().size() > 0)
            {
                std::string param = queryDataManager->GetParamsList().front();
                file = queryDataManager->GetParamFilePath(param);
                fileFiller = true;
            }
            else if(EXIST_FILE(file))
            {
                std::string dir = configurationManager->GetStringValue(SEC_STORAGE_DIR);

                if (file.compare(0, dir.length(), dir) != 0)
                {
                    //To-do move to storage dir in case it is not. Change file var to new path;
                    throw std::runtime_error("File is not in SAVIME storage dir: "+dir+".");
                }
                fileFiller = true;
            }
            
            
            if(!fileFiller)
            {
                if(filler.find(RANGE_FILLER) != std::string::npos)
                {
                    filler.erase(std::remove(filler.begin(), filler.end(), '"'), filler.end());
                    auto range = split(filler, ':');
                    double dRanges[3];
                    
                    if(range.size() != 3)
                        throw std::runtime_error("Invalid range specification. It must be defined as initial:spacing:final.");
                    
                    for(int i = 0; i < range.size(); i++)
                    {
                        try{
                            dRanges[i] = stod(range[i]);
                        }
                        catch(std::invalid_argument& e)
                        {
                            throw std::runtime_error("Invalid range specification. Could not parse numerical values.");
                        }
                        catch(std::out_of_range& e)
                        {
                            throw std::runtime_error("Invalid range specification. Numerical value is out of range.");
                        }
                    }
                    
                    if(dRanges[0] > dRanges[2])
                        throw std::runtime_error("Invalid range specification. Initial value must be lower than the final value.");
                    
                    ds = storageManager->Create(dsType, dRanges[0], dRanges[1], dRanges[2]);
                    
                    if(ds == NULL)
                        throw std::runtime_error("Invalid range specification");
                    
                    ds->id = UNSAVED_ID;
                    ds->name = dsName;
                    ds->sorted = true;
                    
                }
                else if(filler.find(LITERAL_FILLER_LEFT) != std::string::npos)
                {
                    string literalValuesList = between(filler, LITERAL_FILLER_LEFT, LITERAL_FILLER_RIGHT);
                    literalValuesList.erase(std::remove(inBetween.begin(), inBetween.end(), '"'), inBetween.end());
                    vector<string> sValues = split(literalValuesList, LITERAL_FILLER_SEPARATOR[0]);
                    
                    if(sValues.size() == 0)
                        throw std::runtime_error("Invalid literal dataset specification.");
                    try
                    {
                        switch(dsType)
                        {
                            case INTEGER_TYPE :
                            {    
                                ds = storageManager->Create(dsType, sValues.size());
                                auto handler = storageManager->GetHandler(ds);
                                int32_t * buffer = (int32_t*) handler->GetBuffer();

                                for(int64_t i = 0; i < sValues.size(); i++)
                                {
                                    buffer[i] = stoi(sValues[i].c_str());
                                }

                                handler->Close();
                                break;
                            }    
                            case LONG_TYPE :
                            {
                                ds = storageManager->Create(dsType, sValues.size());
                                auto handler = storageManager->GetHandler(ds);
                                int64_t * buffer = (int64_t*) handler->GetBuffer();

                                for(int64_t i = 0; i < sValues.size(); i++)
                                    buffer[i] = stol(sValues[i]);

                                handler->Close();
                                break;
                            }
                            case FLOAT_TYPE :
                            {
                                ds = storageManager->Create(dsType, sValues.size());
                                auto handler = storageManager->GetHandler(ds);
                                float * buffer = (float*) handler->GetBuffer();

                                for(int64_t i = 0; i < sValues.size(); i++)
                                    buffer[i] = stof(sValues[i].c_str());

                                handler->Close();
                                break;
                            }    
                            case DOUBLE_TYPE :
                            {
                                ds = storageManager->Create(dsType, sValues.size());
                                auto handler = storageManager->GetHandler(ds);
                                double * buffer = (double*) handler->GetBuffer();

                                for(int64_t i = 0; i < sValues.size(); i++)
                                    buffer[i] = stod(sValues[i].c_str());

                                handler->Close();
                                break;
                            }
                        }
                    }
                    catch(std::invalid_argument& e)
                    {
                        throw std::runtime_error("Invalid literal dataset specification. Could not parse numerical values.");
                    }
                    catch(std::out_of_range& e)
                    {
                        throw std::runtime_error("Invalid literal dataset specification. Numerical value is out of range.");
                    }
                    
                    ds->id = UNSAVED_ID;
                    ds->name = dsName;
                    
                }
                else
                {
                    throw std::runtime_error("File does not exist: "+file+".");
                }
            }
            else
            {
                ds->id = UNSAVED_ID;
                ds->name = dsName;
                ds->type = dsType;
                ds->sorted = false;
                ds->location = file;
                ds->length = FILE_SIZE(file.c_str());
                ds->entry_count = ds->length/typeSize;
            }
 
            TARSPtr defaultTARS = metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));
            
            if(storageManager->Save(ds) == SAVIME_FAILURE)
            {
                throw std::runtime_error("Could not save dataset: not enough space left. Consider increasing the max storage size.");
            }
            
            if(metadataManager->SaveDataSet(defaultTARS, ds) == SAVIME_FAILURE)
            {
                throw std::runtime_error("Could not save dataset.");
            }
        }
        else
        {
            throw std::runtime_error("Invalid dataset definition.");
        }
    }
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
    
    return SAVIME_SUCCESS;
}

void create_bounds(DimSpecPtr dimensionSpecification, std::string lowerBound, std::string upperBound, StorageManagerPtr storageManager)
{
    LogicalIndex index;
    index.type = DOUBLE_TYPE;
    
    if(lowerBound.c_str()[0] == '#')
    {
        lowerBound = lowerBound.replace(0, 1, "");
        
        double val = atof(lowerBound.c_str());
        SET_LOGICAL_INDEX(index, val);
        dimensionSpecification->lower_bound = storageManager->Logical2Real(dimensionSpecification->dimension->GetDimension(), index);

        if(dimensionSpecification->lower_bound  == -1)
            throw std::runtime_error("Invalid logical index: "+lowerBound);
    }
    else
    {
        dimensionSpecification->lower_bound = atoll(lowerBound.c_str());
    }

    if(upperBound.c_str()[0] == '#')
    {
        upperBound = upperBound.replace(0, 1, "");
        double val = atof(upperBound.c_str());
        SET_LOGICAL_INDEX(index, val);
        dimensionSpecification->upper_bound = storageManager->Logical2Real(dimensionSpecification->dimension->GetDimension(), index);

        if(dimensionSpecification->upper_bound  == -1)
            throw std::runtime_error("Invalid logical index: "+upperBound);
    }
    else
    {
        dimensionSpecification->upper_bound = atoll(upperBound.c_str());
    }
}

/* Auxiliary functions for load_subtar operator:
 * 
 * DimensionSpecs can be either ORDERED, PARTIAL or TOTAL:
 * 
 * ORDERED for and implicit dimension is the simplest possible case. Indexes values can 
 * be generated automatically, lower and upper bounds are actual index values.
 * ORDERED for explicit dimensions means that dimensions values require only a look up in the dimension dataset, 
 * but that they follow a well-behaved pattern. Lower and upper bounds can be actual 
 * index values (real indexes) or lookup values (logical indexes) using the syntax: #value.
 * 
 * PARTIAL means that the dimension is not conforming with original dimension definition, having a particular mapping 
 * of values that repeat themselves index into a value in the mapping. There is a dataset that specifies positions.
 * For implicit and spaced dimensions, the partial mappings give the direct dimension values (real indexes).
 * For explicit dimensions, the partial mapping gives lookup values (logical indexes) into the dimensions dataset.
 * 
 * TOTAL means that the dimension is not conforming with original dimension definition, having a particular mapping of 
 * values for the entire subtar. It means that values for dimensions can not be derived, because they are not well-behaved, 
 * instead they are explicitly stored.
 * For implicit dimensions, the full mappings gives the direct dimension values (real indexes);
 * For explicit dimensions, the partial mapping gives lookup values (logical indexes) into the dataset dimensions specification dataset.
 * 
 * If one of the dimension specifications in a subtar is TOTAL, then all others must also be TOTAL. 
 * Users can use # before a logical index to convert it to a real one in subtar loading command call.
 */
std::list<DimSpecPtr> create_dimensionsSpecs(std::string dimSpecsStringBlock, TARPtr tar, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager)
{
    bool hasTotalDim = false;
    bool hasNonTotalDim = false;
    std::string dimensionName;
    std::list<DimSpecPtr> dimensionsSpecs;  int64_t totalLen = 1;
    std::vector<std::string> dimSpecsBlocks = split(dimSpecsStringBlock, '|');
    
    for(std::string dimSpecsString : dimSpecsBlocks)
    {
        std::vector<std::string> params = split(dimSpecsString, ',');
        DimSpecPtr dimensionSpecification = DimSpecPtr(new DimensionSpecification);
        dimensionSpecification->id = UNSAVED_ID;
        dimensionSpecification->type = STR2SPECTYPE(trim(params[0]).c_str());
        
        dimensionName = trim(params[1]);
        DataElementPtr dataElement = tar->GetDataElement(dimensionName); 
            
        if(dataElement == NULL)
            throw std::runtime_error("Invalid dimension name: "+dimensionName+"."); 
        
        if(dataElement->GetType() != DIMENSION_SCHEMA_ELEMENT)    
               throw std::runtime_error(dimensionName+" is not a dimension.");
        
        dimensionSpecification->dimension = dataElement;
        std::string lowerBound = trim(params[2]);
        std::string upperBound = trim(params[3]);
        create_bounds(dimensionSpecification, lowerBound, upperBound, storageManager);
        
        if(dimensionSpecification->type == ORDERED)
        {
            hasNonTotalDim = true; 
            if(params.size() < 4 )
                throw std::runtime_error("Invalid dimension specification definition. Wrong number of parameters.");
        }
        else if(dimensionSpecification->type == PARTIAL || dimensionSpecification->type == TOTAL )
        { 
            if(dimensionSpecification->type == TOTAL)
                hasTotalDim = true;
            else
                hasNonTotalDim = true;
                
            if(params.size() < 5)
                throw std::runtime_error("Invalid dimension specification definition. Wrong number of parameters.");

            DatasetPtr ds = metadataManager->GetDataSetByName(trim(params[4]));
            if(ds == NULL)
                throw std::runtime_error("Invalid dataset name: "+trim(params[4]));
            
            if(!ds->sorted)
                storageManager->CheckSorted(ds);
            
            dimensionSpecification->dataset = ds;
        }
        else
        {
            throw std::runtime_error("Invalid dimension specification type: "+params[0]+".");
        }
        
        if(dimensionSpecification->upper_bound < dimensionSpecification->lower_bound)
                throw std::runtime_error("Error in dimension specification for "+dimensionName+". Upper bound must be greater than the lower bound.");
        
        if(hasTotalDim && hasNonTotalDim)
                throw std::runtime_error("Invalid dimension specification for subtar: Combination of total and non-total specifications.");
        
        dimensionsSpecs.push_back(dimensionSpecification);
    }
        
    std::map<std::string, std::string> dimensionsInTar;
    for(auto d: tar->GetDimensions())
    {
        dimensionsInTar[d->name] = d->name;
    }
    
    for(auto spec : dimensionsSpecs)
    {
        if (dimensionsInTar.find(spec->dimension->GetName())
             != dimensionsInTar.end())
        {
            dimensionsInTar.erase(spec->dimension->GetName());
        }
        else
        {
            throw std::runtime_error("Duplicated specification definition for dimension "+spec->dimension->GetName()+".");
        }    
    }
    
    if(dimensionsInTar.size() > 0)
    {
        throw std::runtime_error("Insufficient specification definition, there are dimensions in "+tar->GetName()+" without definition.");
    }
    
    for(auto spec : dimensionsSpecs)
    {
        totalLen *= spec->GetLength();
    }
    
    for(auto spec : dimensionsSpecs)
    {
        bool isPosterior = false;
        spec->skew = 1;
        spec->adjacency = 1;
        
        for(auto innerSpec : dimensionsSpecs)
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
    
    return dimensionsSpecs;
}

int validate_subtar_size(SubtarPtr subtar)
{   
    int64_t totalLength = subtar->GetTotalLength();
    
    for(auto entry : subtar->GetDimSpecs())
    {
        if(entry.second->type == TOTAL)
        {
            if(totalLength != entry.second->dataset->entry_count)
                throw std::runtime_error("There are dimension specifications of total type with invalid sizes: "+
                                         entry.second->dimension->GetName()+".");
        }
    }
}

int validate_dimensionSpecs(DimSpecPtr dimSpec, StorageManagerPtr storageManager)
{
    auto ds = dimSpec->dataset;
    auto dataElement = dimSpec->dimension;

    if(dataElement->GetDimension()->dimension_type == IMPLICIT)
    {
        if(dimSpec->type == ORDERED)
        {
            return SAVIME_SUCCESS;
        }
        else if(dimSpec->type == PARTIAL || dimSpec->type == TOTAL)
        {
            DatasetPtr realIndexesDs;
            
            //Check if the dataset type is the same as dimensions type
            if(ds->type != dataElement->GetDataType())
                throw std::runtime_error("Incompatible types for dimension specification and dimension "+dataElement->GetName()+".");
            
            //Check if a dataset entry_count is smaller than the dimspec length
            int64_t length = dimSpec->upper_bound - dimSpec->lower_bound + 1;
            if(dimSpec->type == PARTIAL && length < ds->entry_count)
                throw std::runtime_error("Data set "+ds->name+" is too large for dimension specification for dimension "+dataElement->GetName()+".");
            
            //Check if every value in the dataset is valid. Values must be logic indexes that are mapped 
            //to real indexes between dimspecs->lower_bound and dimspecs->upper_bound and aligned.
            if(storageManager->Logical2Real(dimSpec->dimension->GetDimension(), dimSpec, dimSpec->dataset, realIndexesDs) != SAVIME_SUCCESS)
                throw std::runtime_error("Data set "+ds->name+" contains invalid values for dimension specification.");
            
        }
    }
    else if(dataElement->GetDimension()->dimension_type == EXPLICIT)
    {
        if(dimSpec->type == ORDERED)
        {
            return SAVIME_SUCCESS;
        }
        else if(dimSpec->type == PARTIAL || dimSpec->type == TOTAL)
        {
            DatasetPtr realIndexesDs;
            
            //Check if the dataset type is LONG typed
            if(ds->type != LONG_TYPE)
                throw std::runtime_error("Incompatible types for dataset "+ds->name+" dimension specification of explicit dimension "+dataElement->GetName()+". It must be long typed.");
            
            //Check if ds entry count is smaller than the dimspec length
            int64_t length = dimSpec->upper_bound - dimSpec->lower_bound + 1;
            if(dimSpec->type == PARTIAL && length < ds->entry_count)
                throw std::runtime_error("Data set "+ds->name+" is too large for dimension specification for dimension "+dataElement->GetName()+".");
            
            //Check if every value in the dataset is valid. Values must be real indexes 
            //between Dimspecs->lower and Dimspecs->upper and aligned
            if(storageManager->Real2Logical(dimSpec->dimension->GetDimension(), dimSpec, dimSpec->dataset, realIndexesDs) != SAVIME_SUCCESS)
                throw std::runtime_error("Data set "+ds->name+" contains invalid values for dimension specification.");   
        }
    }
}

/*
 * LOAD_SUBTAR("tar_name", "dimension_specs", "dataset_specs");
 * dimension_specs  
 *      ordered: type, dimensionName, lower_bound, upper_bound, order |
 *      partial: type, dimensionName, lower_bound, upper_bound, dataset | 
 *      total:   type, dimensionName, lower_bound, upper_bound, dataset |
 * dataset_specs: attribute_name, dataset_name | ...
 */
int insert_subtar(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        std::list<ParameterPtr>  parameters = operation->GetParameters();
        ParameterPtr parameter = parameters.front();
        std::string tarName = parameter->literal_str;
        tarName.erase(std::remove(tarName.begin(), tarName.end(), '"'), tarName.end());
        parameters.pop_front();
        
        parameter = parameters.front();
        std::string dimSpecsBlock = parameter->literal_str;
        dimSpecsBlock.erase(std::remove(dimSpecsBlock.begin(), dimSpecsBlock.end(), '"'), dimSpecsBlock.end());
        parameters.pop_front();
        
        parameter = parameters.front();
        std::string dsSpecsBlock = parameter->literal_str;
        dsSpecsBlock.erase(std::remove(dsSpecsBlock.begin(), dsSpecsBlock.end(), '"'), dsSpecsBlock.end());
        parameters.pop_front();
       
        TARSPtr defaultTARS = metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));
        TARPtr tar = metadataManager->GetTARByName(defaultTARS, tarName);
        
        if(tar == NULL)
            throw std::runtime_error(tarName+" does not exist.");
        
        std::list<DimSpecPtr> dimSpecs = create_dimensionsSpecs(dimSpecsBlock, tar, metadataManager, storageManager);
        
        SubtarPtr subtar = SubtarPtr(new Subtar());
        subtar->SetTAR(tar);
        subtar->SetId(UNSAVED_ID);
        
        for(auto dimSpec : dimSpecs)
        {
            subtar->AddDimensionsSpecification(dimSpec);
        }

        validate_subtar_size(subtar);
        
        for(auto dimSpec : dimSpecs)
        {
            validate_dimensionSpecs(dimSpec, storageManager);
        }
       
        int64_t subtarTotalLenght = subtar->GetTotalLength();
        std::vector<std::string> dataSetSpecs = split(dsSpecsBlock, '|');
        for(auto dsSpec : dataSetSpecs)
        {
            std::vector<std::string> dsSpecSplit = split(dsSpec, ',');
            
            if(dsSpecSplit.size() == 2)
            {
                std::string attName = trim(dsSpecSplit.front());
                std::string dsName = trim(dsSpecSplit.back());
                DataElementPtr dataElement =  tar->GetDataElement(attName);
                
                if(dataElement != NULL && dataElement->GetType() == ATTRIBUTE_SCHEMA_ELEMENT)
                {
                    AttributePtr att = dataElement->GetAttribute();
                    DatasetPtr ds = metadataManager->GetDataSetByName(dsName);
                    if(ds == NULL)
                        throw std::runtime_error("Dataset " +dsName+" not found.");
                    
                    //if(att->type != ds->type && ds->entryCount < subtarTotalLenght)
                    if(att->type != ds->type)
                        throw std::runtime_error("Dataset " +dsName+" do not conform with attribute or subtar specification.");
                    
                    subtar->AddDataSet(att->name, ds);
                }
                else
                {
                    throw std::runtime_error("Not a valid attribute name: " + attName+".");
                }
            }
            else
            {
                throw std::runtime_error("Invalid datasets definition.");
            }
            
        }
        
        auto intersectionSubtars = tar->GetIntersectingSubtars(subtar);
        if(intersectionSubtars.size() != 0)
                throw std::runtime_error("This new subtar definition intersects with already existing subtar!");
        
        if(metadataManager->SaveSubtar(tar, subtar) == SAVIME_FAILURE)
        {
            throw std::runtime_error("Could not insert subtar.");
        }
       
    }
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
    
    return SAVIME_SUCCESS;
}

int drop_tars(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    return SAVIME_SUCCESS;
}

int drop_tar(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        std::list<ParameterPtr>  parameters = operation->GetParameters();
        ParameterPtr parameter = parameters.front();
        std::string tarName = parameter->literal_str;
        tarName.erase(std::remove(tarName.begin(), tarName.end(), '"'), tarName.end());
        parameters.pop_front();

        int32_t defaultTarsId = configurationManager->GetIntValue(DEFAULT_TARS);
        TARSPtr defaultTars = metadataManager->GetTARS(defaultTarsId);
        TARPtr tar = metadataManager->GetTARByName(defaultTars, tarName);

        if(tar == NULL)
            throw std::runtime_error("There is no TAR named "+tarName+".");

        if(metadataManager->RemoveTar(defaultTars, tar) != SAVIME_SUCCESS)
            throw std::runtime_error("Could not remove "+tarName+".");
    }   
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
     
    return SAVIME_SUCCESS;
}

int drop_type(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        std::list<ParameterPtr>  parameters = operation->GetParameters();
        ParameterPtr parameter = parameters.front();
        std::string typeName = parameter->literal_str;
        typeName.erase(std::remove(typeName.begin(), typeName.end(), '"'), typeName.end());
        parameters.pop_front();

        int32_t defaultTarsId = configurationManager->GetIntValue(DEFAULT_TARS);
        TARSPtr defaultTars = metadataManager->GetTARS(defaultTarsId);
        TypePtr type = metadataManager->GetTypeByName(defaultTars, typeName);

        if(type == NULL)
            throw std::runtime_error("There is no type named "+typeName+".");

        if(metadataManager->RemoveType(defaultTars, type) != SAVIME_SUCCESS)
            throw std::runtime_error("Could not remove type. It was possibly given to at least one TAR.");
    }   
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
     
    return SAVIME_SUCCESS;
}

int drop_dataset(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        std::list<ParameterPtr>  parameters = operation->GetParameters();
        ParameterPtr parameter = parameters.front();
        std::string dsName = parameter->literal_str;
        dsName.erase(std::remove(dsName.begin(), dsName.end(), '"'), dsName.end());
        parameters.pop_front();

        int32_t defaultTarsId = configurationManager->GetIntValue(DEFAULT_TARS);
        TARSPtr defaultTars = metadataManager->GetTARS(defaultTarsId);
        DatasetPtr ds = metadataManager->GetDataSetByName(dsName);

        if(ds == NULL)
            throw std::runtime_error("There is no dataset named "+dsName+".");

        if(metadataManager->RemoveDataSet(defaultTars, ds) != SAVIME_SUCCESS)
            throw std::runtime_error("Could not remove dataset. It is possibly being used by at least one TAR.");    
    }   
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
     
    return SAVIME_SUCCESS;
}

int show(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{ 
    TARSPtr defaultTARS = metadataManager->GetTARS(configurationManager->GetIntValue(DEFAULT_TARS));
    
    try
    {        
        if(defaultTARS == NULL)
            throw std::runtime_error("Invalid default TARS configuration!");
            
        std::string dumpText = defaultTARS->name+" TARS:\n";
        
        for(auto tar : metadataManager->GetTARs(defaultTARS))
        {
            int64_t subtarCount = 0;
            dumpText = dumpText + tar->toString() + "\n";
            
            for(auto subtar : metadataManager->GetSubtars(tar))
            {
                dumpText = dumpText +"\tsubtar #"+to_string(subtarCount++)+" "+subtar->toString() + "\n";
            }
            
            dumpText = dumpText+"\n";
        }
        
        queryDataManager->SetQueryResponseText(dumpText);    
    }
    catch(std::exception& e)
    {
        queryDataManager->SetErrorResponseText(e.what());
        return SAVIME_FAILURE;
    }
    
    return SAVIME_SUCCESS;
}