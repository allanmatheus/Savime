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
#include "schema_builder.h"
#include "default_parser.h"

void SchemaBuilder::SetResultingType(TARPtr inputTAR, TARPtr resultingTAR)
{
    bool resultMaintainsType = true;
    TypePtr type = inputTAR->GetType();
    
    if(type)
    {
        for(auto roleImplementation : inputTAR->GetRoles())
        {
            DataElementPtr de = resultingTAR->GetDataElement(roleImplementation.first);
            
            //if data element doesn't exist
            if(!de)
            {
                //check if it was implementing a mandatory role
                for(auto entry : type->roles)
                {
                    auto role = entry.second;
                    
                    //if the role implementet for the data element is found
                    if(!role->name.compare(roleImplementation.second->name))
                    {
                        //if is mandatory, then resulting TAR can't be of the same type
                        if(role->is_mandatory)
                        {
                            resultMaintainsType = false;
                            break;
                        }
                    }
                }
                
            }
            
            if(!resultMaintainsType) break;           
        }
        
        //If no mandatory data element is missing
        if(resultMaintainsType)
        {
            //Resulting TAR receives inputTAR type
            resultingTAR->AlterType(type);
            
            //Implementing roles
            for(auto roleImplementation : inputTAR->GetRoles())
            {
                DataElementPtr de = resultingTAR->GetDataElement(roleImplementation.first);
            
                //If data element is present, then it still implements the role
                if(de)
                {
                    resultingTAR->GetRoles()[roleImplementation.first] = roleImplementation.second;
                }
            }
        }
    }
}

TARPtr  SchemaBuilder::InferSchemaForScanOp(OperationPtr  operation)
{
    ParameterPtr inputTARParam = operation->GetParameters().front();
    TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
    SetResultingType(inputTARParam->tar, resultingTAR);
    return resultingTAR;
}
 
TARPtr  SchemaBuilder::InferSchemaForSelectOp(OperationPtr  operation)
{
    TARPtr resultingTAR = TARPtr(new TAR(0, "", NULL));
    TARPtr inputTAR = operation-> GetParameters().front()->tar;
    std::map<std::string, DataElementPtr> inputDimensions;
    double totalTARSize = 1;
    
    //Adding input dimensions into a map 
    for(auto& dataElement: inputTAR->GetDataElements())
    {
        if(dataElement->GetType() == DIMENSION_SCHEMA_ELEMENT)
        {
            inputDimensions[dataElement->GetName()] = dataElement;
            totalTARSize *= dataElement->GetDimension()->GetLength();
        }
    }
    
    //Cheking all dimensions are  specified as parameters for select
    for(auto& param : operation->GetParameters())
    {
        if(param->type == LITERAL_STRING_PARAM)
        {
            if(inputDimensions.find(param->literal_str) !=  inputDimensions.end())
            {
                inputDimensions.erase(param->literal_str);
            }
        }
    }
    
    //If map is empty, then all dimensions are presented in the parameters
    if(inputDimensions.size() == 0)
    {
        for(auto& param: operation->GetParameters())
        {
            if(param->type == LITERAL_STRING_PARAM)
            {
                DataElementPtr dataElement = inputTAR->GetDataElement(param->literal_str);
                
                switch(dataElement->GetType())
                {
                    case DIMENSION_SCHEMA_ELEMENT:resultingTAR->AddDimension(dataElement->GetDimension()); break;
                    case ATTRIBUTE_SCHEMA_ELEMENT: resultingTAR->AddAttribute(dataElement->GetAttribute()); break;
                }
            }
           
        }
    }
    else // Otherwise, dimensions have been dropped, they all become variables plus a synthetic dim
    {
        resultingTAR->AddDimension(DEFAULT_SYNTHETIC_DIMENSION, LONG_TYPE, 1, totalTARSize);
        for(auto& param: operation->GetParameters())
        {
            if(param->type == LITERAL_STRING_PARAM)
            {
                DataElementPtr dataElement = inputTAR->GetDataElement(param->literal_str);

                switch(dataElement->GetType())
                {
                    case DIMENSION_SCHEMA_ELEMENT:resultingTAR->AddAttribute(dataElement->GetDimension()->name, dataElement->GetDimension()->type); break;
                    case ATTRIBUTE_SCHEMA_ELEMENT: resultingTAR->AddAttribute(dataElement->GetAttribute()->name, dataElement->GetAttribute()->type); break;
                }
            }
        }
    }
    
    SetResultingType(inputTAR, resultingTAR);
    return resultingTAR;
}

TARPtr  SchemaBuilder::InferSchemaForFilterOp(OperationPtr  operation)
{
    ParameterPtr inputTARParam = operation->GetParameters().front();
    TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
    SetResultingType(inputTARParam->tar, resultingTAR);
    return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForSubsetOp(OperationPtr operation)
{
    ParameterPtr inputTARParam = operation->GetParameters().front();
    TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
    int32_t numberOfDimensions = (operation->GetParameters().size()-1)/3;
    
    for(int32_t i = 0; i < numberOfDimensions; i++)
    {
        auto param = operation->GetParametersByName(DIM(i));        
        auto dataElement = resultingTAR->GetDataElement(param->literal_str);
        auto dim = dataElement->GetDimension();
        
        if(dim->dimension_type == IMPLICIT)
        {
            dim->lower_bound = operation->GetParametersByName(LB(i))->literal_dbl;
            dim->lower_bound += dim->lower_bound - ((int64_t)(dim->lower_bound/dim->spacing))*dim->spacing;
            dim->upper_bound = operation->GetParametersByName(UP(i))->literal_dbl;
            dim->upper_bound -= dim->upper_bound - ((int64_t)(dim->upper_bound/dim->spacing))*dim->spacing;
        }
        /*else
        {
            dim->lower_bound = operation->GetParametersByName(LB(i))->literal_dbl;
            dim->lower_bound += dim->lower_bound - ((int64_t)(dim->lower_bound/dim->spacing))*dim->spacing;
            dim->upper_bound = operation->GetParametersByName(UP(i))->literal_dbl;
            dim->upper_bound -= dim->upper_bound - ((int64_t)(dim->upper_bound/dim->spacing))*dim->spacing;
        }*/
       
        dim->real_lower_bound = 0;
        dim->real_upper_bound = dim->GetLength()-1;
    }
    
    SetResultingType(inputTARParam->tar, resultingTAR);
    return resultingTAR;
}

TARPtr  SchemaBuilder::InferSchemaForLogicalOp(OperationPtr  operation)
{
    ParameterPtr inputTARParam = operation->GetParametersByName(INPUT_TAR);
    assert(inputTARParam);
    TARPtr  resultingTAR = inputTARParam->tar->Clone(false, false, true);
    assert(inputTARParam);
    resultingTAR->AddAttribute(DEFAULT_MASK_ATTRIBUTE, BOOLEAN_TYPE);
    resultingTAR->AddAttribute(DEFAULT_OFFSET_ATTRIBUTE, LONG_TYPE);
    return resultingTAR;
}

TARPtr  SchemaBuilder::InferSchemaForComparisonOp(OperationPtr  operation)
{
    ParameterPtr inputTARParam = operation->GetParametersByName(INPUT_TAR);
    assert(inputTARParam);
    TARPtr  resultingTAR = inputTARParam->tar->Clone(false, false, true);
    assert(inputTARParam);
    resultingTAR->AddAttribute(DEFAULT_MASK_ATTRIBUTE, BOOLEAN_TYPE);
    resultingTAR->AddAttribute(DEFAULT_OFFSET_ATTRIBUTE, LONG_TYPE);
    return resultingTAR;
}

TARPtr  SchemaBuilder::InferSchemaForArithmeticOp(OperationPtr  operation)
{
    DataType  newMemberType = INTEGER_TYPE; 
    ParameterPtr inputTARParam = operation->GetParametersByName(INPUT_TAR);
    assert(inputTARParam);
    TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
    assert(inputTARParam);
     
    const char * op = operation->GetParametersByName(OP)->literal_str.c_str();
    ParameterPtr operand0 = operation->GetParametersByName(OPERAND(0));
    ParameterPtr operand1 = operation->GetParametersByName(OPERAND(1));
    DataType t1, t2;

    if(operand1 == NULL)
    {
        operand1 = ParameterPtr(new Parameter("dummy", (double)0));
    }

    if(operand0->type == LITERAL_DOUBLE_PARAM)
    {
       double operand = operand0->literal_dbl;

       if(operand == ((int32_t)operand))
           t1 = INTEGER_TYPE;
       else if(operand == ((int64_t)operand))
           t1 = LONG_TYPE;
       else if(operand == ((float)operand))
           t1 = FLOAT_TYPE;
       else
           t1 = DOUBLE_TYPE;
    }
    else if(operand0->type == LITERAL_STRING_PARAM)
    {
        t1 = inputTARParam->tar->GetDataElement(operand0->literal_str)->GetDataType();
    }        

    if(operand1->type == LITERAL_DOUBLE_PARAM)
    {
        double operand = operand1->literal_dbl;
        if(operand == ((int32_t)operand))
            t2 = INTEGER_TYPE;
        else if(operand == ((int64_t)operand))
            t2 = LONG_TYPE;
        else if(operand == (float)operand)
            t2 = FLOAT_TYPE;
        else
            t2 = DOUBLE_TYPE;
    }
    else if(operand1->type == LITERAL_STRING_PARAM)
    {
         t2 = inputTARParam->tar->GetDataElement(operand1->literal_str)->GetDataType();
    }

    newMemberType = SelectType(t1, t2, op);
    
    ParameterPtr newMemberParam = operation->GetParametersByName(NEW_MEMBER);
    if(newMemberParam)
    {
        resultingTAR->AddAttribute(newMemberParam.get()->literal_str, newMemberType); 
    }
    else
    {
        resultingTAR->AddAttribute("op_result", DOUBLE_TYPE);
    }
    
    return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForCrossOp(OperationPtr  operation)
{
    #define NUM_TARS 2
    char* prefix[] = {LEFT_DATAELEMENT_PREFIX, RIGHT_DATAELEMENT_PREFIX};
    TARPtr leftTARParam = operation->GetParameters().front()->tar;
    TARPtr rightTARParam = operation->GetParameters().back()->tar;
    TARPtr tars[] = {leftTARParam, rightTARParam}; 
    TARPtr resultingTAR = TARPtr(new TAR(0, "", NULL));
    
    for(int32_t i = NUM_TARS-1; i >= 0; i--)
    {
        for(auto dim : tars[i]->GetDimensions())
        {
            resultingTAR->AddDimension(dim);
            
            if(i == 1)
            {
                DimensionPtr _dim = resultingTAR->GetDataElement(dim->name)->GetDimension();
                _dim->name = prefix[i]+_dim->name;
            }
        }
        
        for(auto att : tars[i]->GetAttributes())
        {
            resultingTAR->AddAttribute(att);
            
            if(i == 1)
            {
                AttributePtr _att = resultingTAR->GetDataElement(att->name)->GetAttribute();
                _att->name = prefix[i]+_att->name;
            }
        }
    }
   
    SetResultingType(resultingTAR, leftTARParam);
    return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForDimJoinOp(OperationPtr operation)
{
    #define NUM_TARS 2
    char* prefix[] = {LEFT_DATAELEMENT_PREFIX, RIGHT_DATAELEMENT_PREFIX}; int32_t count = 0;
    TARPtr leftTARParam = operation->GetParametersByName(OPERAND(0))->tar;
    TARPtr rightTARParam = operation->GetParametersByName(OPERAND(1))->tar;
    TARPtr tars[] = {leftTARParam, rightTARParam}; 
    TARPtr resultingTAR = TARPtr(new TAR(0, "", NULL));
    
    map<string, string> leftDims, rightDims;
    
    while(true)
    {
        auto param1 = operation->GetParametersByName(DIM(count++));
        auto param2 = operation->GetParametersByName(DIM(count++));
        if(param1 == NULL) break;
        leftDims[param1->literal_str] = param2->literal_str;
        rightDims[param2->literal_str] = param1->literal_str;
    }
    
    for(auto dim : leftTARParam->GetDimensions())
    {
        if(leftDims.find(dim->name) == leftDims.end())
        {
            resultingTAR->AddDimension(dim);
            DimensionPtr _dim = resultingTAR->GetDataElement(dim->name)->GetDimension();
            _dim->name =  prefix[0]+_dim->name;
        }
        else
        {
            DimensionPtr newDim;
            string leftDimName = dim->name;
            string rightDimName = leftDims[dim->name];
            auto leftDim = leftTARParam->GetDataElement(leftDimName)->GetDimension();
            auto rightDim = leftTARParam->GetDataElement(rightDimName)->GetDimension();
            _storageManager->IntersectDimensions(leftDim, rightDim, newDim);
            newDim->name =  prefix[0]+leftDimName;
            resultingTAR->AddDimension(newDim);
        }
    }
    
    for(auto dim : rightTARParam->GetDimensions())
    {
        if(rightDims.find(dim->name) == rightDims.end())
        {
            resultingTAR->AddDimension(dim);
            DimensionPtr _dim = resultingTAR->GetDataElement(dim->name)->GetDimension();
            _dim->name =  prefix[1]+_dim->name;
        }
    }
    
    for(int32_t i = NUM_TARS-1; i >= 0; i--)
    {
        
        for(auto att : tars[i]->GetAttributes())
        {
            resultingTAR->AddAttribute(att);
            
            //if(i == 1)
            {
                AttributePtr _att = resultingTAR->GetDataElement(att->name)->GetAttribute();
                _att->name = prefix[i]+_att->name;
            }
        }
    }
   
    SetResultingType(resultingTAR, leftTARParam);
    return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForAggregationOp(OperationPtr operation)
{
    ParameterPtr inputTARParam = operation->GetParametersByName(INPUT_TAR);
    TARPtr inputTAR = inputTARParam->tar;
    TARPtr resultingTAR = TARPtr(new TAR(0, "", NULL));
    int32_t countOp = 2, dims = 0;
    
    while(true)
    {
       auto param = operation->GetParametersByName(DIM(dims++));
       if(param == NULL) break;
       auto dataElement = inputTAR->GetDataElement(param->literal_str);
       resultingTAR->AddDimension(dataElement->GetDimension());
    }
    
    if(resultingTAR->GetDataElements().size() == 0)
    {
        resultingTAR->AddDimension(DEFAULT_SYNTHETIC_DIMENSION, INTEGER_TYPE, 0, 0);
    }
    
    while(true)
    {
       auto param = operation->GetParametersByName(OPERAND(countOp));
       if(param == NULL) break;
       resultingTAR->AddAttribute(param->literal_str, DOUBLE_TYPE);
       countOp+=3;
    }
    
    SetResultingType(inputTARParam->tar, resultingTAR);
    return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForSplitOp(OperationPtr operation)
{
    ParameterPtr inputTARParam = operation->GetParameters().front();
    TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
    SetResultingType(inputTARParam->tar, resultingTAR);
    return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForUserDefined(OperationPtr operation)
{
    //Get operator name
    std::string operatorName = operation->GetName(); 
    
    //Get schema infer string
    std::string inferSchemaString = _configurationManager->GetStringValue(OPERATOR_SCHEMA_INFER_STRING(operatorName.c_str()));
    throw std::runtime_error("Infer schema for user defined functions not implemented yet.");
    
    return NULL;
}

TARPtr SchemaBuilder::InferSchema(OperationPtr operation)
{
    //switch(operation->GetOperation())
    if(operation->GetOperation() == TAL_SCAN)
    {
        return InferSchemaForScanOp(operation);
    }
    else if(operation->GetOperation() == TAL_SELECT)
    {
        return InferSchemaForSelectOp(operation);
    }
    else if(operation->GetOperation() == TAL_FILTER)
    {
        return InferSchemaForFilterOp(operation);
    }
    else if(operation->GetOperation() == TAL_SUBSET)
    {
        return InferSchemaForSubsetOp(operation);
    }
    else if(operation->GetOperation() == TAL_LOGICAL)
    {
        return InferSchemaForLogicalOp(operation);
    }
    else if(operation->GetOperation() == TAL_COMPARISON)
    {
        return InferSchemaForComparisonOp(operation);
    }
    else if(operation->GetOperation() == TAL_ARITHMETIC)
    {
        return InferSchemaForArithmeticOp(operation);
    }
    else if(operation->GetOperation() == TAL_CROSS)
    {
        return InferSchemaForCrossOp(operation);
    }
    else if(operation->GetOperation() == TAL_DIMJOIN)
    {
        return InferSchemaForDimJoinOp(operation);
    }
    else if(operation->GetOperation() == TAL_AGGREGATE)
    {
        return InferSchemaForAggregationOp(operation);
    }
    else if(operation->GetOperation() == TAL_SPLIT)
    {
        return InferSchemaForSplitOp(operation);
    }
    else if(operation->GetOperation() == TAL_USER_DEFINED)
    {
        return InferSchemaForUserDefined(operation);
    }
    
    return NULL;
}