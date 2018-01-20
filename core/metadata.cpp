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
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "include/metadata.h"
#include "include/parser.h"
#include "include/util.h"


using namespace std;

//------------------------------------------------------------------------------
//Declarations
const char * dataTypeNames[]  = {"string", "float", "double", "boolean", "int", "long"};
const char * dimTypeNames[]   = {"explicit", "implicit", "spaced"};
const char * specsTypeNames[] = {"ordered", "partial", "total", "morton"};

std::mutex TAR::_mutex; 
std::vector<int64_t> TAR::_intersectingSubtarsIndexes;
    
//------------------------------------------------------------------------------
//Misc functions
int32_t TYPE_SIZE(DataType type)
{
    switch(type)
    {
        case STRING_TYPE  : return -1; break;
        case BOOLEAN_TYPE : return 1; break;
        case DOUBLE_TYPE  : return 8; break;
        case FLOAT_TYPE   : return 4; break;
        case INTEGER_TYPE : return 4; break;
        case LONG_TYPE    : return 8; break;
    }

    return 0;
}
    
DataType STR2TYPE(const char * type)
{
    switch(type[0])
    {
        case 's' : return STRING_TYPE;
        case 'f' : return FLOAT_TYPE;
        case 'd' : return DOUBLE_TYPE;
        case 'b' : return BOOLEAN_TYPE;
        case 'i' : return INTEGER_TYPE;
        case 'l' : return LONG_TYPE;
        default : return NO_TYPE;
    }
}

DimensionType STR2DIMTYPE(const char * type)
{
    switch(type[0])
    {
        case 'e' : return EXPLICIT;
        case 'i' : return IMPLICIT;
        default : return NO_DIM_TYPE;
    }
}

SpecsType STR2SPECTYPE(const char * type)
{
    switch(type[0])
    {
        case 'o' : return ORDERED;
        case 'p' : return PARTIAL;
        case 't' : return TOTAL;
        default :  NO_SPEC_TYPE;
    }
}

bool compareAdj(DimSpecPtr a, DimSpecPtr b) { return (a->adjacency>b->adjacency);}

//------------------------------------------------------------------------------
//Dataset member functions
Dataset::~Dataset()
{
    for(auto listener : _listeners)
        listener->DisposeObject((MetadataObject*)this);
}

//------------------------------------------------------------------------------
//Dimension member functions
Dimension::~Dimension()
{
    for(auto listener : _listeners)
        listener->DisposeObject((MetadataObject*)this);
}

//------------------------------------------------------------------------------
//Attribute member functions
Attribute::~Attribute()
{
   for(auto listener : _listeners)
        listener->DisposeObject((MetadataObject*)this);
}

//------------------------------------------------------------------------------
//TARS member functions
TARS::~TARS()
{
   for(auto listener : _listeners)
       listener->DisposeObject((MetadataObject*)this);
}

//------------------------------------------------------------------------------
//DATAELEMENT member Functions
DataElement::DataElement(DimensionPtr dimension)
{
    _dimension = dimension;
    _type = DIMENSION_SCHEMA_ELEMENT;
}

DataElement::DataElement(AttributePtr attribute)
{
    _attribute = attribute;
    _type = ATTRIBUTE_SCHEMA_ELEMENT;
}
    
DimensionPtr DataElement::GetDimension()
{ 
    return _dimension;
}

AttributePtr DataElement::GetAttribute()
{
    return _attribute;
}

DataElementType DataElement::GetType()
{
    return _type;
}

std::string DataElement::GetName()
{
    switch(_type)
    {
        case DIMENSION_SCHEMA_ELEMENT : return _dimension->name; break;
        case ATTRIBUTE_SCHEMA_ELEMENT : return _attribute->name; break;
        default : return "";
    }    
}

DataType DataElement::GetDataType()
{
    switch(_type)
    {
        case DIMENSION_SCHEMA_ELEMENT : return _dimension->type; break;
        case ATTRIBUTE_SCHEMA_ELEMENT : return _attribute->type; break;
    }   
}

bool DataElement::IsNumeric()
{
    DataType dataType;
    
    switch(_type)
    {
        case DIMENSION_SCHEMA_ELEMENT : dataType = _dimension->type; break;
        case ATTRIBUTE_SCHEMA_ELEMENT : dataType = _attribute->type; break;
        default : return "";
    }  
        
    return dataType == DOUBLE_TYPE 
           || dataType == FLOAT_TYPE
           || dataType == INTEGER_TYPE 
           || dataType == LONG_TYPE;
}

DataElement::~DataElement()
{
    for(auto listener : _listeners)
        listener->DisposeObject((MetadataObjectPtr)this);
}

//------------------------------------------------------------------------------
//Subtar members declarations
int32_t Subtar::GetId()
{
    return _id;
}

void Subtar::SetId(int32_t id)
{
    _id = id;
}

void Subtar::SetTAR(TARPtr tar)
{
    _tar = tar;
}

TARPtr Subtar::GetTAR()
{
    return _tar;
}

std::map<std::string, DimSpecPtr>& Subtar::GetDimSpecs()
{
    return _dimSpecs;
}

std::map<std::string, DatasetPtr>&  Subtar::GetDataSets()
{
    return _dataSets;
}

int64_t Subtar::GetTotalLength()
{
    int64_t totalLength = 1;
    for(auto entry : _dimSpecs)
    {
        if(entry.second->type == ORDERED || entry.second->type == PARTIAL)
        {
            totalLength *= entry.second->GetLength();
        }
        else if(entry.second->type == TOTAL)
        {
            return entry.second->GetLength();
        }
    }
    return totalLength;
}

bool Subtar::AddDimensionsSpecification(DataElementPtr dimension, 
                                        size_t offset,  
                                        int64_t lowerBound, 
                                        int64_t upperBound, 
                                        int64_t adjacency,  
                                        int64_t skew,
                                        SpecsType type, 
                                        DatasetPtr associatedDataSet)
{
    if(dimension == NULL)
        return false;
    
    if(_tar != NULL && _tar->GetDataElement(dimension->GetName()) == NULL)
        return false;
    
    if(_tar != NULL && _tar->GetDataElement(dimension->GetName())->GetType() 
                        == ATTRIBUTE_SCHEMA_ELEMENT)
        return false;
    
    DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification);
    dimSpecs->dimension = dimension;
    dimSpecs->offset = offset;
    dimSpecs->lower_bound = lowerBound;
    dimSpecs->upper_bound = upperBound;
    dimSpecs->adjacency = adjacency;
    dimSpecs->skew = skew;
    dimSpecs->type = type;
    dimSpecs->dataset = associatedDataSet;
    _dimSpecs[dimSpecs->dimension->GetName()] = dimSpecs;
    return true; 
}

bool Subtar::AddDimensionsSpecification(DimSpecPtr dimSpecs)
{
    if(dimSpecs->dimension == NULL)
        return false;
    
    if(_tar != NULL && _tar->GetDataElement(dimSpecs->dimension->GetName()) == NULL)
        return false;
    
    if(_tar != NULL && _tar->GetDataElement(dimSpecs->dimension->GetName())->GetType() 
                        == ATTRIBUTE_SCHEMA_ELEMENT)
        return false;
    
    _dimSpecs[dimSpecs->dimension->GetName()] = dimSpecs;
    return true;
}


bool Subtar::AddDataSet(string dataElementName, DatasetPtr dataset)
{
   if(dataset == NULL)
        return false;
    
   if(_tar != NULL && _tar->GetDataElement(dataElementName) == NULL)
        return false;
   
   if(_tar != NULL && _tar->GetDataElement(dataElementName)->GetType() 
                        == DIMENSION_SCHEMA_ELEMENT)
        return false;
    
    _dataSets[dataElementName] = dataset;
    return true;
}

void Subtar::CreateBoundingBox(int64_t min[], int64_t max[], int32_t n)
{
    int32_t i = 0;
    for(auto dimension : _tar->GetDimensions())
    {
        auto dimSpec = _dimSpecs[dimension->name];
        min[i] = dimSpec->lower_bound;
        max[i] = dimSpec->upper_bound;
        if(++i>=n) break;
    }
}

DimSpecPtr Subtar::GetDimensionSpecificationFor(string name)
{
    if(_dimSpecs.find(name) == _dimSpecs.end())
        return NULL;
    
    return _dimSpecs[name];
}

DatasetPtr Subtar::GetDataSetFor(string name)
{
    if(_dataSets.find(name) == _dataSets.end())
        return NULL;
    
    return _dataSets[name];
}

void Subtar::RemoveTempDataElements()
{
    list<string> toRemove;
    
    for(auto entry : _dataSets)
    {
         if(entry.first.compare(0, 4, DEFAULT_TEMP_MEMBER) == 0)
         {
             toRemove.push_back(entry.first);
         }
    }
    
    for(auto dataElement : toRemove)
    {
        _dataSets.erase(dataElement);
    }
    toRemove.clear();
    
    for(auto entry : _dimSpecs)
    {
         if(entry.first.compare(0, 4, DEFAULT_TEMP_MEMBER) == 0)
         {
             toRemove.push_back(entry.first);
         }
    }
    
    for(auto dataElement : toRemove)
    {
        _dimSpecs.erase(dataElement);
    }
}

bool Subtar::IntersectsWith(SubtarPtr subtar)
{
    if(_tar == NULL)
        return false;
    
    for(auto dimension : _tar->GetDimensions())
    {
        auto dimSpec1 = _dimSpecs[dimension->name];
        auto dimSpec2 = subtar->GetDimensionSpecificationFor(dimension->name);
        
        if(dimSpec1 == NULL || dimSpec2 == NULL) return false;
        
        if(dimSpec1->lower_bound <= dimSpec2->lower_bound
            && dimSpec2->lower_bound <= dimSpec1->upper_bound) continue;
        
        if(dimSpec1->lower_bound <= dimSpec2->upper_bound
            && dimSpec2->upper_bound <= dimSpec1->upper_bound) continue;
        
        return false;
    }
    
    return true;
}

string Subtar::toString()
{
    string text = "<";
    for(auto entry : this->_dimSpecs)
    {
        std::stringstream lower, upper;
        lower << std::setprecision(6) << entry.second->lower_bound;
        upper << std::setprecision(6) << entry.second->upper_bound;
                        
        text = text +"["+entry.first+"]"+lower.str()+":"+upper.str()+" ";
    }
    
    text = text.substr(0, text.size()-1)+">";
    return text;
}

Subtar::~Subtar()
{
   for(auto listener : _listeners)
        listener->DisposeObject((MetadataObjectPtr)this);
}

//------------------------------------------------------------------------------
//TAR member functions
bool TAR::validateTARName(string& name)
{
    return validadeIdentifier(name);
}

bool TAR::validateSchemaElementName(string& name)
{
    if(!validadeIdentifier(name))
        return false;
    
    if(HasDataElement(name))
        name = name+"-new";
   
    return true;
}

bool TAR::validadeSubtar(SubtarPtr subtar)
{
    if(GetDimensions().size() != subtar->GetDimSpecs().size())
        return false;
    
    for(auto entry: subtar->GetDimSpecs())
    {
        auto dimSpecs = entry.second;
        string dimName = dimSpecs->dimension->GetName();
        auto dataElement = GetDataElement(dimName);
        
        if(dataElement == NULL)
            return false;
        
        if(dataElement->GetType() == ATTRIBUTE_SCHEMA_ELEMENT)
            return false;
    }
    
    return true;
}

TAR::TAR(int32_t id, string name, TypePtr type)
{
    if(validateTARName(name))
        _name = name;
    else
        _name = "";
    
    _id = id;
    _subtarsIndex = NULL;
    _type = type;
}

TAR::TAR(int32_t id, string name, TypePtr type, bool isTemporary)
{
    if(validateTARName(name))
        _name = name;
    else
        _name = "";
    
    _isTemporary = isTemporary;
    _id = id;
    _subtarsIndex = NULL;
    _type = type;
}

bool TAR::SetRole(std::string dataElementName, RolePtr role)
{
    if(_type == NULL)
        return false;
        
    if(_type->roles.find(role->name) == _type->roles.end())
        return false;
            
    if(!HasDataElement(dataElementName))
        return false;
    
    _roles[dataElementName] = role;
    return true;
}

map<std::string, RolePtr>& TAR::GetRoles()
{
    return _roles;
}

list<DimensionPtr> TAR::GetDimensions()
{
    list<DimensionPtr> dims;
    
    for(auto elem : _elements)
    {
        if(elem->GetType() == DIMENSION_SCHEMA_ELEMENT)
        {
            dims.push_back(elem->GetDimension());
        }
    }
    
    return dims;
}

std::list<AttributePtr> TAR::GetAttributes()
{
   std::list<AttributePtr> attribs;
    
   for(auto elem : _elements)
   {
        if(elem->GetType() == ATTRIBUTE_SCHEMA_ELEMENT)
        {
            attribs.push_back(elem->GetAttribute());
        }
   }
   
   return attribs;
}
    
bool TAR::AddDimension(std::string name, DataType type, double lowerBound, double upperBound)
{
    if(!validateSchemaElementName(name) || _subtars.size() > 0)
        return false;

    DimensionPtr dimension = DimensionPtr(new Dimension());
    dimension->name = name;
    dimension->type = type;
    dimension->lower_bound = lowerBound;
    dimension->upper_bound = upperBound;
    dimension->real_lower_bound = 0;
    dimension->real_upper_bound = upperBound-lowerBound;
    dimension->dimension_type = IMPLICIT;
    dimension->spacing = 1;
    
    DataElementPtr dataElement = DataElementPtr(new DataElement(dimension));

    _elements.push_back(dataElement);

    return true;
}

bool TAR::AddDimension(string name, 
                       DataType type, 
                       double lowerBound, 
                       double upperBound, 
                       int64_t realLower, 
                       int64_t realUpper, 
                       double spacing)
{
    if(!validateSchemaElementName(name) || _subtars.size() > 0)
        return false;
    
    DimensionPtr dimension = DimensionPtr(new Dimension());
    dimension->name = name;
    dimension->type = type;
    dimension->lower_bound = lowerBound;
    dimension->upper_bound = upperBound;
    dimension->real_lower_bound = realLower;
    dimension->real_upper_bound = realUpper;
    dimension->spacing = spacing;
    dimension->dimension_type = IMPLICIT;
    dimension->dataset = NULL;

    DataElementPtr dataElement = DataElementPtr(new DataElement(dimension));
    _elements.push_back(dataElement);
    
    return true;
}

bool TAR::AddDimension(string name, 
                       DataType type, 
                       double lowerBound, 
                       double upperBound, 
                       int64_t realLower, 
                       int64_t realUpper, 
                       DatasetPtr dataset)
{
   if(!validateSchemaElementName(name) || _subtars.size() > 0)
        return false;
   
    DimensionPtr dimension = DimensionPtr(new Dimension);
    dimension->name = name;
    dimension->type = type;
    dimension->lower_bound = lowerBound;
    dimension->upper_bound = upperBound;
    dimension->real_lower_bound = realLower;
    dimension->real_upper_bound = realUpper;
    dimension->dimension_type = EXPLICIT;
    dimension->dataset = dataset;
    dimension->spacing = 1;    
    
    DataElementPtr dataElement = DataElementPtr(new DataElement(dimension));
    _elements.push_back(dataElement);
    
    return true;
}

bool TAR::AddDimension(DimensionPtr dimension)
{
    if(dimension->dimension_type == IMPLICIT)
    {
        return AddDimension(dimension->name, 
                            dimension->type, 
                            dimension->lower_bound, 
                            dimension->upper_bound, 
                            dimension->real_lower_bound, 
                            dimension->real_upper_bound, 
                            dimension->spacing);
    }
    else if(dimension->dimension_type == EXPLICIT)
    {
        return AddDimension(dimension->name, 
                            dimension->type, 
                            dimension->lower_bound, 
                            dimension->upper_bound, 
                            dimension->real_lower_bound, 
                            dimension->real_upper_bound, 
                            dimension->dataset);
    }

    return false;
}

bool TAR::AddAttribute(std::string name, DataType type)
{
    if(!validateSchemaElementName(name) || _subtars.size() > 0)
        return false;
    
    AttributePtr attribute = AttributePtr(new Attribute());
    attribute->name = name;
    attribute->type = type;
    attribute->is_property = false;
    DataElementPtr dataElement = DataElementPtr(new DataElement(attribute));

    _elements.push_back(dataElement);
    return true;
}

bool TAR::AddAttribute(string name, DataType type, list<DimensionPtr> dependecies)
{
    if(!validateSchemaElementName(name) || _subtars.size() > 0)
        return false;
    
    AttributePtr attribute = AttributePtr(new Attribute());
    attribute->name = name;
    attribute->type = type;
    attribute->dependecies = dependecies;

    if(dependecies.size() == 0)
        attribute->is_property = true;
    else
        attribute->is_property = false;

    DataElementPtr dataElement = DataElementPtr(new DataElement(attribute));
    _elements.push_back(dataElement);
    return true;
 
}

bool TAR::AddAttribute(AttributePtr attribute)
{
    if(attribute->is_property || attribute->dependecies.size() != 0)
        return AddAttribute(attribute->name, attribute->type, attribute->dependecies);
    else
        return AddAttribute(attribute->name, attribute->type);
}

bool TAR::AddSubtar(SubtarPtr subtar)
{
    if(!validadeSubtar(subtar))
        return false;
    
    int32_t dimensionsNo = GetDimensions().size();
    int64_t min[dimensionsNo], max[dimensionsNo];
    _subtars.push_back(subtar);
    
    if(_subtarsIndex == NULL)
    {
        _subtarsIndex = SubtarsIndex(new RTree<int64_t,int64_t>(dimensionsNo));
    }

    subtar->CreateBoundingBox(min, max, dimensionsNo);
    _subtarsIndex->Insert(min, max, _subtars.size()-1);
    
    return true;
}

const int32_t TAR::GetId()
{
    return _id;
}

const std::string TAR::GetName()
{
    return _name;
}

TypePtr TAR::GetType()
{
    return _type;
}

void TAR::AlterTAR(int32_t newId, std::string newName, bool isTemporary)
{
    if(validateTARName(newName))
    {
        _id = newId;
        _name = newName;
        _isTemporary = isTemporary;
    }
}

void TAR::AlterType(TypePtr type)
{
    _type = type;
    _roles.clear();
}

int32_t TAR::GetTypeId()
{
    return _idType;
}

void TAR::SetTypeId(int32_t idType)
{
    _idType = idType;
}

std::vector<SubtarPtr>& TAR::GetSubtars()
{
    return _subtars;
}

 vector<SubtarPtr> TAR::GetIntersectingSubtars(SubtarPtr subtar)
 {
    int32_t dimensionsNo = GetDimensions().size();
    int64_t min[dimensionsNo], max[dimensionsNo];
    std::vector<SubtarPtr> intersectingSubtars;
    
    TAR::_mutex.lock();
    if(_subtarsIndex != NULL)
    {
        subtar->CreateBoundingBox(min, max, dimensionsNo);
        _subtarsIndex->Search(min, max, TAR::_callback, NULL);
    }
    
    for(int64_t index : TAR::_intersectingSubtarsIndexes)
    {
        intersectingSubtars.push_back(_subtars[index]);
    }
    
    TAR::_intersectingSubtarsIndexes.clear();
    _mutex.unlock();
    
    return intersectingSubtars;
 }

vector<int32_t>& TAR::GetIdSubtars()
{
    return _idSubtars;
}

bool TAR::HasDataElement(std::string name)
{
    for(auto &dataElement : _elements)
    {
        if(!dataElement->GetName().compare(name))
        {
            return true;
        }
    }
    
    return false;
}

bool TAR::IsTemporaryTar()
{
    return _isTemporary;
}

DataElementPtr TAR::GetDataElement(string name)
{
    for(auto &dataElement : _elements)
    {
        if(!dataElement->GetName().compare(name))
        {
            return dataElement;
        }
    }
    return NULL;
}

std::list<DataElementPtr>& TAR::GetDataElements()
{
    return _elements;
}

bool TAR::RemoveDataElement(std::string name)
{
    for(auto dataElement : _elements)
    {
        if(!dataElement->GetName().compare(name))
        {
            _elements.remove(dataElement);
            return true;
        }
    }
    
    return false;
}

std::string TAR::toSmallString()
{         
    std::string smallString = "";
    for(auto& dataElement : _elements)
    {
       
        switch(dataElement->GetType())
        {
            case DIMENSION_SCHEMA_ELEMENT: smallString = smallString + "|"+dataElement->GetDimension()->name+","
                                            +"d," 
                                            +std::string(dataTypeNames[dataElement->GetDimension()->type]);break;
                                          
            case ATTRIBUTE_SCHEMA_ELEMENT: smallString = smallString + "|"+dataElement->GetAttribute()->name+","
                                            +"a," 
                                            +std::string(dataTypeNames[dataElement->GetAttribute()->type]);break; 
        }
    }
    
    std::string smallStringAux = smallString.replace(0,1, "#");
    return smallString;
}


std::string TAR::toString()
{
    std::string dimensions("(");
    std::string attributes("(");
            
    for(auto& dataElement : _elements)
    {
       
        switch(dataElement->GetType())
        {
            case DIMENSION_SCHEMA_ELEMENT:
            {   
                std::stringstream lower, upper;
                lower << std::setprecision(6) << dataElement->GetDimension()->lower_bound;
                upper << std::setprecision(6) << dataElement->GetDimension()->upper_bound;
                        
                dimensions=dimensions
                +dataElement->GetDimension()->name
                +"["+std::string(dataTypeNames[dataElement->GetDimension()->type])+"]" 
                +lower.str()+":"+upper.str()+" " ;break;
            }                              
            case ATTRIBUTE_SCHEMA_ELEMENT: 
            {                             
                attributes=attributes
                +dataElement->GetAttribute()->name
                +"["+dataTypeNames[dataElement->GetAttribute()->type]+"] ";break;
            }
        }
    }
    
    dimensions = dimensions.substr(0, dimensions.size()-1)+")";   
    attributes = attributes.substr(0, attributes.size()-1)+")";
    
    return _name+"<"+dimensions+"><"+attributes+">";
}

void TAR::RemoveTempDataElements()
{
    std::list<DataElementPtr> toRemove;
    
    for(auto &dataElement : _elements)
    {
        if(dataElement->GetName().compare(0, 4, DEFAULT_TEMP_MEMBER) == 0)
            toRemove.push_back(dataElement);
        
    } 
    
    for(auto dataElement : toRemove)
    {
        _elements.remove(dataElement);
    }
    
}

void TAR::RecalculatesRealBoundaries()
{
    std::unordered_map<std::string, DimensionPtr> dimensions;
    
    for(auto element : _elements)
    {
        if(element->GetType() == DIMENSION_SCHEMA_ELEMENT)
        {
            auto dimension = element->GetDimension();
            dimension->real_lower_bound = -1;
            dimension->real_upper_bound = -1;
            dimensions[dimension->name] = dimension;
        }
    }
    
    for(auto subtar : _subtars)
    {
        for(auto entry : subtar->GetDimSpecs())
        {
            auto dim = dimensions[entry.first];
            auto dimsSpecs = entry.second;
            
            if(dim->real_lower_bound > dimsSpecs->lower_bound 
                || dim->real_lower_bound == -1)
            {
                dim->real_lower_bound = dimsSpecs->lower_bound ;
            }
            
            if(dim->real_upper_bound > dimsSpecs->upper_bound 
                || dim->real_upper_bound == -1)
            {
                dim->real_upper_bound = dimsSpecs->upper_bound ;
            }
        }
    }
}

TARPtr TAR::Clone(bool copyId, bool copySubtars, bool dimensionsOnly)
{
    TARPtr clonedTAR = TARPtr(new TAR(0, "", NULL));
    
    if(copyId)
    {
        clonedTAR->AlterTAR(_id, _name, _isTemporary);
    }
    
    for(auto &dataElement : _elements)
    {
        if(dataElement->GetType()==DIMENSION_SCHEMA_ELEMENT)
        {
            clonedTAR->AddDimension(dataElement->GetDimension());
        }
        else if(dataElement->GetType()==ATTRIBUTE_SCHEMA_ELEMENT && !dimensionsOnly)
        {
            clonedTAR->AddAttribute(dataElement->GetAttribute());
        }
    }
    
    if(copySubtars)
    {
        for(auto subtar : _subtars)
        {
            clonedTAR->AddSubtar(subtar);
        }
    }
    
    return clonedTAR;
}
   
TAR::~TAR()
{
   for(auto listener : _listeners)
        listener->DisposeObject((MetadataObject*)this);   
}