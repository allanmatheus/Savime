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
#include <memory>
#include <mutex>
#include <unistd.h>
#include <condition_variable>
#include <unordered_map>
#include <vtkCellData.h>
#include <vtkCellType.h>
#include <vtkCPDataDescription.h>
#include <vtkCPInputDataDescription.h>
#include <vtkCPProcessor.h>
#include <vtkCPPythonScriptPipeline.h>
#include <vtkCellArray.h>
#include <vtkUnsignedCharArray.h>
#include <vtkDoubleArray.h>
#include <vtkFloatArray.h>
#include <vtkDoubleArray.h>
#include <vtkIntArray.h>
#include <vtkLongArray.h>
#include <vtkImageData.h>
#include <vtkStructuredGrid.h>
#include <vtkUnstructuredGrid.h>
#include <vtkNew.h>
#include <vtkPoints.h>
#include <vtkPointData.h>
#include <vtkXMLImageDataWriter.h>
#include <vtkXMLStructuredGridWriter.h>
#include <vtkXMLUnstructuredGridWriter.h>
#include <future>
#include "viz.h"
#include "default_engine.h"

#define CATERSIAN_FIELD_DATA2D "CartesianFieldData2d"
#define CATERSIAN_FIELD_DATA3D "CartesianFieldData3d"
#define UNSTRUCTURED_FIELD_DATA "UnstructuredFieldData"
#define CATERSIAN_GE02D "CartesianGeometry2d"
#define CATERSIAN_GEO3D "CartesianGeometry3d"
#define INCIDENCE_TOPOLOGY "IncidenceTopology"
#define ADJACENCY_TOPOLOGY "AdjacenceTopology"
#define SIMULATION_ROLE "simulation"
#define TIME_ROLE "time"
#define X_ROLE "x"
#define Y_ROLE "y"
#define Z_ROLE "z"
#define INDEX_ROLE "index"
#define INCIDENT_ROLE "incident"
#define INCIDENTEE_ROLE "incidentee"

#define NONE -1
#define DIM3 3
#define DIM2 2
#define X_DIM 0
#define Y_DIM 1
#define Z_DIM 2

class semaphore
{
private:
  mutex mtx;
  mutex semaphore_mutex;
  condition_variable cv;
  condition_variable cv_all;
  volatile int count;
  volatile int max;
  volatile int waiting=0;
  
public:

  void setCount(int c) {count = max = c;}

  void notify()
  {
    //unique_lock<mutex> lck(mtx);
    
    semaphore_mutex.lock();
    ++count;
    --waiting;
    semaphore_mutex.unlock();

    cv.notify_one();
  }

  void wait()
  {
    unique_lock<mutex> lck(mtx);
    while(count == 0)
      cv.wait(lck);
    
    semaphore_mutex.lock();
    ++waiting;
    --count;
    semaphore_mutex.unlock();
  }
};
typedef semaphore Semaphore;

const char * invalid_param = "Invalid parameter for operator CATALYZE. Expected CATALYZE(field_data_tar1, [geometry_tar], [topology_tar], catalyst_script, output_path)";

enum GridType{
    UNSTRUCTURED, 
    STRUCTURED, 
    IMAGE
};

typedef unordered_map<int64_t, unordered_map<int64_t, vtkSmartPointer<vtkDataSet>>> Gridset;
typedef unordered_map<int64_t, unordered_map<int64_t, int64_t>> GridsetAttribute;
typedef unordered_map<int64_t, unordered_map<int64_t, unordered_map<string, DatasetPtr>>> GridsetData;
typedef unordered_map<int64_t, unordered_map<int64_t, DatasetPtr>> PointsMapping;
typedef unordered_map<int64_t, unordered_map<int64_t, std::future<void>>> AssyncHandlers;
typedef vector<std::future<void>> AssyncHandlersList;
typedef vector<DatasetHandlerPtr> HandlerSet;
typedef vtkSmartPointer<vtkCPProcessor> Processor;

struct VizConfiguration
{
    GridType type;
    
    DataElementPtr index;
    DataElementPtr spatialDims[3];
    DataElementPtr timeDimension;
    DataElementPtr simDimension;
    
    DataElementPtr geoTimeDimension;
    DataElementPtr geoSimDimension;
    DataElementPtr geoIndexDimension;
    
    DataElementPtr topTimeDimension;
    DataElementPtr topSimDimension;
    DataElementPtr topIncidentDimension;
    DataElementPtr topIncidenteeDimension;
    DataElementPtr topValue;
    
    bool singleGrid;
    Gridset grids;
    GridsetAttribute filledCells;
    GridsetAttribute totalCells;
    GridsetData data;
    HandlerSet handlersToClose;
    PointsMapping mapping;
    string outputDir;
    string catalistScript;
    TARPtr fieldData;
    TARPtr geometry;
    TARPtr topology;
    int32_t numCores;
    string catalystExecutable;
    
    mutex singleGridMutex;
    Semaphore semaphore;
    mutex assyncHandlersMutex;
    AssyncHandlers assyncHandlers;
    AssyncHandlersList assyncHandlersToClose;
    
    mutex counterMutex;
    int64_t counter;
    int64_t spawnThreads;
};
typedef std::shared_ptr<VizConfiguration> VizConfigPtr;
#define GET_CATALYST_COMMAND_LINE(F, T,  LT, C) (C->catalystExecutable+" '"+C->catalistScript+"' '"+F+"' '"+C->outputDir+"' "+to_string(T)+" "+to_string(LT)).c_str()

void waitAll(VizConfigPtr vizConfiguration)
{
    while(true)
    {
        vizConfiguration->counterMutex.lock();
        if(vizConfiguration->counter == vizConfiguration->spawnThreads){
              vizConfiguration->counterMutex.unlock();
              break;
        }
        
        vizConfiguration->counterMutex.unlock();
        
        usleep(200);
    }
}


Processor processor = NULL;
Processor GetProcessor()
{
    if(processor == NULL)
    {
        processor = Processor::New();
        processor->Initialize();
    }
    else
    {
        processor->RemoveAllPipelines();   
    }
    
    return processor;
}

void AddTimeToVTK(vtkSmartPointer<vtkDataSet> ds, double time)
{
    auto t = vtkSmartPointer<vtkDoubleArray>::New();
    t->SetName("TIME");
    t->SetNumberOfTuples(1);
    t->SetTuple1(0, time);
    ds->GetFieldData()->AddArray(t);
}

//Validation and configuration
//------------------------------------------------------------------------------
bool isFieldDataTar(TARPtr tar)
{
    auto type = tar->GetType();
    if(!type) return false;
    return type->name == CATERSIAN_FIELD_DATA2D
           || type->name == CATERSIAN_FIELD_DATA3D
           || type->name == UNSTRUCTURED_FIELD_DATA;
}

bool isGeometryTar(TARPtr tar)
{
    auto type = tar->GetType();
    if(!type) return false;
    return type->name == CATERSIAN_GE02D
           || type->name == CATERSIAN_GEO3D;
}

bool isTopologyTar(TARPtr tar)
{
    auto type = tar->GetType();
    if(!type) return false;
    return type->name == INCIDENCE_TOPOLOGY
           || type->name == ADJACENCY_TOPOLOGY;
}

void validateGeometryTar(TARPtr tar, VizConfigPtr vizConfig)
{
    for(auto dim : tar->GetDimensions())
    {
        auto roles = tar->GetRoles();
        if(roles.find(dim->name) == roles.end())
            throw std::runtime_error("Unexpected dimension: "+dim->name+" in geometry TAR.");
    }
    
    for(auto entry : tar->GetRoles())
    {
        auto elementName = entry.first;
        auto role = entry.second;
        
        if(role->name == SIMULATION_ROLE)
            vizConfig->geoSimDimension=tar->GetDataElement(elementName);
        else if(role->name == TIME_ROLE)
            vizConfig->geoTimeDimension=tar->GetDataElement(elementName);
        else if(role->name == X_ROLE)
            vizConfig->spatialDims[X_DIM]=tar->GetDataElement(elementName);
        else if(role->name == Y_ROLE)
            vizConfig->spatialDims[Y_DIM]=tar->GetDataElement(elementName);
        else if(role->name == Z_ROLE)
            vizConfig->spatialDims[Z_DIM]=tar->GetDataElement(elementName);
        else if(role->name == INDEX_ROLE)
            vizConfig->geoIndexDimension=tar->GetDataElement(elementName);
    }
    
    if(vizConfig->geoIndexDimension->GetDataType() != LONG_TYPE)
    {
        throw std::runtime_error("Index role for a Geometry TAR must be implemented by "
                                 "a LONG typed data element.");
    }
}

void validateTopologyTar(TARPtr tar, VizConfigPtr vizConfig)
{
    for(auto dim : tar->GetDimensions())
    {
        auto roles = tar->GetRoles();
        if(roles.find(dim->name) == roles.end())
            throw std::runtime_error("unexpected dimension: "+dim->name+" in topology TAR.");
    }
    
    for(auto entry : tar->GetRoles())
    {
        auto elementName = entry.first;
        auto role = entry.second;
        
        if(role->name == SIMULATION_ROLE)
            vizConfig->topSimDimension=tar->GetDataElement(elementName);
        else if(role->name == TIME_ROLE)
            vizConfig->topTimeDimension=tar->GetDataElement(elementName);
        else if(role->name == INCIDENT_ROLE)
            vizConfig->topIncidentDimension=tar->GetDataElement(elementName);
        else if(role->name == INCIDENTEE_ROLE)
            vizConfig->topIncidenteeDimension=tar->GetDataElement(elementName);
        else if(role->name == INDEX_ROLE)
            vizConfig->topValue=tar->GetDataElement(elementName);
    }
    
    if(vizConfig->topValue->GetDataType() != LONG_TYPE)
    {
        throw std::runtime_error("Index role for Topology TAR must be implemented by "
                                 "a LONG typed data element.");
    }
}

void validateFieldDataTar(TARPtr tar, VizConfigPtr vizConfig)
{
    for(auto dim : tar->GetDimensions())
    {
        auto roles = tar->GetRoles();
        if(roles.find(dim->name) == roles.end())
            throw std::runtime_error("unexpected dimension: "+dim->name+" in TAR.");
    }
    
    for(auto entry : tar->GetRoles())
    {
        auto elementName = entry.first;
        auto role = entry.second;
        
        if(role->name == SIMULATION_ROLE)
            vizConfig->simDimension=tar->GetDataElement(elementName);
        else if(role->name == TIME_ROLE)
            vizConfig->timeDimension=tar->GetDataElement(elementName);
        else if(role->name == X_ROLE)
            vizConfig->spatialDims[X_DIM]=tar->GetDataElement(elementName);
        else if(role->name == Y_ROLE)
            vizConfig->spatialDims[Y_DIM]=tar->GetDataElement(elementName);
        else if(role->name == Z_ROLE)
            vizConfig->spatialDims[Z_DIM]=tar->GetDataElement(elementName);
        else if(role->name == INDEX_ROLE)
            vizConfig->index=tar->GetDataElement(elementName);
    }
    
    if(vizConfig->index != NULL)
    {
        if(vizConfig->index->GetDataType() != LONG_TYPE)
        {
            throw std::runtime_error("Index role for field data TAR must be implemented by "
                                     "a LONG typed data element.");
        }
        
        if(vizConfig->index->GetType() != DIMENSION_SCHEMA_ELEMENT)
        {
            throw std::runtime_error("Index role for field data TAR must be implemented as "
                                     "a dimension.");
        }
    }
}

VizConfigPtr createVizConfiguration(OperationPtr operation, ConfigurationManagerPtr configurationManager)
{
    VizConfigPtr vizConfiguration = VizConfigPtr(new VizConfiguration);
    int32_t paramCount = 1; 
    bool expectingOutdir = true;
    bool expectingGeo = true;
    
    auto firstParam = operation->GetParametersByName(OPERAND(0));
    if(firstParam == NULL || firstParam->type != TAR_PARAM)
        throw std::runtime_error(invalid_param);
    
    auto fieldTar = firstParam->tar;
    if(!isFieldDataTar(fieldTar))
        throw std::runtime_error("Invalid field TAR. Incorrect type.");
    
    vizConfiguration->fieldData = fieldTar;
    vizConfiguration->singleGrid = false;
    vizConfiguration->numCores = configurationManager->GetIntValue(MAX_THREADS);
    vizConfiguration->semaphore.setCount(vizConfiguration->numCores);
    vizConfiguration->catalystExecutable = configurationManager->GetStringValue(CATALYST_EXECUTABLE);
    vizConfiguration->counter = 0;
    vizConfiguration->spawnThreads = 0;
    
    while(true)
    {
        auto param = operation->GetParametersByName(OPERAND(paramCount++));
        if(param == NULL || paramCount > 5) break;  

        if(param->type == LITERAL_STRING_PARAM)
        {
            if(expectingOutdir)
            {
                expectingOutdir = false;
                string dir = param->literal_str;
                dir.erase(std::remove(dir.begin(), dir.end(), '"'), dir.end());
                vizConfiguration->outputDir = dir;
            }
            else
            {
                string scr = param->literal_str;
                scr.erase(std::remove(scr.begin(), scr.end(), '"'), scr.end());
                vizConfiguration->catalistScript = scr;
            }      
        }
        else if(param->type == TAR_PARAM)
        {
            if(expectingGeo)
            {
                expectingGeo = false;
                auto geoTar = param->tar;
                if(!isGeometryTar(geoTar))
                    throw std::runtime_error("Invalid geometry TAR. Incorrect type.");
                vizConfiguration->geometry = geoTar;
            }
            else
            {
                auto topTar = param->tar;
                if(!isTopologyTar(topTar))
                    throw std::runtime_error("Invalid topology TAR. Incorrect type.");
                vizConfiguration->topology = topTar;
            }
        }
        else
        {
            throw std::runtime_error(invalid_param);
        }
    }
    
    if(vizConfiguration->outputDir.empty())
        throw std::runtime_error("Output directory not informed.");
    
    if(!EXIST_FILE(vizConfiguration->outputDir))
        throw std::runtime_error("Output directory "+vizConfiguration->outputDir+
                                 " not found.");
    
    if(!vizConfiguration->catalistScript.empty())
    {
        if(!EXIST_FILE(vizConfiguration->catalistScript))
            throw std::runtime_error("Catalyst script not found.");
        
        /*processor = GetProcessor();
        vtkNew<vtkCPPythonScriptPipeline> pipeline;
        pipeline->Initialize(vizConfiguration->catalistScript.c_str());
        processor->AddPipeline(pipeline.GetPointer());
        chdir(vizConfiguration->outputDir.c_str());
        */
    }
    
    if(vizConfiguration->fieldData->GetType()->name == UNSTRUCTURED_FIELD_DATA
       && vizConfiguration->geometry == NULL)
    {
        throw std::runtime_error("Unstructured field data detected but no "
                                 "geometry TAR specified.");
    }
    
    return vizConfiguration;
}

//Aux functions
//------------------------------------------------------------------------------
DatasetPtr createMapping(VizConfigPtr vizConfig, 
                         StorageManagerPtr storageManager, 
                         SubtarPtr subtar, 
                         DatasetPtr filter)
{
    
    #define LOWER(X) X->lower_bound
    #define UPPER(X) X->upper_bound
    
    if(vizConfig->type != IMAGE && vizConfig->type != STRUCTURED)
        return NULL;
    
    DatasetPtr spatialDatasets[DIM3], aux[DIM3]; 
    int64_t totalLen = subtar->GetTotalLength();
    bool is3D = vizConfig->spatialDims[Z_DIM] != NULL;

    string spatialDimNames[] = {vizConfig->spatialDims[X_DIM]->GetName(),
                                vizConfig->spatialDims[Y_DIM]->GetName(),
                                (is3D)?vizConfig->spatialDims[Z_DIM]->GetName(): ""};

    int64_t spatialDimLen[] = {vizConfig->spatialDims[X_DIM]->GetDimension()->GetLength(),
                               vizConfig->spatialDims[Y_DIM]->GetDimension()->GetLength(),
                               (is3D)?vizConfig->spatialDims[Z_DIM]->GetDimension()->GetLength():1};

    DimSpecPtr spatialDimSpecs[] = {subtar->GetDimensionSpecificationFor(spatialDimNames[X_DIM]),
                                  subtar->GetDimensionSpecificationFor(spatialDimNames[Y_DIM]),
                                  subtar->GetDimensionSpecificationFor(spatialDimNames[Z_DIM])};

    int64_t preamble0 = spatialDimLen[Y_DIM];
    int64_t preamble1 = spatialDimLen[Y_DIM]*spatialDimLen[Z_DIM];
    int64_t preamble2 = spatialDimLen[Z_DIM];

    if(filter)
    {
        DatasetHandlerPtr handlerX, handlerY, handlerZ;
        int64_t *bufferX, *bufferY, *bufferZ;
        int32_t dimNum = (is3D)?DIM3:DIM2;         

        for(int32_t i = 0; i < dimNum; i++)
        {
            storageManager->PartiatMaterializeDim(filter, 
                                                  spatialDimSpecs[i], 
                                                  totalLen, aux[i], 
                                                  spatialDatasets[i]);
        }
        int64_t length = spatialDatasets[X_DIM]->entry_count;

        handlerX = storageManager->GetHandler(spatialDatasets[0]);
        bufferX = (int64_t*)handlerX->GetBuffer();

        handlerY = storageManager->GetHandler(spatialDatasets[1]);
        bufferY = (int64_t*)handlerY->GetBuffer();

        if(is3D)
        {
            handlerZ = storageManager->GetHandler(spatialDatasets[2]);
            bufferZ = (int64_t*)handlerZ->GetBuffer();
        }

        DatasetPtr mapping = storageManager->Create(LONG_TYPE, length);
        DatasetHandlerPtr mappingHandler = storageManager->GetHandler(mapping);
        int64_t* mappingBuffer = (int64_t*)mappingHandler->GetBuffer();
        omp_set_num_threads(vizConfig->numCores);
        
        if(is3D)
        {
            #pragma omp parallel for
            for(int64_t i = 0; i < length; i++)
            {
                int64_t pos = bufferX[i]*preamble1+bufferY[i]*preamble2+bufferZ[i];
                mappingBuffer[i] = pos;
            }
        }
        else
        {
            #pragma omp parallel for
            for(int64_t i = 0; i < length; i++)
            {
                int64_t pos = bufferX[i]*preamble0+bufferY[i];
                mappingBuffer[i] = pos;
            }
        }

        handlerX->Close();
        handlerY->Close();
        if(is3D) handlerZ->Close();
        mappingHandler->Close();
        return mapping;
    }
    else
    {
        DatasetPtr mapping = storageManager->Create(LONG_TYPE, totalLen); int64_t i =0;
        DatasetHandlerPtr mappingHandler = storageManager->GetHandler(mapping);
        int64_t* mappingBuffer = (int64_t*)mappingHandler->GetBuffer();
        omp_set_num_threads(vizConfig->numCores);

        if(is3D)
        {
            #pragma omp parallel for collapse(3)
            for(int64_t x = LOWER(spatialDimSpecs[0]); x <= UPPER(spatialDimSpecs[0]); x++)
            {
                for(int64_t y = LOWER(spatialDimSpecs[1]); y <= UPPER(spatialDimSpecs[1]); y++)
                {
                    for(int64_t z = LOWER(spatialDimSpecs[2]); z <= UPPER(spatialDimSpecs[2]); z++)
                    {
                        int64_t pos = x*preamble1+y*preamble2+z;
                        mappingBuffer[i++] = pos;
                    }
                }
            }
        }
        else
        {
            #pragma omp parallel for collapse(2)
            for(int64_t x = LOWER(spatialDimSpecs[0]); x <= UPPER(spatialDimSpecs[0]); x++)
            {
                for(int64_t y = LOWER(spatialDimSpecs[1]); y <= UPPER(spatialDimSpecs[1]); y++)
                {
                    int64_t pos = x*preamble0+y;
                    mappingBuffer[i++] = pos;
                }
            }
        }

        mappingHandler->Close();
        return mapping;
    }
}

unordered_map<string, DatasetPtr> sliceSubtars(DataElementPtr simDimension,
                                               DataElementPtr timeDimension,
                                               StorageManagerPtr storageManager, 
                                               SubtarPtr subtar, 
                                               int64_t simulation, 
                                               int64_t time, 
                                               DatasetPtr& filter)
{
    unordered_map<string, DatasetPtr> datasets;
    DimSpecPtr simSpecs, timeSpecs;
    bool hasManySims = simulation != NONE;
    bool hasManyTimeSteps = time != NONE;
    
    for(auto entry : subtar->GetDataSets())
    {
        string attributeName = entry.first;
        DatasetPtr dataset = entry.second;
        DatasetPtr filteredDataset = dataset;        
        DatasetPtr filterTime, filterSim;
        double logicalTimeIndex, logicalSimIndex;
        
        if(hasManySims && hasManyTimeSteps)
        {
            simSpecs = subtar->GetDimensionSpecificationFor(simDimension->GetName());
            timeSpecs = subtar->GetDimensionSpecificationFor(timeDimension->GetName());
            LogicalIndex _sim = storageManager->Real2Logical(simDimension->GetDimension(), simulation);
            GET_LOGICAL_INDEX(logicalSimIndex, _sim, double);
            storageManager->ComparisonDim(string("="), simSpecs, subtar->GetTotalLength(), logicalSimIndex, filterSim);
            LogicalIndex _time = storageManager->Real2Logical(timeDimension->GetDimension(), time);
            GET_LOGICAL_INDEX(logicalTimeIndex, _time, double);
            storageManager->ComparisonDim(string("="), timeSpecs, subtar->GetTotalLength(), logicalTimeIndex, filterTime);
            storageManager->And(filterSim, filterTime, filter);
            storageManager->Filter(dataset, filter, filteredDataset);
            datasets[attributeName] = filteredDataset;
            assert(filteredDataset->entry_count != 0);
        }
        else if(hasManySims)
        {   
            simSpecs = subtar->GetDimensionSpecificationFor(simDimension->GetName());
            LogicalIndex _sim = storageManager->Real2Logical(simDimension->GetDimension(), simulation);
            GET_LOGICAL_INDEX(logicalSimIndex, _sim, double);
            storageManager->ComparisonDim(string("="), simSpecs, subtar->GetTotalLength(), logicalSimIndex, filterSim);
            filter = filterSim;
            storageManager->Filter(dataset, filter, filteredDataset);
            datasets[attributeName] = filteredDataset;
            assert(filteredDataset->entry_count != 0);
        }
        else if(hasManyTimeSteps)
        {
            timeSpecs = subtar->GetDimensionSpecificationFor(timeDimension->GetName());
            LogicalIndex _time = storageManager->Real2Logical(timeDimension->GetDimension(), time);
            GET_LOGICAL_INDEX(logicalTimeIndex, _time, double);
            storageManager->ComparisonDim(string("="), timeSpecs, subtar->GetTotalLength(), logicalTimeIndex, filterTime);
            filter = filterTime;
            storageManager->Filter(dataset, filterTime, filteredDataset);
            datasets[attributeName] = filteredDataset;
            assert(filteredDataset->entry_count != 0);
        }
        else
        {
             datasets[attributeName] = dataset;
        }
    }
    
    return datasets;
}



//Grids initialization
//------------------------------------------------------------------------------
void initializeUnstructuredGrids(VizConfigPtr vizConfiguration, 
                                 StorageManagerPtr storageManager,
                                 DefaultEnginePtr defaultEngine)
{
    #define INVALID -1
    #define IN_RANGE(X,Y,Z) (X>=Y) && (X<Z)
    
    int32_t geoSubtarCount = 0, topSubtarCount = 0; 
    int64_t simulation=0, timeStep=0;
    int32_t components = 0; DatasetPtr interleavedCoord;
    string geoTarName = vizConfiguration->geometry->GetName();
    string topTarName = vizConfiguration->topology->GetName();
    
    auto geoGeneretor = defaultEngine->GetGenerators()[geoTarName];
    auto topGeneretor = defaultEngine->GetGenerators()[topTarName];
    
    bool is3D = vizConfiguration->spatialDims[Z_DIM] != NULL;
    string geoTimeName="", geoSimName="", topTimeName="", topSimName="";
    vizConfiguration->singleGrid = true;                
    vizConfiguration->type = UNSTRUCTURED;
            
    if(vizConfiguration->geoSimDimension != NULL)
    {
        geoSimName = vizConfiguration->geoIndexDimension->GetName();
        vizConfiguration->singleGrid = false;
    }
    
    if(vizConfiguration->geoTimeDimension != NULL)
    {
        geoTimeName = vizConfiguration->geoTimeDimension->GetName();
        vizConfiguration->singleGrid = false;
    }
    
    if(vizConfiguration->topSimDimension != NULL)
    {
        topSimName = vizConfiguration->topSimDimension->GetName();
        vizConfiguration->singleGrid = false;
    }
    
    if(vizConfiguration->topTimeDimension != NULL)
    {
        topTimeName = vizConfiguration->topTimeDimension->GetName();
        vizConfiguration->singleGrid = false;
    }
    
    string indexElementName = vizConfiguration->geoIndexDimension->GetName();
    string topIndexElementName = vizConfiguration->topValue->GetName();
    string spatialDataElementNames[DIM3] = {vizConfiguration->spatialDims[X_DIM]->GetName(),
                             vizConfiguration->spatialDims[Y_DIM]->GetName(),
                            (is3D)? vizConfiguration->spatialDims[Z_DIM]->GetName(): ""};
    
    //Creating geometry
    while(true)
    {
        DatasetPtr spatialDatasets[DIM3], indexDataset;
        int64_t lens[DIM3] = {1, 1, 1}, totalLen = 0;
        
        auto subtar = geoGeneretor->GetSubtar(geoSubtarCount++);
        if(subtar == NULL) break;
        totalLen = subtar->GetTotalLength();
        
        DimSpecPtr simSpecs = subtar->GetDimensionSpecificationFor(geoSimName);
        if(simSpecs != NULL)
        {
            if(simSpecs->GetLength() != 1)
                throw std::runtime_error("Invalid geometry TAR. Non conformant subtars.");
            simulation = simSpecs->lower_bound;
        }
        
        DimSpecPtr timSpecs = subtar->GetDimensionSpecificationFor(geoTimeName);
        if(timSpecs != NULL)
        {
            if(timSpecs->GetLength() != 1)
                throw std::runtime_error("Invalid geometry TAR. Non conformant subtars.");
            timeStep = timSpecs->lower_bound;
        }
        
        DimSpecPtr dimSpecs = subtar->GetDimensionSpecificationFor(indexElementName);
        if(dimSpecs != NULL)
        {
            storageManager->MaterializeDim(dimSpecs, totalLen, indexDataset);
        }
        else
        {
            indexDataset = subtar->GetDataSetFor(indexElementName);
        }
        
        int64_t numberOfPoints = 1;
        for(int32_t i = 0; i < DIM3; i++)
        {
             dimSpecs = subtar->GetDimensionSpecificationFor(spatialDataElementNames[i]);
             
             if(dimSpecs != NULL)
             {
                bool wholeExtent = dimSpecs->GetLength() == dimSpecs->dimension->GetDimension()->GetLength();
                if(!wholeExtent) 
                    throw std::runtime_error("Invalid geometry TAR. Non conformant subtars.");
                
                storageManager->MaterializeDim(dimSpecs, totalLen, spatialDatasets[i]);
                numberOfPoints *= dimSpecs->GetLength();
             }
             else
             {
                spatialDatasets[i] = subtar->GetDataSetFor(spatialDataElementNames[i]);
                numberOfPoints = (spatialDatasets[i])? spatialDatasets[i]->entry_count : numberOfPoints;
             }
        }
           
        double spacings[DIM3]; int32_t offsets[DIM3] = {0,1,2};
      
        if(is3D)
        {
            components = DIM3;
            spacings[X_DIM] = spacings[Y_DIM] = spacings[Z_DIM] = DIM3;
            interleavedCoord = storageManager->Create(DOUBLE_TYPE, numberOfPoints*3);
        }
        else
        {
            components = DIM2;
            spacings[X_DIM] = spacings[Y_DIM] = DIM2;
            interleavedCoord = storageManager->Create(DOUBLE_TYPE, numberOfPoints*2);
        }   
        
        for(int32_t i = 0 ; i < DIM3; i++)
        {
            DatasetPtr matDim = spatialDatasets[i];
            if(matDim)
            {
                storageManager->Copy(matDim, 0, matDim->entry_count-1, offsets[i], spacings[i], interleavedCoord);
            }
        }
        
        auto pointsArray = vtkSmartPointer<vtkDoubleArray>::New();
        DatasetHandlerPtr handler = storageManager->GetHandler(interleavedCoord);
        vizConfiguration->handlersToClose.push_back(handler);
        pointsArray->SetNumberOfComponents(components);
        pointsArray->SetArray((double*)handler->GetBuffer(),interleavedCoord->entry_count,1);
        vtkSmartPointer<vtkPoints> points = vtkSmartPointer<vtkPoints>::New();
        points->SetData(pointsArray);
        
        auto grid = vtkSmartPointer<vtkUnstructuredGrid>::New();
        grid->SetPoints(points);
        vizConfiguration->grids[simulation][timeStep] = grid;
        vizConfiguration->totalCells[simulation][timeStep] = numberOfPoints;
        
        string indexName = vizConfiguration->index->GetName();
        int64_t mappingLen;
                
        if(vizConfiguration->index->GetType() == DIMENSION_SCHEMA_ELEMENT)
            mappingLen = vizConfiguration->index->GetDimension()->GetLength();
        else
            mappingLen = indexDataset->entry_count;
        
        
        DatasetPtr mapping = storageManager->Create(LONG_TYPE, mappingLen);
        int64_t * mappingBuffer, *indexBuffer;
        DatasetHandlerPtr mappingHandler = storageManager->GetHandler(mapping);
        DatasetHandlerPtr indexHandler = storageManager->GetHandler(indexDataset);
        mappingBuffer = (int64_t*)mappingHandler->GetBuffer();
        indexBuffer = (int64_t*)indexHandler->GetBuffer();
        
        omp_set_num_threads(vizConfiguration->numCores);
        #pragma omp parallel for
        for(int64_t i = 0; i < mappingLen; i++)
        {
            mappingBuffer[i] = INVALID;
        }
        
        #pragma omp parallel for
        for(int64_t i = 0; i < indexDataset->entry_count; i++)
        {
            mappingBuffer[indexBuffer[i]] = i;
        }
        
        vizConfiguration->mapping[simulation][timeStep] = mapping;
        vizConfiguration->totalCells[simulation][timeStep] = numberOfPoints;
        mappingHandler->Close();
        indexHandler->Close();
    }
    
    //Creating topology
    while(true)
    {
        auto subtar = topGeneretor->GetSubtar(topSubtarCount++);
        if(subtar == NULL) break;
        int64_t totalLen = subtar->GetTotalLength();
        
        DimSpecPtr simSpecs = subtar->GetDimensionSpecificationFor(topSimName);
        if(simSpecs != NULL)
        {
            if(simSpecs->GetLength() != 1)
                throw std::runtime_error("Invalid topology TAR. Non conformant subtars.");
            simulation = simSpecs->lower_bound;
        }
        
        DimSpecPtr timSpecs = subtar->GetDimensionSpecificationFor(topTimeName);
        if(timSpecs != NULL)
        {
            if(timSpecs->GetLength() != 1)
                throw std::runtime_error("Invalid topology TAR. Non conformant subtars.");
            timeStep = timSpecs->lower_bound;
        }
        
        DatasetPtr mapping = vizConfiguration->mapping[simulation][timeStep];
        auto grid = vizConfiguration->grids[simulation][timeStep];
        
        if(grid == NULL) 
            throw std::runtime_error("Invalid topology TAR. Non conformant subtars.");;
        
        auto unstructuredGrid = vtkUnstructuredGrid::SafeDownCast(grid.Get());
        auto topIndexName = vizConfiguration->topValue->GetName();
        DatasetPtr topologyData = subtar->GetDataSetFor(topIndexName); 
        
        auto incidentDimName = vizConfiguration->topIncidentDimension->GetName();
        DimSpecPtr incidentDimSpecs = subtar->GetDimensionSpecificationFor(incidentDimName);
        auto incidenteeName = vizConfiguration->topIncidenteeDimension->GetName();
        DimSpecPtr incidenteeDimSpecs = subtar->GetDimensionSpecificationFor(incidenteeName);
        
        int64_t numCells =  incidenteeDimSpecs->GetLength();
        int64_t entriesPerCell = incidentDimSpecs->GetLength();
        
        DatasetPtr cellArrayDs = storageManager->Create(LONG_TYPE, numCells*(entriesPerCell-1));
        DatasetPtr cellTypesDs = storageManager->Create(INTEGER_TYPE, numCells);
        
        DatasetHandlerPtr topologyHandler = storageManager->GetHandler(topologyData);
        DatasetHandlerPtr mappingHandler = storageManager->GetHandler(mapping);
        DatasetHandlerPtr cellArrayHandler = storageManager->GetHandler(cellArrayDs);
        vizConfiguration->handlersToClose.push_back(cellArrayHandler);
        DatasetHandlerPtr cellTypesHandler = storageManager->GetHandler(cellTypesDs);
        vizConfiguration->handlersToClose.push_back(cellTypesHandler);
          
        int64_t * topologyBufer = (int64_t*)topologyHandler->GetBuffer();
        int64_t * mappingBuffer = (int64_t*)mappingHandler->GetBuffer();
        int64_t * cellArrayBuffer = (int64_t*)cellArrayHandler->GetBuffer();
        int32_t * types = (int32_t*)cellTypesHandler->GetBuffer();
        
        omp_set_num_threads(vizConfiguration->numCores);
 
        #pragma omp parallel for
        for(int64_t i = 0; i < numCells; i++)
        {
           
            int64_t offset = i*entriesPerCell;
            int64_t destOffset = i*(entriesPerCell-1);
            int64_t destOffsetPoints = destOffset+1;
            int64_t cellType = topologyBufer[offset];
            int64_t numPoints = topologyBufer[offset+1];
            int64_t lbPoints = offset+2;
            int64_t upEntry = entriesPerCell-2;
            
            cellArrayBuffer[destOffset] = numPoints;
            
            for(int64_t j=0; j < upEntry; j++)
            {
                int64_t pos = destOffsetPoints+j;
                int64_t pos_top = lbPoints+j;
                
                if(IN_RANGE(topologyBufer[pos_top], 0, mapping->entry_count))
                    cellArrayBuffer[pos] = mappingBuffer[topologyBufer[pos_top]];
                else
                    cellArrayBuffer[pos] = INVALID;
                
                bool invalid = cellArrayBuffer[pos] == INVALID && j < numPoints;
                if(invalid) cellType = 0;
            }
            
            types[i] = cellType;
        }
        
        auto idArrays = vtkSmartPointer<vtkIdTypeArray>::New();
        idArrays->SetArray((vtkIdType*)cellArrayBuffer, numCells*(entriesPerCell-1), 1); 
        auto cellArray = vtkSmartPointer<vtkCellArray>::New();
        cellArray->SetCells(numCells, idArrays);
        unstructuredGrid->SetCells(types, cellArray);
        
        //cellArrayHandler->Close();
        mappingHandler->Close();
        topologyHandler->Close();
    }
}


void initializeStructuredGrids(VizConfigPtr vizConfiguration, 
                               StorageManagerPtr storageManager, 
                               DataElementPtr dimensions[DIM3], 
                               int64_t lens[DIM3], 
                               int64_t simulationsNumber, 
                               int64_t timeStepNumber)
{
    
    int64_t lenX=lens[X_DIM], lenY=lens[Y_DIM], lenZ=lens[Z_DIM];
    DataElementPtr xDim=dimensions[X_DIM], yDim=dimensions[Y_DIM], zDim=dimensions[Z_DIM];
    
    vtkSmartPointer<vtkDoubleArray> pointsArray = vtkSmartPointer<vtkDoubleArray>::New();
    DatasetPtr interleavedCoord; DataElementPtr dims[] = {xDim, yDim, zDim};
    int32_t components = 0;
    double spacings[DIM3]; int32_t offsets[DIM3] = {0,1,2};
    double adjcency[DIM3] = {lenY*lenZ, lenZ, 1};
    double skew[DIM3] = {lenX*lenY*lenZ, lenY*lenZ, lenZ};

    if(zDim)
    {
        components = DIM3;
        spacings[X_DIM] = spacings[Y_DIM] = spacings[Z_DIM] = DIM3;
        interleavedCoord = storageManager->Create(DOUBLE_TYPE, lenX*lenY*lenZ*3);
    }
    else
    {
        components = DIM2;
        spacings[X_DIM] = spacings[Y_DIM] = DIM2;
        interleavedCoord = storageManager->Create(DOUBLE_TYPE, lenX*lenY*2);
    }   

    for(int32_t i = 0 ; i < DIM3; i++)
    {
        auto element = dims[i];
        DatasetPtr matDim; int64_t totalLen = lenX*lenY*lenZ;

        if(element)
        {
            auto dim = element->GetDimension();
            DimSpecPtr dimsSpecs = DimSpecPtr(new DimensionSpecification());
            dimsSpecs->lower_bound = dim->real_lower_bound;
            dimsSpecs->upper_bound = dim->real_upper_bound;
            dimsSpecs->adjacency = adjcency[i];
            dimsSpecs->skew = skew[i];
            storageManager->MaterializeDim(dimsSpecs, totalLen, matDim);
            storageManager->Copy(matDim, 0, matDim->entry_count-1, offsets[i], spacings[i], interleavedCoord);
        }
    }

    DatasetHandlerPtr handler = storageManager->GetHandler(interleavedCoord);
    pointsArray->SetNumberOfComponents(components);
    pointsArray->SetArray((double*)handler->GetBuffer(),interleavedCoord->entry_count,1);
    vtkSmartPointer<vtkPoints> points = vtkSmartPointer<vtkPoints>::New();
    points->SetData(pointsArray);

    vizConfiguration->singleGrid = true;
    auto grid = vtkSmartPointer<vtkStructuredGrid>::New();
    grid->SetDimensions(lenX, lenY, lenZ);
    grid->SetPoints(points);
    vizConfiguration->grids[0][0] = grid;
    vizConfiguration->totalCells[0][0] = lenX*lenY*lenZ;
    vizConfiguration->singleGrid = true;
    vizConfiguration->type = STRUCTURED;
}

void initializeImageGrids(VizConfigPtr vizConfiguration, 
                         StorageManagerPtr storageManager, 
                         int64_t lens[DIM3], 
                         double spacings[DIM3], 
                         int64_t simulationsNumber, 
                         int64_t timeStepNumber)
{
    int64_t lenX=lens[X_DIM], lenY=lens[Y_DIM], lenZ=lens[Z_DIM];
    double spacingX=spacings[0], spacingY=spacings[1], spacingZ=spacings[2];

    vizConfiguration->singleGrid = true;
    auto grid = vtkSmartPointer<vtkImageData>::New();
    grid->SetDimensions(lenX, lenY, lenZ);
    grid->SetSpacing(spacingX, spacingY, spacingZ);
    grid->SetOrigin(lenX/2,lenY/2,lenZ/2);
    vizConfiguration->grids[0][0] = grid;
    vizConfiguration->totalCells[0][0] = lenX*lenY*lenZ;
    vizConfiguration->singleGrid = true;
    vizConfiguration->type = IMAGE;
}

void initializeGrids(VizConfigPtr vizConfiguration, StorageManagerPtr storageManager, DefaultEnginePtr defaultEngine)
{
    TARPtr fieldData = vizConfiguration->fieldData;
    int32_t simulationsNumber = 1, timeStepNumber = 1;
    validateFieldDataTar(fieldData, vizConfiguration);    
    DataElementPtr simulation = vizConfiguration->simDimension; 
    DataElementPtr time = vizConfiguration->timeDimension; 
    
    if((simulation != NULL) && (simulation->GetType() == DIMENSION_SCHEMA_ELEMENT))
          simulationsNumber = simulation->GetDimension()->GetLength();

    if((time != NULL) && (time->GetType() == DIMENSION_SCHEMA_ELEMENT))
          timeStepNumber = time->GetDimension()->GetLength();
    
    if(fieldData->GetType()->name == UNSTRUCTURED_FIELD_DATA)
    {
        validateGeometryTar(vizConfiguration->geometry, vizConfiguration);
        validateTopologyTar(vizConfiguration->topology, vizConfiguration);
        initializeUnstructuredGrids(vizConfiguration, storageManager, defaultEngine);
    }
    else
    {
        int64_t lens[DIM3] = {1, 1, 1}; double spacings[DIM3];
        DataElementPtr dims[DIM3] = vizConfiguration->spatialDims;
        
        for(int32_t i = 0; i < DIM3; i++)
        {
            if((dims[i] != NULL) && (dims[i]->GetType() == DIMENSION_SCHEMA_ELEMENT))
            {
                lens[i] = dims[i]->GetDimension()->GetLength();
                spacings[i] = dims[i]->GetDimension()->spacing;
            }   
            else if(i != Z_DIM)
            {
                throw std::runtime_error("Spatial roles must be implemented as dimensions.");
            }
        }
          
        
        bool hasExplicitDim = dims[X_DIM]->GetDimension()->dimension_type == EXPLICIT
                              || dims[Y_DIM]->GetDimension()->dimension_type == EXPLICIT
                              || (dims[Z_DIM] != NULL)? dims[Z_DIM]->GetDimension()->dimension_type == EXPLICIT : false;
        
        if(hasExplicitDim)
        {
            
            initializeStructuredGrids(vizConfiguration, 
                                     storageManager, 
                                     dims, 
                                     lens, 
                                     simulationsNumber, 
                                     timeStepNumber);
        }
        else
        {
            initializeImageGrids(vizConfiguration, 
                                   storageManager, 
                                   lens, 
                                   spacings,
                                   simulationsNumber, 
                                   timeStepNumber);
        }
        
    }
}


//Processing functions
//------------------------------------------------------------------------------
void processGrid(VizConfigPtr vizConfiguration, StorageManagerPtr storageManager, int64_t time, int64_t simulation)
{
    #define SIM_NAME "input"
    #define BASE_NAME "savime_"
    #define SUFFIX(S, T) "s"+to_string(S)+"_t"+to_string(T)
    #define VTU_EXT ".vtu"
    #define VTI_EXT ".vti"
    #define VTS_EXT ".vts"
    vtkSmartPointer<vtkDataSet> grid; 
 
    double logicalTimeIndex = 0;
    auto timeDimension = vizConfiguration->timeDimension;
    if(timeDimension)
    {
        LogicalIndex _time = storageManager->Real2Logical(timeDimension->GetDimension(), time);
        GET_LOGICAL_INDEX(logicalTimeIndex, _time, double);
    }   
    
    if(vizConfiguration->singleGrid)
    {
        if(vizConfiguration->type == UNSTRUCTURED)
        {
            auto _grid = vtkUnstructuredGrid::SafeDownCast(vizConfiguration->grids[0][0]);
            grid = vtkSmartPointer<vtkUnstructuredGrid>::New();
            vizConfiguration->singleGridMutex.lock();
            grid->CopyStructure(_grid);
            vizConfiguration->singleGridMutex.unlock();
        }
        else if(vizConfiguration->type == STRUCTURED)
        {
            auto _grid = vtkStructuredGrid::SafeDownCast(vizConfiguration->grids[0][0]);
            grid = vtkSmartPointer<vtkStructuredGrid>::New();
            vizConfiguration->singleGridMutex.lock();
            grid->CopyStructure(_grid);
            vizConfiguration->singleGridMutex.unlock();
        }
        else if(vizConfiguration->type == IMAGE)
        {
            auto _grid = vtkImageData::SafeDownCast(vizConfiguration->grids[0][0]);
            grid = vtkSmartPointer<vtkImageData>::New();
            vizConfiguration->singleGridMutex.lock();
            grid->CopyStructure(_grid);
            vizConfiguration->singleGridMutex.unlock();
        }   
    }
    else
    {
        grid = vizConfiguration->grids[simulation][time];
    }
    
    for(auto entry : vizConfiguration->data[simulation][time])
    {
        auto attributeName = entry.first;
        auto dataset = entry.second;
        DatasetHandlerPtr handler = storageManager->GetHandler(dataset);
        
        switch(dataset->type)
        {
            case INTEGER_TYPE:
            {
                int32_t * buffer = (int32_t*) handler->GetBuffer();
                vtkSmartPointer<vtkIntArray> vtkArray = vtkSmartPointer<vtkIntArray>::New();
                vtkArray->SetName(attributeName.c_str());
                vtkArray->SetArray(buffer, dataset->entry_count, 1);
                grid->GetPointData()->AddArray(vtkArray);
                break;
            }    
            case LONG_TYPE: 
            {
                int64_t * buffer = (int64_t*) handler->GetBuffer();
                vtkSmartPointer<vtkLongArray> vtkArray = vtkSmartPointer<vtkLongArray>::New();
                vtkArray->SetName(attributeName.c_str());
                vtkArray->SetArray(buffer, dataset->entry_count, 1);
                grid->GetPointData()->AddArray(vtkArray);
                break;
            }    
            case FLOAT_TYPE:
            {
                float * buffer = (float*) handler->GetBuffer();
                vtkSmartPointer<vtkFloatArray> vtkArray = vtkSmartPointer<vtkFloatArray>::New();
                vtkArray->SetName(attributeName.c_str());
                vtkArray->SetArray(buffer, dataset->entry_count, 1);
                grid->GetPointData()->AddArray(vtkArray);
                break;
            }   
            case DOUBLE_TYPE:
            {
                double * buffer = (double*) handler->GetBuffer();
                vtkSmartPointer<vtkDoubleArray> vtkArray = vtkSmartPointer<vtkDoubleArray>::New();
                vtkArray->SetName(attributeName.c_str());
                vtkArray->SetArray(buffer, dataset->entry_count, 1);
                grid->GetPointData()->AddArray(vtkArray);
                break;
            }   
        }
    }
    
    string fileName = vizConfiguration->outputDir+BASE_NAME+SUFFIX(simulation, time);
    switch(vizConfiguration->type)
    {
        case IMAGE:
        {
            vtkSmartPointer<vtkXMLImageDataWriter > writer = 
            vtkSmartPointer<vtkXMLImageDataWriter>::New();
            writer->SetInputData(grid);
            fileName = fileName+VTI_EXT;
            writer->SetFileName(fileName.c_str());
            writer->Write();
            break;
        }
        case STRUCTURED:
        {
            vtkSmartPointer<vtkXMLStructuredGridWriter > writer = 
            vtkSmartPointer<vtkXMLStructuredGridWriter>::New();
            writer->SetInputData(grid);
            fileName = fileName+VTS_EXT;
            writer->SetFileName(fileName.c_str());
            writer->Write();
            break;
        }
        case UNSTRUCTURED:
        {
            vtkSmartPointer<vtkXMLUnstructuredGridWriter > writer = 
            vtkSmartPointer<vtkXMLUnstructuredGridWriter>::New();
            writer->SetInputData(grid);
            fileName = fileName+VTU_EXT;
            writer->SetFileName(fileName.c_str());
            writer->Write();
            break;
        }
    }
    
    //Run catalyst script
    if(simulation == 0 && !vizConfiguration->catalistScript.empty())
    {
        /* bool isLast = time == vizConfiguration->timeDimension->GetDimension()->GetLength()-1;
        auto dataDescription = vtkSmartPointer<vtkCPDataDescription>::New();
        
        if(isLast)
            dataDescription->ForceOutputOn();
        
        dataDescription->AddInput(SIM_NAME);
             
        AddTimeToVTK(grid, logicalTimeIndex);
        dataDescription->SetTimeData(logicalTimeIndex, time);
        dataDescription->GetInputDescriptionByName(SIM_NAME)->SetGrid(grid);
        vizConfiguration->singleGridMutex.lock();
        processor->CoProcess(dataDescription);
        vizConfiguration->singleGridMutex.unlock();
        */
        int32_t returnValue = system(GET_CATALYST_COMMAND_LINE(fileName, time, logicalTimeIndex, vizConfiguration));
        remove(fileName.c_str( ));
    }
         
    if(!vizConfiguration->singleGrid)
    {
        vizConfiguration->grids[simulation][time] = NULL;
        vizConfiguration->filledCells[simulation].erase(time);
        vizConfiguration->totalCells[simulation].erase(time);
    }
    /*else
    {
        for(auto entry : vizConfiguration->data[simulation][time])
        {
            auto attributeName = entry.first;
            vizConfiguration->grids[0][0]->GetPointData()->RemoveArray(attributeName.c_str());
        }
    }*/
    
    vizConfiguration->data[simulation][time].clear();
    vizConfiguration->semaphore.notify();    
    vizConfiguration->assyncHandlersMutex.lock();
    vizConfiguration->assyncHandlersToClose.push_back(std::move(vizConfiguration->assyncHandlers[simulation][time]));
    vizConfiguration->assyncHandlers[simulation].erase(time);
    vizConfiguration->assyncHandlersMutex.unlock();
    
    vizConfiguration->counterMutex.lock();
    vizConfiguration->counter++;
    vizConfiguration->counterMutex.unlock();
    //vizConfiguration->filledCells[simulation][time] = NULL;
    //vizConfiguration->totalCells[simulation][time] = NULL;
}

void processSubtar(VizConfigPtr vizConfiguration, SubtarPtr subtar, StorageManagerPtr storageManager)
{
    DatasetPtr filter;
    int64_t simLower=0, simUpper=0;
    int64_t timeLower=0, timeUpper=0;

    for(auto entry : subtar->GetDimSpecs())
    {
        auto dimSpecs = entry.second;
        if(dimSpecs->type != ORDERED)
            throw std::runtime_error("Subtars must be implemented with ORDERED specifications.");
    }

    if(vizConfiguration->simDimension != NULL)
    {
        auto simSpecs = subtar->GetDimensionSpecificationFor(vizConfiguration->simDimension->GetName());
        simLower = simSpecs->lower_bound;
        simUpper = simSpecs->upper_bound;
    }

    if(vizConfiguration->timeDimension != NULL)
    {
        auto timeSpecs = subtar->GetDimensionSpecificationFor(vizConfiguration->timeDimension->GetName());
        timeLower = timeSpecs->lower_bound;
        timeUpper = timeSpecs->upper_bound;
    }

    unordered_map<string, DatasetPtr> datasets;
    bool manySims = simLower != simUpper;
    bool manyTimeSteps = timeLower != timeUpper;
    DatasetPtr mapping = NULL; 
    DataElementPtr simDimension = vizConfiguration->simDimension;
    DataElementPtr timeDimension = vizConfiguration->timeDimension;

    for(int64_t simulation = simLower; simulation <= simUpper; simulation++)
    {
        for(int64_t time = timeLower; time <= timeUpper; time++)
        {
            int64_t filledCells = 0;
            filter = NULL;
            if(manySims && manyTimeSteps)
            {
                datasets = sliceSubtars(simDimension, timeDimension, storageManager, 
                            subtar, simulation, time, filter);
            }
            else if(manySims)
            {   
                datasets = sliceSubtars(simDimension, timeDimension, storageManager, 
                            subtar, simulation, NONE, filter);
            }
            else if(manyTimeSteps)
            {
                datasets = sliceSubtars(simDimension, timeDimension, storageManager, 
                            subtar, NONE, time, filter);
            }
            else
            {
                datasets = sliceSubtars(simDimension, timeDimension, storageManager, 
                            subtar, NONE, NONE, filter);
            }
            
            if(vizConfiguration->type == IMAGE || vizConfiguration->type == STRUCTURED)
            {
                if(mapping == NULL)
                {
                    mapping = createMapping(vizConfiguration, storageManager, subtar, filter);
                }
            }
            else if(vizConfiguration->type == UNSTRUCTURED)
            {
                string indexElementName = vizConfiguration->index->GetName();
                DatasetPtr fullMapping; DimSpecPtr dimSpecs = subtar->GetDimensionSpecificationFor(indexElementName);
                
                if(vizConfiguration->singleGrid)
                    fullMapping = vizConfiguration->mapping[0][0];
                else
                    fullMapping = vizConfiguration->mapping[simulation][time];
                
                mapping = storageManager->Create(LONG_TYPE, dimSpecs->GetLength());
                storageManager->Copy(fullMapping, dimSpecs->lower_bound, dimSpecs->upper_bound, 0, 1, mapping);
            }

            for(auto entry : datasets)
            {
                string attributeName = entry.first;
                DatasetPtr dataset = entry.second;

                if(vizConfiguration->data[simulation][time][attributeName] == NULL)
                {
                    int64_t totalSize;
                    if(vizConfiguration->singleGrid)
                        totalSize = vizConfiguration->totalCells[0][0];
                    else 
                        totalSize = vizConfiguration->totalCells[simulation][time];
                    
                    DatasetPtr newDataset = storageManager->Create(dataset->type, totalSize);
                    vizConfiguration->data[simulation][time][attributeName] = newDataset;
                }
                
                if(mapping != NULL)
                {
                    storageManager->Copy(dataset, mapping,
                        vizConfiguration->data[simulation][time][attributeName], filledCells);
                }
                else
                {
                    vizConfiguration->data[simulation][time][attributeName] = dataset;
                    filledCells=dataset->entry_count;
                }
            }

            int64_t totalCells = 0;
            int64_t filled = vizConfiguration->filledCells[simulation][time];
            vizConfiguration->filledCells[simulation][time]=filled+filledCells;
       
            if(!vizConfiguration->singleGrid)
               totalCells = vizConfiguration->totalCells[simulation][time];
            else
               totalCells = vizConfiguration->totalCells[0][0];
            
            bool totallyFilled = vizConfiguration->filledCells[simulation][time] == totalCells;
                              
            if(totallyFilled)
            {
                vizConfiguration->semaphore.wait();
                auto handler = std::future<void>(std::async(launch::async, processGrid, vizConfiguration, storageManager, time, simulation));
                vizConfiguration->assyncHandlersMutex.lock();
                vizConfiguration->assyncHandlers[simulation][time] = std::move(handler); 
                vizConfiguration->assyncHandlersToClose.clear();
                vizConfiguration->assyncHandlersMutex.unlock();
                
                vizConfiguration->counterMutex.lock();
                vizConfiguration->spawnThreads++;
                vizConfiguration->counterMutex.unlock();
                //processGrid(vizConfiguration, storageManager, time, simulation);
            }
        }
    }
}


void cleanUp(VizConfigPtr vizConfiguration)
{
    for(DatasetHandlerPtr handler : vizConfiguration->handlersToClose)
    {
        handler->Close();
    }   
}

/*catalyze(field_data_tar1, [geometry_tar], [topology_tar], catalyst_script, output_path)*/
int catalyze(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine)
{
    try
    {
        VizConfigPtr vizConfiguration;
        SubtarPtr subtar; int32_t subtarCount = 0;
        DefaultEnginePtr defaultEngine = DEFAULT_ENGINE(engine);
        
        vizConfiguration = createVizConfiguration(operation, configurationManager);      
        initializeGrids(vizConfiguration, storageManager, defaultEngine);
        string fieldTarName = vizConfiguration->fieldData->GetName();
        auto generator = defaultEngine->GetGenerators()[fieldTarName];
        
        while(true)
        {
            subtar = generator->GetSubtar(subtarCount);     
            if(subtar == NULL) break;
            processSubtar(vizConfiguration, subtar, storageManager);
            generator->TestAndDisposeSubtar(subtarCount);
            subtarCount++;
        }
        
        waitAll(vizConfiguration);
        cleanUp(vizConfiguration);
        
    }    
    catch(std::exception& e)
    {
        string error = queryDataManager->GetErrorResponse();
        queryDataManager->SetErrorResponseText(e.what()+string("\n")+error);
        return SAVIME_FAILURE;
    }

    return SAVIME_SUCCESS;
}
