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
#include <cstdlib>
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
#include <vtkXMLImageDataReader.h>
#include <vtkXMLStructuredGridWriter.h>
#include <vtkXMLStructuredGridReader.h>
#include <vtkXMLUnstructuredGridWriter.h>
#include <vtkXMLUnstructuredGridReader.h>
#include <future>
#include <math.h>

using namespace std;

typedef vtkSmartPointer<vtkCPProcessor> Processor;

enum Params
{
    PROG, SCRIPT, INPUT_GRID, OUTPUT_DIR, TIME_STEP, REAL_TIME_STEP
};

string get_file_extension(const string& file_name)
{
    if(file_name.find_last_of(".") != std::string::npos)
        return file_name.substr(file_name.find_last_of(".")+1);
    return "";
}

vtkSmartPointer<vtkDataSet> read_grid(string file)
{
    #define VTI "vti"
    #define VTS "vts"
    #define VTU "vtu"

    vtkSmartPointer<vtkDataSet> dataset;
    string ext = get_file_extension(file);
    
    if(ext == VTI)
    {
        vtkSmartPointer<vtkXMLImageDataReader> reader = 
        vtkSmartPointer<vtkXMLImageDataReader>::New();
        reader->SetFileName(file.c_str());
        reader->Update();
        reader->GetOutput()->Register(reader);
        dataset.TakeReference(vtkDataSet::SafeDownCast(reader->GetOutput()));
    }
    else if(ext == VTS)
    {
        vtkSmartPointer<vtkXMLStructuredGridReader> reader = 
        vtkSmartPointer<vtkXMLStructuredGridReader>::New();
        reader->SetFileName(file.c_str());
        reader->Update();
        reader->GetOutput()->Register(reader);
        dataset.TakeReference(vtkDataSet::SafeDownCast(reader->GetOutput()));
    }
    else if(ext == VTU)
    {
        vtkSmartPointer<vtkXMLUnstructuredGridReader> reader = 
        vtkSmartPointer<vtkXMLUnstructuredGridReader>::New();
        reader->SetFileName(file.c_str());    
        reader->Update();
        reader->GetOutput()->Register(reader);
        dataset.TakeReference(vtkDataSet::SafeDownCast(reader->GetOutput()));
    }
    
    return dataset;
}

void process_grid(vtkSmartPointer<vtkDataSet> grid, string script, string output_dir, int32_t time_step, double real_time_step)
{
    #define SIM_NAME "input"
    chdir(output_dir.c_str());
    
    Processor processor = Processor::New();
    processor->Initialize();
    vtkNew<vtkCPPythonScriptPipeline> pipeline;        
    pipeline->Initialize(script.c_str());
    processor->AddPipeline(pipeline.GetPointer());
    
    auto dataDescription = vtkSmartPointer<vtkCPDataDescription>::New();    
    dataDescription->ForceOutputOn();
    dataDescription->AddInput(SIM_NAME);
    dataDescription->SetTimeData(real_time_step, time_step);
    dataDescription->GetInputDescriptionByName(SIM_NAME)->SetGrid(grid);
    processor->CoProcess(dataDescription);
}

void remove_file(string file)
{
    remove(file.c_str( ));
}

int main(int argc, char** argv) 
{
    #define ARGS_NUMBER 6
    assert(argc == ARGS_NUMBER);
    
    string script = string(argv[SCRIPT]);
    string grid_file = string(argv[INPUT_GRID]);
    string output_dir = string(argv[OUTPUT_DIR]);
    int32_t time_step = atoi(argv[TIME_STEP]);
    double real_time_step = atof(argv[REAL_TIME_STEP]);

    auto grid = read_grid(grid_file);
    process_grid(grid, script, output_dir, time_step, real_time_step);
    //remove_file(grid_file);
        
    return 0;
}

