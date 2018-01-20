#!/bin/bash


#Creating TARs
savimec 'create_tar("top",   "IncidenceTopology", "implicit, incidentee, int, 0, 499999, 1 | implicit, incident, int, 0, 5, 1", "val, long", "incident, incident, incidentee, incidentee, val, index");'
savimec 'create_tar("geo",   "CartesianGeometry3d", "implicit, index, long, 0, 100000, 1", "x, float | y, float | z, float", "index, index, x, x, y, y, z, z");'
savimec 'create_tar("data",  "UnstructuredFieldData", "implicit, time, int, 0, 1000, 1| implicit, index, long, 0, 100000, 1", "pressure,float", "time, time, index, index");'
savimec 'create_tar("data2", "CartesianFieldData2d", "implicit, time, int, 1, 100, 1| implicit, x , int, 1, 100, 1 | implicit, y, long, 1, 100, 1", "pressure, float", "time, time, x, x, y, y");'

#Creating datasets
savimec 'create_dataset("top_data:long", "@'$(pwd)'/top");'
savimec 'create_dataset("x:float", "@'$(pwd)'/x.data");'
savimec 'create_dataset("y:float", "@'$(pwd)'/y.data");'
savimec 'create_dataset("z:float", "@'$(pwd)'/z.data");'
savimec 'create_dataset("d100:float", "@'$(pwd)'/100ts.data");'
savimec 'create_dataset("hemo:float", "@'$(pwd)'/hemo.data");'

#Loading subtars
savimec 'load_subtar("top",   "ordered, incidentee, 0, 460198 | ordered, incident, 0, 5 ", "val, top_data");'
savimec 'load_subtar("geo",   "ordered, index, 0, 92539", "x, x| y, y| z, z");'
savimec 'load_subtar("data",  "ordered, time, 0, 239| ordered, index, 0, 92539", "pressure, hemo");'
savimec 'load_subtar("data2", "ordered, time, #1, #100| ordered, x, #1, #100| ordered, y, #1, #100", "pressure, d100");'
	
#Creating vizualizations
mkdir $(pwd)/viz
#savimec 'catalyze(subset(data2,  time, 1, 10, x, 1, 50, y, 1, 25), "'$(pwd)'/viz/");'
savimec 'catalyze(subset(data, time, 50, 100), where(geo, z < -13.0), top, "'$(pwd)'/viz/");'
