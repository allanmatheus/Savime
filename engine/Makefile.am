if CATALYST
bin_PROGRAMS = savime_catalyst
savime_catalyst_SOURCES = catalyst.cpp
savime_catalyst_CXXFLAGS = -I/usr/local/include/paraview-5.4
savime_catalyst_LDFLAGS = -Wl,-rpath,/usr/local/lib/paraview-5.4/ -L/usr/local/lib/paraview-5.4/ 
savime_catalyst_LDADD = -lpthread ../rdmap/librdmap.a -lrdmacm -libverbs /usr/local/lib/paraview-5.4/*.so  
endif