ACLOCAL_AMFLAGS =-I m4

AM_CPPFLAGS = \
    -I@top_srcdir@/include \
    -I@top_srcdir@/../core/include \
    -I@top_srcdir@/../core/os/linux/include \
    -I@top_srcdir@/../common/include \
    -I@top_srcdir@/../common/os/linux/include \
    -I@top_srcdir@/../binary/include \
    -I@top_srcdir@/../jni/include \
    -I@top_srcdir@/../jni/os/linux/include \
    -I$(JAVA_HOME)/include \
    -I$(JAVA_HOME)/include/linux \
    -DIGNITE_IMPL \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

bin_PROGRAMS = savime

if CATALYST
savime_SOURCES = main.cpp metadata.cpp system_logger.cpp builder.cpp query_data_manager.cpp ../configuration/default_config_manager.cpp ../connection/default_connection_manager.cpp ../engine/default_engine.cpp  ../job/default_job_manager.cpp ../job/default_server_job.cpp ../metada/default_metadata_manager.cpp ../optimizer/default_optimizer.cpp ../parser/default_parser.cpp ../query/default_query_data_manager.cpp ../parser/bison.cpp  ../parser/flex.cpp  ../parser/schema_builder.cpp ../storage/default_storage_manager.cpp ../engine/ddl_operators.cpp ../engine/dml_operators.cpp ../engine/viz.cpp
savime_CXXFLAGS = -I/usr/local/include/paraview-5.4 -DCATALYST
savime_LDFLAGS = -Wl,-rpath,/usr/local/lib/paraview-5.4/ -L/usr/local/lib/paraview-5.4/ -I/usr/local/include/paraview-5.4
savime_LDADD = -lpthread /usr/local/lib/libvtk* #../rdmap/librdmap.a -lrdmacm -libverbs
else
savime_SOURCES = main.cpp metadata.cpp system_logger.cpp builder.cpp query_data_manager.cpp ../configuration/default_config_manager.cpp ../connection/default_connection_manager.cpp ../engine/default_engine.cpp  ../job/default_job_manager.cpp ../job/default_server_job.cpp ../metada/default_metadata_manager.cpp ../optimizer/default_optimizer.cpp ../parser/default_parser.cpp ../query/default_query_data_manager.cpp ../parser/bison.cpp  ../parser/flex.cpp  ../parser/schema_builder.cpp ../storage/default_storage_manager.cpp ../engine/ddl_operators.cpp ../engine/dml_operators.cpp 
savime_LDADD = -lpthread #../rdmap/librdmap.a -lrdmacm -libverbs
endif
