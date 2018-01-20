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
#ifndef DDL_OPERATORS_H
#define DDL_OPERATORS_H

#include "include/engine.h"
#include "include/util.h"
#include "include/query_data_manager.h"
#include "include/storage_manager.h"

int create_tars(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int create_tar(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int create_type(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int create_dataset(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int insert_subtar(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int drop_tars(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int drop_tar(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int drop_type(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int drop_dataset(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int show(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);

#endif /* DDL_OPERATORS_H */

