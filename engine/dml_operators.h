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
#ifndef DML_OPERATORS_H
#define DML_OPERATORS_H

#include "include/engine.h"
#include "include/util.h"
#include "include/query_data_manager.h"
#include "include/storage_manager.h"''

int scan(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int select(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int filter(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int subset(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int logical(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int comparison(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int arithmetic(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int cross_join(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int equijoin(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int dimjoin(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int slice(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int aggregate(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int split(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);
int user_defined(int32_t subtarIndex, OperationPtr operation, ConfigurationManagerPtr configurationManager, QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager, EnginePtr engine);

#endif /* DML_OPERATORS_H */

