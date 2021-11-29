/**
 * Copyright (c) 2020-present, The cloudquery authors
 *
 * This source code is licensed as defined by the LICENSE file found in the
 * root directory of this source tree.
 *
 * SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-only)
 */

 package compute

 import (
	 "context"
	 "encoding/json"
	 "fmt"
	 log "github.com/sirupsen/logrus"
	 "sync"
 
	 "github.com/Uptycs/cloudquery/extension/azure"
	 extazure "github.com/Uptycs/cloudquery/extension/azure"
 
	 "github.com/Uptycs/basequery-go/plugin/table"
	 "github.com/Uptycs/cloudquery/utilities"
 
	 "github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-06-01/compute"
 )
 
 const tableName string = "azure_storage_blob_container"

 // StorageBlobContainerColumns returns the list of columns in the table
 func StorageBlobContainerColumns() []table.ColumnDefinition {
	 return []table.ColumnDefinition{

	 }
 }
 
 // StorageBlobContainerGenerate returns the rows in the table for all configured accounts
 func StorageBlobContainerGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	 resultMap := make([]map[string]string, 0)
	 if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		 utilities.GetLogger().WithFields(log.Fields{
			 "tableName": tableName,
			 "account":   "default",
		 }).Info("processing account")
		 results, err := processStorageBlobContainer(nil)
		 if err != nil {
			 return resultMap, err
		 }
		 resultMap = append(resultMap, results...)
	 } else {
		 for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			 utilities.GetLogger().WithFields(log.Fields{
				 "tableName": tableName,
				 "account":   account.SubscriptionID,
			 }).Info("processing account")
			 results, err := processStorageBlobContainer(&account)
			 if err != nil {
				 continue
			 }
			 resultMap = append(resultMap, results...)
		 }
	 }
 
	 return resultMap, nil
 }
 
 func processStorageBlobContainer(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
	 resultMap := make([]map[string]string, 0)
	 var wg sync.WaitGroup
	 session, err := azure.GetAuthSession(account)
	 if err != nil {
		 return resultMap, err
	 }
	 groups, err := azure.GetGroups(session)
 
	 if err != nil {
		 return resultMap, err
	 }
 
	 wg.Add(len(groups))
 
	 tableConfig, ok := utilities.TableConfigurationMap[tableName]
	 if !ok {
		 utilities.GetLogger().WithFields(log.Fields{
			 "tableName": tableName,
		 }).Error("failed to get table configuration")
		 return resultMap, fmt.Errorf("table configuration not found")
	 }
 
	 for _, group := range groups {
		 go getStorageBlobContainer(session, group, &wg, &resultMap, tableConfig)
	 }
	 wg.Wait()
	 return resultMap, nil
 }
 
 func getStorageBlobContainer(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	 defer wg.Done()
 
	 svcClient := compute.NewVirtualMachinesClient(session.SubscriptionId)
	 svcClient.Authorizer = session.Authorizer
 
	 for resourceItr, err := svcClient.ListComplete(context.Background(), rg); resourceItr.NotDone(); err = resourceItr.Next() {
		 if err != nil {
			 utilities.GetLogger().WithFields(log.Fields{
				 "tableName":     tableName,
				 "resourceGroup": rg,
				 "errString":     err.Error(),
			 }).Error("failed to get resource list")
			 continue
		 }
 
		 resource := resourceItr.Value()
		 byteArr, err := json.Marshal(resource)
		 if err != nil {
			 utilities.GetLogger().WithFields(log.Fields{
				 "tableName":     tableName,
				 "resourceGroup": rg,
				 "errString":     err.Error(),
			 }).Error("failed to marshal response")
			 continue
		 }
		 table := utilities.NewTable(byteArr, tableConfig)
		 for _, row := range table.Rows {
			 result := extazure.RowToMap(row, session.SubscriptionId, "", rg, tableConfig)
			 *resultMap = append(*resultMap, result)
		 }
	 }
 }
 