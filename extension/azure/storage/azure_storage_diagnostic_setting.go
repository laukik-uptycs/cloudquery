package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/fatih/structs"
	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/cloudquery/extension/azure"

	"github.com/Uptycs/basequery-go/plugin/table"
	"github.com/Uptycs/cloudquery/utilities"

	diagnostic "github.com/Azure/azure-sdk-for-go/services/preview/monitor/mgmt/2021-07-01-preview/insights"
)

const storageDiagnosticSetting string = "azure_storage_diagnostic_setting"

// StorageDiagnosticSettingsColumns returns the list of columns in the table
func StorageDiagnosticSettingColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("id"),
		table.TextColumn("name"),
		table.TextColumn("storageAccountId"),
		table.TextColumn("type"),
		table.TextColumn("propertise_log"),
		table.TextColumn("propertise_metrics"),
		// table.TextColumn("event_hub_authorization_rule_id"),
		// table.TextColumn("event_hub_name"),
		// table.TextColumn("log_analytics_destination_type"),
		// table.TextColumn("logs"),
		// table.TextColumn("logs_category"),
		// table.TextColumn("logs_enabled"),
		// table.TextColumn("logs_retention_policy"),
		// table.IntegerColumn("logs_retention_policy_days"),
		// table.TextColumn("logs_retention_policy_enabled"),
		// table.TextColumn("metrics"),
		// table.TextColumn("metrics_category"),
		// table.TextColumn("metrics_enabled"),
		// table.TextColumn("metrics_retention_policy"),
		// table.IntegerColumn("metrics_retention_policy_days"),
		// table.TextColumn("metrics_retention_policy_enabled"),
		// table.TextColumn("metrics_time_grain"),
		// table.TextColumn("service_bus_rule_id"),
		// table.TextColumn("storage_account_id"),
		// table.TextColumn("workspace_id"),
	}
}

// StorageDiagnosticSettingsGenerate returns the rows in the table for all configured diagnostic settings
func StorageDiagnosticSettingsGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": storageDiagnosticSetting,
			"account":   "default",
		}).Info("processing diagnostic setting")
		results, err := processStorageDiagnosticSetting(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": storageDiagnosticSetting,
				"account":   account.SubscriptionID,
			}).Info("processing diagnostic setting")
			results, err := processStorageDiagnosticSetting(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processStorageDiagnosticSetting(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[storageDiagnosticSetting]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": storageDiagnosticSetting,
			"error":     err.Error(),
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go getStorageAccountId(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()
	return resultMap, nil
}

func getStorageAccountId(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	var diagnosticSettings []diagnostic.DiagnosticSettingsResource
	for resourceItr, err := getStorageAccounts(session, rg); resourceItr.NotDone(); err = resourceItr.Next() {
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     storageAccount,
				"resourceGroup": rg,
				"errString":     err.Error(),
			}).Error("failed to get resource list")
			continue
		}

		resource := resourceItr.Value()
		diagnosticSetting, err := getStorageDiagnosticSetting(session, rg, wg, resultMap, tableConfig, *resource.ID)

		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     storageDiagnosticSetting,
				"resourceGroup": rg,
				"error":         err.Error(),
			}).Error("failed to get storage diagnostic settings")
			continue
		}

		diagnosticSettings = append(diagnosticSettings, *diagnosticSetting...)
	}

	addStorageDiagnosticSetting(session, rg, resultMap, tableConfig, diagnosticSettings)
}

func addStorageDiagnosticSetting(session *azure.AzureSession, rg string, resultMap *[]map[string]string, tableConfig *utilities.TableConfig, diagnosticSettings []diagnostic.DiagnosticSettingsResource) {
	for _, diagnosticSetting := range diagnosticSettings {
		structs.DefaultTagName = "json"
		resMap := structs.Map(diagnosticSetting)
		byteArr, err := json.Marshal(resMap)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     storageDiagnosticSetting,
				"resourceGroup": rg,
				"errString":     err.Error(),
			}).Error("failed to marshal response")
			continue
		}

		table := utilities.NewTable(byteArr, tableConfig)
		for _, row := range table.Rows {
			result := azure.RowToMap(row, session.SubscriptionId, "", rg, tableConfig)
			*resultMap = append(*resultMap, result)
		}
	}
}

func getStorageDiagnosticSetting(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig, resourceURI string) (*[]diagnostic.DiagnosticSettingsResource, error) {
	svcClient := diagnostic.NewDiagnosticSettingsClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer

	returnObj, err := svcClient.List(context.Background(), resourceURI)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":     storageDiagnosticSetting,
			"resourceGroup": rg,
			"error":         err.Error(),
		}).Error("failed to get List")
		return nil, err
	}
	resource := returnObj.Value

	return resource, nil
}
