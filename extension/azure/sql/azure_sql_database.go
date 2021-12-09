package sql

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

	"github.com/Azure/azure-sdk-for-go/services/sql/mgmt/2014-04-01/sql"
)

const sqlDatabase string = "azure_sql_database"

// SqlDatabaseColumns returns the list of columns in the table
func SqlDatabaseColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("id"),
		table.TextColumn("kind"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		table.TextColumn("properties"),
		table.TextColumn("collation"),
		table.BigIntColumn("containment_state"),
		table.TextColumn("create_mode"),
		table.TextColumn("creation_date"),
		table.TextColumn("current_service_objective_id"),
		table.TextColumn("database_id"),
		table.TextColumn("default_secondary_location"),
		table.TextColumn("earliest_restore_date"),
		table.TextColumn("edition"),
		table.TextColumn("elastic_pool_name"),
		table.TextColumn("failover_group_id"),
		table.TextColumn("max_size_bytes"),
		table.TextColumn("read_scale"),
		table.TextColumn("recommended_index"),
		// table.TextColumn("recommended_index_id"),
		// table.TextColumn("recommended_index_name"),
		// table.TextColumn("recommended_index_type"),
		table.TextColumn("recovery_services_recovery_point_resource_id"),
		table.TextColumn("requested_service_objective_id"),
		table.TextColumn("requested_service_objective_name"),
		table.TextColumn("restore_point_in_time"),
		table.TextColumn("sample_name"),
		table.TextColumn("service_level_objective"),
		table.TextColumn("service_tier_advisors"),
		// table.TextColumn("service_tier_advisors_id"),
		// table.TextColumn("service_tier_advisors_name"),
		// table.TextColumn("service_tier_advisors_type"),
		table.TextColumn("source_database_deletion_date"),
		table.TextColumn("source_database_id"),
		table.TextColumn("status"),
		table.TextColumn("transparent_data_encryption"),
		// table.TextColumn("transparent_data_encryption_id"),
		// table.TextColumn("transparent_data_encryption_location"),
		// table.TextColumn("transparent_data_encryption_name"),
		// table.TextColumn("transparent_data_encryption_type"),
		table.TextColumn("zone_redundant"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
		table.TextColumn("server_name"),
	}
}

// SqlDatabaseGenerate returns the rows in the table for all configured accounts
func SqlDatabaseGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": sqlDatabase,
			"account":   "default",
		}).Info("processing account")
		results, err := processAccountSqlDatabase(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": sqlDatabase,
				"account":   account.SubscriptionID,
			}).Info("processing account")
			results, err := processAccountSqlDatabase(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processAccountSqlDatabase(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[sqlDatabase]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": sqlDatabase,
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go getSqlServerNameForTable(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()

	return resultMap, nil
}

func getSqlServerNameForTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	resourceItr, err := getSqlServer(session, rg)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":     sqlDatabase,
			"resourceGroup": rg,
			"error":         err.Error(),
		}).Error("failed to get server list")
	}

	for _, server := range *resourceItr.Value {
		setSqlDatabaseDataToTable(session, rg, wg, resultMap, tableConfig, *server.Name)
	}
}

func setSqlDatabaseDataToTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig, serverName string) {
	resourceItr, err := getSqlDatabaseData(session, rg, serverName)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":     sqlDatabase,
			"resourceGroup": rg,
			"errString":     err.Error(),
		}).Error("failed to get sql database list")
	}

	for _, resource := range *resourceItr.Value {
		structs.DefaultTagName = "json"
		resMap := structs.Map(resource)
		byteArr, err := json.MarshalIndent(resMap, "", "	")
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     sqlDatabase,
				"resourceGroup": rg,
				"errString":     err.Error(),
			}).Error("failed to marshal response")
			continue
		}
		table := utilities.NewTable(byteArr, tableConfig)

		for _, row := range table.Rows {
			result := azure.RowToMap(row, session.SubscriptionId, "", rg, tableConfig)
			result["server_name"] = serverName
			*resultMap = append(*resultMap, result)
		}
	}
}

func getSqlDatabaseData(session *azure.AzureSession, rg string, serverName string) (result sql.DatabaseListResult, err error) {
	svcClient := sql.NewDatabasesClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	return svcClient.ListByServer(context.Background(), rg, serverName, "", "")
}
