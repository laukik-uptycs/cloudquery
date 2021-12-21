package cosmosdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/cloudquery/extension/azure"

	"github.com/Uptycs/basequery-go/plugin/table"
	"github.com/Uptycs/cloudquery/utilities"

	"github.com/Azure/azure-sdk-for-go/services/cosmos-db/mgmt/2021-10-15/documentdb"
	"github.com/fatih/structs"
)

const cosmosdbSqldb string = "azure_cosmosdb_sqldb"

// CosmosdbSqldbsColumns returns the list of columns in the table
func CosmosdbSqldbsColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("id"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		// table.TextColumn("properties"),
		table.TextColumn("options"),
		// table.TextColumn("options_autoscale_settings"),
		// table.IntegerColumn("options_autoscale_settings_max_throughput"),
		// table.IntegerColumn("options_throughput"),
		table.TextColumn("resource"),
		// table.TextColumn("resource__colls"),
		// table.TextColumn("resource__etag"),
		// table.TextColumn("resource__rid"),
		// table.DoubleColumn("resource__ts"),
		// table.TextColumn("resource__users"),
		// table.TextColumn("resource_id"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
	}
}

// CosmosdbSqldbsGenerate returns the rows in the table for all configured accounts
func CosmosdbSqldbsGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": cosmosdbSqldb,
			"account":   "default",
		}).Info("processing account")
		results, err := processAccountCosmosdbSqldbs(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": cosmosdbSqldb,
				"account":   account.SubscriptionID,
			}).Info("processing account")
			results, err := processAccountCosmosdbSqldbs(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processAccountCosmosdbSqldbs(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[cosmosdbSqldb]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": cosmosdbSqldb,
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go getCosmosdbAccountforsqldb(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()
	return resultMap, nil
}

func getCosmosdbAccountforsqldb(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()
	accoutnamelist, err := getCosmosdbAccountData(session, rg)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":      cosmosdbSqldb,
			"rescourceGroup": rg,
			"errString":      err.Error(),
		}).Error("failed to get cosmosdb account list from api")
	}
	for _, accountnameinfo := range *accoutnamelist.Value {
		setCosmosdbSqldbDataToTable(session, rg, wg, resultMap, tableConfig, *accountnameinfo.Name)
	}

}
func setCosmosdbSqldbDataToTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig, accountName string) {
	sqldblist, err := getCosmosdbSqldbData(session, rg, accountName)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":     cosmosdbSqldb,
			"resourceGroup": rg,
			"errString":     err.Error(),
		}).Error("failed to get sql database list")
	}

	for _, sqldb := range *sqldblist.Value {
		structs.DefaultTagName = "json"
		resMap := structs.Map(sqldb)
		byteArr, err := json.Marshal(resMap)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     cosmosdbSqldb,
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
func getCosmosdbSqldbData(session *azure.AzureSession, rg string, accountName string) (result documentdb.SQLDatabaseListResult, err error) {
	svcClient := documentdb.NewSQLResourcesClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	return svcClient.ListSQLDatabases(context.Background(), rg, accountName)
}
