package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/fatih/structs"
	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/basequery-go/plugin/table"
	"github.com/Uptycs/cloudquery/extension/azure"
	"github.com/Uptycs/cloudquery/utilities"

	"github.com/Azure/azure-sdk-for-go/services/sql/mgmt/2014-04-01/sql"
)

const sqlServer string = "azure_sql_server"

// SqlServerCloumns returns the list of cloums in the table
func SqlServerCloumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("id"),
		table.TextColumn("kind"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		table.TextColumn("properties"),
		table.TextColumn("administrator_login"),
		table.TextColumn("administrator_login_password"),
		table.TextColumn("external_administrator_login"),
		table.TextColumn("external_administrator_sid"),
		table.TextColumn("fully_qualified_domain_name"),
		table.TextColumn("state"),
		table.TextColumn("version"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
	}
}

// SqlServerGenerate returns the row in the table for all configured sql server
func SqlServerGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": sqlServer,
			"account":   "default",
		}).Info("processing sql server")

		results, err := processSqlServer(nil)

		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":      sqlServer,
				"account":        account,
				"SubscriptionId": account.SubscriptionID,
			}).Info("processing accounts")

			results, err :=
				processSqlServer(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processSqlServer(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[sqlServer]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": sqlServer,
		}).Error("failed to get table configuration")

		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go addSqlServer(session, group, &wg, &resultMap, tableConfig)
	}

	wg.Wait()
	return resultMap, nil
}

func addSqlServer(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	resources, err := getSqlServer(session, rg)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":      sqlServer,
			"rescourceGroup": rg,
			"errString":      err.Error(),
		}).Error("failed to get server list from api")
	}

	for _, sqlServer := range *resources.Value {
		structs.DefaultTagName = "json"
		resMap := structs.Map(sqlServer)
		byteArr, err := json.Marshal(resMap)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     sqlServer,
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

func getSqlServer(session *azure.AzureSession, rg string) (result sql.ServerListResult, err error) {
	svcClient := sql.NewServersClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	return svcClient.ListByResourceGroup(context.Background(), rg)
}
