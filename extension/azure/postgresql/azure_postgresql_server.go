package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/cloudquery/extension/azure"

	"github.com/Uptycs/basequery-go/plugin/table"
	"github.com/Uptycs/cloudquery/utilities"

	"github.com/Azure/azure-sdk-for-go/services/postgresql/mgmt/2020-01-01/postgresql"
	"github.com/fatih/structs"
)

const postgresqlServer string = "azure_postgresql_server"

// PostgresqlServerColumns returns the list of columns in the table
func PostgresqlServerColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("id"),
		table.TextColumn("identity"),
		// table.TextColumn("identity_principal_id"),
		// table.TextColumn("identity_tenant_id"),
		// table.TextColumn("identity_type"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		// table.TextColumn("properties"),
		table.TextColumn("administrator_login"),
		table.TextColumn("byok_enforcement"),
		table.TextColumn("earliest_restore_date"),
		table.TextColumn("fully_qualified_domain_name"),
		table.TextColumn("infrastructure_encryption"),
		table.TextColumn("master_server_id"),
		table.TextColumn("minimal_tls_version"),
		table.TextColumn("private_endpoint_connections"),
		// table.TextColumn("private_endpoint_connections_id"),
		// table.TextColumn("private_endpoint_connections_properties"),
		// table.TextColumn("private_endpoint_connections_properties_private_endpoint"),
		// table.TextColumn("private_endpoint_connections_properties_private_endpoint_id"),
		// table.TextColumn("private_endpoint_connections_properties_private_link_service_connection_state"),
		// table.TextColumn("private_endpoint_connections_properties_private_link_service_connection_state_actions_required"),
		// table.TextColumn("private_endpoint_connections_properties_private_link_service_connection_state_description"),
		// table.TextColumn("private_endpoint_connections_properties_private_link_service_connection_state_status"),
		// table.TextColumn("private_endpoint_connections_properties_provisioning_state"),
		table.TextColumn("public_network_access"),
		table.IntegerColumn("replica_capacity"),
		table.TextColumn("replication_role"),
		table.TextColumn("ssl_enforcement"),
		table.TextColumn("storage_profile"),
		// table.TextColumn("storage_profile_backup_retention_days"),
		// table.TextColumn("storage_profile_geo_redundant_backup"),
		// table.TextColumn("storage_profile_storage_autogrow"),
		// table.TextColumn("storage_profile_storage_mb"),
		table.TextColumn("user_visible_state"),
		table.TextColumn("version"),
		table.TextColumn("sku"),
		// table.TextColumn("sku_capacity"),
		// table.TextColumn("sku_family"),
		// table.TextColumn("sku_name"),
		// table.TextColumn("sku_size"),
		// table.TextColumn("sku_tier"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
	}
}

// PostgresqlServersGenerate returns the rows in the table for all configured accounts
func PostgresqlServersGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": postgresqlServer,
			"account":   "default",
		}).Info("processing account")
		results, err := processAccountPostgresqlServers(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": postgresqlServer,
				"account":   account.SubscriptionID,
			}).Info("processing account")
			results, err := processAccountPostgresqlServers(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processAccountPostgresqlServers(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[postgresqlServer]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": postgresqlServer,
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go setPostgresqlServertoTable(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()
	return resultMap, nil
}

func setPostgresqlServertoTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	resources, err := getPostgresqlServerData(session, rg)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":      postgresqlServer,
			"rescourceGroup": rg,
			"errString":      err.Error(),
		}).Error("failed to get postgresql server list from api")
	}

	for _, server := range *resources.Value {
		structs.DefaultTagName = "json"
		resMap := structs.Map(server)
		byteArr, err := json.Marshal(resMap)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     postgresqlServer,
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
func getPostgresqlServerData(session *azure.AzureSession, rg string) (result postgresql.ServerListResult, err error) {

	svcClient := postgresql.NewServersClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	return svcClient.ListByResourceGroup(context.Background(), rg)

}
