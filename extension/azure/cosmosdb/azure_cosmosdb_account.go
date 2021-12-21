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

const cosmosdbAccount string = "azure_cosmosdb_account"

// CosmosdbAccountColumns returns the list of columns in the table
func CosmosdbAccountColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("id"),
		table.TextColumn("identity"),
		// table.TextColumn("identity_principal_id"),
		// table.TextColumn("identity_tenant_id"),
		// table.TextColumn("identity_type"),
		// table.TextColumn("identity_user_assigned_identities"),
		table.TextColumn("kind"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		// table.TextColumn("properties"),
		table.TextColumn("analytical_storage_configuration"),
		// table.TextColumn("analytical_storage_configuration_schema_type"),
		table.TextColumn("api_properties"),
		// table.TextColumn("api_properties_server_version"),
		table.TextColumn("backup_policy"),
		table.TextColumn("capabilities"),
		// table.TextColumn("capabilities_name"),
		table.TextColumn("capacity"),
		// table.TextColumn("capacity_total_throughput_limit"),
		table.TextColumn("connector_offer"),
		table.TextColumn("consistency_policy"),
		// table.TextColumn("consistency_policy_default_consistency_level"),
		// table.TextColumn("consistency_policy_max_interval_in_seconds"),
		// table.BigIntColumn("consistency_policy_max_staleness_prefix"),
		table.TextColumn("cors"),
		// table.TextColumn("cors_allowed_headers"),
		// table.TextColumn("cors_allowed_methods"),
		// table.TextColumn("cors_allowed_origins"),
		// table.TextColumn("cors_exposed_headers"),
		// table.BigIntColumn("cors_max_age_in_seconds"),
		table.TextColumn("create_mode"),
		table.TextColumn("database_account_offer_type"),
		table.TextColumn("default_identity"),
		table.TextColumn("disable_key_based_metadata_write_access"),
		table.TextColumn("disable_local_auth"),
		table.TextColumn("document_endpoint"),
		table.TextColumn("enable_analytical_storage"),
		table.TextColumn("enable_automatic_failover"),
		table.TextColumn("enable_cassandra_connector"),
		table.TextColumn("enable_free_tier"),
		table.TextColumn("enable_multiple_write_locations"),
		table.TextColumn("failover_policies"),
		// table.TextColumn("failover_policies_failover_priority"),
		// table.TextColumn("failover_policies_id"),
		// table.TextColumn("failover_policies_location_name"),
		table.TextColumn("instance_id"),
		table.TextColumn("ip_rules"),
		// table.TextColumn("ip_rules_ip_address_or_range"),
		table.TextColumn("is_virtual_network_filter_enabled"),
		table.TextColumn("key_vault_key_uri"),
		table.TextColumn("locations"),
		// table.TextColumn("locations_document_endpoint"),
		// table.TextColumn("locations_failover_priority"),
		// table.TextColumn("locations_id"),
		// table.TextColumn("locations_is_zone_redundant"),
		// table.TextColumn("locations_location_name"),
		// table.TextColumn("locations_provisioning_state"),
		table.TextColumn("network_acl_bypass"),
		table.TextColumn("network_acl_bypass_resource_ids"),
		table.TextColumn("private_endpoint_connections"),
		// table.TextColumn("private_endpoint_connections_id"),
		// table.TextColumn("private_endpoint_connections_name"),
		// table.TextColumn("private_endpoint_connections_type"),
		table.TextColumn("provisioning_state"),
		table.TextColumn("public_network_access"),
		table.TextColumn("read_locations"),
		// table.TextColumn("read_locations_document_endpoint"),
		// table.TextColumn("read_locations_failover_priority"),
		// table.TextColumn("read_locations_id"),
		// table.TextColumn("read_locations_is_zone_redundant"),
		// table.TextColumn("read_locations_location_name"),
		// table.TextColumn("read_locations_provisioning_state"),
		table.TextColumn("restore_parameters"),
		// table.TextColumn("restore_parameters_databases_to_restore"),
		// table.TextColumn("restore_parameters_databases_to_restore_collection_names"),
		// table.TextColumn("restore_parameters_databases_to_restore_database_name"),
		// table.TextColumn("restore_parameters_restore_mode"),
		// table.TextColumn("restore_parameters_restore_source"),
		// table.TextColumn("restore_parameters_restore_timestamp_in_utc"),
		table.TextColumn("virtual_network_rules"),
		// table.TextColumn("virtual_network_rules_id"),
		// table.TextColumn("virtual_network_rules_ignore_missing_v_net_service_endpoint"),
		table.TextColumn("write_locations"),
		// table.TextColumn("write_locations_document_endpoint"),
		// table.TextColumn("write_locations_failover_priority"),
		// table.TextColumn("write_locations_id"),
		// table.TextColumn("write_locations_is_zone_redundant"),
		// table.TextColumn("write_locations_location_name"),
		// table.TextColumn("write_locations_provisioning_state"),
		table.TextColumn("system_data"),
		// table.TextColumn("system_data_created_at"),
		// table.TextColumn("system_data_created_by"),
		// table.TextColumn("system_data_created_by_type"),
		// table.TextColumn("system_data_last_modified_at"),
		// table.TextColumn("system_data_last_modified_by"),
		// table.TextColumn("system_data_last_modified_by_type"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
	}
}

// CosmosdbAccountsGenerate returns the rows in the table for all configured accounts
func CosmosdbAccountsGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": cosmosdbAccount,
			"account":   "default",
		}).Info("processing account")
		results, err := processAccountCosmosdbAccounts(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": cosmosdbAccount,
				"account":   account.SubscriptionID,
			}).Info("processing account")
			results, err := processAccountCosmosdbAccounts(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processAccountCosmosdbAccounts(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[cosmosdbAccount]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": cosmosdbAccount,
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go setCosmosdbAccounttoTable(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()
	return resultMap, nil
}

func setCosmosdbAccounttoTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	resources, err := getCosmosdbAccountData(session, rg)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName":      cosmosdbAccount,
			"rescourceGroup": rg,
			"errString":      err.Error(),
		}).Error("failed to get cosmosdb account list from api")
	}

	for _, cosmosddaccount := range *resources.Value {
		structs.DefaultTagName = "json"
		resMap := structs.Map(cosmosddaccount)
		byteArr, err := json.Marshal(resMap)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     cosmosdbAccount,
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
func getCosmosdbAccountData(session *azure.AzureSession, rg string) (result documentdb.DatabaseAccountsListResult, err error) {

	svcClient := documentdb.NewDatabaseAccountsClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	return svcClient.ListByResourceGroup(context.Background(), rg)

}
