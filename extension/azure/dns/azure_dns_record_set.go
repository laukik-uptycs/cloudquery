package dns

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/cloudquery/extension/azure"

	"github.com/Uptycs/basequery-go/plugin/table"
	"github.com/Uptycs/cloudquery/utilities"

	"github.com/Azure/azure-sdk-for-go/services/preview/dns/mgmt/2018-03-01-preview/dns"
	"github.com/fatih/structs"
)

const azureDnsRecordSet string = "azure_dns_record_set"

// DnsRecordSetColunmns returns the list of columns in the table
func DnsRecordSetColunmns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("etag"),
		table.TextColumn("id"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		// table.TextColumn("properties"),
		table.BigIntColumn("max_number_of_record_sets"),
		table.BigIntColumn("max_number_of_records_per_record_set"),
		table.TextColumn("name_servers"),
		table.BigIntColumn("number_of_record_sets"),
		table.TextColumn("registration_virtual_networks"),
		// table.TextColumn("registration_virtual_networks_id"),
		table.TextColumn("resolution_virtual_networks"),
		// table.TextColumn("resolution_virtual_networks_id"),
		table.TextColumn("zone_type"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
	}
}

// DnsRecordSetGenerate returns the rows in the table for all configured accounts
func DnsRecordSetGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": azureDnsRecordSet,
			"account":   "default",
		}).Info("processing account")
		results, err := processDnsRecordSet(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": azureDnsRecordSet,
				"account":   account.SubscriptionID,
			}).Info("processing account")
			results, err := processDnsRecordSet(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processDnsRecordSet(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[azureDnsRecordSet]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": azureDnsRecordSet,
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go collectDnsZonetoTable(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()
	return resultMap, nil
}

func collectDnsZonetoTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	for resourcesItr, err := getDnsZoneData(session, rg); resourcesItr.NotDone(); resourcesItr.Next() {
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":      azureDnsRecordSet,
				"rescourceGroup": rg,
				"errString":      err.Error(),
			}).Error("failed to get DNS zones")
		}

		resource := resourcesItr.Value()

		setDnsRecordSettoTable(session, rg, *resource.Name, resultMap, tableConfig)
	}
}

func setDnsRecordSettoTable(session *azure.AzureSession, rg string, zone string, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {

	for resourcesItr, err := getDnsRecordSetData(session, rg, zone); resourcesItr.NotDone(); resourcesItr.Next() {
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":      azureDnsRecordSet,
				"rescourceGroup": rg,
				"errString":      err.Error(),
			}).Error("failed to get DNS zones")
		}

		resource := resourcesItr.Value()

		structs.DefaultTagName = "json"
		resMap := structs.Map(resource)
		byteArr, err := json.Marshal(resMap)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     azureDnsRecordSet,
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
func getDnsRecordSetData(session *azure.AzureSession, rg string, zone string) (result dns.RecordSetListResultIterator, err error) {
	svcClient := dns.NewRecordSetsClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	return svcClient.ListAllByDNSZoneComplete(context.Background(), rg, zone, nil, "")
}
