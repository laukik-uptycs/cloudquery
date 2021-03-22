/**
 * Copyright (c) 2020-present, The cloudquery authors
 *
 * This source code is licensed as defined by the LICENSE file found in the
 * root directory of this source tree.
 *
 * SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-only)
 */

package upt_directoryservice

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Uptycs/basequery-go/plugin/table"
	extaws "github.com/Uptycs/cloudquery/extension/aws"
	"github.com/Uptycs/cloudquery/extension/pubsub"
	"github.com/Uptycs/cloudquery/utilities"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice"
	log "github.com/sirupsen/logrus"
)

type UptDescribeDirectoriesTable struct {
	TableName       string
	MaxResults      int32
	IsGlobalTable   bool
	RegionToProcess string
}

func (inventoryTable *UptDescribeDirectoriesTable) GetName() string {
	return inventoryTable.TableName
}

func (inventoryTable *UptDescribeDirectoriesTable) IsGlobal() bool {
	return inventoryTable.IsGlobalTable
}

func (inventoryTable *UptDescribeDirectoriesTable) GetRegionToProcess() string {
	return inventoryTable.RegionToProcess
}

func (inventoryTable *UptDescribeDirectoriesTable) GetColumnList() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("on_demand"),
		table.TextColumn("account_id"),
		table.TextColumn("region_code"),
		table.TextColumn("access_url"),
		table.TextColumn("alias"),
		//table.TextColumn("connect_settings"),
		//table.TextColumn("connect_settings_availability_zones"),
		//table.TextColumn("connect_settings_connect_ips"),
		//table.TextColumn("connect_settings_customer_user_name"),
		//table.TextColumn("connect_settings_security_group_id"),
		//table.TextColumn("connect_settings_subnet_ids"),
		//table.TextColumn("connect_settings_vpc_id"),
		table.TextColumn("description"),
		table.IntegerColumn("desired_number_of_domain_controllers"),
		table.TextColumn("directory_id"),
		table.TextColumn("dns_ip_addrs"),
		table.TextColumn("edition"),
		table.TextColumn("launch_time"),
		//table.BigIntColumn("launch_time_ext"),
		//table.TextColumn("launch_time_loc"),
		//table.BigIntColumn("launch_time_loc_cache_end"),
		//table.BigIntColumn("launch_time_loc_cache_start"),
		//table.TextColumn("launch_time_loc_cache_zone"),
		//table.TextColumn("launch_time_loc_cache_zone_is_dst"),
		//table.TextColumn("launch_time_loc_cache_zone_name"),
		//table.IntegerColumn("launch_time_loc_cache_zone_offset"),
		//table.TextColumn("launch_time_loc_extend"),
		//table.TextColumn("launch_time_loc_name"),
		//table.TextColumn("launch_time_loc_tx"),
		//table.IntegerColumn("launch_time_loc_tx_index"),
		//table.TextColumn("launch_time_loc_tx_isstd"),
		//table.TextColumn("launch_time_loc_tx_isutc"),
		//table.BigIntColumn("launch_time_loc_tx_when"),
		//table.TextColumn("launch_time_loc_zone"),
		//table.TextColumn("launch_time_loc_zone_is_dst"),
		//table.TextColumn("launch_time_loc_zone_name"),
		//table.IntegerColumn("launch_time_loc_zone_offset"),
		//table.BigIntColumn("launch_time_wall"),
		table.TextColumn("name"),
		//table.TextColumn("owner_directory_description"),
		table.TextColumn("owner_directory_description_account_id"),
		table.TextColumn("owner_directory_description_directory_id"),
		table.TextColumn("owner_directory_description_dns_ip_addrs"),
		//table.TextColumn("owner_directory_description_radius_settings"),
		//table.TextColumn("owner_directory_description_radius_settings_authentication_protocol"),
		//table.TextColumn("owner_directory_description_radius_settings_display_label"),
		//table.IntegerColumn("owner_directory_description_radius_settings_radius_port"),
		//table.IntegerColumn("owner_directory_description_radius_settings_radius_retries"),
		//table.TextColumn("owner_directory_description_radius_settings_radius_servers"),
		//table.IntegerColumn("owner_directory_description_radius_settings_radius_timeout"),
		//table.TextColumn("owner_directory_description_radius_settings_shared_secret"),
		//table.TextColumn("owner_directory_description_radius_settings_use_same_username"),
		table.TextColumn("owner_directory_description_radius_status"),
		//table.TextColumn("owner_directory_description_vpc_settings"),
		//table.TextColumn("owner_directory_description_vpc_settings_availability_zones"),
		//table.TextColumn("owner_directory_description_vpc_settings_security_group_id"),
		//table.TextColumn("owner_directory_description_vpc_settings_subnet_ids"),
		//table.TextColumn("owner_directory_description_vpc_settings_vpc_id"),
		//table.TextColumn("radius_settings"),
		//table.TextColumn("radius_settings_authentication_protocol"),
		//table.TextColumn("radius_settings_display_label"),
		//table.IntegerColumn("radius_settings_radius_port"),
		//table.IntegerColumn("radius_settings_radius_retries"),
		//table.TextColumn("radius_settings_radius_servers"),
		//table.IntegerColumn("radius_settings_radius_timeout"),
		//table.TextColumn("radius_settings_shared_secret"),
		//table.TextColumn("radius_settings_use_same_username"),
		table.TextColumn("radius_status"),
		//table.TextColumn("regions_info"),
		//table.TextColumn("regions_info_additional_regions"),
		//table.TextColumn("regions_info_primary_region"),
		table.TextColumn("share_method"),
		table.TextColumn("share_notes"),
		table.TextColumn("share_status"),
		table.TextColumn("short_name"),
		table.TextColumn("size"),
		//table.TextColumn("sso_enabled"),
		table.TextColumn("stage"),
		table.TextColumn("stage_last_updated_date_time"),
		//table.BigIntColumn("stage_last_updated_date_time_ext"),
		//table.TextColumn("stage_last_updated_date_time_loc"),
		//table.BigIntColumn("stage_last_updated_date_time_loc_cache_end"),
		//table.BigIntColumn("stage_last_updated_date_time_loc_cache_start"),
		//table.TextColumn("stage_last_updated_date_time_loc_cache_zone"),
		//table.TextColumn("stage_last_updated_date_time_loc_cache_zone_is_dst"),
		//table.TextColumn("stage_last_updated_date_time_loc_cache_zone_name"),
		//table.IntegerColumn("stage_last_updated_date_time_loc_cache_zone_offset"),
		//table.TextColumn("stage_last_updated_date_time_loc_extend"),
		//table.TextColumn("stage_last_updated_date_time_loc_name"),
		//table.TextColumn("stage_last_updated_date_time_loc_tx"),
		//table.IntegerColumn("stage_last_updated_date_time_loc_tx_index"),
		//table.TextColumn("stage_last_updated_date_time_loc_tx_isstd"),
		//table.TextColumn("stage_last_updated_date_time_loc_tx_isutc"),
		//table.BigIntColumn("stage_last_updated_date_time_loc_tx_when"),
		//table.TextColumn("stage_last_updated_date_time_loc_zone"),
		//table.TextColumn("stage_last_updated_date_time_loc_zone_is_dst"),
		//table.TextColumn("stage_last_updated_date_time_loc_zone_name"),
		//table.IntegerColumn("stage_last_updated_date_time_loc_zone_offset"),
		//table.BigIntColumn("stage_last_updated_date_time_wall"),
		table.TextColumn("stage_reason"),
		table.TextColumn("type"),
		//table.TextColumn("vpc_settings"),
		//table.TextColumn("vpc_settings_availability_zones"),
		//table.TextColumn("vpc_settings_security_group_id"),
		//table.TextColumn("vpc_settings_subnet_ids"),
		table.TextColumn("vpc_settings_vpc_id"),
		//table.TextColumn("values"),

	}
}

func (inventoryTable *UptDescribeDirectoriesTable) GetEventSelectors() []pubsub.EventSelector {
	selectorList := make([]pubsub.EventSelector, 0)
	eventNames := [...]string{"CreateDirectory", "DeleteDirectory"}
	for _, eventName := range eventNames {
		valueMap := make(map[string]string)
		valueMap["event_name"] = eventName
		selector := pubsub.EventSelector{EventTableName: "aws_cloudtrail_events", FieldValueMap: valueMap}
		selectorList = append(selectorList, selector)
	}
	return selectorList
}

func (inventoryTable *UptDescribeDirectoriesTable) isValidEvent(event map[string]string) bool {
	return true
}

func (inventoryTable *UptDescribeDirectoriesTable) GetFullInventory(ctx context.Context, queryContext table.QueryContext, metadata *pubsub.InventoryTableMetadata) ([]map[string]string, error) {
	account := metadata.AwsAccount
	region := metadata.AwsRegion
	return inventoryTable.getInventory(ctx, queryContext, account, region, &directoryservice.DescribeDirectoriesInput{})
}

func (inventoryTable *UptDescribeDirectoriesTable) GetInventoryFromEvents(ctx context.Context, queryContext table.QueryContext, metadata *pubsub.InventoryTableMetadata, events []map[string]string) ([]map[string]string, error) {
	// As changes in this resource is rare and it is not paginated call,
	// for now, we will fetch all resource for given account in given region for every batch of event
	account := metadata.AwsAccount
	region := metadata.AwsRegion
	resultMap := make([]map[string]string, 0)
	processedAccRegion := make(map[string]bool)
	for _, event := range events {
		if !inventoryTable.isValidEvent(event) {
			continue
		}
		awsRegion := region
		// check whether event has a region
		eventRegion, found := event["region_code"]
		if found {
			// use region from event
			awsRegion = eventRegion
		}
		awsAccount := account
		// check whether event has an account id
		awsAccountId, found := event["account_id"]
		if found {
			// use account from event
			config, err := utilities.GetAwsAccountConfig(awsAccountId)
			if err != nil {
				// Failed to get account config
				continue
			}
			awsAccount = &config
		}
		_, processed := processedAccRegion[awsAccount.ID+"-"+awsRegion]
		if processed {
			// Already processed this account and region
			continue
		}
		results, _ := inventoryTable.getInventory(ctx, queryContext, awsAccount, awsRegion, &directoryservice.DescribeDirectoriesInput{})
		resultMap = append(resultMap, results...)
		processedAccRegion[awsAccount.ID+"-"+awsRegion] = true
	}
	return resultMap, nil
}

func (inventoryTable *UptDescribeDirectoriesTable) GetInventoryFromIds(ctx context.Context, queryContext table.QueryContext, metadata *pubsub.InventoryTableMetadata, ids []map[string]string) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	// TODO
	return resultMap, nil
}

func (inventoryTable *UptDescribeDirectoriesTable) getInventory(ctx context.Context, queryContext table.QueryContext, account *utilities.ExtensionConfigurationAwsAccount, region string, params *directoryservice.DescribeDirectoriesInput) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if params == nil {
		return resultMap, nil
	}
	sess, err := extaws.GetAwsConfig(account, region)
	if err != nil {
		return resultMap, err
	}

	accountId := utilities.AwsAccountID
	if account != nil {
		accountId = account.ID
	}

	utilities.GetLogger().WithFields(log.Fields{
		"tableName": inventoryTable.GetName(),
		"account":   accountId,
		"region":    region,
	}).Debug("processing region")

	svc := directoryservice.NewFromConfig(*sess)
	if !utilities.RateLimiterInstance.IsWithinRateLimits("aws", accountId, "directoryservice", "DescribeDirectories", true) {
		return resultMap, fmt.Errorf("exceeded api rate limits")
	}
	result, err := svc.DescribeDirectories(ctx, params)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": inventoryTable.GetName(),
			"account":   accountId,
			"region":    region,
			"task":      "DescribeDirectories",
			"errString": err.Error(),
		}).Error("failed to process region")
		return resultMap, err
	}
	byteArr, err := json.Marshal(result)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": inventoryTable.GetName(),
			"account":   accountId,
			"region":    region,
			"task":      "DescribeDirectories",
			"errString": err.Error(),
		}).Error("failed to marshal response")
		return resultMap, err
	}
	tableConfig, ok := utilities.TableConfigurationMap[inventoryTable.GetName()]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": inventoryTable.GetName(),
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}
	table := utilities.NewTable(byteArr, tableConfig)
	for _, row := range table.Rows {
		if !extaws.ShouldProcessRow(ctx, queryContext, inventoryTable.GetName(), accountId, region, row) {
			continue
		}
		resultRowMap := extaws.RowToMap(row, accountId, region, tableConfig)
		resultMap = append(resultMap, resultRowMap)
	}
	return resultMap, nil
}
