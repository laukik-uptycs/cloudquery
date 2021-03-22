/**
 * Copyright (c) 2020-present, The cloudquery authors
 *
 * This source code is licensed as defined by the LICENSE file found in the
 * root directory of this source tree.
 *
 * SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-only)
 */

package upt_acm

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Uptycs/basequery-go/plugin/table"
	extaws "github.com/Uptycs/cloudquery/extension/aws"
	"github.com/Uptycs/cloudquery/extension/pubsub"
	"github.com/Uptycs/cloudquery/utilities"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	log "github.com/sirupsen/logrus"
)

type UptDescribeCertificateTable struct {
	TableName       string
	MaxResults      int32
	IsGlobalTable   bool
	RegionToProcess string
}

func (inventoryTable *UptDescribeCertificateTable) GetName() string {
	return inventoryTable.TableName
}

func (inventoryTable *UptDescribeCertificateTable) IsGlobal() bool {
	return inventoryTable.IsGlobalTable
}

func (inventoryTable *UptDescribeCertificateTable) GetRegionToProcess() string {
	return inventoryTable.RegionToProcess
}

func (inventoryTable *UptDescribeCertificateTable) GetColumnList() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("on_demand"),
		table.TextColumn("account_id"),
		table.TextColumn("region_code"),
		table.TextColumn("certificate_arn"),
		table.TextColumn("certificate_authority_arn"),
		table.TextColumn("created_at"),
		//table.BigIntColumn("created_at_ext"),
		//table.TextColumn("created_at_loc"),
		//table.BigIntColumn("created_at_loc_cache_end"),
		//table.BigIntColumn("created_at_loc_cache_start"),
		//table.TextColumn("created_at_loc_cache_zone"),
		//table.TextColumn("created_at_loc_cache_zone_is_dst"),
		//table.TextColumn("created_at_loc_cache_zone_name"),
		//table.IntegerColumn("created_at_loc_cache_zone_offset"),
		//table.TextColumn("created_at_loc_extend"),
		//table.TextColumn("created_at_loc_name"),
		//table.TextColumn("created_at_loc_tx"),
		//table.IntegerColumn("created_at_loc_tx_index"),
		//table.TextColumn("created_at_loc_tx_isstd"),
		//table.TextColumn("created_at_loc_tx_isutc"),
		//table.BigIntColumn("created_at_loc_tx_when"),
		//table.TextColumn("created_at_loc_zone"),
		//table.TextColumn("created_at_loc_zone_is_dst"),
		//table.TextColumn("created_at_loc_zone_name"),
		//table.IntegerColumn("created_at_loc_zone_offset"),
		//table.BigIntColumn("created_at_wall"),
		table.TextColumn("domain_name"),
		//table.TextColumn("domain_validation_options"),
		//table.TextColumn("domain_validation_options_domain_name"),
		//table.TextColumn("domain_validation_options_resource_record"),
		//table.TextColumn("domain_validation_options_resource_record_name"),
		//table.TextColumn("domain_validation_options_resource_record_type"),
		//table.TextColumn("domain_validation_options_resource_record_value"),
		//table.TextColumn("domain_validation_options_validation_domain"),
		//table.TextColumn("domain_validation_options_validation_emails"),
		//table.TextColumn("domain_validation_options_validation_method"),
		//table.TextColumn("domain_validation_options_validation_status"),
		//table.TextColumn("extended_key_usages"),
		table.TextColumn("extended_key_usages_name"),
		//table.TextColumn("extended_key_usages_oid"),
		table.TextColumn("failure_reason"),
		table.TextColumn("imported_at"),
		//table.BigIntColumn("imported_at_ext"),
		//table.TextColumn("imported_at_loc"),
		//table.BigIntColumn("imported_at_loc_cache_end"),
		//table.BigIntColumn("imported_at_loc_cache_start"),
		//table.TextColumn("imported_at_loc_cache_zone"),
		//table.TextColumn("imported_at_loc_cache_zone_is_dst"),
		//table.TextColumn("imported_at_loc_cache_zone_name"),
		//table.IntegerColumn("imported_at_loc_cache_zone_offset"),
		//table.TextColumn("imported_at_loc_extend"),
		//table.TextColumn("imported_at_loc_name"),
		//table.TextColumn("imported_at_loc_tx"),
		//table.IntegerColumn("imported_at_loc_tx_index"),
		//table.TextColumn("imported_at_loc_tx_isstd"),
		//table.TextColumn("imported_at_loc_tx_isutc"),
		//table.BigIntColumn("imported_at_loc_tx_when"),
		//table.TextColumn("imported_at_loc_zone"),
		//table.TextColumn("imported_at_loc_zone_is_dst"),
		//table.TextColumn("imported_at_loc_zone_name"),
		//table.IntegerColumn("imported_at_loc_zone_offset"),
		//table.BigIntColumn("imported_at_wall"),
		table.TextColumn("in_use_by"),
		table.TextColumn("issued_at"),
		//table.BigIntColumn("issued_at_ext"),
		//table.TextColumn("issued_at_loc"),
		//table.BigIntColumn("issued_at_loc_cache_end"),
		//table.BigIntColumn("issued_at_loc_cache_start"),
		//table.TextColumn("issued_at_loc_cache_zone"),
		//table.TextColumn("issued_at_loc_cache_zone_is_dst"),
		//table.TextColumn("issued_at_loc_cache_zone_name"),
		//table.IntegerColumn("issued_at_loc_cache_zone_offset"),
		//table.TextColumn("issued_at_loc_extend"),
		//table.TextColumn("issued_at_loc_name"),
		//table.TextColumn("issued_at_loc_tx"),
		//table.IntegerColumn("issued_at_loc_tx_index"),
		//table.TextColumn("issued_at_loc_tx_isstd"),
		//table.TextColumn("issued_at_loc_tx_isutc"),
		//table.BigIntColumn("issued_at_loc_tx_when"),
		//table.TextColumn("issued_at_loc_zone"),
		//table.TextColumn("issued_at_loc_zone_is_dst"),
		//table.TextColumn("issued_at_loc_zone_name"),
		//table.IntegerColumn("issued_at_loc_zone_offset"),
		//table.BigIntColumn("issued_at_wall"),
		table.TextColumn("issuer"),
		table.TextColumn("key_algorithm"),
		//table.TextColumn("key_usages"),
		table.TextColumn("key_usages_name"),
		table.TextColumn("not_after"),
		//table.BigIntColumn("not_after_ext"),
		//table.TextColumn("not_after_loc"),
		//table.BigIntColumn("not_after_loc_cache_end"),
		//table.BigIntColumn("not_after_loc_cache_start"),
		//table.TextColumn("not_after_loc_cache_zone"),
		//table.TextColumn("not_after_loc_cache_zone_is_dst"),
		//table.TextColumn("not_after_loc_cache_zone_name"),
		//table.IntegerColumn("not_after_loc_cache_zone_offset"),
		//table.TextColumn("not_after_loc_extend"),
		//table.TextColumn("not_after_loc_name"),
		//table.TextColumn("not_after_loc_tx"),
		//table.IntegerColumn("not_after_loc_tx_index"),
		//table.TextColumn("not_after_loc_tx_isstd"),
		//table.TextColumn("not_after_loc_tx_isutc"),
		//table.BigIntColumn("not_after_loc_tx_when"),
		//table.TextColumn("not_after_loc_zone"),
		//table.TextColumn("not_after_loc_zone_is_dst"),
		//table.TextColumn("not_after_loc_zone_name"),
		//table.IntegerColumn("not_after_loc_zone_offset"),
		//table.BigIntColumn("not_after_wall"),
		table.TextColumn("not_before"),
		//table.BigIntColumn("not_before_ext"),
		//table.TextColumn("not_before_loc"),
		//table.BigIntColumn("not_before_loc_cache_end"),
		//table.BigIntColumn("not_before_loc_cache_start"),
		//table.TextColumn("not_before_loc_cache_zone"),
		//table.TextColumn("not_before_loc_cache_zone_is_dst"),
		//table.TextColumn("not_before_loc_cache_zone_name"),
		//table.IntegerColumn("not_before_loc_cache_zone_offset"),
		//table.TextColumn("not_before_loc_extend"),
		//table.TextColumn("not_before_loc_name"),
		//table.TextColumn("not_before_loc_tx"),
		//table.IntegerColumn("not_before_loc_tx_index"),
		//table.TextColumn("not_before_loc_tx_isstd"),
		//table.TextColumn("not_before_loc_tx_isutc"),
		//table.BigIntColumn("not_before_loc_tx_when"),
		//table.TextColumn("not_before_loc_zone"),
		//table.TextColumn("not_before_loc_zone_is_dst"),
		//table.TextColumn("not_before_loc_zone_name"),
		//table.IntegerColumn("not_before_loc_zone_offset"),
		//table.BigIntColumn("not_before_wall"),
		//table.TextColumn("options"),
		//table.TextColumn("options_certificate_transparency_logging_preference"),
		table.TextColumn("renewal_eligibility"),
		//table.TextColumn("renewal_summary"),
		//table.TextColumn("renewal_summary_domain_validation_options"),
		//table.TextColumn("renewal_summary_domain_validation_options_domain_name"),
		//table.TextColumn("renewal_summary_domain_validation_options_resource_record"),
		//table.TextColumn("renewal_summary_domain_validation_options_resource_record_name"),
		//table.TextColumn("renewal_summary_domain_validation_options_resource_record_type"),
		//table.TextColumn("renewal_summary_domain_validation_options_resource_record_value"),
		//table.TextColumn("renewal_summary_domain_validation_options_validation_domain"),
		//table.TextColumn("renewal_summary_domain_validation_options_validation_emails"),
		//table.TextColumn("renewal_summary_domain_validation_options_validation_method"),
		//table.TextColumn("renewal_summary_domain_validation_options_validation_status"),
		table.TextColumn("renewal_summary_renewal_status"),
		//table.TextColumn("renewal_summary_renewal_status_reason"),
		//table.TextColumn("renewal_summary_updated_at"),
		//table.BigIntColumn("renewal_summary_updated_at_ext"),
		//table.TextColumn("renewal_summary_updated_at_loc"),
		//table.BigIntColumn("renewal_summary_updated_at_loc_cache_end"),
		//table.BigIntColumn("renewal_summary_updated_at_loc_cache_start"),
		//table.TextColumn("renewal_summary_updated_at_loc_cache_zone"),
		//table.TextColumn("renewal_summary_updated_at_loc_cache_zone_is_dst"),
		//table.TextColumn("renewal_summary_updated_at_loc_cache_zone_name"),
		//table.IntegerColumn("renewal_summary_updated_at_loc_cache_zone_offset"),
		//table.TextColumn("renewal_summary_updated_at_loc_extend"),
		//table.TextColumn("renewal_summary_updated_at_loc_name"),
		//table.TextColumn("renewal_summary_updated_at_loc_tx"),
		//table.IntegerColumn("renewal_summary_updated_at_loc_tx_index"),
		//table.TextColumn("renewal_summary_updated_at_loc_tx_isstd"),
		//table.TextColumn("renewal_summary_updated_at_loc_tx_isutc"),
		//table.BigIntColumn("renewal_summary_updated_at_loc_tx_when"),
		//table.TextColumn("renewal_summary_updated_at_loc_zone"),
		//table.TextColumn("renewal_summary_updated_at_loc_zone_is_dst"),
		//table.TextColumn("renewal_summary_updated_at_loc_zone_name"),
		//table.IntegerColumn("renewal_summary_updated_at_loc_zone_offset"),
		//table.BigIntColumn("renewal_summary_updated_at_wall"),
		table.TextColumn("revocation_reason"),
		table.TextColumn("revoked_at"),
		//table.BigIntColumn("revoked_at_ext"),
		//table.TextColumn("revoked_at_loc"),
		//table.BigIntColumn("revoked_at_loc_cache_end"),
		//table.BigIntColumn("revoked_at_loc_cache_start"),
		//table.TextColumn("revoked_at_loc_cache_zone"),
		//table.TextColumn("revoked_at_loc_cache_zone_is_dst"),
		//table.TextColumn("revoked_at_loc_cache_zone_name"),
		//table.IntegerColumn("revoked_at_loc_cache_zone_offset"),
		//table.TextColumn("revoked_at_loc_extend"),
		//table.TextColumn("revoked_at_loc_name"),
		//table.TextColumn("revoked_at_loc_tx"),
		//table.IntegerColumn("revoked_at_loc_tx_index"),
		//table.TextColumn("revoked_at_loc_tx_isstd"),
		//table.TextColumn("revoked_at_loc_tx_isutc"),
		//table.BigIntColumn("revoked_at_loc_tx_when"),
		//table.TextColumn("revoked_at_loc_zone"),
		//table.TextColumn("revoked_at_loc_zone_is_dst"),
		//table.TextColumn("revoked_at_loc_zone_name"),
		//table.IntegerColumn("revoked_at_loc_zone_offset"),
		//table.BigIntColumn("revoked_at_wall"),
		table.TextColumn("serial"),
		table.TextColumn("signature_algorithm"),
		table.TextColumn("status"),
		table.TextColumn("subject"),
		table.TextColumn("subject_alternative_names"),
		table.TextColumn("type"),
		//table.TextColumn("values"),

	}
}

func (inventoryTable *UptDescribeCertificateTable) GetEventSelectors() []pubsub.EventSelector {
	selectorList := make([]pubsub.EventSelector, 0)
	eventNames := [...]string{"RequestCertificate", "DeleteCertificate", "ImportCertificate", "RenewCertificate"}
	for _, eventName := range eventNames {
		valueMap := make(map[string]string)
		valueMap["event_name"] = eventName
		selector := pubsub.EventSelector{EventTableName: "aws_cloudtrail_events", FieldValueMap: valueMap}
		selectorList = append(selectorList, selector)
	}
	return selectorList
}

func (inventoryTable *UptDescribeCertificateTable) isValidEvent(event map[string]string) bool {
	return true
}

func (inventoryTable *UptDescribeCertificateTable) GetFullInventory(ctx context.Context, queryContext table.QueryContext, metadata *pubsub.InventoryTableMetadata) ([]map[string]string, error) {
	account := metadata.AwsAccount
	region := metadata.AwsRegion
	return inventoryTable.getInventory(ctx, queryContext, account, region, &acm.DescribeCertificateInput{})
}

func (inventoryTable *UptDescribeCertificateTable) GetInventoryFromEvents(ctx context.Context, queryContext table.QueryContext, metadata *pubsub.InventoryTableMetadata, events []map[string]string) ([]map[string]string, error) {
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
		results, _ := inventoryTable.getInventory(ctx, queryContext, awsAccount, awsRegion, &acm.DescribeCertificateInput{})
		resultMap = append(resultMap, results...)
		processedAccRegion[awsAccount.ID+"-"+awsRegion] = true
	}
	return resultMap, nil
}

func (inventoryTable *UptDescribeCertificateTable) GetInventoryFromIds(ctx context.Context, queryContext table.QueryContext, metadata *pubsub.InventoryTableMetadata, ids []map[string]string) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	// TODO
	return resultMap, nil
}

func (inventoryTable *UptDescribeCertificateTable) getInventory(ctx context.Context, queryContext table.QueryContext, account *utilities.ExtensionConfigurationAwsAccount, region string, params *acm.DescribeCertificateInput) ([]map[string]string, error) {
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

	svc := acm.NewFromConfig(*sess)
	if !utilities.RateLimiterInstance.IsWithinRateLimits("aws", accountId, "acm", "DescribeCertificate", true) {
		return resultMap, fmt.Errorf("exceeded api rate limits")
	}
	result, err := svc.DescribeCertificate(ctx, params)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": inventoryTable.GetName(),
			"account":   accountId,
			"region":    region,
			"task":      "DescribeCertificate",
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
			"task":      "DescribeCertificate",
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
