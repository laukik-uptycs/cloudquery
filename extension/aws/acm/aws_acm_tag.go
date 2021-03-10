/**
 * Copyright (c) 2020-present, The cloudquery authors
 *
 * This source code is licensed as defined by the LICENSE file found in the
 * root directory of this source tree.
 *
 * SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-only)
 */

package acm

import (
	"context"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/cloudquery/utilities"

	"github.com/Uptycs/basequery-go/plugin/table"
	extaws "github.com/Uptycs/cloudquery/extension/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// ListTagsForCertificateColumns returns the list of columns in the table
func ListTagsForCertificateColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("account_id"),
		table.TextColumn("region_code"),
		table.TextColumn("key"),
		table.TextColumn("value"),
	}
}

// ListTagsForCertificateGenerate returns the rows in the table for all configured accounts
func ListTagsForCertificateGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAws.Accounts) == 0 && extaws.ShouldProcessAccount("aws_acm_tag", utilities.AwsAccountID) {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": "aws_acm_tag",
			"account":   "default",
		}).Info("processing account")
		results, err := processAccountListCertificatesForTag(osqCtx, queryContext, nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAws.Accounts {
			if !extaws.ShouldProcessAccount("aws_acm_tag", account.ID) {
				continue
			}
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": "aws_acm_tag",
				"account":   account.ID,
			}).Info("processing account")
			results, err := processAccountListCertificatesForTag(osqCtx, queryContext, &account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}
	return resultMap, nil
}

func processAccountListCertificatesForTag(osqCtx context.Context, queryContext table.QueryContext, account *utilities.ExtensionConfigurationAwsAccount) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	awsSession, err := extaws.GetAwsConfig(account, "us-east-1")
	if err != nil {
		return resultMap, err
	}
	regions, err := extaws.FetchRegions(osqCtx, awsSession)
	if err != nil {
		return resultMap, err
	}
	tableConfig, ok := utilities.TableConfigurationMap["aws_acm_tag"]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": "aws_acm_tag",
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}
	for _, region := range regions {
		accountId := utilities.AwsAccountID
		if account != nil {
			accountId = account.ID
		}
		if !extaws.ShouldProcessRegion("aws_acm_tag", accountId, *region.RegionName) {
			continue
		}
		result, err := processRegionListCertificatesForTag(osqCtx, queryContext, tableConfig, account, region)
		if err != nil {
			continue
		}
		resultMap = append(resultMap, result...)
	}
	return resultMap, nil
}

func processRegionListCertificatesForTag(osqCtx context.Context, queryContext table.QueryContext, tableConfig *utilities.TableConfig, account *utilities.ExtensionConfigurationAwsAccount, region types.Region) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	sess, err := extaws.GetAwsConfig(account, *region.RegionName)
	if err != nil {
		return resultMap, err
	}

	accountId := utilities.AwsAccountID
	if account != nil {
		accountId = account.ID
	}

	utilities.GetLogger().WithFields(log.Fields{
		"tableName": "aws_acm_tag",
		"account":   accountId,
		"region":    *region.RegionName,
	}).Debug("processing region")

	svc := acm.NewFromConfig(*sess)
	params := &acm.ListCertificatesInput{}

	paginator := acm.NewListCertificatesPaginator(svc, params)

	for {
		page, err := paginator.NextPage(osqCtx)
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": "aws_acm_tag",
				"account":   accountId,
				"region":    *region.RegionName,
				"task":      "ListTagsForCertificate",
				"errString": err.Error(),
			}).Error("failed to process region")
			return resultMap, err
		}

		for _, certificateSummary := range page.CertificateSummaryList {
			result, err := processListTagsForCertificate(osqCtx, queryContext, tableConfig, account, region, *certificateSummary.CertificateArn)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, result...)
		}
		if !paginator.HasMorePages() {
			break
		}
	}
	return resultMap, nil
}

func processListTagsForCertificate(osqCtx context.Context, queryContext table.QueryContext, tableConfig *utilities.TableConfig, account *utilities.ExtensionConfigurationAwsAccount, region types.Region, CertificateArn string) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	sess, err := extaws.GetAwsConfig(account, *region.RegionName)
	if err != nil {
		return resultMap, err
	}
	accountId := utilities.AwsAccountID
	if account != nil {
		accountId = account.ID
	}

	utilities.GetLogger().WithFields(log.Fields{
		"tableName": "aws_acm_tag",
		"account":   accountId,
		//"region":    *region.RegionName,
	}).Debug("processing region")

	svc := acm.NewFromConfig(*sess)
	params := &acm.ListTagsForCertificateInput{
		CertificateArn: &CertificateArn,
	}
	page, err := svc.ListTagsForCertificate(osqCtx, params)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": "aws_acm_tag",
			"account":   accountId,
			"region":    *region.RegionName,
			"task":      "ListTagsForCertificate",
			"errString": err.Error(),
		}).Error("failed to process region")
		return resultMap, err
	}

	byteArr, err := json.Marshal(page)
	if err != nil {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": "aws_acm_tag",
			"account":   accountId,
			"region":    *region.RegionName,
			"errString": err.Error(),
		}).Error("failed to marshal response")
		return resultMap, err
	}
	table := utilities.NewTable(byteArr, tableConfig)
	for _, row := range table.Rows {
		if !extaws.ShouldProcessRow(osqCtx, queryContext, "aws_acm_tag", accountId, *region.RegionName, row) {
			continue
		}
		result := extaws.RowToMap(row, accountId, *region.RegionName, tableConfig)
		resultMap = append(resultMap, result)
	}
	return resultMap, nil
}
