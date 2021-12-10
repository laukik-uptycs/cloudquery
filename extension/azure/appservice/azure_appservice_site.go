package appservice

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/Uptycs/cloudquery/extension/azure"

	"github.com/Uptycs/basequery-go/plugin/table"
	"github.com/Uptycs/cloudquery/utilities"

	"github.com/Azure/azure-sdk-for-go/services/web/mgmt/2021-02-01/web"
	"github.com/fatih/structs"
)

const appserviceSite string = "azure_appservice_site"

// AppserviceSitesColumns returns the list of columns in the table
func AppserviceSiteColumns() []table.ColumnDefinition {
	return []table.ColumnDefinition{
		table.TextColumn("extended_location"),
		table.TextColumn("extended_location_name"),
		table.TextColumn("extended_location_type"),
		table.TextColumn("id"),
		table.TextColumn("identity"),
		table.TextColumn("identity_principal_id"),
		table.TextColumn("identity_tenant_id"),
		table.TextColumn("identity_type"),
		table.TextColumn("identity_user_assigned_identities"),
		table.TextColumn("kind"),
		table.TextColumn("location"),
		table.TextColumn("name"),
		table.TextColumn("properties"),
		table.TextColumn("availability_state"),
		table.TextColumn("client_affinity_enabled"),
		table.TextColumn("client_cert_enabled"),
		table.TextColumn("client_cert_exclusion_paths"),
		table.TextColumn("client_cert_mode"),
		table.TextColumn("cloning_info"),
		// table.TextColumn("cloning_info_app_settings_overrides"),
		// table.TextColumn("cloning_info_clone_custom_host_names"),
		// table.TextColumn("cloning_info_clone_source_control"),
		// table.TextColumn("cloning_info_configure_load_balancing"),
		// table.TextColumn("cloning_info_correlation_id"),
		// table.TextColumn("cloning_info_hosting_environment"),
		// table.TextColumn("cloning_info_overwrite"),
		// table.TextColumn("cloning_info_source_web_app_id"),
		// table.TextColumn("cloning_info_source_web_app_location"),
		// table.TextColumn("cloning_info_traffic_manager_profile_id"),
		// table.TextColumn("cloning_info_traffic_manager_profile_name"),
		table.IntegerColumn("container_size"),
		table.TextColumn("custom_domain_verification_id"),
		table.IntegerColumn("daily_memory_time_quota"),
		table.TextColumn("default_host_name"),
		table.TextColumn("enabled"),
		table.TextColumn("enabled_host_names"),
		table.TextColumn("host_name_ssl_states"),
		// table.TextColumn("host_name_ssl_states_host_type"),
		// table.TextColumn("host_name_ssl_states_name"),
		// table.TextColumn("host_name_ssl_states_ssl_state"),
		// table.TextColumn("host_name_ssl_states_thumbprint"),
		// table.TextColumn("host_name_ssl_states_to_update"),
		// table.TextColumn("host_name_ssl_states_virtual_ip"),
		table.TextColumn("host_names"),
		table.TextColumn("host_names_disabled"),
		table.TextColumn("hosting_environment_profile"),
		// table.TextColumn("hosting_environment_profile_id"),
		// table.TextColumn("hosting_environment_profile_name"),
		// table.TextColumn("hosting_environment_profile_type"),
		table.TextColumn("https_only"),
		table.TextColumn("hyper_v"),
		table.TextColumn("in_progress_operation_id"),
		table.TextColumn("is_default_container"),
		table.TextColumn("is_xenon"),
		table.TextColumn("key_vault_reference_identity"),
		table.TextColumn("last_modified_time_utc"),
		table.IntegerColumn("max_number_of_workers"),
		table.TextColumn("outbound_ip_addresses"),
		table.TextColumn("possible_outbound_ip_addresses"),
		table.TextColumn("redundancy_mode"),
		table.TextColumn("repository_site_name"),
		table.TextColumn("reserved"),
		table.TextColumn("resource_group"),
		table.TextColumn("scm_site_also_stopped"),
		table.TextColumn("server_farm_id"),
		table.TextColumn("site_config"),
		// table.TextColumn("site_config_acr_use_managed_identity_creds"),
		// table.TextColumn("site_config_acr_user_managed_identity_id"),
		// table.TextColumn("site_config_always_on"),
		// table.TextColumn("site_config_api_definition"),
		// table.TextColumn("site_config_api_definition_url"),
		// table.TextColumn("site_config_api_management_config"),
		// table.TextColumn("site_config_api_management_config_id"),
		// table.TextColumn("site_config_app_command_line"),
		// table.TextColumn("site_config_app_settings"),
		// table.TextColumn("site_config_app_settings_name"),
		// table.TextColumn("site_config_app_settings_value"),
		// table.TextColumn("site_config_auto_heal_enabled"),
		// table.TextColumn("site_config_auto_heal_rules"),
		// table.TextColumn("site_config_auto_heal_rules_actions"),
		// table.TextColumn("site_config_auto_heal_rules_actions_action_type"),
		// table.TextColumn("site_config_auto_heal_rules_actions_custom_action"),
		// table.TextColumn("site_config_auto_heal_rules_actions_custom_action_exe"),
		// table.TextColumn("site_config_auto_heal_rules_actions_custom_action_parameters"),
		// table.TextColumn("site_config_auto_heal_rules_actions_min_process_execution_time"),
		// table.TextColumn("site_config_auto_heal_rules_triggers"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_private_bytes_in_kb"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_requests"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_requests_count"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_requests_time_interval"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_with_path"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_slow_requests_with_path_count"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_with_path_path"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_with_path_time_interval"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_with_path_time_taken"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_slow_requests_count"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_path"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_time_interval"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_slow_requests_time_taken"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes_range"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_status_codes_range_count"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes_range_path"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes_range_status_codes"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes_range_time_interval"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_status_codes_count"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes_path"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_status_codes_status"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_status_codes_sub_status"),
		// table.TextColumn("site_config_auto_heal_rules_triggers_status_codes_time_interval"),
		// table.IntegerColumn("site_config_auto_heal_rules_triggers_status_codes_win32_status"),
		// table.TextColumn("site_config_auto_swap_slot_name"),
		// table.TextColumn("site_config_azure_storage_accounts"),
		// table.TextColumn("site_config_connection_strings"),
		// table.TextColumn("site_config_connection_strings_connection_string"),
		// table.TextColumn("site_config_connection_strings_name"),
		// table.TextColumn("site_config_connection_strings_type"),
		// table.TextColumn("site_config_cors"),
		// table.TextColumn("site_config_cors_allowed_origins"),
		// table.TextColumn("site_config_cors_support_credentials"),
		// table.TextColumn("site_config_default_documents"),
		// table.TextColumn("site_config_detailed_error_logging_enabled"),
		// table.TextColumn("site_config_document_root"),
		// table.TextColumn("site_config_experiments"),
		// table.TextColumn("site_config_experiments_ramp_up_rules"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_action_host_name"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_change_decision_callback_url"),
		// table.IntegerColumn("site_config_experiments_ramp_up_rules_change_interval_in_minutes"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_change_step"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_max_reroute_percentage"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_min_reroute_percentage"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_name"),
		// table.TextColumn("site_config_experiments_ramp_up_rules_reroute_percentage"),
		// table.TextColumn("site_config_ftps_state"),
		// table.IntegerColumn("site_config_function_app_scale_limit"),
		// table.TextColumn("site_config_functions_runtime_scale_monitoring_enabled"),
		// table.TextColumn("site_config_handler_mappings"),
		// table.TextColumn("site_config_handler_mappings_arguments"),
		// table.TextColumn("site_config_handler_mappings_extension"),
		// table.TextColumn("site_config_handler_mappings_script_processor"),
		// table.TextColumn("site_config_health_check_path"),
		// table.TextColumn("site_config_http20_enabled"),
		// table.TextColumn("site_config_http_logging_enabled"),
		// table.TextColumn("site_config_ip_security_restrictions"),
		// table.TextColumn("site_config_ip_security_restrictions_action"),
		// table.TextColumn("site_config_ip_security_restrictions_description"),
		// table.TextColumn("site_config_ip_security_restrictions_headers"),
		// table.TextColumn("site_config_ip_security_restrictions_ip_address"),
		// table.TextColumn("site_config_ip_security_restrictions_name"),
		// table.IntegerColumn("site_config_ip_security_restrictions_priority"),
		// table.TextColumn("site_config_ip_security_restrictions_subnet_mask"),
		// table.IntegerColumn("site_config_ip_security_restrictions_subnet_traffic_tag"),
		// table.TextColumn("site_config_ip_security_restrictions_tag"),
		// table.TextColumn("site_config_ip_security_restrictions_vnet_subnet_resource_id"),
		// table.IntegerColumn("site_config_ip_security_restrictions_vnet_traffic_tag"),
		// table.TextColumn("site_config_java_container"),
		// table.TextColumn("site_config_java_container_version"),
		// table.TextColumn("site_config_java_version"),
		// table.TextColumn("site_config_key_vault_reference_identity"),
		// table.TextColumn("site_config_limits"),
		// table.BigIntColumn("site_config_limits_max_disk_size_in_mb"),
		// table.BigIntColumn("site_config_limits_max_memory_in_mb"),
		// table.TextColumn("site_config_limits_max_percentage_cpu"),
		// table.TextColumn("site_config_linux_fx_version"),
		// table.TextColumn("site_config_load_balancing"),
		// table.TextColumn("site_config_local_my_sql_enabled"),
		// table.IntegerColumn("site_config_logs_directory_size_limit"),
		// table.TextColumn("site_config_machine_key"),
		// table.TextColumn("site_config_machine_key_decryption"),
		// table.TextColumn("site_config_machine_key_decryption_key"),
		// table.TextColumn("site_config_machine_key_validation"),
		// table.TextColumn("site_config_machine_key_validation_key"),
		// table.TextColumn("site_config_managed_pipeline_mode"),
		// table.IntegerColumn("site_config_managed_service_identity_id"),
		// table.TextColumn("site_config_min_tls_version"),
		// table.IntegerColumn("site_config_minimum_elastic_instance_count"),
		// table.TextColumn("site_config_net_framework_version"),
		// table.TextColumn("site_config_node_version"),
		// table.IntegerColumn("site_config_number_of_workers"),
		// table.TextColumn("site_config_php_version"),
		// table.TextColumn("site_config_power_shell_version"),
		// table.IntegerColumn("site_config_pre_warmed_instance_count"),
		// table.TextColumn("site_config_public_network_access"),
		// table.TextColumn("site_config_publishing_username"),
		// table.TextColumn("site_config_push"),
		// table.TextColumn("site_config_push_id"),
		// table.TextColumn("site_config_push_kind"),
		// table.TextColumn("site_config_push_name"),
		// table.TextColumn("site_config_push_type"),
		// table.TextColumn("site_config_python_version"),
		// table.TextColumn("site_config_remote_debugging_enabled"),
		// table.TextColumn("site_config_remote_debugging_version"),
		// table.TextColumn("site_config_request_tracing_enabled"),
		// table.TextColumn("site_config_request_tracing_expiration_time"),
		// table.TextColumn("site_config_scm_ip_security_restrictions"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_use_main"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_action"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_description"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_headers"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_ip_address"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_name"),
		// table.IntegerColumn("site_config_scm_ip_security_restrictions_priority"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_subnet_mask"),
		// table.IntegerColumn("site_config_scm_ip_security_restrictions_subnet_traffic_tag"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_tag"),
		// table.TextColumn("site_config_scm_ip_security_restrictions_vnet_subnet_resource_id"),
		// table.IntegerColumn("site_config_scm_ip_security_restrictions_vnet_traffic_tag"),
		// table.TextColumn("site_config_scm_min_tls_version"),
		// table.TextColumn("site_config_scm_type"),
		// table.TextColumn("site_config_tracing_options"),
		// table.TextColumn("site_config_use32_bit_worker_process"),
		// table.TextColumn("site_config_virtual_applications"),
		// table.TextColumn("site_config_virtual_applications_physical_path"),
		// table.TextColumn("site_config_virtual_applications_preload_enabled"),
		// table.TextColumn("site_config_virtual_applications_virtual_directories"),
		// table.TextColumn("site_config_virtual_applications_virtual_directories_physical_path"),
		// table.TextColumn("site_config_virtual_applications_virtual_directories_virtual_path"),
		// table.TextColumn("site_config_virtual_applications_virtual_path"),
		// table.TextColumn("site_config_vnet_name"),
		// table.IntegerColumn("site_config_vnet_private_ports_count"),
		// table.TextColumn("site_config_vnet_route_all_enabled"),
		// table.TextColumn("site_config_web_sockets_enabled"),
		// table.TextColumn("site_config_website_time_zone"),
		// table.TextColumn("site_config_windows_fx_version"),
		// table.IntegerColumn("site_config_x_managed_service_identity_id"),
		table.TextColumn("slot_swap_status"),
		// table.TextColumn("slot_swap_status_destination_slot_name"),
		// table.TextColumn("slot_swap_status_source_slot_name"),
		// table.TextColumn("slot_swap_status_timestamp_utc"),
		table.TextColumn("state"),
		table.TextColumn("storage_account_required"),
		table.TextColumn("suspended_till"),
		table.TextColumn("target_swap_slot"),
		table.TextColumn("traffic_manager_host_names"),
		table.TextColumn("usage_state"),
		table.TextColumn("virtual_network_subnet_id"),
		table.TextColumn("tags"),
		table.TextColumn("type"),
	}
}

// AppserviceSitesGenerate returns the rows in the table for all configured accounts
func AppserviceSitesGenerate(osqCtx context.Context, queryContext table.QueryContext) ([]map[string]string, error) {
	resultMap := make([]map[string]string, 0)
	if len(utilities.ExtConfiguration.ExtConfAzure.Accounts) == 0 {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": appserviceSite,
			"account":   "default",
		}).Info("processing account")
		results, err := processAccountAppserviceSites(nil)
		if err != nil {
			return resultMap, err
		}
		resultMap = append(resultMap, results...)
	} else {
		for _, account := range utilities.ExtConfiguration.ExtConfAzure.Accounts {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName": appserviceSite,
				"account":   account.SubscriptionID,
			}).Info("processing account")
			results, err := processAccountAppserviceSites(&account)
			if err != nil {
				continue
			}
			resultMap = append(resultMap, results...)
		}
	}

	return resultMap, nil
}

func processAccountAppserviceSites(account *utilities.ExtensionConfigurationAzureAccount) ([]map[string]string, error) {
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

	tableConfig, ok := utilities.TableConfigurationMap[appserviceSite]
	if !ok {
		utilities.GetLogger().WithFields(log.Fields{
			"tableName": appserviceSite,
		}).Error("failed to get table configuration")
		return resultMap, fmt.Errorf("table configuration not found")
	}

	for _, group := range groups {
		go setAppserviceSiteDataToTable(session, group, &wg, &resultMap, tableConfig)
	}
	wg.Wait()
	return resultMap, nil
}

func setAppserviceSiteDataToTable(session *azure.AzureSession, rg string, wg *sync.WaitGroup, resultMap *[]map[string]string, tableConfig *utilities.TableConfig) {
	defer wg.Done()

	for resourceItr, err := getAppserviceSiteData(session, rg); resourceItr.NotDone(); err = resourceItr.Next() {
		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     appserviceSite,
				"resourceGroup": rg,
				"errString":     err.Error(),
			}).Error("failed to get resource list")
			continue
		}

		resource := resourceItr.Value()
		structs.DefaultTagName = "json"
		resMap := structs.Map(resource)
		byteArr, err := json.Marshal(resMap)

		if err != nil {
			utilities.GetLogger().WithFields(log.Fields{
				"tableName":     appserviceSite,
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

func getAppserviceSiteData(session *azure.AzureSession, rg string) (web.AppCollectionIterator, error) {
	svcClient := web.NewAppsClient(session.SubscriptionId)
	svcClient.Authorizer = session.Authorizer
	var flag bool = true
	return svcClient.ListByResourceGroupComplete(context.Background(), rg, &flag)
}
