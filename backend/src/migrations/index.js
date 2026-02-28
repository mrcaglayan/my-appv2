import migration001GlobalMultiEntity from "./m001_global_multi_entity.js";
import migration002AuthzOnboardingFoundations from "./m002_authz_onboarding_foundations.js";
import migration003RbacAuditAndConsolidationRunEntries from "./m003_rbac_audit_and_consolidation_run_entries.js";
import migration004TenantSafeConstraints from "./m004_tenant_safe_constraints.js";
import migration005PeriodCloseRuns from "./m005_period_close_runs.js";
import migration006ProviderControlPlane from "./m006_provider_control_plane.js";
import migration007ShareholdersMaster from "./m007_shareholders_master.js";
import migration008ShareholderCapitalSubAccount from "./m008_shareholder_capital_sub_account.js";
import migration009JournalPurposeAccounts from "./m009_journal_purpose_accounts.js";
import migration010IntercompanyJournalLinks from "./m010_intercompany_journal_links.js";
import migration011JournalLineSubledgerReference from "./m011_journal_line_subledger_reference.js";
import migration012ShareholderCommitmentDebitSubAccount from "./m012_shareholder_commitment_debit_sub_account.js";
import migration013GlReclassificationRuns from "./m013_gl_reclassification_runs.js";
import migration014ShareholderCommitmentJournalEntries from "./m014_shareholder_commitment_journal_entries.js";
import migration015CashControlFoundation from "./m015_cash_control_foundation.js";
import migration016CashControlIntegrity from "./m016_cash_control_integrity.js";
import migration017CariSchemaFoundation from "./m017_cari_schema_foundation.js";
import migration018CariReportIndexes from "./m018_cari_report_indexes.js";
import migration019CounterpartyRoleFlags from "./m019_counterparty_role_flags.js";
import migration020ContractsFoundation from "./m020_contracts_foundation.js";
import migration021RevenueRecognitionSchedules from "./m021_revenue_recognition_schedules.js";
import migration022CounterpartyAccountMapping from "./m022_counterparty_account_mapping.js";
import migration023ContractDocumentLinkEvents from "./m023_contract_document_link_events.js";
import migration024ContractAmendmentsAndVersioning from "./m024_contract_amendments_and_versioning.js";
import migration025ContractDocumentLinkFxSnapshots from "./m025_contract_document_link_fx_snapshots.js";
import migration026ContractLineReportingOptimization from "./m026_contract_line_reporting_optimization.js";
import migration027CariCashIntegrationFoundation from "./m027_cari_cash_integration_foundation.js";
import migration028ContractBillingGeneration from "./m028_contract_billing_generation.js";
import migration029CashTransitWorkflow from "./m029_cash_transit_workflow.js";
import migration030LegalEntityPolicyPacks from "./m030_legal_entity_policy_packs.js";
import migration031BankFoundation from "./m031_bank_foundation.js";
import migration032BankStatementImports from "./m032_bank_statement_imports.js";
import migration033BankReconciliation from "./m033_bank_reconciliation.js";
import migration034PaymentBatches from "./m034_payment_batches.js";
import migration039PayrollImportFoundation from "./m039_payroll_import_foundation.js";
import migration040PayrollAccrualPosting from "./m040_payroll_accrual_posting.js";
import migration041PayrollLiabilitiesPaymentPrep from "./m041_payroll_liabilities_payment_prep.js";
import migration042PayrollPaymentSettlementSync from "./m042_payroll_payment_settlement_sync.js";
import migration043PayrollCorrectionsReversals from "./m043_payroll_corrections_reversals.js";
import migration044PayrollPartialSettlementAndManualOverride from "./m044_payroll_partial_settlement_and_manual_override.js";
import migration045PayrollBeneficiarySnapshots from "./m045_payroll_beneficiary_snapshots.js";
import migration046PayrollCloseControls from "./m046_payroll_close_controls.js";
import migration047PayrollProviderAdapters from "./m047_payroll_provider_adapters.js";
import migration054SensitiveDataSecurity from "./m054_sensitive_data_security.js";
import migration055JobEngine from "./m055_job_engine.js";
import migration056BankPaymentFileAcks from "./m056_bank_payment_file_acks.js";
import migration057BankReconciliationRulesAndExceptions from "./m057_bank_reconciliation_rules_and_exceptions.js";
import migration058BankConnectivityAdapters from "./m058_bank_connectivity_adapters.js";
import migration059BankReconciliationAutopostTemplates from "./m059_bank_reconciliation_autopost_templates.js";
import migration060BankReturnsAndReconDifferences from "./m060_bank_returns_and_recon_differences.js";
import migration061BankGovernanceApprovalsSod from "./m061_bank_governance_approvals_sod.js";
import migration062PerformanceIndexesAndPaginationHardening from "./m062_performance_indexes_and_pagination_hardening.js";
import migration063ApprovalPolicyEngine from "./m063_approval_policy_engine.js";
import migration064ExceptionWorkbench from "./m064_exception_workbench.js";
import migration065DataRetentionArchival from "./m065_data_retention_archival.js";
import migration066BankReconAutopostTaxModes from "./m066_bank_recon_autopost_tax_modes.js";
import migration067UserPreferences from "./m067_user_preferences.js";
import migration068ExceptionWorkbenchSlaDueAt from "./m068_exception_workbench_sla_due_at.js";

const migrations = [
  migration001GlobalMultiEntity,
  migration002AuthzOnboardingFoundations,
  migration003RbacAuditAndConsolidationRunEntries,
  migration004TenantSafeConstraints,
  migration005PeriodCloseRuns,
  migration006ProviderControlPlane,
  migration007ShareholdersMaster,
  migration008ShareholderCapitalSubAccount,
  migration009JournalPurposeAccounts,
  migration010IntercompanyJournalLinks,
  migration011JournalLineSubledgerReference,
  migration012ShareholderCommitmentDebitSubAccount,
  migration013GlReclassificationRuns,
  migration014ShareholderCommitmentJournalEntries,
  migration015CashControlFoundation,
  migration016CashControlIntegrity,
  migration017CariSchemaFoundation,
  migration018CariReportIndexes,
  migration019CounterpartyRoleFlags,
  migration020ContractsFoundation,
  migration021RevenueRecognitionSchedules,
  migration022CounterpartyAccountMapping,
  migration023ContractDocumentLinkEvents,
  migration024ContractAmendmentsAndVersioning,
  migration025ContractDocumentLinkFxSnapshots,
  migration026ContractLineReportingOptimization,
  migration027CariCashIntegrationFoundation,
  migration028ContractBillingGeneration,
  migration029CashTransitWorkflow,
  migration030LegalEntityPolicyPacks,
  migration031BankFoundation,
  migration032BankStatementImports,
  migration033BankReconciliation,
  migration034PaymentBatches,
  migration039PayrollImportFoundation,
  migration040PayrollAccrualPosting,
  migration041PayrollLiabilitiesPaymentPrep,
  migration042PayrollPaymentSettlementSync,
  migration043PayrollCorrectionsReversals,
  migration044PayrollPartialSettlementAndManualOverride,
  migration045PayrollBeneficiarySnapshots,
  migration046PayrollCloseControls,
  migration047PayrollProviderAdapters,
  migration054SensitiveDataSecurity,
  migration055JobEngine,
  migration056BankPaymentFileAcks,
  migration057BankReconciliationRulesAndExceptions,
  migration058BankConnectivityAdapters,
  migration059BankReconciliationAutopostTemplates,
  migration060BankReturnsAndReconDifferences,
  migration061BankGovernanceApprovalsSod,
  migration062PerformanceIndexesAndPaginationHardening,
  migration063ApprovalPolicyEngine,
  migration064ExceptionWorkbench,
  migration065DataRetentionArchival,
  migration066BankReconAutopostTaxModes,
  migration067UserPreferences,
  migration068ExceptionWorkbenchSlaDueAt,
];

export default migrations;
