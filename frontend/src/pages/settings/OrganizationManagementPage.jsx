import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  autoProvisionShareholderSubAccounts,
  createShareholderCommitmentBatchJournal,
  generateFiscalPeriods,
  listCountries,
  listCurrencies,
  listFiscalCalendars,
  listFiscalPeriods,
  listGroupCompanies,
  listLegalEntities,
  listOperatingUnits,
  listShareholderJournalConfigs,
  listShareholders,
  previewShareholderCommitmentBatchJournal,
  upsertFiscalCalendar,
  upsertGroupCompany,
  upsertLegalEntity,
  upsertOperatingUnit,
  upsertShareholderJournalConfig,
  upsertShareholder,
} from "../../api/orgAdmin.js";
import { listAccounts } from "../../api/glAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import { useModuleReadiness } from "../../readiness/useModuleReadiness.js";
import TenantReadinessChecklist from "../../readiness/TenantReadinessChecklist.jsx";

const UNIT_TYPES = ["BRANCH", "PLANT", "STORE", "DEPARTMENT", "OTHER"];
const SHAREHOLDER_TYPES = ["INDIVIDUAL", "CORPORATE"];
const SHAREHOLDER_STATUSES = ["ACTIVE", "INACTIVE"];
const SHAREHOLDER_BATCH_QUEUE_STORAGE_KEY =
  "org.shareholderCommitmentBatchQueueByEntity.v1";

function toNumber(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function normalizeAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.round(parsed * 1_000_000) / 1_000_000;
}

function formatAmount(value) {
  return normalizeAmount(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function getAccountNormalSide(account) {
  return String(account?.normal_side || "").trim().toUpperCase();
}

function isPostingEnabled(account) {
  return !(
    account?.allow_posting === false ||
    account?.allow_posting === 0 ||
    account?.allow_posting === "0"
  );
}

function isDescendantAccount(accountId, parentAccountId, parentById) {
  const normalizedAccountId = toNumber(accountId);
  const normalizedParentAccountId = toNumber(parentAccountId);
  if (!normalizedAccountId || !normalizedParentAccountId) {
    return false;
  }

  const visited = new Set();
  let currentParentId = toNumber(parentById.get(normalizedAccountId));
  while (currentParentId) {
    if (currentParentId === normalizedParentAccountId) {
      return true;
    }
    if (visited.has(currentParentId)) {
      break;
    }
    visited.add(currentParentId);
    currentParentId = toNumber(parentById.get(currentParentId));
  }
  return false;
}

function getShareholderTypeLabel(value, l) {
  switch (String(value || "").toUpperCase()) {
    case "INDIVIDUAL":
      return l("Individual", "Bireysel");
    case "CORPORATE":
      return l("Corporate", "Kurumsal");
    default:
      return value || "-";
  }
}

function getShareholderStatusLabel(value, l) {
  switch (String(value || "").toUpperCase()) {
    case "ACTIVE":
      return l("Active", "Aktif");
    case "INACTIVE":
      return l("Inactive", "Pasif");
    default:
      return value || "-";
  }
}

function formatShareholderReadinessReason(reason, l) {
  switch (String(reason || "").trim().toUpperCase()) {
    case "ACCOUNT_NOT_FOUND":
      return l("Mapped account no longer exists.", "Eslenen hesap artik mevcut degil.");
    case "ACCOUNT_INACTIVE":
      return l("Mapped account is inactive.", "Eslenen hesap aktif degil.");
    case "ACCOUNT_TYPE_NOT_EQUITY":
      return l("Mapped account must be EQUITY.", "Eslenen hesap EQUITY olmalidir.");
    case "ACCOUNT_MUST_BE_NON_POSTABLE":
      return l(
        "Mapped account must be non-postable parent.",
        "Eslenen hesap post edilemeyen parent olmali."
      );
    case "ACCOUNT_NORMAL_SIDE_MISMATCH":
      return l(
        "Mapped account has invalid normal side.",
        "Eslenen hesap normal bakiye yonu gecersiz."
      );
    case "PURPOSES_MUST_MAP_TO_DIFFERENT_ACCOUNTS":
      return l(
        "Shareholder parent purposes must map to different accounts.",
        "Ortak parent amaclari farkli hesaplara eslenmeli."
      );
    case "ACCOUNT_SCOPE_NOT_LEGAL_ENTITY":
      return l(
        "Mapped account is not in a legal-entity chart.",
        "Eslenen hesap legal entity hesap planinda degil."
      );
    case "ACCOUNT_LEGAL_ENTITY_MISMATCH":
      return l(
        "Mapped account belongs to a different legal entity.",
        "Eslenen hesap farkli bir legal entity'e ait."
      );
    case "MAPPED_ACCOUNT_ID_INVALID":
      return l("Mapped account id is invalid.", "Eslenen hesap id gecersiz.");
    case "ACCOUNT_TENANT_MISMATCH":
      return l("Mapped account belongs to another tenant.", "Eslenen hesap baska tenant'a ait.");
    default:
      return String(reason || "-");
  }
}

export default function OrganizationManagementPage() {
  const { hasPermission } = useAuth();
  const { language } = useI18n();
  const { getModuleRow, refreshLegalEntity } = useModuleReadiness();
  const isTr = language === "tr";
  const l = useCallback((en, tr) => (isTr ? tr : en), [isTr]);
  const canReadOrgTree = hasPermission("org.tree.read");
  const canReadFiscalCalendars = hasPermission("org.fiscal_calendar.read");
  const canReadFiscalPeriods = hasPermission("org.fiscal_period.read");
  const canReadAccounts = hasPermission("gl.account.read");
  const canUpsertGroupCompany = hasPermission("org.group_company.upsert");
  const canUpsertLegalEntity = hasPermission("org.legal_entity.upsert");
  const canUpsertOperatingUnit = hasPermission("org.operating_unit.upsert");
  const canReadShareholders = hasPermission("org.tree.read");
  const canUpsertShareholder = hasPermission("org.legal_entity.upsert");
  const canUpsertAccounts = hasPermission("gl.account.upsert");
  const canUpsertFiscalCalendar = hasPermission("org.fiscal_calendar.upsert");
  const canGenerateFiscalPeriods = hasPermission("org.fiscal_period.generate");

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [shareholderJournalModal, setShareholderJournalModal] = useState(null);
  const [shareholderCardExpanded, setShareholderCardExpanded] = useState(false);
  const [autoSubAccountSetupModalOpen, setAutoSubAccountSetupModalOpen] =
    useState(false);
  const [autoSubAccountSetupSaving, setAutoSubAccountSetupSaving] =
    useState(false);
  const [
    commitmentBatchQueueByEntity,
    setCommitmentBatchQueueByEntity,
  ] = useState({});
  const [batchCommitmentModalOpen, setBatchCommitmentModalOpen] =
    useState(false);
  const [batchCommitmentSaving, setBatchCommitmentSaving] = useState(false);
  const [commitmentIncreaseModalOpen, setCommitmentIncreaseModalOpen] =
    useState(false);
  const [commitmentIncreaseForm, setCommitmentIncreaseForm] = useState({
    shareholderId: "",
    commitmentDate: new Date().toISOString().slice(0, 10),
    increaseAmount: "0",
  });
  const [batchCommitmentDate, setBatchCommitmentDate] = useState(
    new Date().toISOString().slice(0, 10)
  );
  const [batchPreviewLoading, setBatchPreviewLoading] = useState(false);
  const [batchPreviewData, setBatchPreviewData] = useState(null);

  const [groups, setGroups] = useState([]);
  const [countries, setCountries] = useState([]);
  const [currencies, setCurrencies] = useState([]);
  const [accounts, setAccounts] = useState([]);
  const [legalEntities, setLegalEntities] = useState([]);
  const [operatingUnits, setOperatingUnits] = useState([]);
  const [shareholders, setShareholders] = useState([]);
  const [shareholderJournalConfigs, setShareholderJournalConfigs] = useState(
    []
  );
  const [calendars, setCalendars] = useState([]);
  const [periods, setPeriods] = useState([]);

  const [groupForm, setGroupForm] = useState({ code: "", name: "" });
  const [groupEditingCode, setGroupEditingCode] = useState("");
  const [entityForm, setEntityForm] = useState({
    groupCompanyId: "",
    code: "",
    name: "",
    taxId: "",
    countryId: "",
    functionalCurrencyCode: "USD",
    isIntercompanyEnabled: true,
    intercompanyPartnerRequired: false,
    autoProvisionDefaults: true,
    useCustomPaymentTerms: false,
    paymentTermsJson: "",
  });
  const [unitForm, setUnitForm] = useState({
    legalEntityId: "",
    code: "",
    name: "",
    unitType: "BRANCH",
    hasSubledger: false,
  });
  const [shareholderForm, setShareholderForm] = useState({
    legalEntityId: "",
    code: "",
    name: "",
    shareholderType: "INDIVIDUAL",
    taxId: "",
    commitmentDate: new Date().toISOString().slice(0, 10),
    committedCapital: "0",
    capitalSubAccountId: "",
    commitmentDebitSubAccountId: "",
    currencyCode: "USD",
    status: "ACTIVE",
    notes: "",
  });
  const [shareholderParentConfigForm, setShareholderParentConfigForm] =
    useState({
      capitalCreditParentAccountId: "",
      commitmentDebitParentAccountId: "",
    });
  const [calendarForm, setCalendarForm] = useState({
    code: "",
    name: "",
    yearStartMonth: 1,
    yearStartDay: 1,
  });
  const [periodForm, setPeriodForm] = useState({
    calendarId: "",
    fiscalYear: new Date().getUTCFullYear(),
  });

  async function loadCoreData() {
    if (!canReadOrgTree && !canReadFiscalCalendars) {
      setLoading(false);
      return;
    }

    setLoading(true);
    setError("");
    try {
      if (canReadOrgTree) {
        const [
          groupsRes,
          countriesRes,
          currenciesRes,
          accountsRes,
          entitiesRes,
          unitsRes,
          shareholdersRes,
          shareholderConfigsRes,
        ] =
          await Promise.all([
            listGroupCompanies(),
            listCountries(),
            listCurrencies(),
            canReadAccounts
              ? listAccounts({ includeInactive: true })
              : Promise.resolve({ rows: [] }),
            listLegalEntities(),
            listOperatingUnits(),
            canReadShareholders
              ? listShareholders()
              : Promise.resolve({ rows: [] }),
            canReadShareholders
              ? listShareholderJournalConfigs()
              : Promise.resolve({ rows: [] }),
          ]);

        const groupRows = groupsRes?.rows || [];
        const countryRows = countriesRes?.rows || [];
        const currencyRows = currenciesRes?.rows || [];
        const accountRows = accountsRes?.rows || [];
        const entityRows = entitiesRes?.rows || [];
        const unitRows = unitsRes?.rows || [];
        const shareholderRows = shareholdersRes?.rows || [];
        const shareholderConfigRows = shareholderConfigsRes?.rows || [];

        setGroups(groupRows);
        setCountries(countryRows);
        setCurrencies(currencyRows);
        setAccounts(accountRows);
        setLegalEntities(entityRows);
        setOperatingUnits(unitRows);
        setShareholders(shareholderRows);
        setShareholderJournalConfigs(shareholderConfigRows);

        setEntityForm((prev) => {
          const nextCountryId =
            prev.countryId || String(countryRows[0]?.id || "");
          const selectedCountry = countryRows.find(
            (row) => String(row.id) === String(nextCountryId)
          );
          const countryDefaultCurrency = String(
            selectedCountry?.default_currency_code || ""
          ).toUpperCase();

          return {
            ...prev,
            groupCompanyId:
              prev.groupCompanyId || String(groupRows[0]?.id || ""),
            countryId: nextCountryId,
            functionalCurrencyCode:
              prev.functionalCurrencyCode || countryDefaultCurrency || "USD",
          };
        });
        setUnitForm((prev) => ({
          ...prev,
          legalEntityId: prev.legalEntityId || String(entityRows[0]?.id || ""),
        }));
        setShareholderForm((prev) => {
          const nextLegalEntityId =
            prev.legalEntityId || String(entityRows[0]?.id || "");
          const selectedEntity = entityRows.find(
            (row) => String(row.id) === String(nextLegalEntityId)
          );
          const legalEntityCurrency = String(
            selectedEntity?.functional_currency_code || ""
          ).toUpperCase();
          return {
            ...prev,
            legalEntityId: nextLegalEntityId,
            currencyCode: prev.currencyCode || legalEntityCurrency || "USD",
          };
        });
      }

      if (canReadFiscalCalendars) {
        const calendarsRes = await listFiscalCalendars();
        const calendarRows = calendarsRes?.rows || [];
        setCalendars(calendarRows);
        setPeriodForm((prev) => ({
          ...prev,
          calendarId: prev.calendarId || String(calendarRows[0]?.id || ""),
        }));
      }
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to load organization data.", "Organizasyon verileri yuklenemedi."));
    } finally {
      setLoading(false);
    }
  }

  async function loadPeriods(calendarId, fiscalYear) {
    if (!canReadFiscalPeriods || !calendarId) {
      setPeriods([]);
      return;
    }

    try {
      const response = await listFiscalPeriods(calendarId, {
        fiscalYear: fiscalYear || undefined,
      });
      setPeriods(response?.rows || []);
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to load fiscal periods.", "Mali donemler yuklenemedi."));
    }
  }

  useEffect(() => {
    loadCoreData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    canReadOrgTree,
    canReadFiscalCalendars,
    canReadShareholders,
    canReadAccounts,
  ]);

  useEffect(() => {
    loadPeriods(periodForm.calendarId, periodForm.fiscalYear);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [periodForm.calendarId, periodForm.fiscalYear, canReadFiscalPeriods]);

  const countrySelectOptions = useMemo(
    () =>
      countries.map((row) => ({
        id: row.id,
        label: `${row.iso2} - ${row.name}`,
        defaultCurrencyCode: String(row.default_currency_code || "").toUpperCase(),
      })),
    [countries]
  );

  const currencySelectOptions = useMemo(
    () =>
      currencies.map((row) => ({
        code: String(row.code || "").toUpperCase(),
        label: `${String(row.code || "").toUpperCase()} - ${row.name}`,
      })),
    [currencies]
  );

  const selectedShareholderLegalEntityId = toNumber(
    shareholderForm.legalEntityId
  );
  const selectedShareholderEntityAccounts = useMemo(
    () =>
      accounts.filter(
        (row) =>
          Number(row.legal_entity_id) === Number(selectedShareholderLegalEntityId)
      ),
    [accounts, selectedShareholderLegalEntityId]
  );
  const selectedShareholderParentConfig = useMemo(() => {
    if (!selectedShareholderLegalEntityId) {
      return null;
    }
    return (
      shareholderJournalConfigs.find(
        (row) =>
          Number(row.legal_entity_id) === Number(selectedShareholderLegalEntityId)
      ) || null
    );
  }, [shareholderJournalConfigs, selectedShareholderLegalEntityId]);
  const selectedCapitalCreditParentAccountId = toNumber(
    selectedShareholderParentConfig?.capital_credit_parent_account_id
  );
  const selectedCommitmentDebitParentAccountId = toNumber(
    selectedShareholderParentConfig?.commitment_debit_parent_account_id
  );
  const selectedShareholderEntityAccountById = useMemo(() => {
    const next = new Map();
    for (const account of selectedShareholderEntityAccounts) {
      const accountId = toNumber(account.id);
      if (accountId) {
        next.set(accountId, account);
      }
    }
    return next;
  }, [selectedShareholderEntityAccounts]);
  const selectedShareholderParentById = useMemo(() => {
    const next = new Map();
    for (const account of selectedShareholderEntityAccounts) {
      const accountId = toNumber(account.id);
      if (!accountId) {
        continue;
      }
      next.set(accountId, toNumber(account.parent_account_id));
    }
    return next;
  }, [selectedShareholderEntityAccounts]);
  const selectedCapitalCreditParentAccount = useMemo(
    () => selectedShareholderEntityAccountById.get(selectedCapitalCreditParentAccountId) || null,
    [selectedShareholderEntityAccountById, selectedCapitalCreditParentAccountId]
  );
  const selectedCommitmentDebitParentAccount = useMemo(
    () =>
      selectedShareholderEntityAccountById.get(
        selectedCommitmentDebitParentAccountId
      ) || null,
    [selectedShareholderEntityAccountById, selectedCommitmentDebitParentAccountId]
  );
  const parentMappingStatus = useMemo(() => {
    const reasons = [];
    const validateParent = (account, expectedSide, label) => {
      if (!account) {
        reasons.push(
          l(
            `${label} account mapping is missing.`,
            `${label} hesap eslesmesi eksik.`
          )
        );
        return;
      }
      if (!account.is_active) {
        reasons.push(
          l(
            `${label} must be active.`,
            `${label} aktif olmalidir.`
          )
        );
      }
      if (String(account.account_type || "").toUpperCase() !== "EQUITY") {
        reasons.push(
          l(
            `${label} must be EQUITY.`,
            `${label} EQUITY olmalidir.`
          )
        );
      }
      if (getAccountNormalSide(account) !== expectedSide) {
        reasons.push(
          l(
            `${label} must have ${expectedSide} normal side.`,
            `${label} ${expectedSide} normal tarafa sahip olmalidir.`
          )
        );
      }
      if (isPostingEnabled(account)) {
        reasons.push(
          l(
            `${label} must be a non-postable header account.`,
            `${label} post edilemeyen ust hesap olmalidir.`
          )
        );
      }
    };

    if (!selectedCapitalCreditParentAccountId || !selectedCommitmentDebitParentAccountId) {
      reasons.push(
        l(
          "Save parent mapping accounts first.",
          "Once parent hesap eslesmelerini kaydedin."
        )
      );
    }
    if (selectedCapitalCreditParentAccountId === selectedCommitmentDebitParentAccountId) {
      reasons.push(
        l(
          "Capital and commitment parent mapping cannot be the same account.",
          "Sermaye ve taahhut parent eslesmesi ayni hesap olamaz."
        )
      );
    }
    validateParent(
      selectedCapitalCreditParentAccount,
      "CREDIT",
      l("Capital parent", "Sermaye parent")
    );
    validateParent(
      selectedCommitmentDebitParentAccount,
      "DEBIT",
      l("Commitment parent", "Taahhut parent")
    );

    return {
      valid:
        Boolean(selectedCapitalCreditParentAccountId) &&
        Boolean(selectedCommitmentDebitParentAccountId) &&
        reasons.length === 0,
      reasons,
    };
  }, [
    l,
    selectedCapitalCreditParentAccount,
    selectedCapitalCreditParentAccountId,
    selectedCommitmentDebitParentAccount,
    selectedCommitmentDebitParentAccountId,
  ]);
  const hasShareholderParentMapping = parentMappingStatus.valid;

  const equityParentShareholderAccounts = useMemo(
    () =>
      selectedShareholderEntityAccounts.filter((row) => {
        const isEquity = String(row.account_type || "").toUpperCase() === "EQUITY";
        return (
          isEquity &&
          Boolean(row.is_active) &&
          Boolean(toNumber(row.id)) &&
          !isPostingEnabled(row)
        );
      }),
    [selectedShareholderEntityAccounts]
  );
  const equityCreditParentShareholderAccounts = useMemo(
    () =>
      equityParentShareholderAccounts.filter(
        (row) => getAccountNormalSide(row) === "CREDIT"
      ),
    [equityParentShareholderAccounts]
  );
  const equityDebitParentShareholderAccounts = useMemo(
    () =>
      equityParentShareholderAccounts.filter(
        (row) => getAccountNormalSide(row) === "DEBIT"
      ),
    [equityParentShareholderAccounts]
  );

  const equityLeafShareholderAccounts = useMemo(() => {
    if (!selectedShareholderLegalEntityId) {
      return [];
    }
    const parentIds = new Set(
      selectedShareholderEntityAccounts
        .filter(
          (row) =>
            Number(row.legal_entity_id) === Number(selectedShareholderLegalEntityId) &&
            Boolean(row.is_active)
        )
        .map((row) => toNumber(row.parent_account_id))
        .filter(Boolean)
    );
    return selectedShareholderEntityAccounts.filter((row) => {
      const isEquity = String(row.account_type || "").toUpperCase() === "EQUITY";
      const isActive = Boolean(row.is_active);
      const allowPosting = isPostingEnabled(row);
      const accountId = toNumber(row.id);
      if (!accountId) {
        return false;
      }
      return (
        isEquity &&
        isActive &&
        allowPosting &&
        !parentIds.has(accountId)
      );
    });
  }, [selectedShareholderEntityAccounts, selectedShareholderLegalEntityId]);
  const mappedCapitalCreditLeafAccounts = useMemo(() => {
    if (!selectedCapitalCreditParentAccountId) {
      return [];
    }
    return equityLeafShareholderAccounts.filter(
      (row) =>
        getAccountNormalSide(row) === "CREDIT" &&
        isDescendantAccount(
          toNumber(row.id),
          selectedCapitalCreditParentAccountId,
          selectedShareholderParentById
        )
    );
  }, [
    equityLeafShareholderAccounts,
    selectedCapitalCreditParentAccountId,
    selectedShareholderParentById,
  ]);
  const mappedCommitmentDebitLeafAccounts = useMemo(() => {
    if (!selectedCommitmentDebitParentAccountId) {
      return [];
    }
    return equityLeafShareholderAccounts.filter(
      (row) =>
        getAccountNormalSide(row) === "DEBIT" &&
        isDescendantAccount(
          toNumber(row.id),
          selectedCommitmentDebitParentAccountId,
          selectedShareholderParentById
        )
    );
  }, [
    equityLeafShareholderAccounts,
    selectedCommitmentDebitParentAccountId,
    selectedShareholderParentById,
  ]);

  const visibleShareholders = useMemo(() => {
    if (!selectedShareholderLegalEntityId) {
      return shareholders;
    }
    return shareholders.filter(
      (row) =>
        Number(row.legal_entity_id) === Number(selectedShareholderLegalEntityId)
    );
  }, [shareholders, selectedShareholderLegalEntityId]);
  const existingShareholderForForm = useMemo(() => {
    if (!selectedShareholderLegalEntityId) {
      return null;
    }
    const normalizedCode = String(shareholderForm.code || "")
      .trim()
      .toUpperCase();
    if (!normalizedCode) {
      return null;
    }
    return (
      visibleShareholders.find(
        (row) =>
          String(row.code || "")
            .trim()
            .toUpperCase() === normalizedCode
      ) || null
    );
  }, [selectedShareholderLegalEntityId, shareholderForm.code, visibleShareholders]);
  const formCommitmentIncreaseAmount = useMemo(() => {
    const parsed = Number(shareholderForm.committedCapital || 0);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 0;
    }
    return normalizeAmount(parsed);
  }, [shareholderForm.committedCapital]);
  const formExistingCommittedCapitalAmount = useMemo(
    () => normalizeAmount(existingShareholderForForm?.committed_capital || 0),
    [existingShareholderForForm]
  );
  const formProjectedCommittedCapitalAmount = useMemo(
    () =>
      normalizeAmount(
        formExistingCommittedCapitalAmount + formCommitmentIncreaseAmount
      ),
    [formCommitmentIncreaseAmount, formExistingCommittedCapitalAmount]
  );
  const eligibleShareholdersForCommitmentIncrease = useMemo(
    () =>
      visibleShareholders.filter(
        (row) =>
          Boolean(toNumber(row.capital_sub_account_id)) &&
          Boolean(toNumber(row.commitment_debit_sub_account_id))
      ),
    [visibleShareholders]
  );
  const selectedCommitmentIncreaseShareholder = useMemo(
    () =>
      eligibleShareholdersForCommitmentIncrease.find(
        (row) =>
          toNumber(row.id) === toNumber(commitmentIncreaseForm.shareholderId)
      ) || null,
    [eligibleShareholdersForCommitmentIncrease, commitmentIncreaseForm.shareholderId]
  );
  const commitmentIncreaseAmount = useMemo(() => {
    const parsed = Number(commitmentIncreaseForm.increaseAmount || 0);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 0;
    }
    return normalizeAmount(parsed);
  }, [commitmentIncreaseForm.increaseAmount]);
  const commitmentIncreaseCurrentCommittedCapital = useMemo(
    () => normalizeAmount(selectedCommitmentIncreaseShareholder?.committed_capital || 0),
    [selectedCommitmentIncreaseShareholder]
  );
  const commitmentIncreaseProjectedCommittedCapital = useMemo(
    () =>
      normalizeAmount(
        commitmentIncreaseCurrentCommittedCapital + commitmentIncreaseAmount
      ),
    [commitmentIncreaseAmount, commitmentIncreaseCurrentCommittedCapital]
  );
  const selectedEntityCommitmentQueueIds = useMemo(() => {
    const key = String(selectedShareholderLegalEntityId || "");
    const raw = commitmentBatchQueueByEntity?.[key];
    if (!Array.isArray(raw)) {
      return [];
    }
    return Array.from(new Set(raw.map((value) => toNumber(value)).filter(Boolean)));
  }, [commitmentBatchQueueByEntity, selectedShareholderLegalEntityId]);
  const selectedEntityCommitmentQueueIdSet = useMemo(
    () => new Set(selectedEntityCommitmentQueueIds),
    [selectedEntityCommitmentQueueIds]
  );
  const pendingBatchCommitmentShareholders = useMemo(() => {
    if (selectedEntityCommitmentQueueIds.length === 0) {
      return [];
    }
    const pendingIdSet = new Set(selectedEntityCommitmentQueueIds);
    return visibleShareholders.filter((row) => pendingIdSet.has(toNumber(row.id)));
  }, [selectedEntityCommitmentQueueIds, visibleShareholders]);
  const pendingBatchQueueCurrencyGroups = useMemo(() => {
    const byCurrency = new Map();
    for (const row of pendingBatchCommitmentShareholders) {
      const shareholderId = toNumber(row.id);
      if (!shareholderId) {
        continue;
      }
      const currencyCode = String(row.currency_code || "").trim().toUpperCase() || "-";
      if (!byCurrency.has(currencyCode)) {
        byCurrency.set(currencyCode, []);
      }
      byCurrency.get(currencyCode).push(shareholderId);
    }

    return Array.from(byCurrency.entries()).map(([currencyCode, ids]) => {
      const shareholderIds = Array.from(new Set(ids));
      return {
        currencyCode,
        shareholderIds,
        count: shareholderIds.length,
      };
    });
  }, [pendingBatchCommitmentShareholders]);
  const eligibleShareholdersForQueue = useMemo(
    () =>
      visibleShareholders.filter(
        (row) =>
          Number(row.committed_capital || 0) > 0 &&
          Boolean(toNumber(row.capital_sub_account_id)) &&
          Boolean(toNumber(row.commitment_debit_sub_account_id))
      ),
    [visibleShareholders]
  );
  const eligibleQueueCurrencyGroups = useMemo(() => {
    const byCurrency = new Map();
    for (const row of eligibleShareholdersForQueue) {
      const shareholderId = toNumber(row.id);
      if (!shareholderId) {
        continue;
      }
      const currencyCode = String(row.currency_code || "").trim().toUpperCase() || "-";
      if (!byCurrency.has(currencyCode)) {
        byCurrency.set(currencyCode, []);
      }
      byCurrency.get(currencyCode).push(shareholderId);
    }

    return Array.from(byCurrency.entries()).map(([currencyCode, ids]) => {
      const shareholderIds = Array.from(new Set(ids));
      return {
        currencyCode,
        shareholderIds,
        count: shareholderIds.length,
      };
    });
  }, [eligibleShareholdersForQueue]);
  const usedCapitalSubAccountIds = useMemo(
    () =>
      new Set(
        visibleShareholders
          .map((row) => toNumber(row.capital_sub_account_id))
          .filter(Boolean)
      ),
    [visibleShareholders]
  );
  const usedCommitmentDebitSubAccountIds = useMemo(
    () =>
      new Set(
        visibleShareholders
          .map((row) => toNumber(row.commitment_debit_sub_account_id))
          .filter(Boolean)
      ),
    [visibleShareholders]
  );
  const availableCapitalCreditShareholderAccounts = useMemo(
    () =>
      mappedCapitalCreditLeafAccounts.filter(
        (row) => !usedCapitalSubAccountIds.has(toNumber(row.id))
      ),
    [mappedCapitalCreditLeafAccounts, usedCapitalSubAccountIds]
  );
  const availableCommitmentDebitShareholderAccounts = useMemo(
    () =>
      mappedCommitmentDebitLeafAccounts.filter(
        (row) => !usedCommitmentDebitSubAccountIds.has(toNumber(row.id))
      ),
    [mappedCommitmentDebitLeafAccounts, usedCommitmentDebitSubAccountIds]
  );
  const hasMissingCreditEquitySubAccount =
    hasShareholderParentMapping &&
    availableCapitalCreditShareholderAccounts.length === 0;
  const hasMissingDebitEquitySubAccount =
    hasShareholderParentMapping &&
    availableCommitmentDebitShareholderAccounts.length === 0;
  const shareholdersWithCommittedCapital = useMemo(
    () =>
      visibleShareholders.filter((row) => Number(row.committed_capital || 0) > 0),
    [visibleShareholders]
  );
  const batchPreviewMixedCurrencyIssue = useMemo(
    () =>
      Boolean(
        batchPreviewData &&
          Array.isArray(batchPreviewData?.validation?.mixed_currency) &&
          batchPreviewData.validation.mixed_currency.length > 1
      ),
    [batchPreviewData]
  );
  const selectedShareholderSetupChecks = useMemo(() => {
    if (!selectedShareholderLegalEntityId) {
      return [];
    }
    return [
      {
        key: "parentMappings",
        label: l(
          "Parent mappings are valid (active, equity, correct side, header account)",
          "Parent eslemeleri gecerli (aktif, ozkaynak, dogru taraf, ust hesap)"
        ),
        ready: parentMappingStatus.valid,
        reasons: parentMappingStatus.reasons,
      },
      {
        key: "commitmentSubAccounts",
        label: l(
          "Commitment sub-accounts are assigned per shareholder",
          "Taahhut icin ortak alt hesaplari atanmis"
        ),
        ready: shareholdersWithCommittedCapital.every(
          (row) =>
            Boolean(toNumber(row.capital_sub_account_id)) &&
            Boolean(toNumber(row.commitment_debit_sub_account_id))
        ),
      },
      {
        key: "equitySubAccountPool",
        label: l(
          "Debit/credit sub-account pool exists for new shareholders",
          "Yeni ortaklar icin borc/alacak alt hesap havuzu mevcut"
        ),
        ready:
          hasShareholderParentMapping &&
          availableCapitalCreditShareholderAccounts.length > 0 &&
          availableCommitmentDebitShareholderAccounts.length > 0,
      },
      {
        key: "fiscalPeriods",
        label: l(
          "Fiscal period exists",
          "Mali donem mevcut"
        ),
        ready: periods.length > 0,
      },
      {
        key: "batchCurrency",
        label: l(
          "Batch queue uses one currency",
          "Toplu fis icin ayni para birimi"
        ),
        ready:
          pendingBatchCommitmentShareholders.length === 0 ||
          !batchPreviewMixedCurrencyIssue,
      },
    ];
  }, [
    availableCapitalCreditShareholderAccounts.length,
    availableCommitmentDebitShareholderAccounts.length,
    batchPreviewMixedCurrencyIssue,
    hasShareholderParentMapping,
    l,
    parentMappingStatus.reasons,
    parentMappingStatus.valid,
    pendingBatchCommitmentShareholders.length,
    periods.length,
    selectedShareholderLegalEntityId,
    shareholdersWithCommittedCapital,
  ]);
  const selectedShareholderMissingChecks = useMemo(
    () => selectedShareholderSetupChecks.filter((row) => !row.ready),
    [selectedShareholderSetupChecks]
  );
  const selectedShareholderLegalEntity = useMemo(
    () =>
      legalEntities.find(
        (row) =>
          Number(row.id) === Number(selectedShareholderLegalEntityId)
      ) || null,
    [legalEntities, selectedShareholderLegalEntityId]
  );
  const selectedShareholderCommitmentReadiness = getModuleRow(
    "shareholderCommitment",
    selectedShareholderLegalEntityId
  );
  const shareholderCommitmentModuleNotReady = Boolean(
    selectedShareholderCommitmentReadiness &&
      !selectedShareholderCommitmentReadiness.ready
  );
  const shareholderSetupSteps = useMemo(() => {
    const queueCount = pendingBatchCommitmentShareholders.length;
    const previewReady =
      queueCount > 0 &&
      Boolean(batchPreviewData) &&
      !batchPreviewData?.validation?.has_blocking_errors &&
      Number(batchPreviewData?.included_shareholders?.length || 0) > 0;

    const stepDefinitions = [
      {
        key: "selectLegalEntity",
        label: l("Tuzel Kisi Sec", "Tuzel Kisi Sec"),
        done: Boolean(selectedShareholderLegalEntityId),
      },
      {
        key: "saveParentMapping",
        label: l(
          "Parent Hesap Eslemesi Kaydet",
          "Parent Hesap Eslemesi Kaydet"
        ),
        done: hasShareholderParentMapping,
      },
      {
        key: "autoProvisionSubAccounts",
        label: l(
          "Alt Hesaplari Otomatik Olustur",
          "Alt Hesaplari Otomatik Olustur"
        ),
        done:
          hasShareholderParentMapping &&
          !hasMissingCreditEquitySubAccount &&
          !hasMissingDebitEquitySubAccount,
      },
      {
        key: "saveShareholders",
        label: l(
          "Ortaklari Kaydet / Guncelle",
          "Ortaklari Kaydet / Guncelle"
        ),
        done: visibleShareholders.length > 0,
      },
      {
        key: "previewBatchJournal",
        label: l(
          "Toplu Taahhut Fisi Taslagi Olustur",
          "Toplu Taahhut Fisi Taslagi Olustur"
        ),
        done: queueCount === 0 ? true : previewReady,
      },
    ];

    const firstWaitingIndex = stepDefinitions.findIndex((step) => !step.done);
    return stepDefinitions.map((step, index) => ({
      ...step,
      status:
        step.done
          ? "DONE"
          : firstWaitingIndex === index
            ? "CURRENT"
            : "WAITING",
    }));
  }, [
    batchPreviewData,
    hasMissingCreditEquitySubAccount,
    hasMissingDebitEquitySubAccount,
    hasShareholderParentMapping,
    l,
    pendingBatchCommitmentShareholders.length,
    selectedShareholderLegalEntityId,
    visibleShareholders.length,
  ]);
  const nextShareholderSetupStep = useMemo(
    () =>
      shareholderSetupSteps.find((step) => step.status === "CURRENT") ||
      shareholderSetupSteps.find((step) => step.status !== "DONE") ||
      null,
    [shareholderSetupSteps]
  );
  const batchPreviewBlockingErrors = Array.isArray(
    batchPreviewData?.validation?.blocking_errors
  )
    ? batchPreviewData.validation.blocking_errors
    : [];
  const batchPreviewWarnings = Array.isArray(batchPreviewData?.validation?.warnings)
    ? batchPreviewData.validation.warnings
    : [];
  const batchPreviewIncludedRows = Array.isArray(
    batchPreviewData?.included_shareholders
  )
    ? batchPreviewData.included_shareholders
    : [];
  const batchPreviewSkippedRows = Array.isArray(
    batchPreviewData?.skipped_shareholders
  )
    ? batchPreviewData.skipped_shareholders
    : [];
  const batchPreviewHasBlockingErrors = Boolean(
    batchPreviewData?.validation?.has_blocking_errors ||
      batchPreviewBlockingErrors.length > 0
  );

  useEffect(() => {
    if (!selectedShareholderLegalEntityId) {
      setShareholderParentConfigForm({
        capitalCreditParentAccountId: "",
        commitmentDebitParentAccountId: "",
      });
      return;
    }
    setShareholderParentConfigForm({
      capitalCreditParentAccountId: selectedCapitalCreditParentAccountId
        ? String(selectedCapitalCreditParentAccountId)
        : "",
      commitmentDebitParentAccountId: selectedCommitmentDebitParentAccountId
        ? String(selectedCommitmentDebitParentAccountId)
        : "",
    });
  }, [
    selectedCapitalCreditParentAccountId,
    selectedCommitmentDebitParentAccountId,
    selectedShareholderLegalEntityId,
  ]);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(
        SHAREHOLDER_BATCH_QUEUE_STORAGE_KEY
      );
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") {
        return;
      }
      const sanitized = Object.fromEntries(
        Object.entries(parsed).map(([entityId, ids]) => [
          String(entityId),
          Array.from(new Set((Array.isArray(ids) ? ids : []).map(toNumber).filter(Boolean))),
        ])
      );
      setCommitmentBatchQueueByEntity(sanitized);
    } catch {
      // Ignore localStorage parse failures.
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        SHAREHOLDER_BATCH_QUEUE_STORAGE_KEY,
        JSON.stringify(commitmentBatchQueueByEntity || {})
      );
    } catch {
      // Ignore localStorage write failures.
    }
  }, [commitmentBatchQueueByEntity]);

  useEffect(() => {
    const existingIdsByEntity = new Map();
    for (const row of shareholders) {
      const entityId = String(toNumber(row.legal_entity_id) || "");
      const shareholderId = toNumber(row.id);
      if (!entityId || !shareholderId) {
        continue;
      }
      if (!existingIdsByEntity.has(entityId)) {
        existingIdsByEntity.set(entityId, new Set());
      }
      existingIdsByEntity.get(entityId).add(shareholderId);
    }

    setCommitmentBatchQueueByEntity((prev) => {
      const next = {};
      let changed = false;
      for (const [entityId, ids] of Object.entries(prev || {})) {
        const existingIds = existingIdsByEntity.get(String(entityId)) || new Set();
        const filteredIds = Array.from(
          new Set((Array.isArray(ids) ? ids : []).map(toNumber).filter(Boolean))
        ).filter((id) => existingIds.has(id));
        if (filteredIds.length > 0) {
          next[String(entityId)] = filteredIds;
        }
        if (
          filteredIds.length !== (Array.isArray(ids) ? ids.length : 0) ||
          (filteredIds.length > 0 &&
            JSON.stringify(filteredIds) !==
              JSON.stringify(Array.isArray(ids) ? ids : []))
        ) {
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [shareholders]);

  useEffect(() => {
    setBatchPreviewData(null);
  }, [batchCommitmentDate, selectedShareholderLegalEntityId, selectedEntityCommitmentQueueIds]);

  useEffect(() => {
    if (!commitmentIncreaseModalOpen) {
      return;
    }
    const defaultShareholderId = toNumber(
      eligibleShareholdersForCommitmentIncrease[0]?.id
    );
    if (!defaultShareholderId) {
      setCommitmentIncreaseModalOpen(false);
      return;
    }
    if (!toNumber(commitmentIncreaseForm.shareholderId)) {
      setCommitmentIncreaseForm((prev) => ({
        ...prev,
        shareholderId: String(defaultShareholderId),
      }));
      return;
    }
    const exists = eligibleShareholdersForCommitmentIncrease.some(
      (row) => toNumber(row.id) === toNumber(commitmentIncreaseForm.shareholderId)
    );
    if (!exists) {
      setCommitmentIncreaseForm((prev) => ({
        ...prev,
        shareholderId: String(defaultShareholderId),
      }));
    }
  }, [
    commitmentIncreaseForm.shareholderId,
    commitmentIncreaseModalOpen,
    eligibleShareholdersForCommitmentIncrease,
  ]);

  const updateQueueForSelectedEntity = useCallback(
    (updater) => {
      const entityId = toNumber(selectedShareholderLegalEntityId);
      if (!entityId) {
        setError(l("Select legal entity first.", "Once istirak / bagli ortak secin."));
        return;
      }

      const key = String(entityId);
      setCommitmentBatchQueueByEntity((prev) => {
        const current = Array.from(
          new Set((prev?.[key] || []).map((value) => toNumber(value)).filter(Boolean))
        );
        const nextIdsRaw =
          typeof updater === "function" ? updater(current) : Array.isArray(updater) ? updater : [];
        const nextIds = Array.from(
          new Set((Array.isArray(nextIdsRaw) ? nextIdsRaw : []).map((value) => toNumber(value)).filter(Boolean))
        );
        const next = { ...(prev || {}) };
        if (nextIds.length > 0) {
          next[key] = nextIds;
        } else {
          delete next[key];
        }
        return next;
      });
    },
    [l, selectedShareholderLegalEntityId]
  );

  const handleQueueShareholderToggle = useCallback(
    (shareholderId, shouldQueue) => {
      const normalizedId = toNumber(shareholderId);
      if (!normalizedId) {
        return;
      }
      updateQueueForSelectedEntity((currentIds) => {
        const nextSet = new Set(currentIds);
        if (shouldQueue) {
          nextSet.add(normalizedId);
        } else {
          nextSet.delete(normalizedId);
        }
        return Array.from(nextSet);
      });
    },
    [updateQueueForSelectedEntity]
  );

  const handleQueueKeepOnlyCurrency = useCallback(
    (currencyCode) => {
      const selectedGroup = pendingBatchQueueCurrencyGroups.find(
        (group) => group.currencyCode === currencyCode
      );
      updateQueueForSelectedEntity(selectedGroup?.shareholderIds || []);
      setBatchPreviewData(null);
      setMessage(
        l(
          `Queue filtered to currency ${currencyCode}.`,
          `Kuyruk ${currencyCode} para birimine filtrelendi.`
        )
      );
    },
    [l, pendingBatchQueueCurrencyGroups, updateQueueForSelectedEntity]
  );

  function resetGroupForm() {
    setGroupForm({ code: "", name: "" });
    setGroupEditingCode("");
  }

  function handleGroupEdit(row) {
    const code = String(row?.code || "").trim();
    const name = String(row?.name || "").trim();
    if (!code) {
      return;
    }
    setGroupEditingCode(code);
    setGroupForm({ code, name });
    setError("");
    setMessage("");
  }

  async function handleGroupSubmit(event) {
    event.preventDefault();
    if (!canUpsertGroupCompany) {
      setError(l("Missing permission: org.group_company.upsert", "Eksik yetki: org.group_company.upsert"));
      return;
    }

    const normalizedCode = String(groupForm.code || "").trim();
    const normalizedName = String(groupForm.name || "").trim();
    const isEditMode = Boolean(groupEditingCode);
    if (!normalizedCode || !normalizedName) {
      setError(l("Code and name are required.", "Kod ve ad zorunludur."));
      return;
    }

    setSaving("group");
    setError("");
    setMessage("");
    try {
      await upsertGroupCompany({
        code: normalizedCode,
        name: normalizedName,
      });
      resetGroupForm();
      setMessage(
        isEditMode
          ? l("Group company updated.", "Grup sirketi guncellendi.")
          : l("Group company saved.", "Grup sirketi kaydedildi.")
      );
      await loadCoreData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save group company.", "Grup sirketi kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleLegalEntitySubmit(event) {
    event.preventDefault();
    if (!canUpsertLegalEntity) {
      setError(l("Missing permission: org.legal_entity.upsert", "Eksik yetki: org.legal_entity.upsert"));
      return;
    }

    const groupCompanyId = toNumber(entityForm.groupCompanyId);
    const countryId = toNumber(entityForm.countryId);
    if (!groupCompanyId || !countryId) {
      setError(l("groupCompanyId and countryId are required.", "groupCompanyId ve countryId zorunludur."));
      return;
    }

    let paymentTermsPayload;
    if (entityForm.useCustomPaymentTerms) {
      const rawPaymentTerms = String(entityForm.paymentTermsJson || "").trim();
      if (!rawPaymentTerms) {
        setError(
          l(
            "Custom payment terms JSON is required when custom mode is enabled.",
            "Ozel odeme kosulu modu acikken custom JSON zorunludur."
          )
        );
        return;
      }

      try {
        const parsed = JSON.parse(rawPaymentTerms);
        if (!Array.isArray(parsed) || parsed.length === 0) {
          setError(
            l(
              "Custom payment terms must be a non-empty JSON array.",
              "Ozel odeme kosullari bos olmayan bir JSON dizi olmali."
            )
          );
          return;
        }
        paymentTermsPayload = parsed;
      } catch {
        setError(
          l(
            "Custom payment terms JSON is invalid.",
            "Ozel odeme kosullari JSON formati gecersiz."
          )
        );
        return;
      }
    }

    setSaving("entity");
    setError("");
    setMessage("");
    try {
      const response = await upsertLegalEntity({
        groupCompanyId,
        code: entityForm.code.trim(),
        name: entityForm.name.trim(),
        taxId: entityForm.taxId.trim() || undefined,
        countryId,
        functionalCurrencyCode: entityForm.functionalCurrencyCode
          .trim()
          .toUpperCase(),
        isIntercompanyEnabled: Boolean(entityForm.isIntercompanyEnabled),
        intercompanyPartnerRequired: Boolean(entityForm.intercompanyPartnerRequired),
        autoProvisionDefaults: Boolean(entityForm.autoProvisionDefaults),
        ...(paymentTermsPayload ? { paymentTerms: paymentTermsPayload } : {}),
      });

      setEntityForm((prev) => ({
        ...prev,
        code: "",
        name: "",
        taxId: "",
        functionalCurrencyCode: prev.functionalCurrencyCode || "USD",
        useCustomPaymentTerms: false,
        paymentTermsJson: "",
      }));
      const hasGlProvisioning = Boolean(response?.provisioning?.created);
      const hasPaymentTermProvisioning = Boolean(response?.paymentTermsProvisioning);
      if (hasGlProvisioning || hasPaymentTermProvisioning) {
        const created = response?.provisioning?.created || null;
        const paymentTermsProvisioning = response?.paymentTermsProvisioning || null;
        const glSummary = created
          ? l(
              `Defaults created: calendar ${created.fiscalCalendars}, periods ${created.fiscalPeriods}, CoA ${created.chartsOfAccounts}, accounts ${created.accounts}, books ${created.books}.`,
              `Varsayilanlar olusturuldu: takvim ${created.fiscalCalendars}, donem ${created.fiscalPeriods}, hesap plani ${created.chartsOfAccounts}, hesap ${created.accounts}, defter ${created.books}.`
            )
          : "";
        const paymentTermsSummary = paymentTermsProvisioning
          ? l(
              `Payment terms: created ${paymentTermsProvisioning.createdCount}, skipped ${paymentTermsProvisioning.skippedCount}.`,
              `Odeme kosullari: olusturulan ${paymentTermsProvisioning.createdCount}, atlanan ${paymentTermsProvisioning.skippedCount}.`
            )
          : "";
        const detailMessage = [glSummary, paymentTermsSummary].filter(Boolean).join(" ");
        setMessage(
          `${l("Legal entity saved.", "Istirak / bagli ortak kaydedildi.")} ${detailMessage}`.trim()
        );
      } else {
        setMessage(l("Legal entity saved.", "Istirak / bagli ortak kaydedildi."));
      }
      await loadCoreData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save legal entity.", "Istirak / bagli ortak kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleOperatingUnitSubmit(event) {
    event.preventDefault();
    if (!canUpsertOperatingUnit) {
      setError(l("Missing permission: org.operating_unit.upsert", "Eksik yetki: org.operating_unit.upsert"));
      return;
    }

    const legalEntityId = toNumber(unitForm.legalEntityId);
    if (!legalEntityId) {
      setError(l("legalEntityId is required.", "legalEntityId zorunludur."));
      return;
    }

    setSaving("unit");
    setError("");
    setMessage("");
    try {
      await upsertOperatingUnit({
        legalEntityId,
        code: unitForm.code.trim(),
        name: unitForm.name.trim(),
        unitType: unitForm.unitType,
        hasSubledger: Boolean(unitForm.hasSubledger),
      });
      setUnitForm((prev) => ({
        ...prev,
        code: "",
        name: "",
      }));
      setMessage(l("Operating unit saved.", "Operasyon birimi kaydedildi."));
      await loadCoreData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save operating unit.", "Operasyon birimi kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleShareholderParentConfigSubmit(event) {
    event.preventDefault();
    if (!canUpsertShareholder) {
      setError(
        l(
          "Missing permission: org.legal_entity.upsert",
          "Eksik yetki: org.legal_entity.upsert"
        )
      );
      return;
    }

    const legalEntityId = toNumber(shareholderForm.legalEntityId);
    const capitalCreditParentAccountId = toNumber(
      shareholderParentConfigForm.capitalCreditParentAccountId
    );
    const commitmentDebitParentAccountId = toNumber(
      shareholderParentConfigForm.commitmentDebitParentAccountId
    );
    if (!legalEntityId || !capitalCreditParentAccountId || !commitmentDebitParentAccountId) {
      setError(
        l(
          "Select legal entity, capital credit parent, and commitment debit parent first.",
          "Once istirak / bagli ortak, sermaye alacak parent ve taahhut borc parent secin."
        )
      );
      return;
    }
    if (capitalCreditParentAccountId === commitmentDebitParentAccountId) {
      setError(
        l(
          "Commitment debit parent must be different from capital credit parent.",
          "Taahhut borc parent hesap, sermaye alacak parent hesaptan farkli olmalidir."
        )
      );
      return;
    }

    setSaving("shareholderConfig");
    setError("");
    setMessage("");
    try {
      await upsertShareholderJournalConfig({
        legalEntityId,
        capitalCreditParentAccountId,
        commitmentDebitParentAccountId,
      });
      setShareholderForm((prev) => ({
        ...prev,
        capitalSubAccountId: "",
        commitmentDebitSubAccountId: "",
      }));
      setMessage(
        l(
          "Shareholder parent account mapping saved.",
          "Ortak parent hesap eslesmesi kaydedildi."
        )
      );
      await Promise.all([loadCoreData(), refreshLegalEntity(legalEntityId)]);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to save shareholder parent account mapping.",
            "Ortak parent hesap eslesmesi kaydedilemedi."
          )
      );
    } finally {
      setSaving("");
    }
  }

  async function handleAutoCreateMissingShareholderSubAccounts() {
    if (!canUpsertAccounts) {
      setError(
        l(
          "Missing permission: gl.account.upsert",
          "Eksik yetki: gl.account.upsert"
        )
      );
      return;
    }

    const legalEntityId = toNumber(shareholderForm.legalEntityId);
    if (!legalEntityId) {
      setError(
        l(
          "Select legal entity first.",
          "Once istirak / bagli ortak secin."
        )
      );
      return;
    }

    const shareholderCode = String(shareholderForm.code || "").trim();
    const shareholderName = String(shareholderForm.name || "").trim();
    if (!shareholderCode || !shareholderName) {
      setError(
        l(
          "Enter shareholder code and name before auto setup.",
          "Otomatik kurulumdan once ortak kodu ve adini girin."
        )
      );
      return;
    }
    if (!hasMissingCreditEquitySubAccount && !hasMissingDebitEquitySubAccount) {
      setMessage(
        l(
          "No missing shareholder sub-account setup was detected.",
          "Eksik ortak alt hesap kurulumu tespit edilmedi."
        )
      );
      setAutoSubAccountSetupModalOpen(false);
      return;
    }

    setAutoSubAccountSetupSaving(true);
    setError("");
    setMessage("");

    try {
      const matchingShareholder = visibleShareholders.find(
        (row) => String(row.code || "").trim().toUpperCase() === shareholderCode.toUpperCase()
      );
      const response = await autoProvisionShareholderSubAccounts({
        legalEntityId,
        shareholderCode,
        shareholderName,
        shareholderId: toNumber(matchingShareholder?.id) || undefined,
      });
      const creditSubAccountId = toNumber(response?.capitalSubAccount?.id);
      const debitSubAccountId = toNumber(
        response?.commitmentDebitSubAccount?.id
      );
      if (!creditSubAccountId || !debitSubAccountId) {
        throw new Error(
          l(
            "Auto provisioning did not return both sub-accounts.",
            "Otomatik kurulum iki alt hesabi da donmedi."
          )
        );
      }

      setShareholderForm((prev) => ({
        ...prev,
        capitalSubAccountId: String(creditSubAccountId),
        commitmentDebitSubAccountId: String(debitSubAccountId),
      }));

      await loadCoreData();
      setMessage(
        l(
          "Shareholder sub-accounts are ready and pre-selected.",
          "Ortak alt hesaplari hazirlandi ve forma otomatik secildi."
        )
      );
      setAutoSubAccountSetupModalOpen(false);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          err?.message ||
          l(
            "Failed to auto-create shareholder sub-accounts.",
            "Ortak alt hesaplari otomatik olusturulamadi."
          )
      );
    } finally {
      setAutoSubAccountSetupSaving(false);
    }
  }

  async function handlePreviewBatchCommitmentJournal() {
    if (shareholderCommitmentModuleNotReady) {
      setError(
        l(
          "Shareholder commitment module setup is incomplete. Complete mappings in GL setup first.",
          "Ortak taahhut modul kurulumu eksik. Once GL ayarlarinda eslemeleri tamamlayin."
        )
      );
      return;
    }
    const legalEntityId = toNumber(shareholderForm.legalEntityId);
    if (!legalEntityId) {
      setError(
        l("Select legal entity first.", "Once istirak / bagli ortak secin.")
      );
      return;
    }
    const shareholderIds = pendingBatchCommitmentShareholders
      .map((row) => toNumber(row.id))
      .filter(Boolean);
    if (shareholderIds.length === 0) {
      setError(
        l(
          "No queued shareholders found for batch commitment journal.",
          "Toplu taahhut yevmiyesi icin kuyrukta ortak bulunamadi."
        )
      );
      return;
    }

    setBatchPreviewLoading(true);
    setError("");
    try {
      const preview = await previewShareholderCommitmentBatchJournal({
        legalEntityId,
        shareholderIds,
        commitmentDate: batchCommitmentDate || undefined,
      });
      setBatchPreviewData(preview || null);
      return preview;
    } catch (err) {
      setBatchPreviewData(null);
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to load batch commitment preview.",
            "Toplu taahhut onizlemesi yuklenemedi."
          )
      );
      return null;
    } finally {
      setBatchPreviewLoading(false);
    }
  }

  async function handleCreateBatchCommitmentJournal() {
    if (batchCommitmentSaving) {
      return;
    }
    if (shareholderCommitmentModuleNotReady) {
      setError(
        l(
          "Shareholder commitment module setup is incomplete. Complete mappings in GL setup first.",
          "Ortak taahhut modul kurulumu eksik. Once GL ayarlarinda eslemeleri tamamlayin."
        )
      );
      return;
    }

    const legalEntityId = toNumber(shareholderForm.legalEntityId);
    if (!legalEntityId) {
      setError(
        l("Select legal entity first.", "Once istirak / bagli ortak secin.")
      );
      return;
    }

    const shareholderIds = pendingBatchCommitmentShareholders
      .map((row) => toNumber(row.id))
      .filter(Boolean);
    if (shareholderIds.length === 0) {
      setError(
        l(
          "No queued shareholders found for batch commitment journal.",
          "Toplu taahhut yevmiyesi icin kuyrukta ortak bulunamadi."
        )
      );
      return;
    }

    let preview = batchPreviewData;
    if (!preview || batchPreviewLoading) {
      preview = await handlePreviewBatchCommitmentJournal();
    }
    if (!preview) {
      return;
    }
    if (preview?.validation?.has_blocking_errors) {
      setError(
        l(
          "Fix preview validation errors before creating the batch journal.",
          "Toplu fis olusturmadan once onizleme dogrulama hatalarini duzeltin."
        )
      );
      return;
    }

    setBatchCommitmentSaving(true);
    setError("");
    setMessage("");

    try {
      const response = await createShareholderCommitmentBatchJournal({
        legalEntityId,
        shareholderIds,
        commitmentDate: batchCommitmentDate || undefined,
      });

      const amountLabel = Number(response?.totalAmount || 0).toLocaleString(
        undefined,
        {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        }
      );
      setMessage(
        l(
          "Batch commitment draft journal created.",
          "Toplu taahhut taslak yevmiyesi olusturuldu."
        )
      );
      setShareholderJournalModal({
        title: l(
          "Batch Commitment Journal Created",
          "Toplu Taahhut Yevmiye Kaydi Olusturuldu"
        ),
        message: l(
          `Draft journal ${response?.journalNo || "-"} created for ${response?.shareholderCount || shareholderIds.length} shareholders. Total amount: ${amountLabel}.`,
          `${response?.shareholderCount || shareholderIds.length} ortak icin ${response?.journalNo || "-"} numarali taslak fis olusturuldu. Toplam tutar: ${amountLabel}.`
        ),
        journalNo: response?.journalNo || "-",
        journalEntryId: response?.journalEntryId || "-",
        bookCode: response?.bookCode || "-",
        fiscalPeriodId: response?.fiscalPeriodId || "-",
      });

      const processedIds = Array.isArray(response?.processedShareholderIds)
        ? response.processedShareholderIds
        : shareholderIds;
      const processedIdSet = new Set(
        processedIds.map((value) => toNumber(value)).filter(Boolean)
      );
      setCommitmentBatchQueueByEntity((prev) => {
        const key = String(legalEntityId);
        const currentQueue = Array.from(
          new Set((prev?.[key] || []).map((value) => toNumber(value)).filter(Boolean))
        );
        const remainingQueue = currentQueue.filter((id) => !processedIdSet.has(id));
        const next = { ...(prev || {}) };
        if (remainingQueue.length > 0) {
          next[key] = remainingQueue;
        } else {
          delete next[key];
        }
        return next;
      });
      setBatchPreviewData(null);
      setBatchCommitmentModalOpen(false);
      await loadCoreData();
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to create batch commitment journal.",
            "Toplu taahhut yevmiyesi olusturulamadi."
          )
      );
    } finally {
      setBatchCommitmentSaving(false);
    }
  }

  function openCommitmentIncreaseModal() {
    if (!selectedShareholderLegalEntityId) {
      setError(
        l(
          "Select legal entity first.",
          "Once istirak / bagli ortak secin."
        )
      );
      return;
    }
    if (eligibleShareholdersForCommitmentIncrease.length === 0) {
      setError(
        l(
          "No eligible shareholder found. Shareholder must have both capital and commitment sub-accounts.",
          "Uygun ortak bulunamadi. Ortakta hem sermaye hem taahhut alt hesap tanimli olmalidir."
        )
      );
      return;
    }
    setError("");
    setCommitmentIncreaseForm({
      shareholderId: String(eligibleShareholdersForCommitmentIncrease[0].id),
      commitmentDate:
        shareholderForm.commitmentDate || new Date().toISOString().slice(0, 10),
      increaseAmount: "0",
    });
    setCommitmentIncreaseModalOpen(true);
  }

  async function handleCommitmentIncreaseSubmit(event) {
    event.preventDefault();
    if (!canUpsertShareholder) {
      setError(
        l(
          "Missing permission: org.legal_entity.upsert",
          "Eksik yetki: org.legal_entity.upsert"
        )
      );
      return;
    }

    const selectedShareholder = selectedCommitmentIncreaseShareholder;
    if (!selectedShareholder) {
      setError(
        l(
          "Select an existing shareholder first.",
          "Once mevcut bir ortak secin."
        )
      );
      return;
    }

    const legalEntityId = toNumber(selectedShareholder.legal_entity_id);
    if (!legalEntityId) {
      setError(l("legalEntityId is required.", "legalEntityId zorunludur."));
      return;
    }

    const increaseAmount = Number(commitmentIncreaseForm.increaseAmount || 0);
    if (!Number.isFinite(increaseAmount) || increaseAmount <= 0) {
      setError(
        l(
          "Commitment increase must be greater than 0.",
          "Taahhut artisi 0'dan buyuk olmalidir."
        )
      );
      return;
    }

    const capitalSubAccountId = toNumber(selectedShareholder.capital_sub_account_id);
    const commitmentDebitSubAccountId = toNumber(
      selectedShareholder.commitment_debit_sub_account_id
    );
    if (!capitalSubAccountId || !commitmentDebitSubAccountId) {
      setError(
        l(
          "Selected shareholder is missing mapped sub-accounts.",
          "Secilen ortakta eslenmis alt hesaplar eksik."
        )
      );
      return;
    }

    const committedCapital = normalizeAmount(
      Number(selectedShareholder.committed_capital || 0) + increaseAmount
    );

    setSaving("shareholderIncrease");
    setError("");
    setMessage("");
    try {
      const response = await upsertShareholder({
        legalEntityId,
        code: String(selectedShareholder.code || "").trim(),
        name: String(selectedShareholder.name || "").trim(),
        shareholderType:
          String(selectedShareholder.shareholder_type || "INDIVIDUAL").toUpperCase(),
        taxId: selectedShareholder.tax_id
          ? String(selectedShareholder.tax_id).trim()
          : undefined,
        commitmentDate: commitmentIncreaseForm.commitmentDate || undefined,
        committedCapital,
        capitalSubAccountId,
        commitmentDebitSubAccountId,
        autoCommitmentJournal: false,
        currencyCode: String(selectedShareholder.currency_code || "USD")
          .trim()
          .toUpperCase(),
        status: String(selectedShareholder.status || "ACTIVE").toUpperCase(),
        notes: selectedShareholder.notes
          ? String(selectedShareholder.notes).trim()
          : undefined,
      });

      const savedShareholderId =
        toNumber(response?.id) || toNumber(selectedShareholder.id);
      const committedCapitalDelta = normalizeAmount(
        response?.committedCapitalDelta || 0
      );
      if (committedCapitalDelta > 0 && savedShareholderId) {
        setCommitmentBatchQueueByEntity((prev) => {
          const key = String(legalEntityId);
          const currentQueue = Array.from(
            new Set((prev?.[key] || []).map((value) => toNumber(value)).filter(Boolean))
          );
          if (!currentQueue.includes(savedShareholderId)) {
            currentQueue.push(savedShareholderId);
          }
          return {
            ...(prev || {}),
            [key]: currentQueue,
          };
        });
        setMessage(
          l(
            "Commitment increase saved and queued for batch commitment journal.",
            "Taahhut artisi kaydedildi ve toplu taahhut yevmiyesi icin kuyruga alindi."
          )
        );
      } else {
        setMessage(l("Commitment increase saved.", "Taahhut artisi kaydedildi."));
      }

      setCommitmentIncreaseModalOpen(false);
      setCommitmentIncreaseForm((prev) => ({
        ...prev,
        increaseAmount: "0",
      }));
      await loadCoreData();
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to save commitment increase.",
            "Taahhut artisi kaydedilemedi."
          )
      );
    } finally {
      setSaving("");
    }
  }

  async function handleShareholderSubmit(event) {
    event.preventDefault();
    if (!canUpsertShareholder) {
      setError(
        l(
          "Missing permission: org.legal_entity.upsert",
          "Eksik yetki: org.legal_entity.upsert"
        )
      );
      return;
    }

    const legalEntityId = toNumber(shareholderForm.legalEntityId);
    if (!legalEntityId) {
      setError(l("legalEntityId is required.", "legalEntityId zorunludur."));
      return;
    }
    const commitmentIncreaseAmount = Number(shareholderForm.committedCapital || 0);
    if (!Number.isFinite(commitmentIncreaseAmount) || commitmentIncreaseAmount < 0) {
      setError(
        l(
          "Commitment increase must be a non-negative number.",
          "Taahhut artisi 0 veya daha buyuk bir sayi olmalidir."
        )
      );
      return;
    }
    const normalizedShareholderCode = String(shareholderForm.code || "")
      .trim()
      .toUpperCase();
    const existingShareholder = visibleShareholders.find(
      (row) =>
        String(row.code || "")
          .trim()
          .toUpperCase() === normalizedShareholderCode
    );
    const previousCommittedCapital = normalizeAmount(
      existingShareholder?.committed_capital || 0
    );
    const committedCapital = normalizeAmount(
      previousCommittedCapital + commitmentIncreaseAmount
    );
    const capitalSubAccountId = toNumber(shareholderForm.capitalSubAccountId);
    const commitmentDebitSubAccountId = toNumber(
      shareholderForm.commitmentDebitSubAccountId
    );
    if (committedCapital > 0 && !hasShareholderParentMapping) {
      setError(
        parentMappingStatus?.reasons?.[0] ||
          l(
            "Save valid shareholder parent account mapping before entering commitment increase.",
            "Taahhut artisi girmeden once gecerli ortak parent hesap eslesmesini kaydedin."
          )
      );
      return;
    }
    if (committedCapital > 0 && !capitalSubAccountId) {
      setError(
        l(
          "Capital sub-account is required when committed capital is greater than 0.",
          "Taahhut toplam sermaye 0'dan buyukse sermaye alt hesap zorunludur."
        )
      );
      return;
    }
    if (committedCapital > 0 && !commitmentDebitSubAccountId) {
      setError(
        l(
          "Commitment debit sub-account is required when committed capital is greater than 0.",
          "Taahhut toplam sermaye 0'dan buyukse taahhut borc alt hesap zorunludur."
        )
      );
      return;
    }
    if (
      capitalSubAccountId &&
      commitmentDebitSubAccountId &&
      capitalSubAccountId === commitmentDebitSubAccountId
    ) {
      setError(
        l(
          "Commitment debit sub-account must be different from capital sub-account.",
          "Taahhut borc alt hesap, sermaye alt hesaptan farkli olmalidir."
        )
      );
      return;
    }

    setSaving("shareholder");
    setError("");
    setMessage("");
    try {
      const response = await upsertShareholder({
        legalEntityId,
        code: shareholderForm.code.trim(),
        name: shareholderForm.name.trim(),
        shareholderType: shareholderForm.shareholderType,
        taxId: shareholderForm.taxId.trim() || undefined,
        commitmentDate: shareholderForm.commitmentDate || undefined,
        committedCapital,
        capitalSubAccountId: capitalSubAccountId || undefined,
        commitmentDebitSubAccountId: commitmentDebitSubAccountId || undefined,
        autoCommitmentJournal: false,
        currencyCode: shareholderForm.currencyCode.trim().toUpperCase(),
        status: shareholderForm.status,
        notes: shareholderForm.notes.trim() || undefined,
      });

      setShareholderForm((prev) => ({
        ...prev,
        code: "",
        name: "",
        taxId: "",
        committedCapital: "0",
        capitalSubAccountId: "",
        commitmentDebitSubAccountId: "",
        notes: "",
      }));

      const savedShareholderId = toNumber(response?.id);
      const committedCapitalDelta = normalizeAmount(
        response?.committedCapitalDelta || 0
      );
      if (committedCapitalDelta > 0 && savedShareholderId) {
        setCommitmentBatchQueueByEntity((prev) => {
          const key = String(legalEntityId);
          const currentQueue = Array.from(
            new Set((prev?.[key] || []).map((value) => toNumber(value)).filter(Boolean))
          );
          if (!currentQueue.includes(savedShareholderId)) {
            currentQueue.push(savedShareholderId);
          }
          return {
            ...(prev || {}),
            [key]: currentQueue,
          };
        });
        setMessage(
          l(
            "Shareholder saved and queued for batch commitment journal.",
            "Ortak kaydedildi ve toplu taahhut yevmiyesi icin kuyruga alindi."
          )
        );
      } else {
        setMessage(l("Shareholder saved.", "Ortak kaydedildi."));
      }
      await loadCoreData();
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to save shareholder.", "Ortak kaydedilemedi.")
      );
    } finally {
      setSaving("");
    }
  }

  async function handleFiscalCalendarSubmit(event) {
    event.preventDefault();
    if (!canUpsertFiscalCalendar) {
      setError(l("Missing permission: org.fiscal_calendar.upsert", "Eksik yetki: org.fiscal_calendar.upsert"));
      return;
    }

    setSaving("calendar");
    setError("");
    setMessage("");
    try {
      await upsertFiscalCalendar({
        code: calendarForm.code.trim(),
        name: calendarForm.name.trim(),
        yearStartMonth: Number(calendarForm.yearStartMonth),
        yearStartDay: Number(calendarForm.yearStartDay),
      });
      setCalendarForm({
        code: "",
        name: "",
        yearStartMonth: 1,
        yearStartDay: 1,
      });
      setMessage(l("Fiscal calendar saved.", "Mali takvim kaydedildi."));
      await loadCoreData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save fiscal calendar.", "Mali takvim kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleGeneratePeriods(event) {
    event.preventDefault();
    if (!canGenerateFiscalPeriods) {
      setError(l("Missing permission: org.fiscal_period.generate", "Eksik yetki: org.fiscal_period.generate"));
      return;
    }

    const calendarId = toNumber(periodForm.calendarId);
    const fiscalYear = toNumber(periodForm.fiscalYear);
    if (!calendarId || !fiscalYear) {
      setError(l("calendarId and fiscalYear are required.", "calendarId ve fiscalYear zorunludur."));
      return;
    }

    setSaving("periods");
    setError("");
    setMessage("");
    try {
      await generateFiscalPeriods({ calendarId, fiscalYear });
      setMessage(l("Fiscal periods generated.", "Mali donemler olusturuldu."));
      await loadPeriods(calendarId, fiscalYear);
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to generate fiscal periods.", "Mali donemler olusturulamadi."));
    } finally {
      setSaving("");
    }
  }

  if (!canReadOrgTree && !canReadFiscalCalendars) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {l(
          "You need `org.tree.read` and/or `org.fiscal_calendar.read` to use this page.",
          "Bu sayfayi kullanmak icin `org.tree.read` ve/veya `org.fiscal_calendar.read` yetkisi gerekir."
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <TenantReadinessChecklist />

      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {l("Organization Management", "Organizasyon Yonetimi")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {l(
            "Maintain company structure, branches, and fiscal structure after onboarding.",
            "Kurulumdan sonra sirket yapisini, subeleri ve mali yapilari yonetin."
          )}
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      )}
      {message && (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      )}

      <div className="grid gap-4 xl:grid-cols-2">
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Group Companies", "Grup Sirketleri")}
          </h2>
          {groupEditingCode ? (
            <p className="mb-2 text-xs text-slate-600">
              {l(
                `Editing group ${groupEditingCode}. Group code is locked.`,
                `${groupEditingCode} grubu duzenleniyor. Grup kodu kilitli.`
              )}
            </p>
          ) : null}
          <form onSubmit={handleGroupSubmit} className="grid gap-2 md:grid-cols-4">
            <input
              value={groupForm.code}
              onChange={(event) =>
                setGroupForm((prev) => ({ ...prev, code: event.target.value }))
              }
              disabled={Boolean(groupEditingCode)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Code", "Kod")}
              required
            />
            <input
              value={groupForm.name}
              onChange={(event) =>
                setGroupForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("Name", "Ad")}
              required
            />
            <button
              type="submit"
              disabled={saving === "group" || !canUpsertGroupCompany}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "group"
                ? l("Saving...", "Kaydediliyor...")
                : groupEditingCode
                  ? l("Update", "Guncelle")
                  : l("Save", "Kaydet")}
            </button>
            {groupEditingCode ? (
              <button
                type="button"
                onClick={resetGroupForm}
                disabled={saving === "group"}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60 md:col-start-4"
              >
                {l("Cancel Edit", "Duzenlemeyi Iptal Et")}
              </button>
            ) : null}
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Action", "Islem")}</th>
                </tr>
              </thead>
              <tbody>
                {(groups || []).map((row) => (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.id}</td>
                    <td className="px-3 py-2">{row.code}</td>
                    <td className="px-3 py-2">{row.name}</td>
                    <td className="px-3 py-2">
                      <button
                        type="button"
                        onClick={() => handleGroupEdit(row)}
                        disabled={saving === "group" || !canUpsertGroupCompany}
                        className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-60"
                      >
                        {l("Edit", "Duzenle")}
                      </button>
                    </td>
                  </tr>
                ))}
                {groups.length === 0 && !loading && (
                  <tr>
                    <td colSpan={4} className="px-3 py-3 text-slate-500">
                      {l("No group companies found.", "Grup sirketi bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
            </section>

            <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Legal Entities", "Istirakler / Bagli Ortaklar")}
          </h2>
          <form onSubmit={handleLegalEntitySubmit} className="grid gap-2 md:grid-cols-3">
            <select
              value={entityForm.groupCompanyId}
              onChange={(event) =>
                setEntityForm((prev) => ({
                  ...prev,
                  groupCompanyId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select group company", "Grup sirketi secin")}</option>
              {groups.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.code} - {row.name}
                </option>
              ))}
            </select>
            <input
              value={entityForm.code}
              onChange={(event) =>
                setEntityForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Entity code", "Birim kodu")}
              required
            />
            <input
              value={entityForm.name}
              onChange={(event) =>
                setEntityForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Entity name", "Birim adi")}
              required
            />

            <select
              value={entityForm.countryId}
              onChange={(event) => {
                const nextCountryId = event.target.value;
                const selectedCountry = countrySelectOptions.find(
                  (option) => String(option.id) === String(nextCountryId)
                );
                setEntityForm((prev) => ({
                  ...prev,
                  countryId: nextCountryId,
                  functionalCurrencyCode:
                    selectedCountry?.defaultCurrencyCode || prev.functionalCurrencyCode,
                }));
              }}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select country", "Ulke secin")}</option>
              {countrySelectOptions.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.label}
                </option>
              ))}
            </select>
            <select
              value={entityForm.functionalCurrencyCode}
              onChange={(event) =>
                setEntityForm((prev) => ({
                  ...prev,
                  functionalCurrencyCode: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select currency", "Para birimi secin")}</option>
              {currencySelectOptions.map((option) => (
                <option key={option.code} value={option.code}>
                  {option.label}
                </option>
              ))}
            </select>

            <input
              value={entityForm.taxId}
              onChange={(event) =>
                setEntityForm((prev) => ({ ...prev, taxId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("Tax ID (optional)", "Vergi No (opsiyonel)")}
            />
            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={entityForm.isIntercompanyEnabled}
                onChange={(event) =>
                  setEntityForm((prev) => ({
                    ...prev,
                    isIntercompanyEnabled: event.target.checked,
                  }))
                }
              />
              {l("Intercompany enabled", "Intercompany aktif")}
            </label>
            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={entityForm.intercompanyPartnerRequired}
                onChange={(event) =>
                  setEntityForm((prev) => ({
                    ...prev,
                    intercompanyPartnerRequired: event.target.checked,
                  }))
                }
              />
              {l("Partner required", "Karsi taraf zorunlu")}
            </label>
            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700 md:col-span-2">
              <input
                type="checkbox"
                checked={entityForm.autoProvisionDefaults}
                onChange={(event) =>
                  setEntityForm((prev) => ({
                    ...prev,
                    autoProvisionDefaults: event.target.checked,
                  }))
                }
              />
              {l(
                "Auto-create defaults (calendar, periods, CoA, accounts, book)",
                "Varsayilanlari otomatik olustur (takvim, donemler, hesap plani, hesaplar, defter)"
              )}
            </label>
            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700 md:col-span-2">
              <input
                type="checkbox"
                checked={entityForm.useCustomPaymentTerms}
                onChange={(event) =>
                  setEntityForm((prev) => ({
                    ...prev,
                    useCustomPaymentTerms: event.target.checked,
                  }))
                }
              />
              {l(
                "Use custom payment terms (JSON array)",
                "Ozel odeme kosulu kullan (JSON dizi)"
              )}
            </label>
            {entityForm.useCustomPaymentTerms ? (
              <textarea
                value={entityForm.paymentTermsJson}
                onChange={(event) =>
                  setEntityForm((prev) => ({
                    ...prev,
                    paymentTermsJson: event.target.value,
                  }))
                }
                rows={6}
                className="rounded-lg border border-slate-300 px-3 py-2 text-xs font-mono md:col-span-4"
                placeholder={`[
  {"code":"NET_30","name":"Net 30","dueDays":30},
  {"code":"NET_45","name":"Net 45","dueDays":45,"graceDays":2}
]`}
              />
            ) : null}
            <button
              type="submit"
              disabled={saving === "entity" || !canUpsertLegalEntity}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "entity" ? l("Saving...", "Kaydediliyor...") : l("Save", "Kaydet")}
            </button>
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Group", "Grup")}</th>
                  <th className="px-3 py-2">{l("Country", "Ulke")}</th>
                  <th className="px-3 py-2">{l("Currency", "Para birimi")}</th>
                </tr>
              </thead>
              <tbody>
                {(legalEntities || []).map((row) => (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.id}</td>
                    <td className="px-3 py-2">{row.code}</td>
                    <td className="px-3 py-2">{row.name}</td>
                    <td className="px-3 py-2">{row.group_company_id}</td>
                    <td className="px-3 py-2">{row.country_id}</td>
                    <td className="px-3 py-2">{row.functional_currency_code}</td>
                  </tr>
                ))}
                {legalEntities.length === 0 && !loading && (
                  <tr>
                    <td colSpan={6} className="px-3 py-3 text-slate-500">
                      {l("No legal entities found.", "Istirak / bagli ortak bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

            <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Operating Units / Branches", "Operasyon Birimleri / Subeler")}
          </h2>
          <form onSubmit={handleOperatingUnitSubmit} className="grid gap-2 md:grid-cols-5">
            <select
              value={unitForm.legalEntityId}
              onChange={(event) =>
                setUnitForm((prev) => ({
                  ...prev,
                  legalEntityId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              required
            >
              <option value="">{l("Select legal entity", "Istirak / bagli ortak secin")}</option>
              {legalEntities.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.code} - {row.name}
                </option>
              ))}
            </select>
            <input
              value={unitForm.code}
              onChange={(event) =>
                setUnitForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Unit code", "Birim kodu")}
              required
            />
            <input
              value={unitForm.name}
              onChange={(event) =>
                setUnitForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Unit name", "Birim adi")}
              required
            />
            <select
              value={unitForm.unitType}
              onChange={(event) =>
                setUnitForm((prev) => ({ ...prev, unitType: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {UNIT_TYPES.map((unitType) => (
                <option key={unitType} value={unitType}>
                  {unitType}
                </option>
              ))}
            </select>
            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={unitForm.hasSubledger}
                onChange={(event) =>
                  setUnitForm((prev) => ({
                    ...prev,
                    hasSubledger: event.target.checked,
                  }))
                }
              />
              {l("Has subledger", "Alt defter var")}
            </label>
            <button
              type="submit"
              disabled={saving === "unit" || !canUpsertOperatingUnit}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "unit" ? l("Saving...", "Kaydediliyor...") : l("Save", "Kaydet")}
            </button>
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Entity ID", "Birim ID")}</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Type", "Tur")}</th>
                  <th className="px-3 py-2">{l("Subledger", "Alt Defter")}</th>
                </tr>
              </thead>
              <tbody>
                {(operatingUnits || []).map((row) => (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.id}</td>
                    <td className="px-3 py-2">{row.legal_entity_id}</td>
                    <td className="px-3 py-2">{row.code}</td>
                    <td className="px-3 py-2">{row.name}</td>
                    <td className="px-3 py-2">{row.unit_type}</td>
                    <td className="px-3 py-2">{row.has_subledger ? l("Yes", "Evet") : l("No", "Hayir")}</td>
                  </tr>
                ))}
                {operatingUnits.length === 0 && !loading && (
                  <tr>
                    <td colSpan={6} className="px-3 py-3 text-slate-500">
                      {l("No operating units found.", "Operasyon birimi bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        {shareholderCardExpanded && (
          <div
            className="fixed inset-0 z-40 bg-slate-950/45"
            onClick={() => setShareholderCardExpanded(false)}
          />
        )}

        <section
          className={`border border-slate-200 bg-white p-4 ${
            shareholderCardExpanded
              ? "fixed inset-4 z-50 overflow-auto rounded-xl shadow-2xl"
              : "relative rounded-xl"
          }`}
        >
          <div className="mb-3 flex items-center justify-between gap-2">
            <h2 className="text-sm font-semibold text-slate-700">
              {l("Shareholders", "Ortaklar")}
            </h2>
            <button
              type="button"
              onClick={() => setShareholderCardExpanded((value) => !value)}
              className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-slate-300 bg-white text-slate-600 transition hover:bg-slate-50 hover:text-slate-900"
              title={
                shareholderCardExpanded
                  ? l("Exit expanded view", "Genis gorunumden cik")
                  : l("Expand card", "Karti genislet")
              }
              aria-label={
                shareholderCardExpanded
                  ? l("Exit expanded view", "Genis gorunumden cik")
                  : l("Expand card", "Karti genislet")
              }
            >
              <svg
                viewBox="0 0 20 20"
                className="h-4 w-4"
                fill="none"
                aria-hidden="true"
              >
                {shareholderCardExpanded ? (
                  <path
                    d="M7.5 4.5H4.5v3m8-3h3v3m-8 8h-3v-3m8 3h3v-3M4.5 7.5l4-4m7 4l-4-4m-7 9l4 4m7-4l-4 4"
                    stroke="currentColor"
                    strokeWidth="1.6"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                ) : (
                  <path
                    d="M7.5 4.5H4.5v3m8-3h3v3m-8 8h-3v-3m8 3h3v-3M8 8l-3.5-3.5m7 3.5l3.5-3.5M8 12l-3.5 3.5m7-3.5l3.5 3.5"
                    stroke="currentColor"
                    strokeWidth="1.6"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                )}
              </svg>
            </button>
          </div>
          {selectedShareholderLegalEntityId ? (
            <div className="mb-3 rounded-lg border border-slate-200 bg-slate-50 p-3">
              <div className="text-xs font-semibold text-slate-700">
                {l(
                  "Kurulum Adimlari / Next Recommended Action",
                  "Kurulum Adimlari / Sonraki Onerilen Aksiyon"
                )}
              </div>
              <div className="mt-2 grid gap-1 md:grid-cols-2">
                {shareholderSetupSteps.map((step) => (
                  <div
                    key={step.key}
                    className="flex items-center justify-between rounded border border-slate-200 bg-white px-2 py-1 text-xs"
                  >
                    <span className="text-slate-700">{step.label}</span>
                    <span
                      className={`rounded px-2 py-0.5 font-semibold ${
                        step.status === "DONE"
                          ? "bg-emerald-100 text-emerald-700"
                          : step.status === "CURRENT"
                            ? "bg-sky-100 text-sky-800"
                            : "bg-slate-100 text-slate-700"
                      }`}
                    >
                      {step.status === "DONE"
                        ? l("Done", "Tamam")
                        : step.status === "CURRENT"
                          ? l("Current", "Siradaki")
                          : l("Waiting", "Bekliyor")}
                    </span>
                  </div>
                ))}
              </div>
              {nextShareholderSetupStep ? (
                <div className="mt-2 rounded border border-sky-200 bg-sky-50 px-2 py-2 text-xs text-sky-900">
                  <span className="font-semibold">
                    {l("Next recommended action:", "Sonraki onerilen aksiyon:")}
                  </span>{" "}
                  {nextShareholderSetupStep.label}
                </div>
              ) : null}
              {selectedShareholderCommitmentReadiness ? (
                <div
                  className={`mt-2 rounded border px-2 py-2 text-xs ${
                    selectedShareholderCommitmentReadiness.ready
                      ? "border-emerald-200 bg-emerald-50 text-emerald-900"
                      : "border-amber-200 bg-amber-50 text-amber-900"
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-semibold">
                      {l(
                        "Module readiness: shareholder commitment",
                        "Modul hazirligi: ortak taahhut"
                      )}
                    </span>
                    <span
                      className={`rounded px-2 py-0.5 font-semibold ${
                        selectedShareholderCommitmentReadiness.ready
                          ? "bg-emerald-100 text-emerald-700"
                          : "bg-amber-100 text-amber-800"
                      }`}
                    >
                      {selectedShareholderCommitmentReadiness.ready
                        ? l("READY", "HAZIR")
                        : l("NOT READY", "HAZIR DEGIL")}
                    </span>
                  </div>
                  {!selectedShareholderCommitmentReadiness.ready ? (
                    <>
                      {Array.isArray(
                        selectedShareholderCommitmentReadiness.missingPurposeCodes
                      ) &&
                      selectedShareholderCommitmentReadiness.missingPurposeCodes.length > 0 ? (
                        <p className="mt-1">
                          {l("Missing purpose codes:", "Eksik amac kodlari:")}{" "}
                          {selectedShareholderCommitmentReadiness.missingPurposeCodes.join(
                            ", "
                          )}
                        </p>
                      ) : null}
                      {Array.isArray(
                        selectedShareholderCommitmentReadiness.invalidMappings
                      ) &&
                      selectedShareholderCommitmentReadiness.invalidMappings.length > 0 ? (
                        <ul className="mt-1 list-disc space-y-0.5 pl-4">
                          {selectedShareholderCommitmentReadiness.invalidMappings.map(
                            (row, index) => (
                              <li key={`shareholder-readiness-invalid-${index}`}>
                                {String(row?.purposeCode || "-")}:{" "}
                                {formatShareholderReadinessReason(row?.reason, l)}
                              </li>
                            )
                          )}
                        </ul>
                      ) : null}
                      <div className="mt-2 flex flex-wrap gap-2">
                        <Link
                          to="/app/ayarlar/hesap-plani-ayarlari#manual-purpose-mappings"
                          className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900"
                        >
                          {l("Fix manually", "Elle duzelt")}
                        </Link>
                        <Link
                          to="/app/ayarlar/hesap-plani-ayarlari#template-wizard"
                          className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900"
                        >
                          {l("Use template", "Sablon kullan")}
                        </Link>
                      </div>
                    </>
                  ) : null}
                </div>
              ) : null}
              <div className="mt-2 flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() =>
                    document
                      .getElementById("shareholder-parent-mapping-form")
                      ?.scrollIntoView({ behavior: "smooth", block: "start" })
                  }
                  className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-700"
                >
                  {l("Parent eslemeye git", "Parent eslemeye git")}
                </button>
                <button
                  type="button"
                  onClick={() => setAutoSubAccountSetupModalOpen(true)}
                  disabled={
                    !canUpsertAccounts ||
                    !hasShareholderParentMapping ||
                    (!hasMissingCreditEquitySubAccount &&
                      !hasMissingDebitEquitySubAccount)
                  }
                  className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-700 disabled:opacity-50"
                >
                  {l("Otomatik alt hesap olustur", "Otomatik alt hesap olustur")}
                </button>
                <button
                  type="button"
                  onClick={() =>
                    document
                      .getElementById("shareholder-form-block")
                      ?.scrollIntoView({ behavior: "smooth", block: "start" })
                  }
                  className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-700"
                >
                  {l("Ortak ekle", "Ortak ekle")}
                </button>
                <button
                  type="button"
                  onClick={openCommitmentIncreaseModal}
                  disabled={
                    !canUpsertShareholder ||
                    eligibleShareholdersForCommitmentIncrease.length === 0
                  }
                  className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-700 disabled:opacity-50"
                >
                  {l("Sermaye taahhut arttirimi", "Sermaye taahhut arttirimi")}
                </button>
                <button
                  type="button"
                  onClick={async () => {
                    setBatchCommitmentDate(
                      shareholderForm.commitmentDate ||
                        new Date().toISOString().slice(0, 10)
                    );
                    setBatchCommitmentModalOpen(true);
                    await handlePreviewBatchCommitmentJournal();
                  }}
                  disabled={
                    pendingBatchCommitmentShareholders.length === 0 ||
                    shareholderCommitmentModuleNotReady
                  }
                  className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-semibold text-slate-700 disabled:opacity-50"
                >
                  {l("Toplu fis onizle", "Toplu fis onizle")}
                </button>
              </div>
              <div className="mt-3 grid gap-1 md:grid-cols-2">
                {selectedShareholderSetupChecks.map((check) => (
                  <div
                    key={check.key}
                    className="flex items-center justify-between rounded border border-slate-200 bg-white px-2 py-1 text-xs"
                  >
                    <span className="text-slate-700">{check.label}</span>
                    <span
                      className={`rounded px-2 py-0.5 font-semibold ${
                        check.ready
                          ? "bg-emerald-100 text-emerald-700"
                          : "bg-amber-100 text-amber-800"
                      }`}
                    >
                      {check.ready ? l("OK", "Tamam") : l("Missing", "Eksik")}
                    </span>
                  </div>
                ))}
              </div>
              {selectedShareholderMissingChecks.length > 0 ? (
                <div className="mt-2 rounded border border-amber-200 bg-amber-50 px-2 py-2 text-xs text-amber-900">
                  <div className="font-semibold">
                    {l(
                      "System notice: complete missing setup before relying on automatic commitment journals.",
                      "Sistem uyarisi: otomatik taahhut yevmiyesine gecmeden once eksik kurulumlari tamamlayin."
                    )}
                  </div>
                  {parentMappingStatus.reasons.length > 0 ? (
                    <ul className="mt-1 list-disc space-y-0.5 pl-4">
                      {parentMappingStatus.reasons.slice(0, 3).map((reason) => (
                        <li key={reason}>{reason}</li>
                      ))}
                    </ul>
                  ) : null}
                  <div className="mt-2 flex flex-wrap gap-2">
                    <button
                      type="button"
                      onClick={() =>
                        document
                          .getElementById("shareholder-form-block")
                          ?.scrollIntoView({ behavior: "smooth", block: "start" })
                      }
                      className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900"
                    >
                      {l("Go to shareholder form", "Ortak formuna git")}
                    </button>
                    <Link
                      to="/app/ayarlar/hesap-plani-ayarlari"
                      className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900"
                    >
                      {l("Open GL setup", "GL ayarlarini ac")}
                    </Link>
                  </div>
                </div>
              ) : (
                <div className="mt-2 rounded border border-emerald-200 bg-emerald-50 px-2 py-2 text-xs text-emerald-800">
                  {l(
                    "System notice: setup is complete for automatic capital commitment draft journals.",
                    "Sistem bildirimi: otomatik sermaye taahhut taslak yevmiyesi icin kurulum tamamlandi."
                  )}
                </div>
              )}
            </div>
          ) : null}
          {selectedShareholderLegalEntityId ? (
            <form
              id="shareholder-parent-mapping-form"
              onSubmit={handleShareholderParentConfigSubmit}
              className="mb-3 rounded-lg border border-slate-200 bg-slate-50 p-3"
            >
              <div className="text-xs font-semibold text-slate-700">
                {l(
                  "Shareholder parent account mapping (per legal entity)",
                  "Ortak parent hesap eslesmesi (legal entity bazli)"
                )}
              </div>
              <p className="mt-1 text-xs text-slate-600">
                {l(
                  "Select non-postable header parent accounts used by shareholder sub-accounts. Example (TR): capital credit parent 500, commitment debit parent 501.",
                  "Ortak alt hesaplarinin baglanacagi post edilemeyen ust parent hesaplari secin. Ornek (TR): sermaye alacak parent 500, taahhut borc parent 501."
                )}
              </p>
              <div className="mt-2 grid gap-2 md:grid-cols-3">
                <select
                  value={shareholderParentConfigForm.capitalCreditParentAccountId}
                  onChange={(event) =>
                    setShareholderParentConfigForm((prev) => ({
                      ...prev,
                      capitalCreditParentAccountId: event.target.value,
                    }))
                  }
                  className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  disabled={!canReadAccounts}
                >
                  <option value="">
                    {canReadAccounts
                      ? l(
                          "Capital credit parent (CREDIT/EQUITY)",
                          "Sermaye alacak parent (CREDIT/EQUITY)"
                        )
                      : l("Need gl.account.read", "gl.account.read yetkisi gerekli")}
                  </option>
                  {equityCreditParentShareholderAccounts.map((account) => (
                    <option key={account.id} value={account.id}>
                      {account.code} - {account.name}
                    </option>
                  ))}
                </select>
                <select
                  value={shareholderParentConfigForm.commitmentDebitParentAccountId}
                  onChange={(event) =>
                    setShareholderParentConfigForm((prev) => ({
                      ...prev,
                      commitmentDebitParentAccountId: event.target.value,
                    }))
                  }
                  className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs"
                  disabled={!canReadAccounts}
                >
                  <option value="">
                    {canReadAccounts
                      ? l(
                          "Commitment debit parent (DEBIT/EQUITY)",
                          "Taahhut borc parent (DEBIT/EQUITY)"
                        )
                      : l("Need gl.account.read", "gl.account.read yetkisi gerekli")}
                  </option>
                  {equityDebitParentShareholderAccounts.map((account) => (
                    <option key={account.id} value={account.id}>
                      {account.code} - {account.name}
                    </option>
                  ))}
                </select>
                <button
                  type="submit"
                  disabled={saving === "shareholderConfig" || !canUpsertShareholder}
                  className="rounded-lg bg-slate-900 px-3 py-2 text-xs font-semibold text-white disabled:opacity-60"
                >
                  {saving === "shareholderConfig"
                    ? l("Saving...", "Kaydediliyor...")
                    : l("Save Parent Mapping", "Parent Eslesmesini Kaydet")}
                </button>
              </div>
            </form>
          ) : null}
          {pendingBatchCommitmentShareholders.length > 0 ? (
            <div className="mb-3 rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-3 text-xs text-indigo-900">
              <div className="font-semibold">
                {l(
                  "Batch commitment journal queue",
                  "Toplu taahhut yevmiye kuyrugu"
                )}
              </div>
              <p className="mt-1">
                {l(
                  `${pendingBatchCommitmentShareholders.length} shareholder(s) queued for one draft journal entry.`,
                  `${pendingBatchCommitmentShareholders.length} ortak tek bir taslak yevmiye fisine alinmak uzere kuyrukta.`
                )}
              </p>
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  onClick={async () => {
                    setBatchCommitmentDate(
                      shareholderForm.commitmentDate ||
                        new Date().toISOString().slice(0, 10)
                    );
                    setBatchCommitmentModalOpen(true);
                    await handlePreviewBatchCommitmentJournal();
                  }}
                  disabled={shareholderCommitmentModuleNotReady}
                  className="rounded border border-indigo-300 bg-white px-2.5 py-1 font-semibold text-indigo-900 disabled:opacity-50"
                >
                  {l(
                    "Create one batch commitment journal",
                    "Tek bir toplu taahhut yevmiyesi olustur"
                  )}
                </button>
                <button
                  type="button"
                  onClick={() => updateQueueForSelectedEntity([])}
                  className="rounded border border-slate-300 bg-white px-2.5 py-1 font-semibold text-slate-700"
                >
                  {l("Clear queue", "Kuyrugu temizle")}
                </button>
              </div>
              {pendingBatchQueueCurrencyGroups.length > 1 ? (
                <div className="mt-2 rounded border border-amber-200 bg-amber-50 px-2 py-2 text-[11px] text-amber-900">
                  <div className="font-semibold">
                    {l(
                      "Mixed currencies in queue. Keep one currency per batch.",
                      "Kuyrukta birden fazla para birimi var. Batch icin tek para birimi birakin."
                    )}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2">
                    {pendingBatchQueueCurrencyGroups.map((group) => (
                      <button
                        key={`queue-currency-${group.currencyCode}`}
                        type="button"
                        onClick={() => handleQueueKeepOnlyCurrency(group.currencyCode)}
                        className="rounded border border-amber-300 bg-white px-2 py-1 font-semibold text-amber-900"
                      >
                        {l(
                          `Keep only ${group.currencyCode} (${group.count})`,
                          `Sadece ${group.currencyCode} (${group.count}) birak`
                        )}
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
          {selectedShareholderLegalEntityId &&
          canReadAccounts &&
          (hasMissingCreditEquitySubAccount || hasMissingDebitEquitySubAccount) ? (
            <div className="mb-3 rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-3 text-xs text-cyan-900">
              <div className="font-semibold">
                {l(
                  "Sub-account setup module",
                  "Alt hesap kurulum modulu"
                )}
              </div>
              <p className="mt-1">
                {l(
                  "No available mapped shareholder sub-account remains for this legal entity.",
                  "Bu istirak / bagli ortak icin eslenmis kullanilabilir ortak alt hesap kalmadi."
                )}
              </p>
              <div className="mt-2 space-y-1">
                {hasMissingCreditEquitySubAccount ? (
                  <div>
                    {l(
                      `Missing: no available CREDIT leaf sub-account under ${selectedCapitalCreditParentAccount?.code || "-"}.`,
                      `${selectedCapitalCreditParentAccount?.code || "-"} altinda kullanilabilir CREDIT leaf alt hesap yok.`
                    )}
                  </div>
                ) : null}
                {hasMissingDebitEquitySubAccount ? (
                  <div>
                    {l(
                      `Missing: no available DEBIT leaf sub-account under ${selectedCommitmentDebitParentAccount?.code || "-"}.`,
                      `${selectedCommitmentDebitParentAccount?.code || "-"} altinda kullanilabilir DEBIT leaf alt hesap yok.`
                    )}
                  </div>
                ) : null}
              </div>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                {canUpsertAccounts ? (
                  <button
                    type="button"
                    onClick={() => setAutoSubAccountSetupModalOpen(true)}
                    className="rounded border border-cyan-300 bg-white px-2.5 py-1 font-semibold text-cyan-900"
                  >
                    {l(
                      "Auto-create missing sub-accounts",
                      "Eksik alt hesaplari otomatik olustur"
                    )}
                  </button>
                ) : (
                  <span className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900">
                    {l(
                      "Need gl.account.upsert permission for auto setup",
                      "Otomatik kurulum icin gl.account.upsert yetkisi gerekli"
                    )}
                  </span>
                )}
                <Link
                  to="/app/ayarlar/hesap-plani-ayarlari"
                  className="rounded border border-cyan-300 bg-white px-2.5 py-1 font-semibold text-cyan-900"
                >
                  {l("Open GL setup", "GL ayarlarini ac")}
                </Link>
              </div>
            </div>
          ) : null}
          <form
            id="shareholder-form-block"
            onSubmit={handleShareholderSubmit}
            className="grid gap-2 md:grid-cols-4"
          >
            <select
              value={shareholderForm.legalEntityId}
              onChange={(event) => {
                const nextLegalEntityId = event.target.value;
                const selectedEntity = legalEntities.find(
                  (row) => String(row.id) === String(nextLegalEntityId)
                );
                const defaultCurrency = String(
                  selectedEntity?.functional_currency_code || ""
                ).toUpperCase();
                setShareholderForm((prev) => ({
                  ...prev,
                  legalEntityId: nextLegalEntityId,
                  capitalSubAccountId: "",
                  commitmentDebitSubAccountId: "",
                  currencyCode: defaultCurrency || prev.currencyCode,
                }));
              }}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select legal entity", "Istirak / bagli ortak secin")}</option>
              {legalEntities.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.code} - {row.name}
                </option>
              ))}
            </select>
            <input
              value={shareholderForm.code}
              onChange={(event) =>
                setShareholderForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Shareholder code", "Ortak kodu")}
              required
            />
            <input
              value={shareholderForm.name}
              onChange={(event) =>
                setShareholderForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("Shareholder name", "Ortak adi")}
              required
            />

            <select
              value={shareholderForm.shareholderType}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  shareholderType: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {SHAREHOLDER_TYPES.map((type) => (
                <option key={type} value={type}>
                  {getShareholderTypeLabel(type, l)}
                </option>
              ))}
            </select>
            <input
              value={shareholderForm.taxId}
              onChange={(event) =>
                setShareholderForm((prev) => ({ ...prev, taxId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Tax ID (optional)", "Vergi No (opsiyonel)")}
            />
            <div className="rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2 text-xs text-indigo-900">
              {l(
                "Ownership % is auto-calculated from committed capital for all shareholders in this legal entity.",
                "Sahiplik %, bu legal entity icindeki tum ortaklar icin taahhut edilen sermayeye gore otomatik hesaplanir."
              )}
            </div>
            <select
              value={shareholderForm.capitalSubAccountId}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  capitalSubAccountId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              disabled={!canReadAccounts || !hasShareholderParentMapping}
            >
              <option value="">
                {canReadAccounts
                  ? !hasShareholderParentMapping
                    ? l(
                        "Save parent mapping first",
                        "Once parent eslesmesini kaydedin"
                      )
                    : availableCapitalCreditShareholderAccounts.length > 0
                      ? l(
                          `Capital credit sub-account (child of ${selectedCapitalCreditParentAccount?.code || "-"})`,
                          `${selectedCapitalCreditParentAccount?.code || "-"} altinda sermaye alacak alt hesap`
                        )
                      : l(
                          "No available mapped capital credit sub-account found",
                          "Eslenmis kullanilabilir sermaye alacak alt hesap bulunamadi"
                        )
                  : l(
                      "Need gl.account.read",
                      "gl.account.read yetkisi gerekli"
                    )}
              </option>
              {availableCapitalCreditShareholderAccounts.map((account) => (
                <option key={account.id} value={account.id}>
                  {account.code} - {account.name}
                </option>
              ))}
            </select>
            <select
              value={shareholderForm.commitmentDebitSubAccountId}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  commitmentDebitSubAccountId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              disabled={!canReadAccounts || !hasShareholderParentMapping}
            >
              <option value="">
                {canReadAccounts
                  ? !hasShareholderParentMapping
                    ? l(
                        "Save parent mapping first",
                        "Once parent eslesmesini kaydedin"
                      )
                    : availableCommitmentDebitShareholderAccounts.length > 0
                      ? l(
                          `Commitment debit sub-account (child of ${selectedCommitmentDebitParentAccount?.code || "-"})`,
                          `${selectedCommitmentDebitParentAccount?.code || "-"} altinda taahhut borc alt hesap`
                        )
                      : l(
                          "No available mapped commitment debit sub-account found",
                          "Eslenmis kullanilabilir taahhut borc alt hesap bulunamadi"
                        )
                  : l(
                      "Need gl.account.read",
                      "gl.account.read yetkisi gerekli"
                    )}
              </option>
              {availableCommitmentDebitShareholderAccounts.map((account) => (
                <option key={account.id} value={account.id}>
                  {account.code} - {account.name}
                </option>
              ))}
            </select>
            <select
              value={shareholderForm.currencyCode}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  currencyCode: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select currency", "Para birimi secin")}</option>
              {currencySelectOptions.map((option) => (
                <option key={option.code} value={option.code}>
                  {option.label}
                </option>
              ))}
            </select>
            <input
              type="date"
              value={shareholderForm.commitmentDate}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  commitmentDate: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              title={l("Commitment date", "Taahhut tarihi")}
              required
            />
            <input
              type="number"
              min={0}
              step="0.01"
              value={shareholderForm.committedCapital}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  committedCapital: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l(
                "Commitment increase (this entry)",
                "Taahhut artisi (bu kayit)"
              )}
            />
            <div className="rounded-lg border border-sky-200 bg-sky-50 px-3 py-2 text-xs text-sky-900 md:col-span-2">
              {existingShareholderForForm
                ? l(
                    `Existing committed total: ${formatAmount(
                      formExistingCommittedCapitalAmount
                    )}. This entry increase: ${formatAmount(
                      formCommitmentIncreaseAmount
                    )}. New committed total: ${formatAmount(
                      formProjectedCommittedCapitalAmount
                    )}.`,
                    `Mevcut taahhut toplami: ${formatAmount(
                      formExistingCommittedCapitalAmount
                    )}. Bu kayit artisi: ${formatAmount(
                      formCommitmentIncreaseAmount
                    )}. Yeni taahhut toplami: ${formatAmount(
                      formProjectedCommittedCapitalAmount
                    )}.`
                  )
                : l(
                    "Enter only the increase amount. For a new shareholder, this becomes the initial commitment.",
                    "Sadece artis tutarini girin. Yeni ortakta bu tutar ilk taahhut olur."
                  )}
            </div>
            <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
              {l(
                "Paid capital is auto-calculated from posted journals that credit the mapped commitment debit sub-account (e.g. 501.xx).",
                "Odenen sermaye, eslenen taahhut borc alt hesabini (orn. 501.xx) alacaklandiran post edilmis yevmiye kayitlarindan otomatik hesaplanir."
              )}
            </div>
            <select
              value={shareholderForm.status}
              onChange={(event) =>
                setShareholderForm((prev) => ({
                  ...prev,
                  status: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {SHAREHOLDER_STATUSES.map((status) => (
                <option key={status} value={status}>
                  {getShareholderStatusLabel(status, l)}
                </option>
              ))}
            </select>
            <input
              value={shareholderForm.notes}
              onChange={(event) =>
                setShareholderForm((prev) => ({ ...prev, notes: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("Notes (optional)", "Notlar (opsiyonel)")}
            />
            <button
              type="submit"
              disabled={saving === "shareholder" || !canUpsertShareholder}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "shareholder"
                ? l("Saving...", "Kaydediliyor...")
                : l("Save Shareholder", "Ortagi Kaydet")}
            </button>
          </form>

          <div className="mt-3 flex flex-wrap items-center gap-2 text-xs">
            <button
              type="button"
              onClick={() =>
                updateQueueForSelectedEntity(
                  eligibleShareholdersForQueue.map((row) => toNumber(row.id)).filter(Boolean)
                )
              }
              disabled={eligibleShareholdersForQueue.length === 0}
              className="rounded border border-slate-300 bg-white px-2.5 py-1 font-semibold text-slate-700 disabled:opacity-50"
            >
              {l(
                `Queue all eligible (${eligibleShareholdersForQueue.length})`,
                `Uygunlarin hepsini kuyruga ekle (${eligibleShareholdersForQueue.length})`
              )}
            </button>
            {eligibleQueueCurrencyGroups.map((group) => (
              <button
                key={`eligible-currency-${group.currencyCode}`}
                type="button"
                onClick={() => updateQueueForSelectedEntity(group.shareholderIds)}
                className="rounded border border-slate-300 bg-white px-2.5 py-1 font-semibold text-slate-700"
              >
                {l(
                  `Queue ${group.currencyCode} (${group.count})`,
                  `${group.currencyCode} (${group.count}) kuyruga ekle`
                )}
              </button>
            ))}
          </div>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Entity", "Birim")}</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Type", "Tur")}</th>
                  <th className="px-3 py-2">{l("Ownership %", "Sahiplik %")}</th>
                  <th className="px-3 py-2">
                    {l("Capital Sub-Account", "Sermaye Alt Hesap")}
                  </th>
                  <th className="px-3 py-2">
                    {l(
                      "Commitment Debit Sub-Account",
                      "Taahhut Borc Alt Hesap"
                    )}
                  </th>
                  <th className="px-3 py-2">{l("Committed", "Taahhut")}</th>
                  <th className="px-3 py-2">{l("Paid", "Odenen")}</th>
                  <th className="px-3 py-2">{l("Currency", "Para birimi")}</th>
                  <th className="px-3 py-2">{l("Status", "Durum")}</th>
                  <th className="px-3 py-2">{l("Queue", "Kuyruk")}</th>
                </tr>
              </thead>
              <tbody>
                {visibleShareholders.map((row) => {
                  const shareholderId = toNumber(row.id);
                  const isQueued = selectedEntityCommitmentQueueIdSet.has(shareholderId);
                  const hasMappedSubAccounts =
                    Boolean(toNumber(row.capital_sub_account_id)) &&
                    Boolean(toNumber(row.commitment_debit_sub_account_id));
                  return (
                    <tr key={row.id} className="border-t border-slate-100">
                      <td className="px-3 py-2">{row.id}</td>
                      <td className="px-3 py-2">{row.legal_entity_id}</td>
                      <td className="px-3 py-2">{row.code}</td>
                      <td className="px-3 py-2">{row.name}</td>
                      <td className="px-3 py-2">
                        {getShareholderTypeLabel(row.shareholder_type, l)}
                      </td>
                      <td className="px-3 py-2">
                        {row.ownership_pct === null || row.ownership_pct === undefined
                          ? "-"
                          : Number(row.ownership_pct).toFixed(4)}
                      </td>
                      <td className="px-3 py-2">
                        {row.capital_sub_account_code
                          ? row.capital_sub_account_name
                            ? `${row.capital_sub_account_code} - ${row.capital_sub_account_name}`
                            : row.capital_sub_account_code
                          : "-"}
                      </td>
                      <td className="px-3 py-2">
                        {row.commitment_debit_sub_account_code
                          ? row.commitment_debit_sub_account_name
                            ? `${row.commitment_debit_sub_account_code} - ${row.commitment_debit_sub_account_name}`
                            : row.commitment_debit_sub_account_code
                          : "-"}
                      </td>
                      <td className="px-3 py-2">
                        {Number(row.committed_capital || 0).toLocaleString(undefined, {
                          minimumFractionDigits: 2,
                          maximumFractionDigits: 2,
                        })}
                      </td>
                      <td className="px-3 py-2">
                        {Number(row.paid_capital || 0).toLocaleString(undefined, {
                          minimumFractionDigits: 2,
                          maximumFractionDigits: 2,
                        })}
                      </td>
                      <td className="px-3 py-2">{row.currency_code}</td>
                      <td className="px-3 py-2">
                        {getShareholderStatusLabel(row.status, l)}
                      </td>
                      <td className="px-3 py-2">
                        <button
                          type="button"
                          onClick={() =>
                            handleQueueShareholderToggle(shareholderId, !isQueued)
                          }
                          disabled={!shareholderId || !hasMappedSubAccounts}
                          className={`rounded border px-2 py-1 text-[11px] font-semibold disabled:opacity-50 ${
                            isQueued
                              ? "border-rose-300 bg-rose-50 text-rose-800"
                              : "border-slate-300 bg-white text-slate-700"
                          }`}
                        >
                          {isQueued
                            ? l("Remove", "Kuyruktan cikar")
                            : l("Add", "Kuyruga ekle")}
                        </button>
                      </td>
                    </tr>
                  );
                })}
                {visibleShareholders.length === 0 && !loading && (
                  <tr>
                    <td colSpan={13} className="px-3 py-3 text-slate-500">
                      {l("No shareholders found.", "Ortak bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Fiscal Calendars and Periods", "Mali Takvimler ve Donemler")}
          </h2>

          <form onSubmit={handleFiscalCalendarSubmit} className="grid gap-2 md:grid-cols-5">
            <input
              value={calendarForm.code}
              onChange={(event) =>
                setCalendarForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Calendar code", "Takvim kodu")}
              required
            />
            <input
              value={calendarForm.name}
              onChange={(event) =>
                setCalendarForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("Calendar name", "Takvim adi")}
              required
            />
            <input
              type="number"
              min={1}
              max={12}
              value={calendarForm.yearStartMonth}
              onChange={(event) =>
                setCalendarForm((prev) => ({
                  ...prev,
                  yearStartMonth: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Start month", "Baslangic ayi")}
              required
            />
            <input
              type="number"
              min={1}
              max={31}
              value={calendarForm.yearStartDay}
              onChange={(event) =>
                setCalendarForm((prev) => ({
                  ...prev,
                  yearStartDay: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Start day", "Baslangic gunu")}
              required
            />
            <button
              type="submit"
              disabled={saving === "calendar" || !canUpsertFiscalCalendar}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60 md:col-span-5"
            >
              {saving === "calendar" ? l("Saving...", "Kaydediliyor...") : l("Save Calendar", "Takvimi Kaydet")}
            </button>
          </form>

          <form onSubmit={handleGeneratePeriods} className="mt-3 grid gap-2 md:grid-cols-4">
            <select
              value={periodForm.calendarId}
              onChange={(event) =>
                setPeriodForm((prev) => ({ ...prev, calendarId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select calendar", "Takvim secin")}</option>
              {calendars.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.code} - {row.name}
                </option>
              ))}
            </select>
            <input
              type="number"
              min={2000}
              value={periodForm.fiscalYear}
              onChange={(event) =>
                setPeriodForm((prev) => ({ ...prev, fiscalYear: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Fiscal year", "Mali yil")}
            />
            <button
              type="button"
              onClick={() => loadPeriods(periodForm.calendarId, periodForm.fiscalYear)}
              disabled={!canReadFiscalPeriods}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
            >
              {l("Reload Periods", "Donemleri Yeniden Yukle")}
            </button>
            <button
              type="submit"
              disabled={saving === "periods" || !canGenerateFiscalPeriods}
              className="rounded-lg bg-cyan-700 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "periods" ? l("Generating...", "Olusturuluyor...") : l("Generate 12 Periods", "12 Donem Olustur")}
            </button>
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Year", "Yil")}</th>
                  <th className="px-3 py-2">{l("Period", "Donem")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Start", "Baslangic")}</th>
                  <th className="px-3 py-2">{l("End", "Bitis")}</th>
                </tr>
              </thead>
              <tbody>
                {(periods || []).map((row) => (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.id}</td>
                    <td className="px-3 py-2">{row.fiscal_year}</td>
                    <td className="px-3 py-2">{row.period_no}</td>
                    <td className="px-3 py-2">{row.period_name}</td>
                    <td className="px-3 py-2">{row.start_date}</td>
                    <td className="px-3 py-2">{row.end_date}</td>
                  </tr>
                ))}
                {periods.length === 0 && !loading && (
                  <tr>
                    <td colSpan={6} className="px-3 py-3 text-slate-500">
                      {l("No periods found for selected filters.", "Secilen filtreler icin donem bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      {commitmentIncreaseModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 px-4">
          <div className="w-full max-w-xl rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h3 className="text-base font-semibold text-slate-900">
              {l(
                "Sermaye Taahhut Arttirimi",
                "Sermaye Taahhut Arttirimi"
              )}
            </h3>
            <p className="mt-2 text-sm text-slate-700">
              {l(
                "Select existing shareholder and enter only increase amount. Accounts are auto-used from shareholder mapping.",
                "Mevcut ortagi secin ve sadece artis tutarini girin. Hesaplar ortak eslesmesinden otomatik kullanilir."
              )}
            </p>

            <form
              onSubmit={handleCommitmentIncreaseSubmit}
              className="mt-3 grid gap-2 md:grid-cols-2"
            >
              <label className="block">
                <span className="mb-1 block text-[11px] font-semibold text-slate-600">
                  {l("Shareholder", "Ortak")}
                </span>
                <select
                  value={commitmentIncreaseForm.shareholderId}
                  onChange={(event) =>
                    setCommitmentIncreaseForm((prev) => ({
                      ...prev,
                      shareholderId: event.target.value,
                    }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                  required
                >
                  <option value="">
                    {l("Select shareholder", "Ortak secin")}
                  </option>
                  {eligibleShareholdersForCommitmentIncrease.map((row) => (
                    <option key={row.id} value={row.id}>
                      {row.code} - {row.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="block">
                <span className="mb-1 block text-[11px] font-semibold text-slate-600">
                  {l("Commitment date", "Taahhut tarihi")}
                </span>
                <input
                  type="date"
                  value={commitmentIncreaseForm.commitmentDate}
                  onChange={(event) =>
                    setCommitmentIncreaseForm((prev) => ({
                      ...prev,
                      commitmentDate: event.target.value,
                    }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                  required
                />
              </label>
              <label className="block">
                <span className="mb-1 block text-[11px] font-semibold text-slate-600">
                  {l("Increase amount", "Artis tutari")}
                </span>
                <input
                  type="number"
                  min={0.01}
                  step="0.01"
                  value={commitmentIncreaseForm.increaseAmount}
                  onChange={(event) =>
                    setCommitmentIncreaseForm((prev) => ({
                      ...prev,
                      increaseAmount: event.target.value,
                    }))
                  }
                  className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                  required
                />
              </label>
              <div className="rounded border border-sky-200 bg-sky-50 px-2 py-2 text-xs text-sky-900">
                {l(
                  `Current committed total: ${formatAmount(
                    commitmentIncreaseCurrentCommittedCapital
                  )}. New projected total: ${formatAmount(
                    commitmentIncreaseProjectedCommittedCapital
                  )}.`,
                  `Mevcut taahhut toplami: ${formatAmount(
                    commitmentIncreaseCurrentCommittedCapital
                  )}. Yeni taahhut toplami: ${formatAmount(
                    commitmentIncreaseProjectedCommittedCapital
                  )}.`
                )}
              </div>
            </form>

            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
              <div>
                {l("Legal Entity", "Istirak / bagli ortak")}:{" "}
                <span className="font-mono">
                  {selectedShareholderLegalEntity
                    ? `${selectedShareholderLegalEntity.code} - ${selectedShareholderLegalEntity.name}`
                    : "-"}
                </span>
              </div>
              <div>
                {l("Capital sub-account", "Sermaye alt hesap")}:{" "}
                <span className="font-mono">
                  {selectedCommitmentIncreaseShareholder?.capital_sub_account_code
                    ? selectedCommitmentIncreaseShareholder.capital_sub_account_name
                      ? `${selectedCommitmentIncreaseShareholder.capital_sub_account_code} - ${selectedCommitmentIncreaseShareholder.capital_sub_account_name}`
                      : selectedCommitmentIncreaseShareholder.capital_sub_account_code
                    : "-"}
                </span>
              </div>
              <div>
                {l("Commitment debit sub-account", "Taahhut borc alt hesap")}:{" "}
                <span className="font-mono">
                  {selectedCommitmentIncreaseShareholder?.commitment_debit_sub_account_code
                    ? selectedCommitmentIncreaseShareholder.commitment_debit_sub_account_name
                      ? `${selectedCommitmentIncreaseShareholder.commitment_debit_sub_account_code} - ${selectedCommitmentIncreaseShareholder.commitment_debit_sub_account_name}`
                      : selectedCommitmentIncreaseShareholder.commitment_debit_sub_account_code
                    : "-"}
                </span>
              </div>
              <div>
                {l("Currency", "Para birimi")}:{" "}
                <span className="font-mono">
                  {selectedCommitmentIncreaseShareholder?.currency_code || "-"}
                </span>
              </div>
            </div>

            <div className="mt-4 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setCommitmentIncreaseModalOpen(false)}
                disabled={saving === "shareholderIncrease"}
                className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
              >
                {l("Cancel", "Iptal")}
              </button>
              <button
                type="button"
                onClick={handleCommitmentIncreaseSubmit}
                disabled={
                  saving === "shareholderIncrease" ||
                  !selectedCommitmentIncreaseShareholder
                }
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                {saving === "shareholderIncrease"
                  ? l("Saving...", "Kaydediliyor...")
                  : l("Kaydet ve kuyruga ekle", "Kaydet ve kuyruga ekle")}
              </button>
            </div>
          </div>
        </div>
      )}

      {batchCommitmentModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 px-4">
          <div className="w-full max-w-5xl rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h3 className="text-base font-semibold text-slate-900">
              {l(
                "Create one batch commitment journal",
                "Tek bir toplu taahhut yevmiyesi olustur"
              )}
            </h3>
            <p className="mt-2 text-sm text-slate-700">
              {l(
                "All queued shareholders will be posted into one draft journal entry.",
                "Kuyruktaki tum ortaklar tek bir taslak yevmiye fisinde olusturulacak."
              )}
            </p>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
              <div>
                {l("Legal Entity", "Istirak / bagli ortak")}:{" "}
                <span className="font-mono">
                  {selectedShareholderLegalEntity
                    ? `${selectedShareholderLegalEntity.code} - ${selectedShareholderLegalEntity.name}`
                    : "-"}
                </span>
              </div>
              <div>
                {l("Queued shareholders", "Kuyruktaki ortaklar")}:{" "}
                <span className="font-mono">
                  {pendingBatchCommitmentShareholders.length}
                </span>
              </div>
              <div className="mt-2 flex flex-wrap items-end gap-2">
                <label className="block min-w-[220px] flex-1">
                  <span className="mb-1 block text-[11px] font-semibold text-slate-600">
                    {l("Commitment date", "Taahhut tarihi")}
                  </span>
                  <input
                    type="date"
                    value={batchCommitmentDate}
                    onChange={(event) => setBatchCommitmentDate(event.target.value)}
                    className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                    required
                  />
                </label>
                <button
                  type="button"
                  onClick={handlePreviewBatchCommitmentJournal}
                  disabled={
                    batchPreviewLoading ||
                    batchCommitmentSaving ||
                    shareholderCommitmentModuleNotReady
                  }
                  className="rounded border border-slate-300 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 disabled:opacity-60"
                >
                  {batchPreviewLoading
                    ? l("Loading preview...", "Onizleme yukleniyor...")
                    : l("Refresh preview", "Onizlemeyi yenile")}
                </button>
              </div>
            </div>

            {batchPreviewBlockingErrors.length > 0 ? (
              <div className="mt-3 rounded-lg border border-rose-200 bg-rose-50 p-3 text-xs text-rose-800">
                <div className="font-semibold">
                  {l(
                    "Blocking validation errors",
                    "Engelleyici dogrulama hatalari"
                  )}
                </div>
                <ul className="mt-1 list-disc space-y-0.5 pl-4">
                  {batchPreviewBlockingErrors.map((errorItem, index) => (
                    <li key={`${errorItem.code || "ERR"}-${index}`}>
                      {errorItem.message || errorItem.code || "-"}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}

            {batchPreviewWarnings.length > 0 ? (
              <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
                <div className="font-semibold">{l("Warnings", "Uyarilar")}</div>
                <ul className="mt-1 list-disc space-y-0.5 pl-4">
                  {batchPreviewWarnings.map((warningItem, index) => (
                    <li key={`${warningItem.code || "WARN"}-${index}`}>
                      {warningItem.message || warningItem.code || "-"}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}

            <div className="mt-3 rounded-lg border border-slate-200 bg-white p-2">
              <div className="mb-2 text-xs font-semibold text-slate-700">
                {l("Preview rows", "Onizleme satirlari")}
              </div>
              <div className="max-h-64 overflow-auto">
                <table className="min-w-full text-xs">
                  <thead className="bg-slate-50 text-left text-slate-600">
                    <tr>
                      <th className="px-2 py-1">{l("Code / Name", "Kod / Ad")}</th>
                      <th className="px-2 py-1">{l("Currency", "Para birimi")}</th>
                      <th className="px-2 py-1">{l("Committed", "Taahhut edilen")}</th>
                      <th className="px-2 py-1">
                        {l("Already journaled", "Daha once fislenen")}
                      </th>
                      <th className="px-2 py-1">{l("Delta", "Bu islem delta")}</th>
                      <th className="px-2 py-1">{l("Debit account", "Borc hesap")}</th>
                      <th className="px-2 py-1">{l("Credit account", "Alacak hesap")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {batchPreviewIncludedRows.map((row) => (
                      <tr
                        key={`preview-row-${row.shareholder_id}`}
                        className="border-t border-slate-100"
                      >
                        <td className="px-2 py-1">
                          {row.code || "-"} - {row.name || "-"}
                        </td>
                        <td className="px-2 py-1">{row.currency_code || "-"}</td>
                        <td className="px-2 py-1">
                          {Number(row.committed_capital || 0).toLocaleString(undefined, {
                            minimumFractionDigits: 2,
                            maximumFractionDigits: 2,
                          })}
                        </td>
                        <td className="px-2 py-1">
                          {Number(row.already_journaled_amount || 0).toLocaleString(
                            undefined,
                            {
                              minimumFractionDigits: 2,
                              maximumFractionDigits: 2,
                            }
                          )}
                        </td>
                        <td className="px-2 py-1">
                          {Number(row.delta_amount || 0).toLocaleString(undefined, {
                            minimumFractionDigits: 2,
                            maximumFractionDigits: 2,
                          })}
                        </td>
                        <td className="px-2 py-1">
                          {row.debit_account_code
                            ? `${row.debit_account_code} - ${row.debit_account_name || ""}`
                            : "-"}
                        </td>
                        <td className="px-2 py-1">
                          {row.credit_account_code
                            ? `${row.credit_account_code} - ${row.credit_account_name || ""}`
                            : "-"}
                        </td>
                      </tr>
                    ))}
                    {batchPreviewIncludedRows.length === 0 ? (
                      <tr>
                        <td
                          colSpan={7}
                          className="px-2 py-2 text-center text-slate-500"
                        >
                          {batchPreviewLoading
                            ? l("Loading preview...", "Onizleme yukleniyor...")
                            : l("No includable rows in preview.", "Onizlemede dahil satir yok.")}
                        </td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
              <div className="mt-2 grid gap-2 rounded border border-slate-200 bg-slate-50 p-2 text-xs text-slate-700 md:grid-cols-3">
                <div>
                  {l("Total debit", "Toplam borc")}:{" "}
                  <span className="font-mono">
                    {Number(batchPreviewData?.totals?.total_debit || 0).toLocaleString(
                      undefined,
                      {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      }
                    )}
                  </span>
                </div>
                <div>
                  {l("Total credit", "Toplam alacak")}:{" "}
                  <span className="font-mono">
                    {Number(batchPreviewData?.totals?.total_credit || 0).toLocaleString(
                      undefined,
                      {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      }
                    )}
                  </span>
                </div>
                <div>
                  {l("Currency", "Para birimi")}:{" "}
                  <span className="font-mono">
                    {batchPreviewData?.totals?.currency_code || "-"}
                  </span>
                </div>
              </div>
            </div>

            {batchPreviewSkippedRows.length > 0 ? (
              <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900">
                <div className="font-semibold">{l("Skipped rows", "Atlananlar")}</div>
                <div className="mt-1 max-h-32 space-y-1 overflow-auto">
                  {batchPreviewSkippedRows.map((row) => (
                    <div key={`skipped-${row.shareholder_id}`}>
                      {row.code || row.shareholder_id}: {row.skipped_reason || "-"}
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            <div className="mt-4 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => {
                  setBatchCommitmentModalOpen(false);
                  setBatchPreviewData(null);
                }}
                disabled={batchCommitmentSaving}
                className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
              >
                {l("Cancel", "Iptal")}
              </button>
              <button
                type="button"
                onClick={handleCreateBatchCommitmentJournal}
                disabled={
                  batchCommitmentSaving ||
                  batchPreviewLoading ||
                  batchPreviewHasBlockingErrors ||
                  batchPreviewIncludedRows.length === 0 ||
                  shareholderCommitmentModuleNotReady
                }
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                {batchCommitmentSaving
                  ? l("Creating...", "Olusturuluyor...")
                  : l("Create batch journal", "Toplu fis olustur")}
              </button>
            </div>
          </div>
        </div>
      )}

      {autoSubAccountSetupModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 px-4">
          <div className="w-full max-w-lg rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h3 className="text-base font-semibold text-slate-900">
              {l(
                "Auto setup for missing shareholder sub-accounts",
                "Eksik ortak alt hesaplari icin otomatik kurulum"
              )}
            </h3>
            <p className="mt-2 text-sm text-slate-700">
              {l(
                "This action will create missing shareholder sub-accounts under mapped parent accounts and pre-fill the shareholder form.",
                "Bu islem eslenmis parent hesaplar altinda eksik ortak alt hesaplarini olusturur ve ortak formunda otomatik secer."
              )}
            </p>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
              <div>
                {l("Legal Entity", "Istirak / bagli ortak")}:{" "}
                <span className="font-mono">
                  {selectedShareholderLegalEntity
                    ? `${selectedShareholderLegalEntity.code} - ${selectedShareholderLegalEntity.name}`
                    : "-"}
                </span>
              </div>
              <div>
                {l("Shareholder", "Ortak")}:{" "}
                <span className="font-mono">
                  {String(shareholderForm.code || "").trim() || "-"} -{" "}
                  {String(shareholderForm.name || "").trim() || "-"}
                </span>
              </div>
              <div className="mt-1">
                {hasMissingCreditEquitySubAccount ? (
                  <div>
                    {l(
                      `Will create: CREDIT leaf sub-account under ${selectedCapitalCreditParentAccount?.code || "-"}`,
                      `${selectedCapitalCreditParentAccount?.code || "-"} altinda CREDIT leaf alt hesap olusturulacak`
                    )}
                  </div>
                ) : null}
                {hasMissingDebitEquitySubAccount ? (
                  <div>
                    {l(
                      `Will create: DEBIT leaf sub-account under ${selectedCommitmentDebitParentAccount?.code || "-"}`,
                      `${selectedCommitmentDebitParentAccount?.code || "-"} altinda DEBIT leaf alt hesap olusturulacak`
                    )}
                  </div>
                ) : null}
              </div>
            </div>

            <div className="mt-4 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setAutoSubAccountSetupModalOpen(false)}
                disabled={autoSubAccountSetupSaving}
                className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 disabled:opacity-60"
              >
                {l("Cancel", "Iptal")}
              </button>
              <button
                type="button"
                onClick={handleAutoCreateMissingShareholderSubAccounts}
                disabled={autoSubAccountSetupSaving}
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                {autoSubAccountSetupSaving
                  ? l("Creating...", "Olusturuluyor...")
                  : l("Confirm and Create", "Onayla ve Olustur")}
              </button>
            </div>
          </div>
        </div>
      )}

      {shareholderJournalModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 px-4">
          <div className="w-full max-w-lg rounded-xl border border-slate-200 bg-white p-4 shadow-xl">
            <h3 className="text-base font-semibold text-slate-900">
              {shareholderJournalModal.title}
            </h3>
            <p className="mt-2 text-sm text-slate-700">
              {shareholderJournalModal.message}
            </p>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs text-slate-700">
              <div>
                {l("Journal No", "Fis No")}:{" "}
                <span className="font-mono">{shareholderJournalModal.journalNo}</span>
              </div>
              <div>
                {l("Journal ID", "Fis ID")}:{" "}
                <span className="font-mono">
                  {shareholderJournalModal.journalEntryId}
                </span>
              </div>
              <div>
                {l("Book", "Defter")}:{" "}
                <span className="font-mono">{shareholderJournalModal.bookCode}</span>
              </div>
              <div>
                {l("Fiscal Period ID", "Mali Donem ID")}:{" "}
                <span className="font-mono">
                  {shareholderJournalModal.fiscalPeriodId}
                </span>
              </div>
            </div>
            <div className="mt-4 flex justify-end">
              <button
                type="button"
                onClick={() => setShareholderJournalModal(null)}
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white"
              >
                OK
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

