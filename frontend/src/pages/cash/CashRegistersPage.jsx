import { useEffect, useMemo, useState } from "react";
import {
  listCashRegisters,
  setCashRegisterStatus,
  upsertCashRegister,
} from "../../api/cashAdmin.js";
import { listAccounts } from "../../api/glAdmin.js";
import {
  listCurrencies,
  listLegalEntities,
  listOperatingUnits,
} from "../../api/orgAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useWorkingContextDefaults } from "../../context/useWorkingContextDefaults.js";
import { useToastMessage } from "../../hooks/useToastMessage.js";
import { useI18n } from "../../i18n/useI18n.js";
import CashControlModeBanner from "./CashControlModeBanner.jsx";

const REGISTER_TYPES = ["VAULT", "DRAWER", "TILL"];
const SESSION_MODES = ["REQUIRED", "OPTIONAL", "NONE"];
const REGISTER_STATUSES = ["ACTIVE", "INACTIVE"];

const EMPTY_FORM = {
  id: "",
  code: "",
  name: "",
  registerType: "DRAWER",
  sessionMode: "REQUIRED",
  legalEntityId: "",
  operatingUnitId: "",
  accountId: "",
  currencyCode: "",
  allowNegative: false,
  varianceGainAccountId: "",
  varianceLossAccountId: "",
  maxTxnAmount: "",
  requiresApprovalOverAmount: "",
  status: "ACTIVE",
};

const CASH_REGISTER_CONTEXT_MAPPINGS = [
  { stateKey: "legalEntityId" },
  {
    stateKey: "operatingUnitId",
    allowContextValue: (_contextValue, previousState, workingContext) => {
      const selectedLegalEntityId = String(previousState?.legalEntityId || "").trim();
      const contextLegalEntityId = String(workingContext?.legalEntityId || "").trim();
      return !selectedLegalEntityId || selectedLegalEntityId === contextLegalEntityId;
    },
  },
];

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toOptionalAmount(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function parseDbBoolean(value) {
  return value === true || value === 1 || value === "1";
}

function mapRowToForm(row) {
  return {
    id: String(row?.id || ""),
    code: String(row?.code || ""),
    name: String(row?.name || ""),
    registerType: String(row?.register_type || "DRAWER").toUpperCase(),
    sessionMode: String(row?.session_mode || "REQUIRED").toUpperCase(),
    legalEntityId: String(row?.legal_entity_id || ""),
    operatingUnitId: String(row?.operating_unit_id || ""),
    accountId: String(row?.account_id || ""),
    currencyCode: String(row?.currency_code || "").toUpperCase(),
    allowNegative: parseDbBoolean(row?.allow_negative),
    varianceGainAccountId: String(row?.variance_gain_account_id || ""),
    varianceLossAccountId: String(row?.variance_loss_account_id || ""),
    maxTxnAmount:
      row?.max_txn_amount === null || row?.max_txn_amount === undefined
        ? ""
        : String(row.max_txn_amount),
    requiresApprovalOverAmount:
      row?.requires_approval_over_amount === null ||
      row?.requires_approval_over_amount === undefined
        ? ""
        : String(row.requires_approval_over_amount),
    status: String(row?.status || "ACTIVE").toUpperCase(),
  };
}

export default function CashRegistersPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const canReadRegisters = hasPermission("cash.register.read");
  const canUpsertRegisters = hasPermission("cash.register.upsert");
  const canReadOrgTree = hasPermission("org.tree.read");
  const canReadAccounts = hasPermission("gl.account.read");

  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [updatingStatusId, setUpdatingStatusId] = useState(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useToastMessage("", { toastType: "success" });
  const [lookupWarning, setLookupWarning] = useState("");

  const [rows, setRows] = useState([]);
  const [legalEntities, setLegalEntities] = useState([]);
  const [operatingUnits, setOperatingUnits] = useState([]);
  const [accounts, setAccounts] = useState([]);
  const [currencies, setCurrencies] = useState([]);

  const [form, setForm] = useState(EMPTY_FORM);

  useWorkingContextDefaults(setForm, CASH_REGISTER_CONTEXT_MAPPINGS, [
    form.legalEntityId,
    form.operatingUnitId,
  ]);

  const selectedLegalEntityId = toPositiveInt(form.legalEntityId);

  const legalEntityOptions = useMemo(
    () =>
      [...legalEntities].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [legalEntities]
  );

  const operatingUnitOptions = useMemo(() => {
    const filtered = operatingUnits.filter((row) => {
      if (!selectedLegalEntityId) {
        return true;
      }
      return toPositiveInt(row?.legal_entity_id) === selectedLegalEntityId;
    });
    return [...filtered].sort((a, b) =>
      String(a?.code || "").localeCompare(String(b?.code || ""))
    );
  }, [operatingUnits, selectedLegalEntityId]);

  const accountOptions = useMemo(() => {
    const filtered = accounts.filter((row) => {
      if (!parseDbBoolean(row?.is_active) || !parseDbBoolean(row?.allow_posting)) {
        return false;
      }
      if (!selectedLegalEntityId) {
        return true;
      }
      return toPositiveInt(row?.legal_entity_id) === selectedLegalEntityId;
    });
    return [...filtered].sort((a, b) =>
      String(a?.code || "").localeCompare(String(b?.code || ""))
    );
  }, [accounts, selectedLegalEntityId]);

  const currencyOptions = useMemo(
    () =>
      [...currencies].sort((a, b) =>
        String(a?.code || "").localeCompare(String(b?.code || ""))
      ),
    [currencies]
  );

  useEffect(() => {
    if (form.id) {
      return;
    }
    if (!form.legalEntityId && legalEntityOptions.length > 0) {
      setForm((prev) => ({
        ...prev,
        legalEntityId: String(legalEntityOptions[0].id || ""),
      }));
    }
  }, [form.id, form.legalEntityId, legalEntityOptions]);

  useEffect(() => {
    if (form.id) {
      return;
    }
    if (!form.currencyCode && currencyOptions.length > 0) {
      setForm((prev) => ({
        ...prev,
        currencyCode: String(currencyOptions[0].code || "").toUpperCase(),
      }));
    }
  }, [currencyOptions, form.currencyCode, form.id]);

  async function loadRegisters() {
    if (!canReadRegisters) {
      setRows([]);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const response = await listCashRegisters({ limit: 200, offset: 0 });
      setRows(response?.rows || []);
    } catch (err) {
      setError(
        err?.response?.data?.message || t("cashRegisters.errors.loadRegisters")
      );
    } finally {
      setLoading(false);
    }
  }

  async function loadLookups() {
    if (!canUpsertRegisters) {
      setLegalEntities([]);
      setOperatingUnits([]);
      setAccounts([]);
      setCurrencies([]);
      setLookupWarning("");
      return;
    }

    const warnings = [];

    if (canReadOrgTree) {
      try {
        const [legalEntityRes, operatingUnitRes, currencyRes] = await Promise.all([
          listLegalEntities(),
          listOperatingUnits(),
          listCurrencies(),
        ]);
        setLegalEntities(legalEntityRes?.rows || []);
        setOperatingUnits(operatingUnitRes?.rows || []);
        setCurrencies(currencyRes?.rows || []);
      } catch (err) {
        setLegalEntities([]);
        setOperatingUnits([]);
        setCurrencies([]);
        warnings.push(err?.response?.data?.message || t("cashRegisters.errors.loadOrgLookups"));
      }
    } else {
      warnings.push(t("cashRegisters.errors.missingOrgLookupPermission"));
      setLegalEntities([]);
      setOperatingUnits([]);
      setCurrencies([]);
    }

    if (canReadAccounts) {
      try {
        const accountRes = await listAccounts({ includeInactive: true, limit: 500 });
        setAccounts(accountRes?.rows || []);
      } catch (err) {
        setAccounts([]);
        warnings.push(
          err?.response?.data?.message || t("cashRegisters.errors.loadAccountLookups")
        );
      }
    } else {
      warnings.push(t("cashRegisters.errors.missingAccountLookupPermission"));
      setAccounts([]);
    }

    setLookupWarning(warnings.join(" "));
  }

  useEffect(() => {
    loadRegisters();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadRegisters]);

  useEffect(() => {
    loadLookups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canUpsertRegisters, canReadOrgTree, canReadAccounts]);

  function resetForm() {
    setForm((prev) => ({
      ...EMPTY_FORM,
      legalEntityId: prev.id
        ? String(legalEntityOptions[0]?.id || "")
        : prev.legalEntityId || String(legalEntityOptions[0]?.id || ""),
      currencyCode: prev.id
        ? String(currencyOptions[0]?.code || "")
        : prev.currencyCode || String(currencyOptions[0]?.code || ""),
    }));
  }

  function handleEdit(row) {
    setForm(mapRowToForm(row));
    setError("");
    setMessage("");
  }

  async function handleSubmit(event) {
    event.preventDefault();
    if (!canUpsertRegisters) {
      setError(t("cashRegisters.errors.missingUpsertPermission"));
      return;
    }

    const legalEntityId = toPositiveInt(form.legalEntityId);
    const accountId = toPositiveInt(form.accountId);
    const operatingUnitId = toPositiveInt(form.operatingUnitId);
    const varianceGainAccountId = toPositiveInt(form.varianceGainAccountId);
    const varianceLossAccountId = toPositiveInt(form.varianceLossAccountId);
    const maxTxnAmount = toOptionalAmount(form.maxTxnAmount);
    const requiresApprovalOverAmount = toOptionalAmount(form.requiresApprovalOverAmount);

    if (!form.code.trim() || !form.name.trim()) {
      setError(t("cashRegisters.errors.requiredCodeName"));
      return;
    }
    if (!legalEntityId || !accountId) {
      setError(t("cashRegisters.errors.requiredEntityAccount"));
      return;
    }
    if (!String(form.currencyCode || "").trim()) {
      setError(t("cashRegisters.errors.requiredCurrency"));
      return;
    }
    if (Number.isNaN(maxTxnAmount) || Number.isNaN(requiresApprovalOverAmount)) {
      setError(t("cashRegisters.errors.invalidAmount"));
      return;
    }

    const payload = {
      legalEntityId,
      operatingUnitId,
      accountId,
      code: form.code.trim().toUpperCase(),
      name: form.name.trim(),
      registerType: form.registerType,
      sessionMode: form.sessionMode,
      currencyCode: form.currencyCode.trim().toUpperCase(),
      status: form.status,
      allowNegative: Boolean(form.allowNegative),
      varianceGainAccountId,
      varianceLossAccountId,
      maxTxnAmount,
      requiresApprovalOverAmount,
    };
    const editingId = toPositiveInt(form.id);
    if (editingId) {
      payload.id = editingId;
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      await upsertCashRegister(payload);
      setMessage(
        editingId
          ? t("cashRegisters.messages.updated")
          : t("cashRegisters.messages.created")
      );
      resetForm();
      await loadRegisters();
    } catch (err) {
      setError(err?.response?.data?.message || t("cashRegisters.errors.save"));
    } finally {
      setSaving(false);
    }
  }

  async function handleToggleStatus(row) {
    if (!canUpsertRegisters) {
      return;
    }
    const rowId = toPositiveInt(row?.id);
    if (!rowId) {
      return;
    }

    const currentStatus = String(row?.status || "").toUpperCase();
    const targetStatus = currentStatus === "ACTIVE" ? "INACTIVE" : "ACTIVE";

    setUpdatingStatusId(rowId);
    setError("");
    setMessage("");
    try {
      await setCashRegisterStatus(rowId, { status: targetStatus });
      setMessage(
        t("cashRegisters.messages.statusUpdated", {
          code: row?.code || rowId,
          status: targetStatus,
        })
      );
      await loadRegisters();
    } catch (err) {
      setError(
        err?.response?.data?.message || t("cashRegisters.errors.statusUpdate")
      );
    } finally {
      setUpdatingStatusId(null);
    }
  }

  if (!canReadRegisters) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {t("cashRegisters.errors.missingReadPermission")}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {t("cashRegisters.title")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {t("cashRegisters.subtitle")}
        </p>
      </div>

      <CashControlModeBanner />

      {error ? (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      ) : null}
      {message ? (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      ) : null}
      {lookupWarning ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          {lookupWarning}
        </div>
      ) : null}

      {canUpsertRegisters ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="mb-3 flex items-center justify-between gap-2">
            <h2 className="text-sm font-semibold text-slate-700">
              {form.id
                ? t("cashRegisters.sections.edit")
                : t("cashRegisters.sections.create")}
            </h2>
            {form.id ? (
              <button
                type="button"
                onClick={resetForm}
                className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50"
              >
                {t("cashRegisters.actions.cancelEdit")}
              </button>
            ) : null}
          </div>

          <form onSubmit={handleSubmit} className="grid gap-2 md:grid-cols-3">
            <input
              value={form.code}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashRegisters.form.code")}
              maxLength={60}
              required
            />
            <input
              value={form.name}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashRegisters.form.name")}
              maxLength={255}
              required
            />
            <select
              value={form.registerType}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, registerType: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {REGISTER_TYPES.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>

            <select
              value={form.sessionMode}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, sessionMode: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {SESSION_MODES.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>

            {legalEntityOptions.length > 0 ? (
              <select
                value={form.legalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    legalEntityId: event.target.value,
                    operatingUnitId: "",
                    accountId: "",
                    varianceGainAccountId: "",
                    varianceLossAccountId: "",
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                required
              >
                <option value="">{t("cashRegisters.placeholders.legalEntity")}</option>
                {legalEntityOptions.map((entity) => (
                  <option key={entity.id} value={entity.id}>
                    {entity.code} - {entity.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.legalEntityId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, legalEntityId: event.target.value }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("cashRegisters.form.legalEntityId")}
                required
              />
            )}

            {currencyOptions.length > 0 ? (
              <select
                value={form.currencyCode}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    currencyCode: String(event.target.value || "").toUpperCase(),
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                required
              >
                <option value="">{t("cashRegisters.placeholders.currencyCode")}</option>
                {currencyOptions.map((currency) => (
                  <option key={currency.code} value={currency.code}>
                    {currency.code} - {currency.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                value={form.currencyCode}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    currencyCode: event.target.value.toUpperCase(),
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("cashRegisters.form.currencyCode")}
                maxLength={3}
                required
              />
            )}

            {operatingUnitOptions.length > 0 ? (
              <select
                value={form.operatingUnitId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, operatingUnitId: event.target.value }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              >
                <option value="">{t("cashRegisters.placeholders.operatingUnit")}</option>
                {operatingUnitOptions.map((unit) => (
                  <option key={unit.id} value={unit.id}>
                    {unit.code} - {unit.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.operatingUnitId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, operatingUnitId: event.target.value }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("cashRegisters.form.operatingUnitIdOptional")}
              />
            )}

            {accountOptions.length > 0 ? (
              <select
                value={form.accountId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, accountId: event.target.value }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                required
              >
                <option value="">{t("cashRegisters.placeholders.account")}</option>
                {accountOptions.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.code} - {account.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.accountId}
                onChange={(event) =>
                  setForm((prev) => ({ ...prev, accountId: event.target.value }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("cashRegisters.form.accountId")}
                required
              />
            )}

            {accountOptions.length > 0 ? (
              <select
                value={form.varianceGainAccountId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    varianceGainAccountId: event.target.value,
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              >
                <option value="">{t("cashRegisters.placeholders.varianceGainAccount")}</option>
                {accountOptions.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.code} - {account.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.varianceGainAccountId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    varianceGainAccountId: event.target.value,
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("cashRegisters.form.varianceGainAccountIdOptional")}
              />
            )}

            {accountOptions.length > 0 ? (
              <select
                value={form.varianceLossAccountId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    varianceLossAccountId: event.target.value,
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              >
                <option value="">{t("cashRegisters.placeholders.varianceLossAccount")}</option>
                {accountOptions.map((account) => (
                  <option key={account.id} value={account.id}>
                    {account.code} - {account.name}
                  </option>
                ))}
              </select>
            ) : (
              <input
                type="number"
                min={1}
                value={form.varianceLossAccountId}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    varianceLossAccountId: event.target.value,
                  }))
                }
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("cashRegisters.form.varianceLossAccountIdOptional")}
              />
            )}

            <input
              type="number"
              min="0.000001"
              step="0.000001"
              value={form.maxTxnAmount}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, maxTxnAmount: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashRegisters.form.maxTxnAmountOptional")}
            />
            <input
              type="number"
              min="0"
              step="0.000001"
              value={form.requiresApprovalOverAmount}
              onChange={(event) =>
                setForm((prev) => ({
                  ...prev,
                  requiresApprovalOverAmount: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("cashRegisters.form.requiresApprovalOverAmountOptional")}
            />
            <select
              value={form.status}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, status: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {REGISTER_STATUSES.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>

            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700 md:col-span-2">
              <input
                type="checkbox"
                checked={Boolean(form.allowNegative)}
                onChange={(event) =>
                  setForm((prev) => ({
                    ...prev,
                    allowNegative: event.target.checked,
                  }))
                }
              />
              {t("cashRegisters.form.allowNegative")}
            </label>

            <button
              type="submit"
              disabled={saving}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving
                ? t("cashRegisters.actions.saving")
                : form.id
                ? t("cashRegisters.actions.update")
                : t("cashRegisters.actions.create")}
            </button>
          </form>
        </section>
      ) : (
        <div className="rounded-lg border border-cyan-200 bg-cyan-50 px-3 py-2 text-sm text-cyan-800">
          {t("cashRegisters.readOnlyNotice")}
        </div>
      )}

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <h2 className="text-sm font-semibold text-slate-700">
            {t("cashRegisters.sections.list")}
          </h2>
          <button
            type="button"
            onClick={loadRegisters}
            disabled={loading}
            className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-60"
          >
            {loading
              ? t("cashRegisters.actions.loading")
              : t("cashRegisters.actions.refresh")}
          </button>
        </div>

        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th>
                <th className="px-3 py-2">{t("cashRegisters.table.code")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.name")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.registerType")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.sessionMode")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.legalEntity")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.operatingUnit")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.account")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.currency")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.allowNegative")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.status")}</th>
                <th className="px-3 py-2">{t("cashRegisters.table.actions")}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => {
                const rowId = toPositiveInt(row?.id);
                const rowStatus = String(row?.status || "").toUpperCase();
                const isStatusBusy = rowId && updatingStatusId === rowId;
                const statusBadgeClass =
                  rowStatus === "ACTIVE"
                    ? "bg-emerald-100 text-emerald-700"
                    : "bg-slate-200 text-slate-700";

                return (
                  <tr key={row.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{row.id}</td>
                    <td className="px-3 py-2">{row.code}</td>
                    <td className="px-3 py-2">{row.name}</td>
                    <td className="px-3 py-2">{row.register_type}</td>
                    <td className="px-3 py-2">{row.session_mode}</td>
                    <td className="px-3 py-2">
                      {row.legal_entity_code || row.legal_entity_id} -{" "}
                      {row.legal_entity_name || "-"}
                    </td>
                    <td className="px-3 py-2">
                      {row.operating_unit_id
                        ? `${row.operating_unit_code || row.operating_unit_id} - ${
                            row.operating_unit_name || "-"
                          }`
                        : "-"}
                    </td>
                    <td className="px-3 py-2">
                      {row.account_code || row.account_id} - {row.account_name || "-"}
                    </td>
                    <td className="px-3 py-2">{row.currency_code || "-"}</td>
                    <td className="px-3 py-2">
                      {parseDbBoolean(row.allow_negative)
                        ? t("cashRegisters.values.yes")
                        : t("cashRegisters.values.no")}
                    </td>
                    <td className="px-3 py-2">
                      <span
                        className={`inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${statusBadgeClass}`}
                      >
                        {rowStatus || "-"}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      {canUpsertRegisters ? (
                        <div className="flex flex-wrap gap-1">
                          <button
                            type="button"
                            onClick={() => handleEdit(row)}
                            className="rounded-md border border-slate-300 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                          >
                            {t("cashRegisters.actions.edit")}
                          </button>
                          <button
                            type="button"
                            onClick={() => handleToggleStatus(row)}
                            disabled={isStatusBusy}
                            className="rounded-md border border-cyan-300 px-2 py-1 text-xs font-semibold text-cyan-700 hover:bg-cyan-50 disabled:opacity-60"
                          >
                            {isStatusBusy
                              ? t("cashRegisters.actions.saving")
                              : rowStatus === "ACTIVE"
                              ? t("cashRegisters.actions.deactivate")
                              : t("cashRegisters.actions.activate")}
                          </button>
                        </div>
                      ) : (
                        <span className="text-slate-400">-</span>
                      )}
                    </td>
                  </tr>
                );
              })}
              {!loading && rows.length === 0 ? (
                <tr>
                  <td colSpan={12} className="px-3 py-3 text-slate-500">
                    {t("cashRegisters.empty")}
                  </td>
                </tr>
              ) : null}
              {loading ? (
                <tr>
                  <td colSpan={12} className="px-3 py-3 text-slate-500">
                    {t("cashRegisters.loading")}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
