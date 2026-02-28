import { useEffect, useMemo, useState } from "react";
import {
  createConsolidationRun,
  executeConsolidationRun,
  finalizeConsolidationRun,
  listConsolidationCoaMappings,
  listConsolidationEliminationPlaceholders,
  listConsolidationGroupMembers,
  listConsolidationGroups,
  listConsolidationRuns,
  upsertConsolidationCoaMapping,
  upsertConsolidationEliminationPlaceholder,
  upsertConsolidationGroup,
  upsertConsolidationGroupMember,
} from "../../api/consolidationAdmin.js";
import { listAccounts, listCoas } from "../../api/glAdmin.js";
import {
  listFiscalCalendars,
  listFiscalPeriods,
  listGroupCompanies,
  listLegalEntities,
} from "../../api/orgAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import TenantReadinessChecklist from "../../readiness/TenantReadinessChecklist.jsx";

const METHODS = ["FULL", "PROPORTIONAL", "EQUITY"];
const DIRECTIONS = ["AUTO", "DEBIT", "CREDIT"];
const RATE_TYPES = ["CLOSING", "SPOT", "AVERAGE"];

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function padPeriod(value) {
  return String(value || "").padStart(2, "0");
}

function isLocked(status) {
  return String(status || "").toUpperCase() === "LOCKED";
}

export default function ConsolidationSetupPage() {
  const { hasPermission } = useAuth();
  const { language } = useI18n();
  const isTr = language === "tr";
  const l = (en, tr) => (isTr ? tr : en);

  const canReadGroups = hasPermission("consolidation.group.read");
  const canUpsertGroups = hasPermission("consolidation.group.upsert");
  const canUpsertMembers = hasPermission("consolidation.group_member.upsert");
  const canReadMappings = hasPermission("consolidation.coa_mapping.read");
  const canUpsertMappings = hasPermission("consolidation.coa_mapping.upsert");
  const canReadPlaceholders = hasPermission("consolidation.elimination_placeholder.read");
  const canUpsertPlaceholders = hasPermission("consolidation.elimination_placeholder.upsert");
  const canReadRuns = hasPermission("consolidation.run.read");
  const canCreateRuns = hasPermission("consolidation.run.create");
  const canExecuteRuns = hasPermission("consolidation.run.execute");
  const canFinalizeRuns = hasPermission("consolidation.run.finalize");

  const canUsePage = [
    canReadGroups,
    canUpsertGroups,
    canUpsertMembers,
    canReadMappings,
    canUpsertMappings,
    canReadPlaceholders,
    canUpsertPlaceholders,
    canReadRuns,
    canCreateRuns,
    canExecuteRuns,
    canFinalizeRuns,
  ].some(Boolean);

  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const [groupCompanies, setGroupCompanies] = useState([]);
  const [calendars, setCalendars] = useState([]);
  const [legalEntities, setLegalEntities] = useState([]);
  const [coas, setCoas] = useState([]);
  const [accounts, setAccounts] = useState([]);

  const [groups, setGroups] = useState([]);
  const [selectedGroupId, setSelectedGroupId] = useState("");
  const [members, setMembers] = useState([]);
  const [mappings, setMappings] = useState([]);
  const [placeholders, setPlaceholders] = useState([]);
  const [runs, setRuns] = useState([]);
  const [periods, setPeriods] = useState([]);

  const [groupForm, setGroupForm] = useState({
    groupCompanyId: "",
    calendarId: "",
    code: "",
    name: "",
    presentationCurrencyCode: "USD",
  });
  const [memberForm, setMemberForm] = useState({
    legalEntityId: "",
    consolidationMethod: "FULL",
    ownershipPct: "1",
    effectiveFrom: todayIso(),
    effectiveTo: "",
  });
  const [mappingForm, setMappingForm] = useState({
    legalEntityId: "",
    groupCoaId: "",
    localCoaId: "",
    status: "ACTIVE",
  });
  const [placeholderForm, setPlaceholderForm] = useState({
    placeholderCode: "",
    name: "",
    accountId: "",
    defaultDirection: "AUTO",
    description: "",
    isActive: true,
  });
  const [runForm, setRunForm] = useState({
    fiscalPeriodId: "",
    runName: "",
    presentationCurrencyCode: "USD",
    rateType: "CLOSING",
  });

  const selectedGroup = useMemo(() => {
    const id = toPositiveInt(selectedGroupId);
    if (!id) return null;
    return groups.find((row) => Number(row.id) === id) || null;
  }, [groups, selectedGroupId]);

  const groupCoaOptions = useMemo(
    () => coas.filter((row) => String(row.scope || "").toUpperCase() === "GROUP"),
    [coas]
  );

  const localCoaOptions = useMemo(() => {
    const legalEntityId = toPositiveInt(mappingForm.legalEntityId);
    return coas.filter((row) => {
      if (String(row.scope || "").toUpperCase() !== "LEGAL_ENTITY") return false;
      if (!legalEntityId) return true;
      return Number(row.legal_entity_id) === legalEntityId;
    });
  }, [coas, mappingForm.legalEntityId]);

  async function loadLookups() {
    const results = await Promise.allSettled([
      listGroupCompanies(),
      listFiscalCalendars(),
      listLegalEntities(),
      listCoas({ includeInactive: true }),
      listAccounts({ includeInactive: true }),
    ]);
    const [companiesRes, calendarsRes, entitiesRes, coasRes, accountsRes] = results;

    if (companiesRes.status === "fulfilled") {
      const rows = companiesRes.value?.rows || [];
      setGroupCompanies(rows);
      setGroupForm((prev) => ({
        ...prev,
        groupCompanyId: prev.groupCompanyId || String(rows[0]?.id || ""),
      }));
    }
    if (calendarsRes.status === "fulfilled") {
      const rows = calendarsRes.value?.rows || [];
      setCalendars(rows);
      setGroupForm((prev) => ({
        ...prev,
        calendarId: prev.calendarId || String(rows[0]?.id || ""),
      }));
    }
    if (entitiesRes.status === "fulfilled") {
      const rows = entitiesRes.value?.rows || [];
      setLegalEntities(rows);
      setMemberForm((prev) => ({
        ...prev,
        legalEntityId: prev.legalEntityId || String(rows[0]?.id || ""),
      }));
      setMappingForm((prev) => ({
        ...prev,
        legalEntityId: prev.legalEntityId || String(rows[0]?.id || ""),
      }));
    }
    if (coasRes.status === "fulfilled") {
      setCoas(coasRes.value?.rows || []);
    }
    if (accountsRes.status === "fulfilled") {
      setAccounts(accountsRes.value?.rows || []);
    }
  }

  async function loadGroups() {
    if (!canReadGroups) {
      setGroups([]);
      return;
    }
    const response = await listConsolidationGroups();
    const rows = response?.rows || [];
    setGroups(rows);
    setSelectedGroupId((prev) => {
      const current = toPositiveInt(prev);
      if (current && rows.some((row) => Number(row.id) === current)) return prev;
      return String(rows[0]?.id || "");
    });
  }

  async function loadPeriods(group) {
    const calendarId = toPositiveInt(group?.calendar_id);
    if (!calendarId) {
      setPeriods([]);
      return;
    }
    try {
      const response = await listFiscalPeriods(calendarId);
      const rows = response?.rows || [];
      setPeriods(rows);
      setRunForm((prev) => ({
        ...prev,
        fiscalPeriodId: prev.fiscalPeriodId || String(rows[0]?.id || ""),
      }));
    } catch {
      setPeriods([]);
    }
  }

  async function loadGroupDetails(groupId) {
    const id = toPositiveInt(groupId);
    if (!id) {
      setMembers([]);
      setMappings([]);
      setPlaceholders([]);
      setRuns([]);
      return;
    }

    const tasks = [];
    if (canReadGroups) {
      tasks.push(
        listConsolidationGroupMembers(id).then((response) =>
          setMembers(response?.rows || [])
        )
      );
    }
    if (canReadMappings) {
      tasks.push(
        listConsolidationCoaMappings(id).then((response) =>
          setMappings(response?.rows || [])
        )
      );
    }
    if (canReadPlaceholders) {
      tasks.push(
        listConsolidationEliminationPlaceholders(id).then((response) =>
          setPlaceholders(response?.rows || [])
        )
      );
    }
    if (canReadRuns) {
      tasks.push(
        listConsolidationRuns({ consolidationGroupId: id }).then((response) =>
          setRuns(response?.rows || [])
        )
      );
    }

    await Promise.allSettled(tasks);
  }

  async function refreshAll() {
    setLoading(true);
    setError("");
    try {
      await Promise.all([loadLookups(), loadGroups()]);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to load setup data.", "Kurulum verileri yuklenemedi.")
      );
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canReadGroups]);

  useEffect(() => {
    loadGroupDetails(selectedGroupId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGroupId, canReadGroups, canReadMappings, canReadPlaceholders, canReadRuns]);

  useEffect(() => {
    loadPeriods(selectedGroup);
    if (selectedGroup?.presentation_currency_code) {
      setRunForm((prev) => ({
        ...prev,
        presentationCurrencyCode: String(selectedGroup.presentation_currency_code).toUpperCase(),
      }));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGroup?.id]);

  async function runAction(key, fn, failText, okText) {
    setSaving(key);
    setError("");
    setMessage("");
    try {
      await fn();
      if (okText) setMessage(okText);
      const groupId = toPositiveInt(selectedGroupId);
      if (groupId) await loadGroupDetails(groupId);
    } catch (err) {
      setError(err?.response?.data?.message || failText);
    } finally {
      setSaving("");
    }
  }

  async function onSaveGroup(event) {
    event.preventDefault();
    if (!canUpsertGroups) {
      setError(l("Missing permission: consolidation.group.upsert", "Eksik yetki: consolidation.group.upsert"));
      return;
    }

    const groupCompanyId = toPositiveInt(groupForm.groupCompanyId);
    const calendarId = toPositiveInt(groupForm.calendarId);
    if (!groupCompanyId || !calendarId) {
      setError(l("groupCompanyId and calendarId are required.", "groupCompanyId ve calendarId zorunludur."));
      return;
    }

    await runAction(
      "group",
      async () => {
        await upsertConsolidationGroup({
          groupCompanyId,
          calendarId,
          code: groupForm.code.trim(),
          name: groupForm.name.trim(),
          presentationCurrencyCode: String(groupForm.presentationCurrencyCode).toUpperCase(),
        });
        await loadGroups();
      },
      l("Failed to save consolidation group.", "Konsolidasyon grubu kaydedilemedi."),
      l("Consolidation group saved.", "Konsolidasyon grubu kaydedildi.")
    );
  }

  async function onSaveMember(event) {
    event.preventDefault();
    if (!canUpsertMembers) {
      setError(l("Missing permission: consolidation.group_member.upsert", "Eksik yetki: consolidation.group_member.upsert"));
      return;
    }

    const groupId = toPositiveInt(selectedGroupId);
    const legalEntityId = toPositiveInt(memberForm.legalEntityId);
    const ownershipPct = Number(memberForm.ownershipPct);
    if (!groupId || !legalEntityId || !memberForm.effectiveFrom) {
      setError(l("Group, legalEntityId and effectiveFrom are required.", "Grup, legalEntityId ve effectiveFrom zorunludur."));
      return;
    }
    if (!Number.isFinite(ownershipPct) || ownershipPct < 0) {
      setError(l("ownershipPct must be zero or positive.", "ownershipPct sifir veya pozitif olmalidir."));
      return;
    }

    await runAction(
      "member",
      async () => {
        await upsertConsolidationGroupMember(groupId, {
          legalEntityId,
          consolidationMethod: memberForm.consolidationMethod,
          ownershipPct,
          effectiveFrom: memberForm.effectiveFrom,
          effectiveTo: memberForm.effectiveTo || undefined,
        });
      },
      l("Failed to save group member.", "Grup uyesi kaydedilemedi."),
      l("Group member saved.", "Grup uyesi kaydedildi.")
    );
  }

  async function onSaveMapping(event) {
    event.preventDefault();
    if (!canUpsertMappings) {
      setError(l("Missing permission: consolidation.coa_mapping.upsert", "Eksik yetki: consolidation.coa_mapping.upsert"));
      return;
    }

    const groupId = toPositiveInt(selectedGroupId);
    const legalEntityId = toPositiveInt(mappingForm.legalEntityId);
    const groupCoaId = toPositiveInt(mappingForm.groupCoaId);
    const localCoaId = toPositiveInt(mappingForm.localCoaId);
    if (!groupId || !legalEntityId || !groupCoaId || !localCoaId) {
      setError(l("Group and mapping IDs are required.", "Grup ve esleme ID alanlari zorunludur."));
      return;
    }

    await runAction(
      "mapping",
      async () => {
        await upsertConsolidationCoaMapping(groupId, {
          legalEntityId,
          groupCoaId,
          localCoaId,
          status: mappingForm.status,
        });
      },
      l("Failed to save mapping.", "Esleme kaydedilemedi."),
      l("Mapping saved.", "Esleme kaydedildi.")
    );
  }

  async function onSavePlaceholder(event) {
    event.preventDefault();
    if (!canUpsertPlaceholders) {
      setError(
        l(
          "Missing permission: consolidation.elimination_placeholder.upsert",
          "Eksik yetki: consolidation.elimination_placeholder.upsert"
        )
      );
      return;
    }

    const groupId = toPositiveInt(selectedGroupId);
    const accountId = placeholderForm.accountId
      ? toPositiveInt(placeholderForm.accountId)
      : null;
    if (!groupId || !placeholderForm.placeholderCode.trim() || !placeholderForm.name.trim()) {
      setError(l("Group, placeholderCode and name are required.", "Grup, placeholderCode ve name zorunludur."));
      return;
    }
    if (placeholderForm.accountId && !accountId) {
      setError(l("accountId must be a positive integer.", "accountId pozitif bir tam sayi olmalidir."));
      return;
    }

    await runAction(
      "placeholder",
      async () => {
        await upsertConsolidationEliminationPlaceholder(groupId, {
          placeholderCode: placeholderForm.placeholderCode.trim().toUpperCase(),
          name: placeholderForm.name.trim(),
          accountId: accountId || undefined,
          defaultDirection: placeholderForm.defaultDirection,
          description: placeholderForm.description.trim() || undefined,
          isActive: Boolean(placeholderForm.isActive),
        });
      },
      l("Failed to save placeholder.", "Placeholder kaydedilemedi."),
      l("Placeholder saved.", "Placeholder kaydedildi.")
    );
  }

  async function onCreateRun(event) {
    event.preventDefault();
    if (!canCreateRuns) {
      setError(l("Missing permission: consolidation.run.create", "Eksik yetki: consolidation.run.create"));
      return;
    }

    const groupId = toPositiveInt(selectedGroupId);
    const fiscalPeriodId = toPositiveInt(runForm.fiscalPeriodId);
    if (!groupId || !fiscalPeriodId || !runForm.runName.trim()) {
      setError(l("Group, fiscalPeriodId and runName are required.", "Grup, fiscalPeriodId ve runName zorunludur."));
      return;
    }

    await runAction(
      "run-create",
      async () => {
        await createConsolidationRun({
          consolidationGroupId: groupId,
          fiscalPeriodId,
          runName: runForm.runName.trim(),
          presentationCurrencyCode: String(runForm.presentationCurrencyCode).toUpperCase(),
        });
      },
      l("Failed to create run.", "Run olusturulamadi."),
      l("Run created.", "Run olusturuldu.")
    );
  }

  async function onExecuteRun(runId) {
    if (!canExecuteRuns) {
      setError(l("Missing permission: consolidation.run.execute", "Eksik yetki: consolidation.run.execute"));
      return;
    }

    await runAction(
      `run-exec-${runId}`,
      async () => {
        await executeConsolidationRun(runId, { rateType: runForm.rateType });
      },
      l("Failed to execute run.", "Run execute edilemedi."),
      l("Run executed.", "Run execute edildi.")
    );
  }

  async function onFinalizeRun(runId) {
    if (!canFinalizeRuns) {
      setError(l("Missing permission: consolidation.run.finalize", "Eksik yetki: consolidation.run.finalize"));
      return;
    }

    await runAction(
      `run-final-${runId}`,
      async () => {
        await finalizeConsolidationRun(runId);
      },
      l("Failed to finalize run.", "Run final edilemedi."),
      l("Run finalized.", "Run final edildi.")
    );
  }

  if (!canUsePage) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {l("You need consolidation setup permissions.", "Konsolidasyon kurulum yetkileri gerekir.")}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <TenantReadinessChecklist />

      <div>
        <h1 className="text-xl font-semibold text-slate-900">{l("Consolidation Setup", "Konsolidasyon Kurulumu")}</h1>
        <p className="mt-1 text-sm text-slate-600">
          {l(
            "Manage groups, members, mappings, elimination placeholders and runs.",
            "Grup, uye, esleme, placeholder ve run yonetimi."
          )}
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</div>
      )}
      {message && (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{message}</div>
      )}

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-sm font-semibold text-slate-700">{l("Groups", "Gruplar")}</h2>
          <button
            type="button"
            onClick={refreshAll}
            disabled={loading}
            className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 disabled:opacity-60"
          >
            {loading ? l("Loading...", "Yukleniyor...") : l("Refresh", "Yenile")}
          </button>
        </div>

        <div className="grid gap-2 md:grid-cols-3">
          <select
            value={selectedGroupId}
            onChange={(event) => setSelectedGroupId(event.target.value)}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
          >
            <option value="">{l("Select group", "Grup secin")}</option>
            {groups.map((row) => (
              <option key={row.id} value={row.id}>
                #{row.id} | {row.code} - {row.name}
              </option>
            ))}
          </select>
          <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
            {selectedGroup
              ? `${selectedGroup.code} (${selectedGroup.presentation_currency_code})`
              : l("No group selected", "Grup secilmedi")}
          </div>
        </div>

        <form onSubmit={onSaveGroup} className="mt-3 grid gap-2 md:grid-cols-5">
          <input
            type="number"
            min={1}
            value={groupForm.groupCompanyId}
            onChange={(event) => setGroupForm((prev) => ({ ...prev, groupCompanyId: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            list="group-company-options"
            placeholder={l("Group company ID", "Grup sirketi ID")}
            required
          />
          <datalist id="group-company-options">
            {groupCompanies.map((row) => (
              <option key={row.id} value={row.id}>{row.code} - {row.name}</option>
            ))}
          </datalist>

          <input
            type="number"
            min={1}
            value={groupForm.calendarId}
            onChange={(event) => setGroupForm((prev) => ({ ...prev, calendarId: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            list="calendar-options"
            placeholder={l("Calendar ID", "Takvim ID")}
            required
          />
          <datalist id="calendar-options">
            {calendars.map((row) => (
              <option key={row.id} value={row.id}>{row.code} - {row.name}</option>
            ))}
          </datalist>

          <input
            value={groupForm.code}
            onChange={(event) => setGroupForm((prev) => ({ ...prev, code: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Code", "Kod")}
            required
          />
          <input
            value={groupForm.name}
            onChange={(event) => setGroupForm((prev) => ({ ...prev, name: event.target.value }))}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={l("Name", "Ad")}
            required
          />

          <div className="flex gap-2">
            <input
              value={groupForm.presentationCurrencyCode}
              onChange={(event) =>
                setGroupForm((prev) => ({ ...prev, presentationCurrencyCode: event.target.value.toUpperCase() }))
              }
              maxLength={3}
              className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Currency", "Para birimi")}
              required
            />
            <button
              type="submit"
              disabled={saving === "group"}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "group" ? l("Saving...", "Kaydediliyor...") : l("Save", "Kaydet")}
            </button>
          </div>
        </form>
      </section>

      {toPositiveInt(selectedGroupId) && (
        <div className="grid gap-4 xl:grid-cols-2">
          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-slate-700">{l("Members", "Uyeler")}</h2>
            <form onSubmit={onSaveMember} className="grid gap-2 md:grid-cols-5">
              <input
                type="number"
                min={1}
                value={memberForm.legalEntityId}
                onChange={(event) => setMemberForm((prev) => ({ ...prev, legalEntityId: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
                list="legal-entity-options"
                placeholder={l("Legal entity ID", "Istirak / bagli ortak ID")}
                required
              />
              <datalist id="legal-entity-options">
                {legalEntities.map((row) => (
                  <option key={row.id} value={row.id}>{row.code} - {row.name}</option>
                ))}
              </datalist>
              <select
                value={memberForm.consolidationMethod}
                onChange={(event) => setMemberForm((prev) => ({ ...prev, consolidationMethod: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              >
                {METHODS.map((method) => (
                  <option key={method} value={method}>{method}</option>
                ))}
              </select>
              <input
                type="number"
                min="0"
                step="0.0001"
                value={memberForm.ownershipPct}
                onChange={(event) => setMemberForm((prev) => ({ ...prev, ownershipPct: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={l("Ownership %", "Sahiplik %")}
                required
              />
              <button type="submit" disabled={saving === "member"} className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60">
                {saving === "member" ? l("Saving...", "Kaydediliyor...") : l("Save", "Kaydet")}
              </button>
              <input
                type="date"
                value={memberForm.effectiveFrom}
                onChange={(event) => setMemberForm((prev) => ({ ...prev, effectiveFrom: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
                required
              />
              <input
                type="date"
                value={memberForm.effectiveTo}
                onChange={(event) => setMemberForm((prev) => ({ ...prev, effectiveTo: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              />
            </form>
            <div className="mt-3 max-h-56 overflow-auto rounded-lg border border-slate-200 p-2 text-xs">
              {members.length === 0 ? (
                <div className="text-slate-500">{l("No members", "Uye yok")}</div>
              ) : (
                members.map((row) => (
                  <div key={row.id} className="border-b border-slate-100 py-1 last:border-0">
                    #{row.id} | LE {row.legal_entity_id} | {row.consolidation_method} | {row.ownership_pct}
                  </div>
                ))
              )}
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-slate-700">{l("CoA Mappings", "Hesap Plani Eslemeleri")}</h2>
            <form onSubmit={onSaveMapping} className="grid gap-2 md:grid-cols-4">
              <input
                type="number"
                min={1}
                value={mappingForm.legalEntityId}
                onChange={(event) => setMappingForm((prev) => ({ ...prev, legalEntityId: event.target.value, localCoaId: "" }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                list="legal-entity-options"
                placeholder={l("Legal entity ID", "Istirak / bagli ortak ID")}
                required
              />
              <input
                type="number"
                min={1}
                value={mappingForm.groupCoaId}
                onChange={(event) => setMappingForm((prev) => ({ ...prev, groupCoaId: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                list="group-coa-options"
                placeholder={l("Group CoA ID", "Grup HP ID")}
                required
              />
              <datalist id="group-coa-options">
                {groupCoaOptions.map((row) => (
                  <option key={row.id} value={row.id}>{row.code} - {row.name}</option>
                ))}
              </datalist>
              <input
                type="number"
                min={1}
                value={mappingForm.localCoaId}
                onChange={(event) => setMappingForm((prev) => ({ ...prev, localCoaId: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                list="local-coa-options"
                placeholder={l("Local CoA ID", "Lokal HP ID")}
                required
              />
              <datalist id="local-coa-options">
                {localCoaOptions.map((row) => (
                  <option key={row.id} value={row.id}>{row.code} - {row.name}</option>
                ))}
              </datalist>
              <div className="flex gap-2">
                <select
                  value={mappingForm.status}
                  onChange={(event) => setMappingForm((prev) => ({ ...prev, status: event.target.value }))}
                  className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
                >
                  <option value="ACTIVE">ACTIVE</option>
                  <option value="INACTIVE">INACTIVE</option>
                </select>
                <button type="submit" disabled={saving === "mapping"} className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60">
                  {saving === "mapping" ? l("Saving...", "Kaydediliyor...") : l("Save", "Kaydet")}
                </button>
              </div>
            </form>
            <div className="mt-3 max-h-56 overflow-auto rounded-lg border border-slate-200 p-2 text-xs">
              {mappings.length === 0 ? (
                <div className="text-slate-500">{l("No mappings", "Esleme yok")}</div>
              ) : (
                mappings.map((row) => (
                  <div key={row.id} className="border-b border-slate-100 py-1 last:border-0">
                    #{row.id} | LE {row.legal_entity_id} | G-COA {row.group_coa_id} | L-COA {row.local_coa_id} | {row.status}
                  </div>
                ))
              )}
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-slate-700">{l("Elimination Placeholders", "Eliminasyon Placeholderlari")}</h2>
            <form onSubmit={onSavePlaceholder} className="grid gap-2 md:grid-cols-5">
              <input
                value={placeholderForm.placeholderCode}
                onChange={(event) => setPlaceholderForm((prev) => ({ ...prev, placeholderCode: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={l("Code", "Kod")}
                required
              />
              <input
                value={placeholderForm.name}
                onChange={(event) => setPlaceholderForm((prev) => ({ ...prev, name: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={l("Name", "Ad")}
                required
              />
              <input
                type="number"
                min={1}
                value={placeholderForm.accountId}
                onChange={(event) => setPlaceholderForm((prev) => ({ ...prev, accountId: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                list="account-options"
                placeholder={l("Account ID", "Hesap ID")}
              />
              <datalist id="account-options">
                {accounts.map((row) => (
                  <option key={row.id} value={row.id}>{row.code} - {row.name}</option>
                ))}
              </datalist>
              <select
                value={placeholderForm.defaultDirection}
                onChange={(event) => setPlaceholderForm((prev) => ({ ...prev, defaultDirection: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              >
                {DIRECTIONS.map((direction) => (
                  <option key={direction} value={direction}>{direction}</option>
                ))}
              </select>
              <button type="submit" disabled={saving === "placeholder"} className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60">
                {saving === "placeholder" ? l("Saving...", "Kaydediliyor...") : l("Save", "Kaydet")}
              </button>
              <input
                value={placeholderForm.description}
                onChange={(event) => setPlaceholderForm((prev) => ({ ...prev, description: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-4"
                placeholder={l("Description", "Aciklama")}
              />
            </form>
            <div className="mt-3 max-h-56 overflow-auto rounded-lg border border-slate-200 p-2 text-xs">
              {placeholders.length === 0 ? (
                <div className="text-slate-500">{l("No placeholders", "Placeholder yok")}</div>
              ) : (
                placeholders.map((row) => (
                  <div key={row.id} className="border-b border-slate-100 py-1 last:border-0">
                    {row.placeholder_code} | {row.name} | ACC {row.account_id || "-"} | {row.default_direction} | {row.is_active ? "ACTIVE" : "INACTIVE"}
                  </div>
                ))
              )}
            </div>
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-slate-700">{l("Runs", "Runlar")}</h2>
            <form onSubmit={onCreateRun} className="grid gap-2 md:grid-cols-4">
              <input
                type="number"
                min={1}
                value={runForm.fiscalPeriodId}
                onChange={(event) => setRunForm((prev) => ({ ...prev, fiscalPeriodId: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                list="period-options"
                placeholder={l("Fiscal period ID", "Mali donem ID")}
                required
              />
              <datalist id="period-options">
                {periods.map((row) => (
                  <option key={row.id} value={row.id}>{row.fiscal_year}-P{padPeriod(row.period_no)} {row.period_name}</option>
                ))}
              </datalist>
              <input
                value={runForm.runName}
                onChange={(event) => setRunForm((prev) => ({ ...prev, runName: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={l("Run name", "Run adi")}
                required
              />
              <input
                value={runForm.presentationCurrencyCode}
                onChange={(event) => setRunForm((prev) => ({ ...prev, presentationCurrencyCode: event.target.value.toUpperCase() }))}
                maxLength={3}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={l("Currency", "Para birimi")}
                required
              />
              <button type="submit" disabled={saving === "run-create"} className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60">
                {saving === "run-create" ? l("Creating...", "Olusturuluyor...") : l("Create Run", "Run Olustur")}
              </button>
              <select
                value={runForm.rateType}
                onChange={(event) => setRunForm((prev) => ({ ...prev, rateType: event.target.value }))}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              >
                {RATE_TYPES.map((rateType) => (
                  <option key={rateType} value={rateType}>{rateType}</option>
                ))}
              </select>
            </form>
            <div className="mt-3 max-h-64 overflow-auto rounded-lg border border-slate-200 p-2 text-xs">
              {runs.length === 0 ? (
                <div className="text-slate-500">{l("No runs", "Run yok")}</div>
              ) : (
                runs.map((row) => (
                  <div key={row.id} className="flex items-center justify-between gap-2 border-b border-slate-100 py-1 last:border-0">
                    <div>
                      #{row.id} | {row.run_name} | {row.fiscal_year}-P{padPeriod(row.period_no)} | {row.status}
                    </div>
                    <div className="flex gap-1">
                      <button
                        type="button"
                        onClick={() => onExecuteRun(row.id)}
                        disabled={saving === `run-exec-${row.id}` || isLocked(row.status)}
                        className="rounded bg-cyan-700 px-2 py-1 text-[11px] font-semibold text-white disabled:opacity-50"
                      >
                        {saving === `run-exec-${row.id}` ? l("Running...", "Calisiyor...") : l("Execute", "Execute")}
                      </button>
                      <button
                        type="button"
                        onClick={() => onFinalizeRun(row.id)}
                        disabled={saving === `run-final-${row.id}` || isLocked(row.status)}
                        className="rounded border border-slate-300 px-2 py-1 text-[11px] font-semibold text-slate-700 disabled:opacity-50"
                      >
                        {saving === `run-final-${row.id}` ? l("Finalizing...", "Final ediliyor...") : l("Finalize", "Finalize")}
                      </button>
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>
        </div>
      )}
    </div>
  );
}

