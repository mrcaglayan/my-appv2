import { useMemo, useState } from "react";
import { bootstrapCompany } from "../../api/onboarding.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import TenantReadinessChecklist from "../../readiness/TenantReadinessChecklist.jsx";

const UNIT_TYPES = ["BRANCH", "PLANT", "STORE", "DEPARTMENT", "OTHER"];

function createId(prefix) {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

function createBranchDraft() {
  return {
    id: createId("branch"),
    code: "",
    name: "",
    unitType: "BRANCH",
    hasSubledger: false,
  };
}

function createEntityDraft() {
  return {
    id: createId("entity"),
    code: "",
    name: "",
    taxId: "",
    countryIso2: "",
    functionalCurrencyCode: "USD",
    isIntercompanyEnabled: true,
    intercompanyPartnerRequired: false,
    coaCode: "",
    coaName: "",
    bookCode: "",
    bookName: "",
    branches: [createBranchDraft()],
  };
}

function createInitialForm() {
  const now = new Date();
  return {
    groupCompany: {
      code: "",
      name: "",
    },
    fiscalCalendar: {
      code: "MAIN",
      name: "Main Calendar",
      yearStartMonth: 1,
      yearStartDay: 1,
    },
    fiscalYear: now.getUTCFullYear(),
    legalEntities: [createEntityDraft()],
  };
}

function compactEntityPayload(entity) {
  const branches = (entity.branches || [])
    .filter((branch) => branch.code.trim() && branch.name.trim())
    .map((branch) => ({
      code: branch.code.trim(),
      name: branch.name.trim(),
      unitType: String(branch.unitType || "BRANCH").toUpperCase(),
      hasSubledger: Boolean(branch.hasSubledger),
    }));

  return {
    code: entity.code.trim(),
    name: entity.name.trim(),
    functionalCurrencyCode: entity.functionalCurrencyCode.trim().toUpperCase(),
    isIntercompanyEnabled: Boolean(entity.isIntercompanyEnabled),
    intercompanyPartnerRequired: Boolean(entity.intercompanyPartnerRequired),
    ...(entity.taxId.trim() ? { taxId: entity.taxId.trim() } : {}),
    ...(entity.countryIso2.trim()
      ? { countryIso2: entity.countryIso2.trim().toUpperCase() }
      : {}),
    ...(entity.coaCode.trim() ? { coaCode: entity.coaCode.trim() } : {}),
    ...(entity.coaName.trim() ? { coaName: entity.coaName.trim() } : {}),
    ...(entity.bookCode.trim() ? { bookCode: entity.bookCode.trim() } : {}),
    ...(entity.bookName.trim() ? { bookName: entity.bookName.trim() } : {}),
    ...(branches.length > 0 ? { branches } : {}),
  };
}

function validateForm(form, l) {
  if (!form.groupCompany.code.trim() || !form.groupCompany.name.trim()) {
    return l(
      "Group company code and name are required.",
      "Grup sirketi kodu ve adi zorunludur."
    );
  }

  if (!form.fiscalCalendar.code.trim() || !form.fiscalCalendar.name.trim()) {
    return l(
      "Fiscal calendar code and name are required.",
      "Mali takvim kodu ve adi zorunludur."
    );
  }

  const yearStartMonth = Number(form.fiscalCalendar.yearStartMonth);
  const yearStartDay = Number(form.fiscalCalendar.yearStartDay);
  if (yearStartMonth < 1 || yearStartMonth > 12) {
    return l(
      "Fiscal calendar start month must be between 1 and 12.",
      "Mali takvim baslangic ayi 1 ile 12 arasinda olmali."
    );
  }
  if (yearStartDay < 1 || yearStartDay > 31) {
    return l(
      "Fiscal calendar start day must be between 1 and 31.",
      "Mali takvim baslangic gunu 1 ile 31 arasinda olmali."
    );
  }

  const fiscalYear = Number(form.fiscalYear);
  if (!Number.isInteger(fiscalYear) || fiscalYear <= 0) {
    return l("Fiscal year must be a positive integer.", "Mali yil pozitif bir tam sayi olmali.");
  }

  if (!Array.isArray(form.legalEntities) || form.legalEntities.length === 0) {
    return l("At least one legal entity is required.", "En az bir istirak / bagli ortak zorunludur.");
  }

  for (let index = 0; index < form.legalEntities.length; index += 1) {
    const entity = form.legalEntities[index];
    const prefix = `Legal entity ${index + 1}`;
    if (!entity.code.trim() || !entity.name.trim()) {
      return l(`${prefix}: code and name are required.`, `Istirak / bagli ortak ${index + 1}: kod ve ad zorunludur.`);
    }
    if (!entity.countryIso2.trim()) {
      return l(
        `${prefix}: country ISO2 is required (e.g. US, TR, DE).`,
        `Istirak / bagli ortak ${index + 1}: ulke ISO2 zorunludur (orn. US, TR, DE).`
      );
    }
    if (!entity.functionalCurrencyCode.trim()) {
      return l(
        `${prefix}: functional currency is required.`,
        `Istirak / bagli ortak ${index + 1}: fonksiyonel para birimi zorunludur.`
      );
    }
  }

  return "";
}

export default function CompanyOnboardingPage() {
  const { hasPermission } = useAuth();
  const { language } = useI18n();
  const isTr = language === "tr";
  const l = (en, tr) => (isTr ? tr : en);
  const canSetupCompany = hasPermission("onboarding.company.setup");
  const [form, setForm] = useState(createInitialForm());
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [result, setResult] = useState(null);

  const entityCount = useMemo(
    () => form.legalEntities.length,
    [form.legalEntities.length]
  );

  function setGroupCompanyField(field, value) {
    setForm((prev) => ({
      ...prev,
      groupCompany: {
        ...prev.groupCompany,
        [field]: value,
      },
    }));
  }

  function setFiscalCalendarField(field, value) {
    setForm((prev) => ({
      ...prev,
      fiscalCalendar: {
        ...prev.fiscalCalendar,
        [field]: value,
      },
    }));
  }

  function setEntityField(entityId, field, value) {
    setForm((prev) => ({
      ...prev,
      legalEntities: prev.legalEntities.map((entity) =>
        entity.id === entityId
          ? {
              ...entity,
              [field]: value,
            }
          : entity
      ),
    }));
  }

  function addEntity() {
    setForm((prev) => ({
      ...prev,
      legalEntities: [...prev.legalEntities, createEntityDraft()],
    }));
  }

  function removeEntity(entityId) {
    setForm((prev) => {
      if (prev.legalEntities.length <= 1) {
        return prev;
      }
      return {
        ...prev,
        legalEntities: prev.legalEntities.filter((entity) => entity.id !== entityId),
      };
    });
  }

  function addBranch(entityId) {
    setForm((prev) => ({
      ...prev,
      legalEntities: prev.legalEntities.map((entity) =>
        entity.id === entityId
          ? { ...entity, branches: [...entity.branches, createBranchDraft()] }
          : entity
      ),
    }));
  }

  function setBranchField(entityId, branchId, field, value) {
    setForm((prev) => ({
      ...prev,
      legalEntities: prev.legalEntities.map((entity) =>
        entity.id === entityId
          ? {
              ...entity,
              branches: entity.branches.map((branch) =>
                branch.id === branchId
                  ? {
                      ...branch,
                      [field]: value,
                    }
                  : branch
              ),
            }
          : entity
      ),
    }));
  }

  function removeBranch(entityId, branchId) {
    setForm((prev) => ({
      ...prev,
      legalEntities: prev.legalEntities.map((entity) => {
        if (entity.id !== entityId) {
          return entity;
        }
        if (entity.branches.length <= 1) {
          return entity;
        }
        return {
          ...entity,
          branches: entity.branches.filter((branch) => branch.id !== branchId),
        };
      }),
    }));
  }

  function loadSample() {
    setForm({
      groupCompany: {
        code: "GLOBAL",
        name: "Global Holdings",
      },
      fiscalCalendar: {
        code: "MAIN",
        name: "Main Calendar",
        yearStartMonth: 1,
        yearStartDay: 1,
      },
      fiscalYear: new Date().getUTCFullYear(),
      legalEntities: [
        {
          id: createId("entity"),
          code: "US01",
          name: "US Operations LLC",
          taxId: "",
          countryIso2: "US",
          functionalCurrencyCode: "USD",
          isIntercompanyEnabled: true,
          intercompanyPartnerRequired: false,
          coaCode: "COA-US01",
          coaName: "US Local CoA",
          bookCode: "BOOK-US01",
          bookName: "US Local Book",
          branches: [
            {
              id: createId("branch"),
              code: "NYC",
              name: "New York Branch",
              unitType: "BRANCH",
              hasSubledger: true,
            },
          ],
        },
      ],
    });
    setError("");
    setMessage(l("Sample template loaded.", "Ornek sablon yuklendi."));
  }

  function resetForm() {
    setForm(createInitialForm());
    setResult(null);
    setError("");
    setMessage("");
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setError("");
    setMessage("");
    setResult(null);

    if (!canSetupCompany) {
      setError(l("Missing permission: onboarding.company.setup", "Eksik yetki: onboarding.company.setup"));
      return;
    }

    const validationError = validateForm(form, l);
    if (validationError) {
      setError(validationError);
      return;
    }

    const payload = {
      groupCompany: {
        code: form.groupCompany.code.trim(),
        name: form.groupCompany.name.trim(),
      },
      fiscalCalendar: {
        code: form.fiscalCalendar.code.trim(),
        name: form.fiscalCalendar.name.trim(),
        yearStartMonth: Number(form.fiscalCalendar.yearStartMonth),
        yearStartDay: Number(form.fiscalCalendar.yearStartDay),
      },
      fiscalYear: Number(form.fiscalYear),
      legalEntities: form.legalEntities.map(compactEntityPayload),
    };

    setSubmitting(true);
    try {
      const response = await bootstrapCompany(payload);
      setResult(response);
      setMessage(l("Company bootstrap completed successfully.", "Sirket temel kurulumu basariyla tamamlandi."));
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to bootstrap company.", "Sirket kurulumu tamamlanamadi."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="space-y-4">
      <TenantReadinessChecklist />

      <div className="rounded-2xl border border-slate-200 bg-white p-5">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h1 className="text-xl font-semibold text-slate-900">
              {l("Company Onboarding Wizard", "Sirket Kurulum Sihirbazi")}
            </h1>
            <p className="mt-1 text-sm text-slate-600">
              {l(
                "Creates group company, fiscal calendar/periods, legal entities, branches, default CoA, accounts, books, and default payment terms in one flow.",
                "Tek akisla grup sirketi, mali takvim/donemler, istirakler / bagli ortaklar, subeler, varsayilan hesap plani, hesaplar, defterler ve varsayilan odeme kosullarini olusturur."
              )}
            </p>
          </div>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={loadSample}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50"
            >
              {l("Load Sample", "Ornek Yukle")}
            </button>
            <button
              type="button"
              onClick={resetForm}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50"
            >
              {l("Reset", "Sifirla")}
            </button>
          </div>
        </div>
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

      <form onSubmit={handleSubmit} className="space-y-4">
        <section className="rounded-2xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Group Company", "Grup Sirketi")}
          </h2>
          <div className="grid gap-3 md:grid-cols-2">
            <input
              value={form.groupCompany.code}
              onChange={(event) => setGroupCompanyField("code", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Code (e.g. GLOBAL)", "Kod (orn. GLOBAL)")}
              required
            />
            <input
              value={form.groupCompany.name}
              onChange={(event) => setGroupCompanyField("name", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Name", "Ad")}
              required
            />
          </div>
        </section>

        <section className="rounded-2xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Fiscal Calendar", "Mali Takvim")}
          </h2>
          <div className="grid gap-3 md:grid-cols-5">
            <input
              value={form.fiscalCalendar.code}
              onChange={(event) => setFiscalCalendarField("code", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Calendar code", "Takvim kodu")}
              required
            />
            <input
              value={form.fiscalCalendar.name}
              onChange={(event) => setFiscalCalendarField("name", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("Calendar name", "Takvim adi")}
              required
            />
            <input
              type="number"
              min={1}
              max={12}
              value={form.fiscalCalendar.yearStartMonth}
              onChange={(event) =>
                setFiscalCalendarField("yearStartMonth", event.target.value)
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Start month", "Baslangic ayi")}
              required
            />
            <input
              type="number"
              min={1}
              max={31}
              value={form.fiscalCalendar.yearStartDay}
              onChange={(event) =>
                setFiscalCalendarField("yearStartDay", event.target.value)
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Start day", "Baslangic gunu")}
              required
            />
          </div>
          <div className="mt-3 grid gap-3 md:w-52">
            <input
              type="number"
              min={2000}
              value={form.fiscalYear}
              onChange={(event) =>
                setForm((prev) => ({ ...prev, fiscalYear: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Fiscal year", "Mali yil")}
              required
            />
          </div>
        </section>

        <section className="rounded-2xl border border-slate-200 bg-white p-4">
          <div className="mb-3 flex items-center justify-between gap-2">
            <h2 className="text-sm font-semibold text-slate-700">
              {l("Legal Entities", "Istirakler / Bagli Ortaklar")} ({entityCount})
            </h2>
            <button
              type="button"
              onClick={addEntity}
              className="rounded-lg border border-slate-300 px-3 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-50"
            >
              {l("Add Legal Entity", "Istirak / Bagli Ortak Ekle")}
            </button>
          </div>

          <div className="space-y-4">
            {form.legalEntities.map((entity, entityIndex) => (
              <article
                key={entity.id}
                className="rounded-xl border border-slate-200 bg-slate-50/50 p-4"
              >
                <div className="mb-3 flex items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold text-slate-700">
                    {l("Entity", "Birim")} {entityIndex + 1}
                  </h3>
                  <button
                    type="button"
                    onClick={() => removeEntity(entity.id)}
                    disabled={form.legalEntities.length <= 1}
                    className="rounded-lg border border-rose-200 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-50 disabled:opacity-50"
                  >
                    {l("Remove", "Kaldir")}
                  </button>
                </div>

                <div className="grid gap-3 md:grid-cols-4">
                  <input
                    value={entity.code}
                    onChange={(event) =>
                      setEntityField(entity.id, "code", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("Entity code", "Birim kodu")}
                    required
                  />
                  <input
                    value={entity.name}
                    onChange={(event) =>
                      setEntityField(entity.id, "name", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
                    placeholder={l("Entity name", "Birim adi")}
                    required
                  />
                  <input
                    value={entity.taxId}
                    onChange={(event) =>
                      setEntityField(entity.id, "taxId", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("Tax ID (optional)", "Vergi No (opsiyonel)")}
                  />
                  <input
                    value={entity.countryIso2}
                    onChange={(event) =>
                      setEntityField(entity.id, "countryIso2", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("Country ISO2 (e.g. US)", "Ulke ISO2 (orn. US)")}
                    maxLength={2}
                    required
                  />
                  <input
                    value={entity.functionalCurrencyCode}
                    onChange={(event) =>
                      setEntityField(
                        entity.id,
                        "functionalCurrencyCode",
                        event.target.value
                      )
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("Functional currency (e.g. USD)", "Fonksiyonel para birimi (orn. USD)")}
                    maxLength={3}
                    required
                  />
                  <input
                    value={entity.coaCode}
                    onChange={(event) =>
                      setEntityField(entity.id, "coaCode", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("CoA code (optional)", "Hesap plani kodu (opsiyonel)")}
                  />
                  <input
                    value={entity.coaName}
                    onChange={(event) =>
                      setEntityField(entity.id, "coaName", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("CoA name (optional)", "Hesap plani adi (opsiyonel)")}
                  />
                  <input
                    value={entity.bookCode}
                    onChange={(event) =>
                      setEntityField(entity.id, "bookCode", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                    placeholder={l("Book code (optional)", "Defter kodu (opsiyonel)")}
                  />
                  <input
                    value={entity.bookName}
                    onChange={(event) =>
                      setEntityField(entity.id, "bookName", event.target.value)
                    }
                    className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
                    placeholder={l("Book name (optional)", "Defter adi (opsiyonel)")}
                  />
                </div>

                <div className="mt-3 flex flex-wrap gap-4">
                  <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                    <input
                      type="checkbox"
                      checked={entity.isIntercompanyEnabled}
                      onChange={(event) =>
                        setEntityField(
                          entity.id,
                          "isIntercompanyEnabled",
                          event.target.checked
                        )
                      }
                    />
                    {l("Intercompany enabled", "Intercompany aktif")}
                  </label>
                  <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                    <input
                      type="checkbox"
                      checked={entity.intercompanyPartnerRequired}
                      onChange={(event) =>
                        setEntityField(
                          entity.id,
                          "intercompanyPartnerRequired",
                          event.target.checked
                        )
                      }
                    />
                    {l("Intercompany partner required", "Intercompany karsi taraf zorunlu")}
                  </label>
                </div>

                <div className="mt-4 rounded-lg border border-slate-200 bg-white p-3">
                  <div className="mb-2 flex items-center justify-between">
                    <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-600">
                      {l("Branches", "Subeler")}
                    </h4>
                    <button
                      type="button"
                      onClick={() => addBranch(entity.id)}
                      className="rounded-lg border border-slate-300 px-2 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-50"
                    >
                      {l("Add Branch", "Sube Ekle")}
                    </button>
                  </div>

                  <div className="space-y-2">
                    {entity.branches.map((branch) => (
                      <div
                        key={branch.id}
                        className="grid gap-2 rounded-lg border border-slate-200 p-2 md:grid-cols-12"
                      >
                        <input
                          value={branch.code}
                          onChange={(event) =>
                            setBranchField(
                              entity.id,
                              branch.id,
                              "code",
                              event.target.value
                            )
                          }
                          className="rounded-lg border border-slate-300 px-2 py-1.5 text-xs md:col-span-2"
                          placeholder={l("Branch code", "Sube kodu")}
                        />
                        <input
                          value={branch.name}
                          onChange={(event) =>
                            setBranchField(
                              entity.id,
                              branch.id,
                              "name",
                              event.target.value
                            )
                          }
                          className="rounded-lg border border-slate-300 px-2 py-1.5 text-xs md:col-span-4"
                          placeholder={l("Branch name", "Sube adi")}
                        />
                        <select
                          value={branch.unitType}
                          onChange={(event) =>
                            setBranchField(
                              entity.id,
                              branch.id,
                              "unitType",
                              event.target.value
                            )
                          }
                          className="rounded-lg border border-slate-300 px-2 py-1.5 text-xs md:col-span-2"
                        >
                          {UNIT_TYPES.map((unitType) => (
                            <option key={unitType} value={unitType}>
                              {unitType}
                            </option>
                          ))}
                        </select>
                        <label className="inline-flex items-center gap-1 text-xs text-slate-700 md:col-span-2">
                          <input
                            type="checkbox"
                            checked={branch.hasSubledger}
                            onChange={(event) =>
                              setBranchField(
                                entity.id,
                                branch.id,
                                "hasSubledger",
                                event.target.checked
                              )
                            }
                          />
                          {l("Subledger", "Alt defter")}
                        </label>
                        <button
                          type="button"
                          onClick={() => removeBranch(entity.id, branch.id)}
                          disabled={entity.branches.length <= 1}
                          className="rounded-lg border border-rose-200 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-50 disabled:opacity-50 md:col-span-2"
                        >
                          {l("Remove", "Kaldir")}
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              </article>
            ))}
          </div>
        </section>

        <div className="flex justify-end">
          <button
            type="submit"
            disabled={submitting || !canSetupCompany}
            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {submitting ? l("Bootstrapping...", "Kurulum calisiyor...") : l("Run Company Bootstrap", "Sirket Kurulumunu Calistir")}
          </button>
        </div>
      </form>

      {result && (
        <section className="rounded-2xl border border-emerald-200 bg-emerald-50/40 p-4">
          <h2 className="text-sm font-semibold text-emerald-900">
            {l("Bootstrap Result", "Kurulum Sonucu")}
          </h2>
          <div className="mt-2 grid gap-2 text-sm text-emerald-900 md:grid-cols-5">
            <div>
              <span className="font-semibold">{l("Tenant:", "Kiraci:")}</span> {result.tenantId}
            </div>
            <div>
              <span className="font-semibold">{l("Group ID:", "Grup ID:")}</span> {result.groupCompanyId}
            </div>
            <div>
              <span className="font-semibold">{l("Calendar ID:", "Takvim ID:")}</span> {result.calendarId}
            </div>
            <div>
              <span className="font-semibold">{l("Periods:", "Donemler:")}</span> {result.periodsGenerated}
            </div>
            <div>
              <span className="font-semibold">{l("Payment terms:", "Odeme kosullari:")}</span>{" "}
              +{Number(result?.paymentTerms?.createdCount || 0)} /{" "}
              {l("skipped", "atlandi")} {Number(result?.paymentTerms?.skippedCount || 0)}
            </div>
          </div>
          <div className="mt-3 overflow-x-auto rounded-lg border border-emerald-200 bg-white">
            <table className="min-w-full text-sm">
              <thead className="bg-emerald-100/70 text-left text-emerald-900">
                <tr>
                  <th className="px-3 py-2">{l("Entity Code", "Birim Kodu")}</th>
                  <th className="px-3 py-2">{l("Legal Entity ID", "Istirak / Bagli Ortak ID")}</th>
                  <th className="px-3 py-2">{l("CoA Code", "Hesap Plani Kodu")}</th>
                  <th className="px-3 py-2">{l("CoA ID", "Hesap Plani ID")}</th>
                  <th className="px-3 py-2">{l("Branch Count", "Sube Sayisi")}</th>
                </tr>
              </thead>
              <tbody>
                {(result.legalEntities || []).map((entity) => (
                  <tr key={`${entity.code}-${entity.legalEntityId}`} className="border-t border-emerald-100">
                    <td className="px-3 py-2">{entity.code}</td>
                    <td className="px-3 py-2">{entity.legalEntityId}</td>
                    <td className="px-3 py-2">{entity.coaCode}</td>
                    <td className="px-3 py-2">{entity.coaId}</td>
                    <td className="px-3 py-2">{entity.branchCount}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}

