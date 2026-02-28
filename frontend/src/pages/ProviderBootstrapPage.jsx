import { useEffect, useState } from "react";
import {
  createProviderTenant,
  listProviderTenants,
  updateProviderTenantStatus,
} from "../api/providerControl.js";
import { useI18n } from "../i18n/useI18n.js";
import { useProviderAuth } from "../provider/useProviderAuth.js";

function createInitialForm() {
  return {
    tenantCode: "",
    tenantName: "",
    adminName: "",
    adminEmail: "",
    adminPassword: "",
  };
}

function toTenantStatusLabel(t, status) {
  return t(
    ["providerBootstrap", "statuses", String(status || "").toUpperCase()],
    status || "-"
  );
}

export default function ProviderBootstrapPage() {
  const { token, providerAdmin, logout, clearSession } = useProviderAuth();
  const { t } = useI18n();
  const [form, setForm] = useState(createInitialForm());
  const [query, setQuery] = useState("");
  const [loadingTenants, setLoadingTenants] = useState(false);
  const [saving, setSaving] = useState(false);
  const [updatingTenantId, setUpdatingTenantId] = useState(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [result, setResult] = useState(null);
  const [tenants, setTenants] = useState([]);

  function setField(field, value) {
    setForm((prev) => ({ ...prev, [field]: value }));
  }

  async function loadTenants(search = query) {
    if (!token) {
      return;
    }

    setLoadingTenants(true);
    setError("");
    try {
      const response = await listProviderTenants(token, {
        q: search || undefined,
        limit: 100,
        offset: 0,
      });
      setTenants(response?.rows || []);
    } catch (err) {
      if (err?.response?.status === 401) {
        clearSession();
      }
      setError(err?.response?.data?.message || t("providerBootstrap.errors.loadTenants"));
    } finally {
      setLoadingTenants(false);
    }
  }

  useEffect(() => {
    loadTenants();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token]);

  async function handleCreateTenant(event) {
    event.preventDefault();
    if (!token) {
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    setResult(null);
    try {
      const response = await createProviderTenant(token, {
        tenantCode: form.tenantCode.trim().toUpperCase(),
        tenantName: form.tenantName.trim(),
        adminName: form.adminName.trim(),
        adminEmail: form.adminEmail.trim(),
        adminPassword: form.adminPassword,
      });
      setResult(response || null);
      setMessage(t("providerBootstrap.messages.created"));
      setForm(createInitialForm());
      await loadTenants();
    } catch (err) {
      if (err?.response?.status === 401) {
        clearSession();
      }
      setError(err?.response?.data?.message || t("providerBootstrap.errors.provisionFailed"));
    } finally {
      setSaving(false);
    }
  }

  async function handleSetTenantStatus(tenantId, status) {
    if (!token) {
      return;
    }

    setUpdatingTenantId(tenantId);
    setError("");
    setMessage("");
    try {
      await updateProviderTenantStatus(token, tenantId, status);
      setMessage(
        t("providerBootstrap.messages.statusUpdated", {
          id: tenantId,
          status: toTenantStatusLabel(t, status),
        })
      );
      await loadTenants();
    } catch (err) {
      if (err?.response?.status === 401) {
        clearSession();
      }
      setError(
        err?.response?.data?.message || t("providerBootstrap.errors.updateStatus")
      );
    } finally {
      setUpdatingTenantId(null);
    }
  }

  return (
    <main className="min-h-dvh bg-slate-100 p-4 md:p-6">
      <div className="mx-auto max-w-6xl space-y-4">
        <section className="rounded-xl border border-slate-200 bg-white p-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h1 className="text-xl font-semibold text-slate-900">
                {t("providerBootstrap.title")}
              </h1>
              <p className="mt-1 text-sm text-slate-600">
                {t("providerBootstrap.subtitle")}
              </p>
              <p className="mt-2 text-xs text-slate-500">
                {t("providerBootstrap.signedInAs")}{" "}
                <span className="font-semibold text-slate-700">
                  {providerAdmin?.name || providerAdmin?.email || t("providerBootstrap.providerAdminFallback")}
                </span>
              </p>
            </div>
            <button
              type="button"
              onClick={logout}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50"
            >
              {t("providerBootstrap.logout")}
            </button>
          </div>
        </section>

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

        <div className="grid gap-4 xl:grid-cols-[1fr_1.4fr]">
          <section className="rounded-xl border border-slate-200 bg-white p-5">
            <h2 className="text-sm font-semibold text-slate-700">
              {t("providerBootstrap.createTenant.title")}
            </h2>
            <form onSubmit={handleCreateTenant} className="mt-3 grid gap-3">
              <input
                value={form.tenantCode}
                onChange={(event) => setField("tenantCode", event.target.value)}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("providerBootstrap.createTenant.placeholders.tenantCode")}
                required
              />
              <input
                value={form.tenantName}
                onChange={(event) => setField("tenantName", event.target.value)}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("providerBootstrap.createTenant.placeholders.tenantName")}
                required
              />
              <input
                value={form.adminName}
                onChange={(event) => setField("adminName", event.target.value)}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("providerBootstrap.createTenant.placeholders.adminName")}
                required
              />
              <input
                type="email"
                value={form.adminEmail}
                onChange={(event) => setField("adminEmail", event.target.value)}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("providerBootstrap.createTenant.placeholders.adminEmail")}
                required
              />
              <input
                type="password"
                value={form.adminPassword}
                onChange={(event) => setField("adminPassword", event.target.value)}
                className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t(
                  "providerBootstrap.createTenant.placeholders.adminPassword"
                )}
                required
                minLength={8}
              />
              <button
                type="submit"
                disabled={saving}
                className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                {saving
                  ? t("providerBootstrap.createTenant.actions.provisioning")
                  : t("providerBootstrap.createTenant.actions.create")}
              </button>
            </form>

            {result ? (
              <div className="mt-4 rounded-lg border border-emerald-200 bg-emerald-50/40 p-3 text-sm text-emerald-900">
                <h3 className="font-semibold">
                  {t("providerBootstrap.createTenant.result.title")}
                </h3>
                <div className="mt-2 grid gap-1 text-xs">
                  <div>
                    {t("providerBootstrap.createTenant.result.tenant", {
                      id: result.tenantId,
                      code: result.tenantCode,
                    })}
                  </div>
                  <div>
                    {t("providerBootstrap.createTenant.result.admin", {
                      id: result.adminUserId,
                      email: result.adminEmail,
                    })}
                  </div>
                  <div>
                    {t("providerBootstrap.createTenant.result.roleId", {
                      id: result.adminRoleId,
                    })}
                  </div>
                </div>
              </div>
            ) : null}
          </section>

          <section className="rounded-xl border border-slate-200 bg-white p-5">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-sm font-semibold text-slate-700">
                {t("providerBootstrap.directory.title")}
              </h2>
              <button
                type="button"
                onClick={() => loadTenants()}
                disabled={loadingTenants}
                className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 disabled:opacity-60"
              >
                {loadingTenants
                  ? t("providerBootstrap.directory.loading")
                  : t("providerBootstrap.directory.refresh")}
              </button>
            </div>

            <form
              onSubmit={(event) => {
                event.preventDefault();
                loadTenants(query);
              }}
              className="mt-3 flex gap-2"
            >
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm"
                placeholder={t("providerBootstrap.directory.searchPlaceholder")}
              />
              <button
                type="submit"
                disabled={loadingTenants}
                className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                {t("providerBootstrap.directory.search")}
              </button>
            </form>

            <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200">
              <table className="min-w-full text-sm">
                <thead className="bg-slate-50 text-left text-slate-600">
                  <tr>
                    <th className="px-3 py-2">ID</th>
                    <th className="px-3 py-2">{t("providerBootstrap.directory.columns.code")}</th>
                    <th className="px-3 py-2">{t("providerBootstrap.directory.columns.name")}</th>
                    <th className="px-3 py-2">{t("providerBootstrap.directory.columns.status")}</th>
                    <th className="px-3 py-2">{t("providerBootstrap.directory.columns.users")}</th>
                    <th className="px-3 py-2">{t("providerBootstrap.directory.columns.actions")}</th>
                  </tr>
                </thead>
                <tbody>
                  {tenants.map((tenant) => (
                    <tr key={tenant.id} className="border-t border-slate-100">
                      <td className="px-3 py-2">{tenant.id}</td>
                      <td className="px-3 py-2">{tenant.code}</td>
                      <td className="px-3 py-2">{tenant.name}</td>
                      <td className="px-3 py-2">
                        {toTenantStatusLabel(t, tenant.status)}
                      </td>
                      <td className="px-3 py-2">
                        {tenant.activeUserCount}/{tenant.userCount}
                      </td>
                      <td className="px-3 py-2">
                        <div className="flex gap-2">
                          <button
                            type="button"
                            onClick={() => handleSetTenantStatus(tenant.id, "ACTIVE")}
                            disabled={
                              updatingTenantId === tenant.id || tenant.status === "ACTIVE"
                            }
                            className="rounded border border-emerald-300 px-2 py-1 text-xs font-semibold text-emerald-700 disabled:opacity-60"
                          >
                            {t("providerBootstrap.directory.actions.activate")}
                          </button>
                          <button
                            type="button"
                            onClick={() => handleSetTenantStatus(tenant.id, "SUSPENDED")}
                            disabled={
                              updatingTenantId === tenant.id ||
                              tenant.status === "SUSPENDED"
                            }
                            className="rounded border border-amber-300 px-2 py-1 text-xs font-semibold text-amber-700 disabled:opacity-60"
                          >
                            {t("providerBootstrap.directory.actions.suspend")}
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {tenants.length === 0 && !loadingTenants ? (
                    <tr>
                      <td colSpan={6} className="px-3 py-3 text-slate-500">
                        {t("providerBootstrap.directory.empty")}
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      </div>
    </main>
  );
}
