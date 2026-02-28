import { useEffect, useMemo, useState } from "react";
import {
  listCountries,
  listDataScopes,
  listGroupCompanies,
  listLegalEntities,
  listOperatingUnits,
  listRoleAssignments,
  listUsers,
  replaceRoleAssignmentScope,
  replaceUserDataScopes,
} from "../../api/rbacAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";

const SCOPE_TYPES = ["TENANT", "GROUP", "COUNTRY", "LEGAL_ENTITY", "OPERATING_UNIT"];
const EFFECTS = ["ALLOW", "DENY"];

function normalizeDataScopeRow(row) {
  return {
    scopeType: String(row.scope_type || "").toUpperCase(),
    scopeId: Number(row.scope_id || 0),
    effect: String(row.effect || "ALLOW").toUpperCase(),
  };
}

function getScopeOptions(scopeType, lookups, tenantScopeId) {
  if (scopeType === "TENANT") {
    return tenantScopeId
      ? [{ id: tenantScopeId, label: `Tenant #${tenantScopeId}` }]
      : [];
  }
  if (scopeType === "GROUP") {
    return lookups.groups.map((row) => ({
      id: Number(row.id),
      label: `${row.code} - ${row.name}`,
    }));
  }
  if (scopeType === "COUNTRY") {
    return lookups.countries.map((row) => ({
      id: Number(row.id),
      label: `${row.iso2} - ${row.name}`,
    }));
  }
  if (scopeType === "LEGAL_ENTITY") {
    return lookups.legalEntities.map((row) => ({
      id: Number(row.id),
      label: `${row.code} - ${row.name}`,
    }));
  }
  if (scopeType === "OPERATING_UNIT") {
    return lookups.operatingUnits.map((row) => ({
      id: Number(row.id),
      label: `${row.code} - ${row.name}`,
    }));
  }
  return [];
}

export default function ScopeAssignmentsPage() {
  const { hasPermission, user } = useAuth();
  const { t } = useI18n();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [users, setUsers] = useState([]);
  const [selectedUserId, setSelectedUserId] = useState("");
  const [dataScopes, setDataScopes] = useState([]);
  const [draftScope, setDraftScope] = useState({
    scopeType: "GROUP",
    scopeId: "",
    effect: "ALLOW",
  });
  const [assignments, setAssignments] = useState([]);
  const [selectedAssignmentId, setSelectedAssignmentId] = useState("");
  const [assignmentForm, setAssignmentForm] = useState({
    scopeType: "GROUP",
    scopeId: "",
    effect: "ALLOW",
  });
  const [lookups, setLookups] = useState({
    groups: [],
    countries: [],
    legalEntities: [],
    operatingUnits: [],
  });
  const tenantScopeId = Number(user?.tenant_id || 0);
  const canReadOrgTree = hasPermission("org.tree.read");
  const canReplaceDataScopes = hasPermission("security.data_scope.upsert");
  const canReplaceRoleAssignmentScope = hasPermission(
    "security.role_assignment.upsert"
  );

  async function loadUserListAndLookups() {
    setLoading(true);
    setError("");
    try {
      const [usersRes, groupsRes, countriesRes, entitiesRes, unitsRes] =
        await Promise.all([
          listUsers(),
          canReadOrgTree ? listGroupCompanies() : Promise.resolve({ rows: [] }),
          canReadOrgTree ? listCountries() : Promise.resolve({ rows: [] }),
          canReadOrgTree ? listLegalEntities() : Promise.resolve({ rows: [] }),
          canReadOrgTree ? listOperatingUnits() : Promise.resolve({ rows: [] }),
        ]);

      const userRows = usersRes?.rows || [];
      setUsers(userRows);
      setLookups({
        groups: groupsRes?.rows || [],
        countries: countriesRes?.rows || [],
        legalEntities: entitiesRes?.rows || [],
        operatingUnits: unitsRes?.rows || [],
      });

      if (userRows[0] && !selectedUserId) {
        setSelectedUserId(String(userRows[0].id));
      }
    } catch (err) {
      setError(err?.response?.data?.message || t("scopeAssignments.loadLookupsFailed"));
    } finally {
      setLoading(false);
    }
  }

  async function loadUserScopeData(userId) {
    if (!userId) {
      setDataScopes([]);
      setAssignments([]);
      return;
    }
    try {
      const [dataScopesRes, assignmentsRes] = await Promise.all([
        listDataScopes({ userId }),
        listRoleAssignments({ userId }),
      ]);
      setDataScopes((dataScopesRes?.rows || []).map(normalizeDataScopeRow));
      setAssignments(assignmentsRes?.rows || []);
    } catch (err) {
      setError(err?.response?.data?.message || t("scopeAssignments.loadUserScopeFailed"));
    }
  }

  useEffect(() => {
    loadUserListAndLookups();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    loadUserScopeData(selectedUserId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedUserId]);

  const scopeOptions = useMemo(() => {
    return getScopeOptions(draftScope.scopeType, lookups, tenantScopeId);
  }, [draftScope.scopeType, lookups, tenantScopeId]);

  const assignmentScopeOptions = useMemo(() => {
    return getScopeOptions(assignmentForm.scopeType, lookups, tenantScopeId);
  }, [assignmentForm.scopeType, lookups, tenantScopeId]);

  function addDataScope() {
    const scopeId =
      draftScope.scopeType === "TENANT" && tenantScopeId
        ? tenantScopeId
        : Number(draftScope.scopeId);
    if (!scopeId) {
      setError(t("scopeAssignments.scopeRequired"));
      return;
    }

    const nextScope = {
      scopeType: draftScope.scopeType,
      scopeId,
      effect: draftScope.effect,
    };

    setDataScopes((prev) => {
      const withoutDuplicate = prev.filter(
        (row) =>
          !(
            row.scopeType === nextScope.scopeType &&
            row.scopeId === nextScope.scopeId
          )
      );
      return [...withoutDuplicate, nextScope];
    });
  }

  function removeDataScope(scopeType, scopeId) {
    setDataScopes((prev) =>
      prev.filter(
        (row) => !(row.scopeType === scopeType && Number(row.scopeId) === Number(scopeId))
      )
    );
  }

  async function handleReplaceDataScopes() {
    if (!selectedUserId) {
      return;
    }
    if (!canReplaceDataScopes) {
      setError(t("scopeAssignments.missingDataScopePermission"));
      return;
    }
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await replaceUserDataScopes(Number(selectedUserId), dataScopes);
      setMessage(t("scopeAssignments.replaceScopesSuccess"));
      await loadUserScopeData(selectedUserId);
    } catch (err) {
      setError(err?.response?.data?.message || t("scopeAssignments.replaceScopesFailed"));
    } finally {
      setSaving(false);
    }
  }

  async function handleReplaceAssignmentScope(event) {
    event.preventDefault();
    if (!selectedAssignmentId) {
      return;
    }
    if (!canReplaceRoleAssignmentScope) {
      setError(t("scopeAssignments.missingAssignmentPermission"));
      return;
    }
    setSaving(true);
    setError("");
    setMessage("");
    try {
      const scopeId =
        assignmentForm.scopeType === "TENANT" && tenantScopeId
          ? tenantScopeId
          : Number(assignmentForm.scopeId);
      if (!scopeId) {
        setError(t("scopeAssignments.scopeRequired"));
        setSaving(false);
        return;
      }

      await replaceRoleAssignmentScope(Number(selectedAssignmentId), {
        scopeType: assignmentForm.scopeType,
        scopeId,
        effect: assignmentForm.effect,
      });
      setMessage(t("scopeAssignments.replaceAssignmentSuccess"));
      await loadUserScopeData(selectedUserId);
    } catch (err) {
      setError(
        err?.response?.data?.message || t("scopeAssignments.replaceAssignmentFailed")
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {t("scopeAssignments.title")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">{t("scopeAssignments.subtitle")}</p>
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

      <div className="rounded-xl border border-slate-200 bg-white p-4">
        <label className="mb-2 block text-sm font-medium text-slate-700">
          {t("scopeAssignments.userLabel")}
        </label>
        <select
          value={selectedUserId}
          onChange={(event) => setSelectedUserId(event.target.value)}
          className="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm md:w-[420px]"
        >
          <option value="">{t("scopeAssignments.userPlaceholder")}</option>
          {users.map((userRow) => (
            <option key={userRow.id} value={userRow.id}>
              {userRow.name} ({userRow.email})
            </option>
          ))}
        </select>
      </div>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("scopeAssignments.dataScopesTitle")}
        </h2>
        <div className="grid gap-2 md:grid-cols-5">
          <select
            value={draftScope.scopeType}
            onChange={(event) =>
              setDraftScope((prev) => {
                const scopeType = event.target.value;
                const options = getScopeOptions(scopeType, lookups, tenantScopeId);
                return {
                  ...prev,
                  scopeType,
                  scopeId: String(options[0]?.id || ""),
                };
              })
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            {SCOPE_TYPES.map((scopeType) => (
              <option key={scopeType} value={scopeType}>
                {scopeType}
              </option>
            ))}
          </select>

          <select
            value={draftScope.scopeId}
            onChange={(event) =>
              setDraftScope((prev) => ({ ...prev, scopeId: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
          >
            <option value="">{t("scopeAssignments.selectScope")}</option>
            {scopeOptions.map((option) => (
              <option key={option.id} value={option.id}>
                {option.label}
              </option>
            ))}
          </select>

          <select
            value={draftScope.effect}
            onChange={(event) =>
              setDraftScope((prev) => ({ ...prev, effect: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            {EFFECTS.map((effect) => (
              <option key={effect} value={effect}>
                {effect}
              </option>
            ))}
          </select>

          <button
            type="button"
            onClick={addDataScope}
            disabled={!canReplaceDataScopes}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-50"
          >
            {t("scopeAssignments.addScope")}
          </button>
        </div>

        <div className="mt-3 overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{t("scopeAssignments.columns.scopeType")}</th>
                <th className="px-3 py-2">{t("scopeAssignments.columns.scopeId")}</th>
                <th className="px-3 py-2">{t("scopeAssignments.columns.effect")}</th>
                <th className="px-3 py-2">{t("scopeAssignments.columns.action")}</th>
              </tr>
            </thead>
            <tbody>
              {dataScopes.map((scope) => (
                <tr
                  key={`${scope.scopeType}-${scope.scopeId}`}
                  className="border-t border-slate-100"
                >
                  <td className="px-3 py-2">{scope.scopeType}</td>
                  <td className="px-3 py-2">{scope.scopeId}</td>
                  <td className="px-3 py-2">{scope.effect}</td>
                  <td className="px-3 py-2">
                    <button
                      type="button"
                      onClick={() => removeDataScope(scope.scopeType, scope.scopeId)}
                      disabled={!canReplaceDataScopes}
                      className="rounded-lg border border-rose-200 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-50"
                    >
                      {t("scopeAssignments.removeScope")}
                    </button>
                  </td>
                </tr>
              ))}
              {dataScopes.length === 0 && (
                <tr>
                  <td className="px-3 py-3 text-slate-500" colSpan={4}>
                    {t("scopeAssignments.emptyScopes")}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <button
          type="button"
          disabled={!selectedUserId || saving || !canReplaceDataScopes}
          onClick={handleReplaceDataScopes}
          className="mt-3 rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
        >
          {saving ? t("scopeAssignments.saving") : t("scopeAssignments.replaceScopesButton")}
        </button>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("scopeAssignments.assignmentTitle")}
        </h2>
        <form onSubmit={handleReplaceAssignmentScope} className="grid gap-2 md:grid-cols-5">
          <select
            value={selectedAssignmentId}
            onChange={(event) => {
              const assignmentId = event.target.value;
              setSelectedAssignmentId(assignmentId);
              const selected = assignments.find(
                (row) => Number(row.id) === Number(assignmentId)
              );
              if (selected) {
                setAssignmentForm({
                  scopeType: String(selected.scope_type || "GROUP"),
                  scopeId: String(selected.scope_id || ""),
                  effect: String(selected.effect || "ALLOW"),
                });
              }
            }}
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
            required
          >
            <option value="">{t("scopeAssignments.selectAssignment")}</option>
            {assignments.map((assignment) => (
              <option key={assignment.id} value={assignment.id}>
                #{assignment.id} {assignment.user_email} {"->"} {assignment.role_code} (
                {assignment.scope_type}:{assignment.scope_id})
              </option>
            ))}
          </select>

          <select
            value={assignmentForm.scopeType}
            onChange={(event) =>
              setAssignmentForm((prev) => {
                const scopeType = event.target.value;
                const options = getScopeOptions(scopeType, lookups, tenantScopeId);
                return {
                  ...prev,
                  scopeType,
                  scopeId: String(options[0]?.id || ""),
                };
              })
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            required
          >
            {SCOPE_TYPES.map((scopeType) => (
              <option key={scopeType} value={scopeType}>
                {scopeType}
              </option>
            ))}
          </select>

          {assignmentScopeOptions.length > 0 ? (
            <select
              value={assignmentForm.scopeId}
              onChange={(event) =>
                setAssignmentForm((prev) => ({ ...prev, scopeId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{t("scopeAssignments.selectScope")}</option>
              {assignmentScopeOptions.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.label}
                </option>
              ))}
            </select>
          ) : (
            <input
              type="number"
              min={1}
              value={assignmentForm.scopeId}
              onChange={(event) =>
                setAssignmentForm((prev) => ({ ...prev, scopeId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={t("scopeAssignments.columns.scopeId")}
              required
            />
          )}

          <div className="flex gap-2">
            <select
              value={assignmentForm.effect}
              onChange={(event) =>
                setAssignmentForm((prev) => ({ ...prev, effect: event.target.value }))
              }
              className="min-w-0 flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              {EFFECTS.map((effect) => (
                <option key={effect} value={effect}>
                  {effect}
                </option>
              ))}
            </select>
            <button
              type="submit"
              disabled={
                saving ||
                !selectedAssignmentId ||
                !canReplaceRoleAssignmentScope
              }
              className="rounded-lg bg-cyan-700 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving
                ? t("scopeAssignments.saving")
                : t("scopeAssignments.replaceAssignmentButton")}
            </button>
          </div>
        </form>
      </section>

      {loading && <p className="text-sm text-slate-500">{t("scopeAssignments.loading")}</p>}
    </div>
  );
}
