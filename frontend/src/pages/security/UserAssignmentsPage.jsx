import { useEffect, useMemo, useState } from "react";
import {
  listCountries,
  listGroupCompanies,
  listLegalEntities,
  listOperatingUnits,
  createSecurityUser,
  createRoleAssignment,
  deleteRoleAssignment,
  listRoleAssignments,
  listRoles,
  listUsers,
} from "../../api/rbacAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";

const SCOPE_TYPES = ["TENANT", "GROUP", "COUNTRY", "LEGAL_ENTITY", "OPERATING_UNIT"];
const EFFECTS = ["ALLOW", "DENY"];

export default function UserAssignmentsPage() {
  const { hasPermission, user } = useAuth();
  const { t } = useI18n();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [users, setUsers] = useState([]);
  const [roles, setRoles] = useState([]);
  const [groups, setGroups] = useState([]);
  const [countries, setCountries] = useState([]);
  const [legalEntities, setLegalEntities] = useState([]);
  const [operatingUnits, setOperatingUnits] = useState([]);
  const [assignments, setAssignments] = useState([]);
  const [userForm, setUserForm] = useState({
    name: "",
    email: "",
    password: "",
    status: "ACTIVE",
  });
  const [form, setForm] = useState({
    userId: "",
    roleId: "",
    scopeType: "TENANT",
    scopeId: "",
    effect: "ALLOW",
  });
  const canUpsertAssignments = hasPermission("security.role_assignment.upsert");
  const canReadOrgTree = hasPermission("org.tree.read");
  const tenantScopeId = Number(user?.tenant_id || 0);

  const scopeOptions = useMemo(() => {
    if (form.scopeType === "TENANT") {
      return tenantScopeId
        ? [{ id: tenantScopeId, label: `Tenant #${tenantScopeId}` }]
        : [];
    }
    if (form.scopeType === "GROUP") {
      return groups.map((row) => ({
        id: Number(row.id),
        label: `${row.code} - ${row.name}`,
      }));
    }
    if (form.scopeType === "COUNTRY") {
      return countries.map((row) => ({
        id: Number(row.id),
        label: `${row.iso2} - ${row.name}`,
      }));
    }
    if (form.scopeType === "LEGAL_ENTITY") {
      return legalEntities.map((row) => ({
        id: Number(row.id),
        label: `${row.code} - ${row.name}`,
      }));
    }
    if (form.scopeType === "OPERATING_UNIT") {
      return operatingUnits.map((row) => ({
        id: Number(row.id),
        label: `${row.code} - ${row.name}`,
      }));
    }
    return [];
  }, [
    form.scopeType,
    tenantScopeId,
    groups,
    countries,
    legalEntities,
    operatingUnits,
  ]);

  async function loadData() {
    setLoading(true);
    setError("");
    try {
      const [
        usersRes,
        rolesRes,
        assignmentsRes,
        groupsRes,
        countriesRes,
        legalEntitiesRes,
        unitsRes,
      ] = await Promise.all([
        listUsers(),
        listRoles(),
        listRoleAssignments(),
        canReadOrgTree ? listGroupCompanies() : Promise.resolve({ rows: [] }),
        canReadOrgTree ? listCountries() : Promise.resolve({ rows: [] }),
        canReadOrgTree ? listLegalEntities() : Promise.resolve({ rows: [] }),
        canReadOrgTree ? listOperatingUnits() : Promise.resolve({ rows: [] }),
      ]);
      setUsers(usersRes?.rows || []);
      setRoles(rolesRes?.rows || []);
      setAssignments(assignmentsRes?.rows || []);
      setGroups(groupsRes?.rows || []);
      setCountries(countriesRes?.rows || []);
      setLegalEntities(legalEntitiesRes?.rows || []);
      setOperatingUnits(unitsRes?.rows || []);
    } catch (err) {
      setError(err?.response?.data?.message || t("userAssignments.loadFailed"));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setForm((prev) => {
      const currentScopeId = Number(prev.scopeId);
      if (
        currentScopeId &&
        scopeOptions.some((option) => Number(option.id) === currentScopeId)
      ) {
        return prev;
      }

      return {
        ...prev,
        scopeId: String(scopeOptions[0]?.id || ""),
      };
    });
  }, [scopeOptions]);

  async function handleCreateUser(event) {
    event.preventDefault();
    if (!canUpsertAssignments) {
      setError(t("userAssignments.missingPermission"));
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      const response = await createSecurityUser({
        name: userForm.name.trim(),
        email: userForm.email.trim(),
        password: userForm.password,
        status: userForm.status,
      });

      const createdUserId = Number(response?.id || 0);
      if (createdUserId > 0) {
        setForm((prev) => ({ ...prev, userId: String(createdUserId) }));
      }
      setUserForm({
        name: "",
        email: "",
        password: "",
        status: "ACTIVE",
      });
      setMessage(t("userAssignments.userCreateSuccess"));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || t("userAssignments.userCreateFailed"));
    } finally {
      setSaving(false);
    }
  }

  async function handleCreate(event) {
    event.preventDefault();
    if (!canUpsertAssignments) {
      setError(t("userAssignments.missingPermission"));
      return;
    }
    const scopeId = Number(form.scopeId);
    if (!Number.isInteger(scopeId) || scopeId <= 0) {
      setError(t("userAssignments.scopeInvalid"));
      return;
    }
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await createRoleAssignment({
        userId: Number(form.userId),
        roleId: Number(form.roleId),
        scopeType: form.scopeType,
        scopeId,
        effect: form.effect,
      });
      setMessage(t("userAssignments.saveSuccess"));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || t("userAssignments.saveFailed"));
    } finally {
      setSaving(false);
    }
  }

  async function handleDelete(assignmentId) {
    if (!canUpsertAssignments) {
      setError(t("userAssignments.missingPermission"));
      return;
    }
    const confirmed = window.confirm(t("userAssignments.deleteConfirm"));
    if (!confirmed) {
      return;
    }

    setSaving(true);
    setError("");
    setMessage("");
    try {
      await deleteRoleAssignment(assignmentId);
      setMessage(t("userAssignments.deleteSuccess"));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || t("userAssignments.deleteFailed"));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {t("userAssignments.title")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">{t("userAssignments.subtitle")}</p>
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

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="mb-3 text-sm font-semibold text-slate-700">
          {t("userAssignments.createUser.title")}
        </h2>
        <form onSubmit={handleCreateUser} className="grid gap-3 md:grid-cols-5">
          <input
            type="text"
            value={userForm.name}
            onChange={(event) =>
              setUserForm((prev) => ({ ...prev, name: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={t("userAssignments.createUser.name")}
            required
          />
          <input
            type="email"
            value={userForm.email}
            onChange={(event) =>
              setUserForm((prev) => ({ ...prev, email: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={t("userAssignments.createUser.email")}
            required
          />
          <input
            type="password"
            minLength={8}
            value={userForm.password}
            onChange={(event) =>
              setUserForm((prev) => ({ ...prev, password: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={t("userAssignments.createUser.password")}
            required
          />
          <select
            value={userForm.status}
            onChange={(event) =>
              setUserForm((prev) => ({ ...prev, status: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          >
            <option value="ACTIVE">{t("userAssignments.createUser.statusActive")}</option>
            <option value="DISABLED">
              {t("userAssignments.createUser.statusDisabled")}
            </option>
          </select>
          <button
            type="submit"
            disabled={saving || !canUpsertAssignments}
            className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {saving
              ? t("userAssignments.createUser.submitting")
              : t("userAssignments.createUser.submit")}
          </button>
        </form>
      </section>

      <form
        onSubmit={handleCreate}
        className="grid gap-3 rounded-xl border border-slate-200 bg-white p-4 md:grid-cols-5"
      >
        <select
          value={form.userId}
          onChange={(event) =>
            setForm((prev) => ({ ...prev, userId: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          required
        >
          <option value="">{t("userAssignments.placeholders.user")}</option>
          {users.map((userRow) => (
            <option key={userRow.id} value={userRow.id}>
              {userRow.name} ({userRow.email})
            </option>
          ))}
        </select>

        <select
          value={form.roleId}
          onChange={(event) =>
            setForm((prev) => ({ ...prev, roleId: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          required
        >
          <option value="">{t("userAssignments.placeholders.role")}</option>
          {roles.map((role) => (
            <option key={role.id} value={role.id}>
              {role.code}
            </option>
          ))}
        </select>

        <select
          value={form.scopeType}
          onChange={(event) =>
            setForm((prev) => ({
              ...prev,
              scopeType: event.target.value,
              scopeId: "",
            }))
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

        {scopeOptions.length > 0 ? (
          <select
            value={form.scopeId}
            onChange={(event) =>
              setForm((prev) => ({ ...prev, scopeId: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            required
          >
            <option value="">{t("userAssignments.placeholders.scope")}</option>
            {scopeOptions.map((option) => (
              <option key={option.id} value={option.id}>
                {option.label}
              </option>
            ))}
          </select>
        ) : (
          <input
            type="number"
            min={1}
            value={form.scopeId}
            onChange={(event) =>
              setForm((prev) => ({ ...prev, scopeId: event.target.value }))
            }
            className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            placeholder={t("userAssignments.placeholders.scopeId")}
            required
          />
        )}

        <div className="flex gap-2">
          <select
            value={form.effect}
            onChange={(event) =>
              setForm((prev) => ({ ...prev, effect: event.target.value }))
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
            disabled={saving || !canUpsertAssignments}
            className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {saving ? t("userAssignments.actions.assigning") : t("userAssignments.actions.assign")}
          </button>
        </div>
      </form>

      <section className="overflow-hidden rounded-xl border border-slate-200 bg-white">
        <div className="border-b border-slate-200 px-4 py-3 text-sm font-semibold text-slate-700">
          {t("userAssignments.list.title")}
        </div>
        {loading ? (
          <p className="px-4 py-3 text-sm text-slate-500">
            {t("userAssignments.list.loading")}
          </p>
        ) : assignments.length === 0 ? (
          <p className="px-4 py-3 text-sm text-slate-500">
            {t("userAssignments.list.empty")}
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-4 py-2">{t("userAssignments.list.user")}</th>
                  <th className="px-4 py-2">{t("userAssignments.list.role")}</th>
                  <th className="px-4 py-2">{t("userAssignments.list.scope")}</th>
                  <th className="px-4 py-2">{t("userAssignments.list.effect")}</th>
                  <th className="px-4 py-2">{t("userAssignments.list.action")}</th>
                </tr>
              </thead>
              <tbody>
                {assignments.map((assignment) => (
                  <tr key={assignment.id} className="border-t border-slate-100">
                    <td className="px-4 py-2">
                      <div className="font-medium text-slate-800">
                        {assignment.user_name}
                      </div>
                      <div className="text-xs text-slate-500">{assignment.user_email}</div>
                    </td>
                    <td className="px-4 py-2">
                      <div className="font-medium text-slate-800">
                        {assignment.role_code}
                      </div>
                      <div className="text-xs text-slate-500">{assignment.role_name}</div>
                    </td>
                    <td className="px-4 py-2">
                      {assignment.scope_type} #{assignment.scope_id}
                    </td>
                    <td className="px-4 py-2">{assignment.effect}</td>
                    <td className="px-4 py-2">
                      <button
                        type="button"
                        disabled={saving || !canUpsertAssignments}
                        onClick={() => handleDelete(assignment.id)}
                        className="rounded-lg border border-rose-200 px-2 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-50 disabled:opacity-60"
                      >
                        {t("userAssignments.actions.delete")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
