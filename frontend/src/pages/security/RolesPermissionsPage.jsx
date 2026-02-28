import { useEffect, useMemo, useState } from "react";
import {
  createOrUpdateRole,
  listPermissions,
  listRoles,
  replaceRolePermissions,
} from "../../api/rbacAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";

export default function RolesPermissionsPage() {
  const { hasPermission } = useAuth();
  const { t } = useI18n();
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [roles, setRoles] = useState([]);
  const [permissions, setPermissions] = useState([]);
  const [selectedRoleId, setSelectedRoleId] = useState(null);
  const [selectedPermissionCodes, setSelectedPermissionCodes] = useState([]);
  const [roleForm, setRoleForm] = useState({ code: "", name: "" });

  async function loadData() {
    setLoading(true);
    setError("");
    try {
      const [rolesRes, permissionsRes] = await Promise.all([
        listRoles({ includePermissions: true }),
        listPermissions(),
      ]);
      const roleRows = rolesRes?.rows || [];
      setRoles(roleRows);
      setPermissions(permissionsRes?.rows || []);

      const selected = selectedRoleId
        ? roleRows.find((row) => Number(row.id) === Number(selectedRoleId))
        : roleRows[0];

      if (selected) {
        setSelectedRoleId(selected.id);
        setSelectedPermissionCodes(selected.permissionCodes || []);
      } else {
        setSelectedRoleId(null);
        setSelectedPermissionCodes([]);
      }
    } catch (err) {
      setError(err?.response?.data?.message || t("rolesPermissions.errors.loadFailed"));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const selectedRole = useMemo(
    () => roles.find((row) => Number(row.id) === Number(selectedRoleId)) || null,
    [roles, selectedRoleId]
  );
  const canUpsertRole = hasPermission("security.role.upsert");
  const canReplaceRolePermissions = hasPermission(
    "security.role_permissions.assign"
  );

  function togglePermission(permissionCode) {
    setSelectedPermissionCodes((prev) => {
      if (prev.includes(permissionCode)) {
        return prev.filter((code) => code !== permissionCode);
      }
      return [...prev, permissionCode];
    });
  }

  async function handleCreateRole(event) {
    event.preventDefault();
    if (!canUpsertRole) {
      setError(t("rolesPermissions.errors.missingUpsertPermission"));
      return;
    }
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await createOrUpdateRole({
        code: roleForm.code.trim(),
        name: roleForm.name.trim(),
      });
      setRoleForm({ code: "", name: "" });
      setMessage(t("rolesPermissions.messages.roleSaved"));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || t("rolesPermissions.errors.saveRoleFailed"));
    } finally {
      setSaving(false);
    }
  }

  async function handleReplacePermissions() {
    if (!selectedRoleId) {
      return;
    }
    if (!canReplaceRolePermissions) {
      setError(t("rolesPermissions.errors.missingAssignPermission"));
      return;
    }
    setSaving(true);
    setError("");
    setMessage("");
    try {
      await replaceRolePermissions(selectedRoleId, selectedPermissionCodes);
      setMessage(t("rolesPermissions.messages.permissionsReplaced"));
      await loadData();
    } catch (err) {
      setError(
        err?.response?.data?.message || t("rolesPermissions.errors.replacePermissionsFailed")
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-slate-900">
          {t("rolesPermissions.title")}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {t("rolesPermissions.subtitle")}
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

      <form
        onSubmit={handleCreateRole}
        className="grid gap-3 rounded-xl border border-slate-200 bg-white p-4 md:grid-cols-4"
      >
        <input
          value={roleForm.code}
          onChange={(event) =>
            setRoleForm((prev) => ({ ...prev, code: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
          placeholder={t("rolesPermissions.placeholders.roleCode")}
          required
        />
        <input
          value={roleForm.name}
          onChange={(event) =>
            setRoleForm((prev) => ({ ...prev, name: event.target.value }))
          }
          className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
          placeholder={t("rolesPermissions.placeholders.roleName")}
          required
        />
        <button
          type="submit"
          disabled={saving || !canUpsertRole}
          className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
        >
          {saving ? t("rolesPermissions.actions.saving") : t("rolesPermissions.actions.saveRole")}
        </button>
      </form>

      <div className="grid gap-4 md:grid-cols-[300px_1fr]">
        <section className="rounded-xl border border-slate-200 bg-white p-3">
          <h2 className="mb-2 text-sm font-semibold text-slate-700">
            {t("rolesPermissions.sections.roles")}
          </h2>
          {loading ? (
            <p className="text-sm text-slate-500">{t("rolesPermissions.sections.loadingRoles")}</p>
          ) : (
            <div className="space-y-1">
              {roles.map((role) => (
                <button
                  key={role.id}
                  type="button"
                  onClick={() => {
                    setSelectedRoleId(role.id);
                    setSelectedPermissionCodes(role.permissionCodes || []);
                  }}
                  className={`w-full rounded-lg px-3 py-2 text-left text-sm ${
                    Number(role.id) === Number(selectedRoleId)
                      ? "bg-slate-900 text-white"
                      : "bg-slate-50 text-slate-700 hover:bg-slate-100"
                  }`}
                >
                  <div className="font-semibold">{role.code}</div>
                  <div className="text-xs opacity-80">{role.name}</div>
                </button>
              ))}
            </div>
          )}
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-3">
          <div className="mb-3 flex items-center justify-between gap-2">
            <h2 className="text-sm font-semibold text-slate-700">
              {selectedRole
                ? t("rolesPermissions.sections.permissionsFor", {
                    code: selectedRole.code,
                  })
                : t("rolesPermissions.sections.permissions")}
            </h2>
            <button
              type="button"
              disabled={!selectedRoleId || saving || !canReplaceRolePermissions}
              onClick={handleReplacePermissions}
              className="rounded-lg bg-cyan-700 px-3 py-2 text-xs font-semibold text-white disabled:opacity-60"
            >
              {saving
                ? t("rolesPermissions.actions.saving")
                : t("rolesPermissions.actions.replacePermissions")}
            </button>
          </div>
          {loading ? (
            <p className="text-sm text-slate-500">
              {t("rolesPermissions.sections.loadingPermissions")}
            </p>
          ) : (
            <div className="grid gap-2 md:grid-cols-2">
              {permissions.map((permission) => {
                const checked = selectedPermissionCodes.includes(permission.code);
                return (
                  <label
                    key={permission.id}
                    className="flex items-start gap-2 rounded-lg border border-slate-200 p-2 text-sm"
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => togglePermission(permission.code)}
                      disabled={!selectedRoleId || !canReplaceRolePermissions}
                      className="mt-0.5"
                    />
                    <span>
                      <span className="font-medium text-slate-800">
                        {permission.code}
                      </span>
                      <span className="block text-xs text-slate-500">
                        {permission.description}
                      </span>
                    </span>
                  </label>
                );
              })}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
