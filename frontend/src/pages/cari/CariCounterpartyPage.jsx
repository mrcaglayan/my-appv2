import { useEffect, useMemo, useState } from "react";
import {
  createCariCounterparty,
  getCariCounterparty,
  listCariCounterparties,
  updateCariCounterparty,
} from "../../api/cariCounterparty.js";
import { listAccounts } from "../../api/glAdmin.js";
import { listCariPaymentTerms } from "../../api/cariPaymentTerms.js";
import { listLegalEntities } from "../../api/orgAdmin.js";
import { useAuth } from "../../auth/useAuth.js";
import CounterpartyForm from "./CounterpartyForm.jsx";
import {
  COUNTERPARTY_LIST_SORT_DIRECTIONS,
  COUNTERPARTY_LIST_SORT_FIELDS,
  ROLE_FILTERS,
  buildCounterpartyListParams,
  buildInitialCounterpartyForm,
  createCounterpartyListFilters,
  mapCounterpartyApiError,
  mapDetailToCounterpartyForm,
  normalizeCounterpartyListSortBy,
  normalizeCounterpartyListSortDir,
  resolveCounterpartyAccountPickerGates,
  toPositiveInt,
} from "./counterpartyFormUtils.js";

const PAGE_CONFIG = {
  buyerCreate: {
    title: "Alici Karti Olustur",
    subtitle: "Musteri odakli yeni cari kart olusturun.",
    mode: "create",
    roleDefault: "CUSTOMER",
  },
  buyerList: {
    title: "Alici Karti Listesi",
    subtitle: "Musteri kartlarini filtreleyin ve duzenleyin.",
    mode: "list",
    roleDefault: "CUSTOMER",
  },
  vendorCreate: {
    title: "Satici Karti Olustur",
    subtitle: "Tedarikci odakli yeni cari kart olusturun.",
    mode: "create",
    roleDefault: "VENDOR",
  },
  vendorList: {
    title: "Satici Karti Listesi",
    subtitle: "Tedarikci kartlarini filtreleyin ve duzenleyin.",
    mode: "list",
    roleDefault: "VENDOR",
  },
};

const SORT_FIELD_LABELS = {
  id: "Newest (ID)",
  code: "Counterparty Code",
  name: "Counterparty Name",
  status: "Status",
  arAccountCode: "AR Account Code",
  arAccountName: "AR Account Name",
  apAccountCode: "AP Account Code",
  apAccountName: "AP Account Name",
};

const SORT_DIRECTION_LABELS = {
  asc: "Ascending",
  desc: "Descending",
};

function roleBadgeClass(role) {
  const normalized = String(role || "").toUpperCase();
  if (normalized === "BOTH") {
    return "bg-violet-100 text-violet-700";
  }
  if (normalized === "VENDOR") {
    return "bg-amber-100 text-amber-800";
  }
  if (normalized === "CUSTOMER") {
    return "bg-emerald-100 text-emerald-700";
  }
  return "bg-slate-200 text-slate-700";
}

function formatMappedAccountLabel(code, name) {
  const codeText = String(code || "").trim();
  const nameText = String(name || "").trim();
  if (!codeText && !nameText) {
    return "-";
  }
  if (codeText && nameText) {
    return `${codeText} - ${nameText}`;
  }
  return codeText || nameText;
}

function normalizeFilterRole(value, fallback) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (!normalized) {
    return fallback;
  }
  if (!ROLE_FILTERS.includes(normalized)) {
    return fallback;
  }
  return normalized;
}

function mapPaymentTermRows(response) {
  if (!Array.isArray(response?.rows)) {
    return [];
  }
  return response.rows.map((row) => ({
    id: Number(row?.id || 0),
    code: String(row?.code || ""),
    name: String(row?.name || ""),
    status: String(row?.status || "ACTIVE").toUpperCase(),
  }));
}

function mapAccountRows(response) {
  if (!Array.isArray(response?.rows)) {
    return [];
  }
  return response.rows.map((row) => ({
    id: Number(row?.id || 0),
    code: String(row?.code || ""),
    name: String(row?.name || ""),
    accountType: String(row?.account_type || row?.accountType || "").toUpperCase(),
    allowPosting: Boolean(row?.allow_posting ?? row?.allowPosting),
    isActive: Boolean(row?.is_active ?? row?.isActive),
  }));
}

export default function CariCounterpartyPage({ pageKey = "buyerList" }) {
  const config = PAGE_CONFIG[pageKey] || PAGE_CONFIG.buyerList;
  const isCreatePage = config.mode === "create";
  const isListPage = config.mode === "list";

  const { hasPermission, permissions } = useAuth();
  const canRead = hasPermission("cari.card.read");
  const canUpsert = hasPermission("cari.card.upsert");
  const canReadOrgTree = hasPermission("org.tree.read");
  const accountPickerGates = useMemo(
    () => resolveCounterpartyAccountPickerGates(permissions),
    [permissions]
  );

  const [legalEntities, setLegalEntities] = useState([]);
  const [legalEntitiesLoading, setLegalEntitiesLoading] = useState(false);
  const [legalEntitiesError, setLegalEntitiesError] = useState("");

  const [createForm, setCreateForm] = useState(() =>
    buildInitialCounterpartyForm(config.roleDefault)
  );
  const [createSaving, setCreateSaving] = useState(false);
  const [createError, setCreateError] = useState("");
  const [createMessage, setCreateMessage] = useState("");
  const [createPaymentTerms, setCreatePaymentTerms] = useState([]);
  const [createPaymentTermsLoading, setCreatePaymentTermsLoading] = useState(false);
  const [createPaymentTermsError, setCreatePaymentTermsError] = useState("");
  const [createAccountOptions, setCreateAccountOptions] = useState([]);
  const [createAccountsLoading, setCreateAccountsLoading] = useState(false);
  const [createAccountsError, setCreateAccountsError] = useState("");

  const [filters, setFilters] = useState(() =>
    createCounterpartyListFilters(config.roleDefault)
  );
  const [rows, setRows] = useState([]);
  const [totalRows, setTotalRows] = useState(0);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState("");

  const [editingId, setEditingId] = useState(null);
  const [editingForm, setEditingForm] = useState(() =>
    buildInitialCounterpartyForm(config.roleDefault)
  );
  const [editLoading, setEditLoading] = useState(false);
  const [editSaving, setEditSaving] = useState(false);
  const [editError, setEditError] = useState("");
  const [editMessage, setEditMessage] = useState("");
  const [editPaymentTerms, setEditPaymentTerms] = useState([]);
  const [editPaymentTermsLoading, setEditPaymentTermsLoading] = useState(false);
  const [editPaymentTermsError, setEditPaymentTermsError] = useState("");
  const [editAccountOptions, setEditAccountOptions] = useState([]);
  const [editAccountsLoading, setEditAccountsLoading] = useState(false);
  const [editAccountsError, setEditAccountsError] = useState("");

  const legalEntityById = useMemo(() => {
    const map = new Map();
    for (const row of legalEntities) {
      map.set(String(row.id), row);
    }
    return map;
  }, [legalEntities]);

  useEffect(() => {
    setCreateForm(buildInitialCounterpartyForm(config.roleDefault));
    setFilters(createCounterpartyListFilters(config.roleDefault));
    setRows([]);
    setTotalRows(0);
    setEditingId(null);
    setEditingForm(buildInitialCounterpartyForm(config.roleDefault));
    setCreateError("");
    setCreateMessage("");
    setListError("");
    setEditError("");
    setEditMessage("");
    setCreatePaymentTerms([]);
    setCreatePaymentTermsError("");
    setCreateAccountOptions([]);
    setCreateAccountsError("");
    setEditPaymentTerms([]);
    setEditPaymentTermsError("");
    setEditAccountOptions([]);
    setEditAccountsError("");
  }, [config.roleDefault, config.mode]);

  useEffect(() => {
    let cancelled = false;
    async function loadLegalEntityOptions() {
      if (!canReadOrgTree) {
        setLegalEntities([]);
        setLegalEntitiesError(
          "Legal entity list permission missing. You can still type legalEntityId manually."
        );
        return;
      }

      setLegalEntitiesLoading(true);
      setLegalEntitiesError("");
      try {
        const response = await listLegalEntities({ limit: 500, includeInactive: true });
        if (cancelled) {
          return;
        }
        setLegalEntities(Array.isArray(response?.rows) ? response.rows : []);
      } catch (err) {
        if (cancelled) {
          return;
        }
        setLegalEntities([]);
        setLegalEntitiesError(
          String(err?.response?.data?.message || "Failed to load legal entities.")
        );
      } finally {
        if (!cancelled) {
          setLegalEntitiesLoading(false);
        }
      }
    }

    loadLegalEntityOptions();
    return () => {
      cancelled = true;
    };
  }, [canReadOrgTree]);

  useEffect(() => {
    if (!isCreatePage) {
      return;
    }
    if (createForm.legalEntityId) {
      return;
    }
    if (!Array.isArray(legalEntities) || legalEntities.length === 0) {
      return;
    }
    setCreateForm((prev) => ({
      ...prev,
      legalEntityId: String(legalEntities[0].id || ""),
    }));
  }, [createForm.legalEntityId, isCreatePage, legalEntities]);

  useEffect(() => {
    let cancelled = false;
    async function loadCreatePaymentTerms() {
      if (!isCreatePage) {
        setCreatePaymentTerms([]);
        setCreatePaymentTermsError("");
        setCreatePaymentTermsLoading(false);
        return;
      }

      await loadPaymentTermsForLegalEntity({
        legalEntityId: createForm.legalEntityId,
        setRows: (rows) => {
          if (!cancelled) {
            setCreatePaymentTerms(rows);
          }
        },
        setLoading: (loading) => {
          if (!cancelled) {
            setCreatePaymentTermsLoading(loading);
          }
        },
        setError: (error) => {
          if (!cancelled) {
            setCreatePaymentTermsError(error);
          }
        },
      });
    }

    loadCreatePaymentTerms();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isCreatePage, createForm.legalEntityId, canRead]);

  useEffect(() => {
    let cancelled = false;
    async function loadCreateAccounts() {
      if (!isCreatePage) {
        setCreateAccountOptions([]);
        setCreateAccountsError("");
        setCreateAccountsLoading(false);
        return;
      }

      await loadAccountsForLegalEntity({
        legalEntityId: createForm.legalEntityId,
        setRows: (rows) => {
          if (!cancelled) {
            setCreateAccountOptions(rows);
          }
        },
        setLoading: (loading) => {
          if (!cancelled) {
            setCreateAccountsLoading(loading);
          }
        },
        setError: (error) => {
          if (!cancelled) {
            setCreateAccountsError(error);
          }
        },
      });
    }

    loadCreateAccounts();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isCreatePage, createForm.legalEntityId, accountPickerGates.shouldFetchGlAccounts]);

  useEffect(() => {
    let cancelled = false;
    async function loadEditPaymentTerms() {
      if (!editingId) {
        setEditPaymentTerms([]);
        setEditPaymentTermsError("");
        setEditPaymentTermsLoading(false);
        return;
      }

      await loadPaymentTermsForLegalEntity({
        legalEntityId: editingForm.legalEntityId,
        setRows: (rows) => {
          if (!cancelled) {
            setEditPaymentTerms(rows);
          }
        },
        setLoading: (loading) => {
          if (!cancelled) {
            setEditPaymentTermsLoading(loading);
          }
        },
        setError: (error) => {
          if (!cancelled) {
            setEditPaymentTermsError(error);
          }
        },
      });
    }

    loadEditPaymentTerms();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [editingId, editingForm.legalEntityId, canRead]);

  useEffect(() => {
    let cancelled = false;
    async function loadEditAccounts() {
      if (!editingId) {
        setEditAccountOptions([]);
        setEditAccountsError("");
        setEditAccountsLoading(false);
        return;
      }

      await loadAccountsForLegalEntity({
        legalEntityId: editingForm.legalEntityId,
        setRows: (rows) => {
          if (!cancelled) {
            setEditAccountOptions(rows);
          }
        },
        setLoading: (loading) => {
          if (!cancelled) {
            setEditAccountsLoading(loading);
          }
        },
        setError: (error) => {
          if (!cancelled) {
            setEditAccountsError(error);
          }
        },
      });
    }

    loadEditAccounts();
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [editingId, editingForm.legalEntityId, accountPickerGates.shouldFetchGlAccounts]);

  async function loadCounterpartyRows(nextFilters = filters) {
    if (!canRead) {
      setRows([]);
      setTotalRows(0);
      return;
    }

    setListLoading(true);
    setListError("");
    try {
      const response = await listCariCounterparties(buildCounterpartyListParams(nextFilters));
      setRows(Array.isArray(response?.rows) ? response.rows : []);
      setTotalRows(Number(response?.total || 0));
    } catch (err) {
      setRows([]);
      setTotalRows(0);
      setListError(mapCounterpartyApiError(err, "Failed to load counterparties."));
    } finally {
      setListLoading(false);
    }
  }

  async function loadPaymentTermsForLegalEntity({
    legalEntityId,
    setRows,
    setLoading,
    setError,
  }) {
    const parsedLegalEntityId = toPositiveInt(legalEntityId);
    if (!parsedLegalEntityId || !canRead) {
      setRows([]);
      setError("");
      setLoading(false);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const response = await listCariPaymentTerms({
        legalEntityId: parsedLegalEntityId,
        limit: 300,
        offset: 0,
      });
      setRows(mapPaymentTermRows(response));
    } catch (err) {
      setRows([]);
      setError(
        mapCounterpartyApiError(err, "Failed to load payment terms for selected legal entity.")
      );
    } finally {
      setLoading(false);
    }
  }

  async function loadAccountsForLegalEntity({
    legalEntityId,
    setRows,
    setLoading,
    setError,
  }) {
    const parsedLegalEntityId = toPositiveInt(legalEntityId);
    if (!parsedLegalEntityId || !accountPickerGates.shouldFetchGlAccounts) {
      setRows([]);
      setError("");
      setLoading(false);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const response = await listAccounts({
        legalEntityId: parsedLegalEntityId,
      });
      setRows(mapAccountRows(response));
    } catch (err) {
      setRows([]);
      setError(
        mapCounterpartyApiError(err, "Failed to load account options for selected legal entity.")
      );
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (!isListPage) {
      return;
    }
    const normalized = {
      ...filters,
      role: normalizeFilterRole(filters.role, config.roleDefault),
      sortBy: normalizeCounterpartyListSortBy(filters.sortBy, "id"),
      sortDir: normalizeCounterpartyListSortDir(filters.sortDir, "desc"),
    };
    loadCounterpartyRows(normalized);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isListPage, config.roleDefault]);

  async function handleCreateSubmit(payload) {
    setCreateSaving(true);
    setCreateError("");
    setCreateMessage("");
    try {
      const response = await createCariCounterparty(payload);
      const createdId = response?.row?.id;
      setCreateMessage(`Counterparty created successfully. id=${createdId || "-"}`);
      setCreateForm((prev) => {
        const reset = buildInitialCounterpartyForm(config.roleDefault);
        return {
          ...reset,
          legalEntityId: prev.legalEntityId || reset.legalEntityId,
        };
      });
    } catch (err) {
      setCreateError(mapCounterpartyApiError(err, "Failed to create counterparty."));
    } finally {
      setCreateSaving(false);
    }
  }

  async function handleStartEdit(counterpartyId) {
    if (!canUpsert) {
      return;
    }
    setEditLoading(true);
    setEditError("");
    setEditMessage("");
    try {
      const response = await getCariCounterparty(counterpartyId);
      const row = response?.row || null;
      setEditingId(counterpartyId);
      setEditingForm(mapDetailToCounterpartyForm(row, config.roleDefault));
    } catch (err) {
      setEditingId(null);
      setEditError(mapCounterpartyApiError(err, "Failed to load counterparty detail."));
    } finally {
      setEditLoading(false);
    }
  }

  async function handleEditSubmit(payload) {
    if (!editingId) {
      return;
    }
    setEditSaving(true);
    setEditError("");
    setEditMessage("");
    try {
      await updateCariCounterparty(editingId, payload);
      setEditMessage("Counterparty updated.");
      await loadCounterpartyRows(filters);
    } catch (err) {
      setEditError(mapCounterpartyApiError(err, "Failed to update counterparty."));
    } finally {
      setEditSaving(false);
    }
  }

  function renderCreatePage() {
    return (
      <CounterpartyForm
        title={config.title}
        description={config.subtitle}
        mode="create"
        form={createForm}
        setForm={setCreateForm}
        legalEntities={legalEntities}
        legalEntitiesLoading={legalEntitiesLoading}
        legalEntitiesError={legalEntitiesError}
        paymentTerms={createPaymentTerms}
        paymentTermsLoading={createPaymentTermsLoading}
        paymentTermsError={createPaymentTermsError}
        accountOptions={createAccountOptions}
        accountOptionsLoading={createAccountsLoading}
        accountOptionsError={createAccountsError}
        canReadGlAccounts={accountPickerGates.showAccountPickers}
        accountReadFallbackMessage={
          "Missing permission: gl.account.read. AR/AP account selectors are hidden."
        }
        canSubmit={canUpsert}
        submitting={createSaving}
        onSubmit={handleCreateSubmit}
        onReset={() => setCreateForm(buildInitialCounterpartyForm(config.roleDefault))}
        submitLabel="Create Card"
        serverError={createError}
        serverMessage={createMessage}
        roleHint={`Default role preset: ${config.roleDefault}`}
      />
    );
  }

  function renderListPage() {
    return (
      <div className="space-y-5">
        <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <h1 className="text-lg font-semibold text-slate-900">{config.title}</h1>
          <p className="mt-1 text-sm text-slate-600">{config.subtitle}</p>

          {!canUpsert ? (
            <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
              You only have read permission. Edit actions are disabled.
            </div>
          ) : null}

          <div className="mt-4 grid gap-3 md:grid-cols-4">
            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                Legal Entity
              </label>
              {legalEntities.length > 0 ? (
                <select
                  className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                  value={filters.legalEntityId}
                  onChange={(event) =>
                    setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))
                  }
                >
                  <option value="">All in scope</option>
                  {legalEntities.map((row) => (
                    <option key={`filter-le-${row.id}`} value={String(row.id)}>
                      {row.code} - {row.name}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                  type="number"
                  min="1"
                  value={filters.legalEntityId}
                  onChange={(event) =>
                    setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))
                  }
                  placeholder="Legal entity id"
                />
              )}
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                Status
              </label>
              <select
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                value={filters.status}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, status: event.target.value }))
                }
              >
                <option value="">All</option>
                <option value="ACTIVE">ACTIVE</option>
                <option value="INACTIVE">INACTIVE</option>
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                Role Filter
              </label>
              <select
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                value={filters.role}
                onChange={(event) =>
                  setFilters((prev) => ({
                    ...prev,
                    role: normalizeFilterRole(event.target.value, config.roleDefault),
                  }))
                }
              >
                {ROLE_FILTERS.map((role) => (
                  <option key={`role-filter-${role}`} value={role}>
                    {role}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                Code / Name
              </label>
              <input
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                type="text"
                value={filters.q}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, q: event.target.value }))
                }
                placeholder="Search"
              />
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                AR Account Code
              </label>
              <input
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                type="text"
                value={filters.arAccountCode}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, arAccountCode: event.target.value }))
                }
                placeholder="Contains..."
              />
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                AR Account Name
              </label>
              <input
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                type="text"
                value={filters.arAccountName}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, arAccountName: event.target.value }))
                }
                placeholder="Contains..."
              />
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                AP Account Code
              </label>
              <input
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                type="text"
                value={filters.apAccountCode}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, apAccountCode: event.target.value }))
                }
                placeholder="Contains..."
              />
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                AP Account Name
              </label>
              <input
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                type="text"
                value={filters.apAccountName}
                onChange={(event) =>
                  setFilters((prev) => ({ ...prev, apAccountName: event.target.value }))
                }
                placeholder="Contains..."
              />
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                Sort Field
              </label>
              <select
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                value={normalizeCounterpartyListSortBy(filters.sortBy, "id")}
                onChange={(event) =>
                  setFilters((prev) => ({
                    ...prev,
                    sortBy: normalizeCounterpartyListSortBy(event.target.value, "id"),
                  }))
                }
              >
                {COUNTERPARTY_LIST_SORT_FIELDS.map((sortField) => (
                  <option key={`counterparty-sort-field-${sortField}`} value={sortField}>
                    {SORT_FIELD_LABELS[sortField] || sortField}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-xs font-semibold uppercase tracking-wide text-slate-600">
                Sort Direction
              </label>
              <select
                className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm"
                value={normalizeCounterpartyListSortDir(filters.sortDir, "desc")}
                onChange={(event) =>
                  setFilters((prev) => ({
                    ...prev,
                    sortDir: normalizeCounterpartyListSortDir(event.target.value, "desc"),
                  }))
                }
              >
                {COUNTERPARTY_LIST_SORT_DIRECTIONS.map((sortDir) => (
                  <option key={`counterparty-sort-dir-${sortDir}`} value={sortDir}>
                    {SORT_DIRECTION_LABELS[sortDir] || sortDir}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="mt-3 flex gap-2">
            <button
              type="button"
              className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white"
              onClick={() => loadCounterpartyRows(filters)}
              disabled={listLoading}
            >
              {listLoading ? "Loading..." : "Apply Filters"}
            </button>
            <button
              type="button"
              className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700"
              onClick={() => {
                const reset = createCounterpartyListFilters(config.roleDefault);
                setFilters(reset);
                loadCounterpartyRows(reset);
              }}
              disabled={listLoading}
            >
              Reset
            </button>
          </div>

          {listError ? (
            <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
              {listError}
            </div>
          ) : null}

          <div className="mt-4 overflow-x-auto">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
              <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-600">
                <tr>
                  <th className="px-3 py-2">Code</th>
                  <th className="px-3 py-2">Name</th>
                  <th className="px-3 py-2">Role</th>
                  <th className="px-3 py-2">Status</th>
                  <th className="px-3 py-2">Legal Entity</th>
                  <th className="px-3 py-2">AR Account</th>
                  <th className="px-3 py-2">AP Account</th>
                  <th className="px-3 py-2">Payment Term</th>
                  <th className="px-3 py-2 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 bg-white">
                {rows.map((row) => (
                  <tr key={`counterparty-row-${row.id}`}>
                    <td className="px-3 py-2 font-mono text-xs text-slate-800">{row.code}</td>
                    <td className="px-3 py-2 text-slate-800">{row.name}</td>
                    <td className="px-3 py-2">
                      <span
                        className={`inline-flex rounded px-2 py-1 text-xs font-semibold ${roleBadgeClass(
                          row.counterpartyType
                        )}`}
                      >
                        {row.counterpartyType}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-slate-700">{row.status}</td>
                    <td className="px-3 py-2 text-slate-700">
                      {legalEntityById.get(String(row.legalEntityId))?.code ||
                        row.legalEntityId}
                    </td>
                    <td className="px-3 py-2 text-slate-700">
                      {formatMappedAccountLabel(row.arAccountCode, row.arAccountName)}
                    </td>
                    <td className="px-3 py-2 text-slate-700">
                      {formatMappedAccountLabel(row.apAccountCode, row.apAccountName)}
                    </td>
                    <td className="px-3 py-2 text-slate-700">
                      {row.defaultPaymentTermCode || row.defaultPaymentTermId || "-"}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <button
                        type="button"
                        className="rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
                        onClick={() => handleStartEdit(row.id)}
                        disabled={!canUpsert || editLoading}
                      >
                        Edit
                      </button>
                    </td>
                  </tr>
                ))}
                {rows.length === 0 ? (
                  <tr>
                    <td
                      className="px-3 py-6 text-center text-sm text-slate-500"
                      colSpan={9}
                    >
                      {listLoading ? "Loading..." : "No rows found for current filters."}
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
          <p className="mt-2 text-xs text-slate-500">Total rows: {totalRows}</p>
        </section>

        {editingId ? (
          <CounterpartyForm
            title={`Edit Counterparty #${editingId}`}
            description="Update card master data. Existing contacts/addresses can be updated or new ones added."
            mode="edit"
            form={editingForm}
            setForm={setEditingForm}
            legalEntities={legalEntities}
            legalEntitiesLoading={legalEntitiesLoading}
            legalEntitiesError={legalEntitiesError}
            paymentTerms={editPaymentTerms}
            paymentTermsLoading={editPaymentTermsLoading}
            paymentTermsError={editPaymentTermsError}
            accountOptions={editAccountOptions}
            accountOptionsLoading={editAccountsLoading}
            accountOptionsError={editAccountsError}
            canReadGlAccounts={accountPickerGates.showAccountPickers}
            accountReadFallbackMessage={
              "Missing permission: gl.account.read. AR/AP account selectors are hidden."
            }
            canSubmit={canUpsert}
            submitting={editSaving}
            onSubmit={handleEditSubmit}
            onCancel={() => {
              setEditingId(null);
              setEditError("");
              setEditMessage("");
            }}
            submitLabel="Save Changes"
            serverError={editError}
            serverMessage={editMessage}
          />
        ) : null}

        {editLoading ? (
          <div className="rounded-xl border border-slate-200 bg-white p-4 text-sm text-slate-600 shadow-sm">
            Loading counterparty detail...
          </div>
        ) : null}
      </div>
    );
  }

  if (isCreatePage) {
    return renderCreatePage();
  }
  return renderListPage();
}
