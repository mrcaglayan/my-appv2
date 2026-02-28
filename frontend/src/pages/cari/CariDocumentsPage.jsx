import { useEffect, useMemo, useState } from "react";
import {
  cancelCariDocument,
  createCariDocument,
  getCariDocument,
  listCariDocuments,
  postCariDocument,
  reverseCariDocument,
  updateCariDocument,
} from "../../api/cariDocuments.js";
import { getCariCounterpartyStatementReport } from "../../api/cariReports.js";
import { Link } from "react-router-dom";
import { useAuth } from "../../auth/useAuth.js";
import { useWorkingContextDefaults } from "../../context/useWorkingContextDefaults.js";
import { usePersistedFilters } from "../../hooks/usePersistedFilters.js";
import { useModuleReadiness } from "../../readiness/useModuleReadiness.js";
import {
  buildDocumentListQuery,
  buildDocumentMutationPayload,
  DOCUMENT_DIRECTIONS,
  DOCUMENT_STATUSES,
  DOCUMENT_TYPES,
  mapDocumentRowToForm,
  requiresDueDate,
  validateDocumentMutationForm,
} from "./cariDocumentsUtils.js";

const DEFAULT_FILTERS = {
  legalEntityId: "",
  counterpartyId: "",
  direction: "",
  documentType: "",
  status: "",
  dateFrom: "",
  dateTo: "",
  documentDateFrom: "",
  documentDateTo: "",
  q: "",
  limit: 100,
  offset: 0,
};

const DOCUMENT_FILTER_CONTEXT_MAPPINGS = [
  { stateKey: "legalEntityId" },
  { stateKey: "dateFrom" },
  { stateKey: "dateTo" },
];

const DOCUMENT_CREATE_CONTEXT_MAPPINGS = [{ stateKey: "legalEntityId" }];
const DOCUMENT_FILTERS_STORAGE_SCOPE = "cari-documents.list";

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function createInitialDraftForm() {
  return {
    legalEntityId: "",
    counterpartyId: "",
    paymentTermId: "",
    direction: "AR",
    documentType: "INVOICE",
    documentDate: todayIsoDate(),
    dueDate: "",
    amountTxn: "",
    amountBase: "",
    currencyCode: "USD",
    fxRate: "",
  };
}

function normalizeApiError(error, fallback = "Operation failed.") {
  const message = String(error?.response?.data?.message || error?.message || fallback).trim();
  const requestId = String(error?.response?.data?.requestId || "").trim();
  return requestId ? `${message} (requestId: ${requestId})` : message || fallback;
}

function formatAmount(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "-";
  return parsed.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 6 });
}

function isDraft(row) {
  return String(row?.status || "").toUpperCase() === "DRAFT";
}

function isPosted(row) {
  return String(row?.status || "").toUpperCase() === "POSTED";
}

function formatReadinessReason(reason) {
  switch (String(reason || "").trim().toUpperCase()) {
    case "ACCOUNT_NOT_FOUND":
      return "Mapped account no longer exists.";
    case "ACCOUNT_INACTIVE":
      return "Mapped account is inactive.";
    case "ACCOUNT_NOT_POSTABLE":
      return "Mapped account is not postable.";
    case "ACCOUNT_SCOPE_NOT_LEGAL_ENTITY":
      return "Mapped account is not in a legal-entity chart.";
    case "ACCOUNT_LEGAL_ENTITY_MISMATCH":
      return "Mapped account belongs to a different legal entity.";
    case "PURPOSES_MUST_MAP_TO_DIFFERENT_ACCOUNTS":
      return "Control and offset must map to different accounts.";
    case "MAPPED_ACCOUNT_ID_INVALID":
      return "Mapped account id is invalid.";
    case "ACCOUNT_TENANT_MISMATCH":
      return "Mapped account belongs to a different tenant.";
    default:
      return String(reason || "Invalid mapping.");
  }
}

export default function CariDocumentsPage() {
  const { hasPermission } = useAuth();
  const { getModuleRow } = useModuleReadiness();
  const canRead = hasPermission("cari.doc.read");
  const canCreate = hasPermission("cari.doc.create");
  const canUpdate = hasPermission("cari.doc.update");
  const canPost = hasPermission("cari.doc.post");
  const canReverse = hasPermission("cari.doc.reverse");
  const canFxOverride = hasPermission("cari.fx.override");
  const canReadReports = hasPermission("cari.report.read");

  const [filters, setFilters, resetFilters] = usePersistedFilters(
    DOCUMENT_FILTERS_STORAGE_SCOPE,
    () => ({ ...DEFAULT_FILTERS })
  );
  const [rows, setRows] = useState([]);
  const [totalRows, setTotalRows] = useState(0);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState("");

  const [createForm, setCreateForm] = useState(() => createInitialDraftForm());
  const [createSaving, setCreateSaving] = useState(false);
  const [createError, setCreateError] = useState("");
  const [createMessage, setCreateMessage] = useState("");

  const [selectedDocumentId, setSelectedDocumentId] = useState(null);
  const [selectedDetail, setSelectedDetail] = useState(null);
  const [detailError, setDetailError] = useState("");

  const [editForm, setEditForm] = useState(() => createInitialDraftForm());
  const [editSaving, setEditSaving] = useState(false);
  const [editError, setEditError] = useState("");
  const [editMessage, setEditMessage] = useState("");
  const [cancelSaving, setCancelSaving] = useState(false);
  const [cancelError, setCancelError] = useState("");

  const [postForm, setPostForm] = useState({ useFxOverride: false, fxOverrideReason: "" });
  const [postSaving, setPostSaving] = useState(false);
  const [postError, setPostError] = useState("");
  const [postMessage, setPostMessage] = useState("");

  const [reverseForm, setReverseForm] = useState({ reason: "Manual reversal", reversalDate: "" });
  const [reverseSaving, setReverseSaving] = useState(false);
  const [reverseError, setReverseError] = useState("");
  const [reverseMessage, setReverseMessage] = useState("");
  const [reverseResult, setReverseResult] = useState(null);
  const [linkedCashRows, setLinkedCashRows] = useState([]);
  const [linkedCashLoading, setLinkedCashLoading] = useState(false);
  const [linkedCashError, setLinkedCashError] = useState("");

  useWorkingContextDefaults(setFilters, DOCUMENT_FILTER_CONTEXT_MAPPINGS, [
    filters.legalEntityId,
    filters.dateFrom,
    filters.dateTo,
  ]);
  useWorkingContextDefaults(setCreateForm, DOCUMENT_CREATE_CONTEXT_MAPPINGS, [
    createForm.legalEntityId,
  ]);

  const selectedRow = useMemo(
    () => rows.find((row) => Number(row?.id || 0) === Number(selectedDocumentId || 0)) || null,
    [rows, selectedDocumentId]
  );
  const selectedSnapshot = selectedDetail || selectedRow;
  const selectedDocumentLegalEntityId = toPositiveInt(
    selectedSnapshot?.legalEntityId || selectedSnapshot?.legal_entity_id
  );
  const selectedCariPostingReadiness = getModuleRow(
    "cariPosting",
    selectedDocumentLegalEntityId
  );
  const cariPostingNotReady = Boolean(
    selectedCariPostingReadiness && !selectedCariPostingReadiness.ready
  );
  const canEditOrCancelSelected = Boolean(selectedSnapshot && isDraft(selectedSnapshot) && canUpdate);
  const canPostSelected = Boolean(
    selectedSnapshot && isDraft(selectedSnapshot) && canPost && !cariPostingNotReady
  );
  const canReverseSelected = Boolean(selectedSnapshot && isPosted(selectedSnapshot) && canReverse);

  async function loadDocuments(nextFilters = filters) {
    if (!canRead) {
      setRows([]);
      setTotalRows(0);
      setListError("Missing permission: cari.doc.read");
      return;
    }
    setListLoading(true);
    setListError("");
    try {
      const response = await listCariDocuments(buildDocumentListQuery(nextFilters));
      setRows(Array.isArray(response?.rows) ? response.rows : []);
      setTotalRows(Number(response?.total || 0));
    } catch (error) {
      setRows([]);
      setTotalRows(0);
      setListError(normalizeApiError(error, "Failed to load documents."));
    } finally {
      setListLoading(false);
    }
  }

  async function loadDocumentDetail(documentId) {
    if (!documentId || !canRead) {
      setSelectedDetail(null);
      return;
    }
    setDetailError("");
    try {
      const response = await getCariDocument(documentId);
      const row = response?.row || null;
      setSelectedDetail(row);
      if (row && isDraft(row)) setEditForm(mapDocumentRowToForm(row));
    } catch (error) {
      setSelectedDetail(null);
      setDetailError(normalizeApiError(error, "Failed to load document detail."));
    }
  }

  useEffect(() => {
    loadDocuments(filters);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canRead, filters]);

  useEffect(() => {
    if (!selectedDocumentId) {
      setSelectedDetail(null);
      return;
    }
    loadDocumentDetail(selectedDocumentId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDocumentId, canRead]);

  useEffect(() => {
    const documentId = Number(selectedSnapshot?.id || 0);
    const legalEntityId = Number(selectedSnapshot?.legalEntityId || 0);
    const counterpartyId = Number(selectedSnapshot?.counterpartyId || 0);
    if (!canReadReports || !documentId || !legalEntityId || !counterpartyId) {
      setLinkedCashRows([]);
      setLinkedCashError("");
      setLinkedCashLoading(false);
      return;
    }

    let active = true;
    async function loadLinkedCashRows() {
      setLinkedCashLoading(true);
      setLinkedCashError("");
      try {
        const payload = await getCariCounterpartyStatementReport({
          legalEntityId,
          counterpartyId,
          asOfDate: todayIsoDate(),
          status: "ALL",
          includeDetails: true,
          limit: 1000,
          offset: 0,
        });
        if (!active) {
          return;
        }
        const allocationRows = Array.isArray(payload?.allocations?.rows)
          ? payload.allocations.rows
          : [];
        const settlementRows = Array.isArray(payload?.settlements?.rows)
          ? payload.settlements.rows
          : [];
        const settlementIdSet = new Set(
          allocationRows
            .filter((row) => Number(row?.documentId || 0) === documentId)
            .map((row) => Number(row?.settlementBatchId || 0))
            .filter((id) => id > 0)
        );
        const linkedRows = settlementRows
          .filter((row) => settlementIdSet.has(Number(row?.settlementBatchId || 0)))
          .map((row) => ({
            settlementBatchId: Number(row?.settlementBatchId || 0) || null,
            settlementNo: row?.settlementNo || null,
            settlementDate: row?.settlementDate || null,
            cashTransactionId: Number(row?.cashTransactionId || 0) || null,
          }));
        setLinkedCashRows(linkedRows);
      } catch (error) {
        if (!active) {
          return;
        }
        setLinkedCashRows([]);
        setLinkedCashError(normalizeApiError(error, "Failed to load settlement/cash links."));
      } finally {
        if (active) {
          setLinkedCashLoading(false);
        }
      }
    }

    loadLinkedCashRows();
    return () => {
      active = false;
    };
  }, [canReadReports, selectedSnapshot?.counterpartyId, selectedSnapshot?.id, selectedSnapshot?.legalEntityId]);

  async function handleCreateDraft(event) {
    event.preventDefault();
    setCreateSaving(true);
    setCreateError("");
    setCreateMessage("");
    try {
      const { errors } = validateDocumentMutationForm(createForm);
      if (errors.length > 0) {
        setCreateError(errors.join(" "));
        return;
      }
      const payload = buildDocumentMutationPayload(createForm);
      const response = await createCariDocument(payload);
      setCreateMessage(`Draft document created. id=${response?.row?.id || "-"}`);
      setCreateForm(createInitialDraftForm());
      await loadDocuments(filters);
      if (response?.row?.id) setSelectedDocumentId(response.row.id);
    } catch (error) {
      setCreateError(normalizeApiError(error, "Failed to create draft document."));
    } finally {
      setCreateSaving(false);
    }
  }

  async function handleUpdateDraft(event) {
    event.preventDefault();
    if (!selectedDocumentId || !canEditOrCancelSelected) {
      setEditError("Only DRAFT documents can be edited with cari.doc.update permission.");
      return;
    }
    setEditSaving(true);
    setEditError("");
    setEditMessage("");
    try {
      const { errors } = validateDocumentMutationForm(editForm);
      if (errors.length > 0) {
        setEditError(errors.join(" "));
        return;
      }
      const payload = buildDocumentMutationPayload(editForm);
      const response = await updateCariDocument(selectedDocumentId, payload);
      setEditMessage("Draft document updated.");
      setSelectedDetail(response?.row || null);
      await loadDocuments(filters);
    } catch (error) {
      setEditError(normalizeApiError(error, "Failed to update draft document."));
    } finally {
      setEditSaving(false);
    }
  }

  async function handleCancelDraft() {
    if (!selectedDocumentId || !canEditOrCancelSelected) {
      setCancelError("Only DRAFT documents can be cancelled with cari.doc.update permission.");
      return;
    }
    setCancelSaving(true);
    setCancelError("");
    try {
      const response = await cancelCariDocument(selectedDocumentId);
      setSelectedDetail(response?.row || null);
      await loadDocuments(filters);
    } catch (error) {
      setCancelError(normalizeApiError(error, "Failed to cancel draft document."));
    } finally {
      setCancelSaving(false);
    }
  }

  async function handlePostDraft() {
    if (cariPostingNotReady) {
      setPostError(
        "Setup incomplete for selected legal entity. Configure CARI purpose mappings in GL Setup first."
      );
      return;
    }
    if (!selectedDocumentId || !canPostSelected) {
      setPostError("Only DRAFT documents can be posted with cari.doc.post permission.");
      return;
    }
    if (postForm.useFxOverride && !canFxOverride) {
      setPostError("FX override requires permission: cari.fx.override. Disable override or request access.");
      return;
    }
    if (postForm.useFxOverride && !String(postForm.fxOverrideReason || "").trim()) {
      setPostError("fxOverrideReason is required when useFxOverride=true.");
      return;
    }

    setPostSaving(true);
    setPostError("");
    setPostMessage("");
    try {
      const response = await postCariDocument(selectedDocumentId, {
        useFxOverride: Boolean(postForm.useFxOverride),
        fxOverrideReason: postForm.useFxOverride ? String(postForm.fxOverrideReason || "").trim() : null,
      });
      setPostMessage(`Draft posted. postedJournalEntryId=${response?.row?.postedJournalEntryId || response?.journal?.journalEntryId || "-"}`);
      setSelectedDetail(response?.row || null);
      await loadDocuments(filters);
      await loadDocumentDetail(selectedDocumentId);
    } catch (error) {
      setPostError(normalizeApiError(error, "Failed to post draft document."));
    } finally {
      setPostSaving(false);
    }
  }

  async function handleReversePosted() {
    if (!selectedDocumentId || !canReverseSelected) {
      setReverseError("Only POSTED documents can be reversed with cari.doc.reverse permission.");
      return;
    }
    setReverseSaving(true);
    setReverseError("");
    setReverseMessage("");
    try {
      const response = await reverseCariDocument(selectedDocumentId, {
        reason: String(reverseForm.reason || "").trim() || "Manual reversal",
        reversalDate: String(reverseForm.reversalDate || "").trim() || undefined,
      });
      setReverseResult({
        reversalDocumentId: response?.row?.id || null,
        reversalDocumentNo: response?.row?.documentNo || null,
        reversalJournalEntryId: response?.journal?.reversalJournalEntryId || null,
      });
      setReverseMessage(`Reverse completed. reversalDocumentId=${response?.row?.id || "-"}`);
      await loadDocuments(filters);
      await loadDocumentDetail(selectedDocumentId);
    } catch (error) {
      setReverseError(normalizeApiError(error, "Failed to reverse posted document."));
    } finally {
      setReverseSaving(false);
    }
  }

  if (!canRead) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        Missing permission: `cari.doc.read`
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h1 className="text-xl font-semibold text-slate-900">Cari Documents</h1>
        {listError ? <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{listError}</div> : null}
        <div className="mt-4 grid gap-3 md:grid-cols-4">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Legal Entity ID<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.legalEntityId} onChange={(event) => setFilters((prev) => ({ ...prev, legalEntityId: event.target.value }))} /></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Counterparty ID<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.counterpartyId} onChange={(event) => setFilters((prev) => ({ ...prev, counterpartyId: event.target.value }))} /></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Direction<select className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.direction} onChange={(event) => setFilters((prev) => ({ ...prev, direction: event.target.value }))}><option value="">ALL</option>{DOCUMENT_DIRECTIONS.map((direction) => <option key={`filter-direction-${direction}`} value={direction}>{direction}</option>)}</select></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Document Type<select className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.documentType} onChange={(event) => setFilters((prev) => ({ ...prev, documentType: event.target.value }))}><option value="">ALL</option>{DOCUMENT_TYPES.map((documentType) => <option key={`filter-document-type-${documentType}`} value={documentType}>{documentType}</option>)}</select></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Status<select className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.status} onChange={(event) => setFilters((prev) => ({ ...prev, status: event.target.value }))}><option value="">ALL</option>{DOCUMENT_STATUSES.map((status) => <option key={`filter-status-${status}`} value={status}>{status}</option>)}</select></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Date From<input type="date" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.dateFrom} onChange={(event) => setFilters((prev) => ({ ...prev, dateFrom: event.target.value }))} /></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Date To<input type="date" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.dateTo} onChange={(event) => setFilters((prev) => ({ ...prev, dateTo: event.target.value }))} /></label>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Search<input type="text" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={filters.q} onChange={(event) => setFilters((prev) => ({ ...prev, q: event.target.value }))} placeholder="documentNo / counterparty snapshot" /></label>
        </div>
        <div className="mt-3 flex gap-2">
          <button type="button" className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white" onClick={() => loadDocuments(filters)} disabled={listLoading}>{listLoading ? "Loading..." : "Refresh List"}</button>
          <button type="button" className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700" onClick={resetFilters} disabled={listLoading}>Reset Filters</button>
        </div>
      </section>

      {canCreate ? (
        <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
          <h2 className="text-lg font-semibold text-slate-900">Create Draft Document</h2>
          {createError ? <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{createError}</div> : null}
          {createMessage ? <div className="mt-3 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{createMessage}</div> : null}
          <form className="mt-4 grid gap-3 md:grid-cols-4" onSubmit={handleCreateDraft}>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Legal Entity ID<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.legalEntityId} onChange={(event) => setCreateForm((prev) => ({ ...prev, legalEntityId: event.target.value }))} required /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Counterparty ID<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.counterpartyId} onChange={(event) => setCreateForm((prev) => ({ ...prev, counterpartyId: event.target.value }))} required /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Payment Term ID (optional)<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.paymentTermId} onChange={(event) => setCreateForm((prev) => ({ ...prev, paymentTermId: event.target.value }))} /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Direction<select className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.direction} onChange={(event) => setCreateForm((prev) => ({ ...prev, direction: event.target.value }))} required>{DOCUMENT_DIRECTIONS.map((direction) => <option key={`create-direction-${direction}`} value={direction}>{direction}</option>)}</select></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Document Type<select className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.documentType} onChange={(event) => setCreateForm((prev) => ({ ...prev, documentType: event.target.value }))} required>{DOCUMENT_TYPES.map((documentType) => <option key={`create-document-type-${documentType}`} value={documentType}>{documentType}</option>)}</select></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Document Date<input type="date" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.documentDate} onChange={(event) => setCreateForm((prev) => ({ ...prev, documentDate: event.target.value }))} required /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Due Date {requiresDueDate(createForm.documentType) ? "(required for this type)" : "(optional)"}<input type="date" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.dueDate} onChange={(event) => setCreateForm((prev) => ({ ...prev, dueDate: event.target.value }))} required={requiresDueDate(createForm.documentType)} /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Amount Txn<input type="number" min="0.000001" step="0.000001" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.amountTxn} onChange={(event) => setCreateForm((prev) => ({ ...prev, amountTxn: event.target.value }))} required /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Amount Base<input type="number" min="0.000001" step="0.000001" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.amountBase} onChange={(event) => setCreateForm((prev) => ({ ...prev, amountBase: event.target.value }))} required /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Currency<input type="text" maxLength={3} className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal uppercase" value={createForm.currencyCode} onChange={(event) => setCreateForm((prev) => ({ ...prev, currencyCode: event.target.value }))} required /></label>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">FX Rate (optional)<input type="number" min="0.0000000001" step="0.0000000001" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={createForm.fxRate} onChange={(event) => setCreateForm((prev) => ({ ...prev, fxRate: event.target.value }))} /></label>
            <div className="md:col-span-4 flex gap-2">
              <button type="submit" className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white" disabled={createSaving}>{createSaving ? "Creating..." : "Create Draft Document"}</button>
              <button type="button" className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700" onClick={() => setCreateForm(createInitialDraftForm())} disabled={createSaving}>Reset Draft Form</button>
            </div>
          </form>
        </section>
      ) : null}

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">Document List</h2>
        <p className="mt-1 text-sm text-slate-600">Total rows: {totalRows}</p>
        <div className="mt-4 overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">ID</th><th className="px-3 py-2">Document No</th><th className="px-3 py-2">Direction</th><th className="px-3 py-2">Type</th><th className="px-3 py-2">Status</th><th className="px-3 py-2">Document Date</th><th className="px-3 py-2">Amount Txn</th><th className="px-3 py-2">Posted Journal</th><th className="px-3 py-2">Reversal Of</th><th className="px-3 py-2 text-right">Action</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`doc-row-${row.id}`} className={`border-t border-slate-100 ${Number(row.id) === Number(selectedDocumentId) ? "bg-cyan-50" : "bg-white"}`}>
                  <td className="px-3 py-2 font-mono text-xs">{row.id}</td><td className="px-3 py-2">{row.documentNo || "-"}</td><td className="px-3 py-2">{row.direction}</td><td className="px-3 py-2">{row.documentType}</td><td className="px-3 py-2">{row.status}</td><td className="px-3 py-2">{row.documentDate}</td><td className="px-3 py-2">{formatAmount(row.amountTxn)}</td><td className="px-3 py-2">{row.postedJournalEntryId || "-"}</td><td className="px-3 py-2">{row.reversalOfDocumentId || "-"}</td>
                  <td className="px-3 py-2 text-right"><button type="button" className="rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700" onClick={() => setSelectedDocumentId(row.id)}>View / Actions</button></td>
                </tr>
              ))}
              {rows.length === 0 ? <tr><td className="px-3 py-4 text-slate-500" colSpan={10}>{listLoading ? "Loading documents..." : "No documents found for current filters."}</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h2 className="text-lg font-semibold text-slate-900">Detail + Actions</h2>
        {detailError ? <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{detailError}</div> : null}
        {selectedSnapshot ? (
          <div className="mt-4 grid gap-4 lg:grid-cols-2">
            <div className="rounded-lg border border-slate-200 p-4">
              <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-700">Document Detail</h3>
              <dl className="mt-3 grid grid-cols-2 gap-2 text-sm">
                <dt className="font-semibold text-slate-600">documentNo</dt><dd>{selectedSnapshot.documentNo || "-"}</dd>
                <dt className="font-semibold text-slate-600">status</dt><dd>{selectedSnapshot.status || "-"}</dd>
                <dt className="font-semibold text-slate-600">postedJournalEntryId</dt><dd>{selectedSnapshot.postedJournalEntryId || "-"}</dd>
                <dt className="font-semibold text-slate-600">reversalOfDocumentId</dt><dd>{selectedSnapshot.reversalOfDocumentId || "-"}</dd>
                <dt className="font-semibold text-slate-600">counterpartyCodeSnapshot</dt><dd>{selectedSnapshot.counterpartyCodeSnapshot || "-"}</dd>
                <dt className="font-semibold text-slate-600">counterpartyNameSnapshot</dt><dd>{selectedSnapshot.counterpartyNameSnapshot || "-"}</dd>
                <dt className="font-semibold text-slate-600">dueDateSnapshot</dt><dd>{selectedSnapshot.dueDateSnapshot || "-"}</dd>
                <dt className="font-semibold text-slate-600">currencyCodeSnapshot</dt><dd>{selectedSnapshot.currencyCodeSnapshot || "-"}</dd>
                <dt className="font-semibold text-slate-600">fxRateSnapshot</dt><dd>{selectedSnapshot.fxRateSnapshot || "-"}</dd>
              </dl>
              {reverseResult ? <div className="mt-4 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">Reverse linkage: `response.row.id`={reverseResult.reversalDocumentId || "-"}, `response.row.documentNo`={reverseResult.reversalDocumentNo || "-"}, `response.journal.reversalJournalEntryId`={reverseResult.reversalJournalEntryId || "-"}</div> : null}
              {canReadReports ? (
                <div className="mt-4 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                  <p className="font-semibold text-slate-800">Linked settlements / cash transactions</p>
                  {linkedCashError ? <p className="mt-1 text-rose-700">{linkedCashError}</p> : null}
                  {linkedCashLoading ? <p className="mt-1 text-slate-600">Loading linkage...</p> : null}
                  {!linkedCashLoading && linkedCashRows.length === 0 ? (
                    <p className="mt-1 text-slate-600">No linked settlements found for this document as of today.</p>
                  ) : null}
                  {!linkedCashLoading && linkedCashRows.length > 0 ? (
                    <ul className="mt-2 space-y-1">
                      {linkedCashRows.map((row, index) => (
                        <li key={`doc-link-${row.settlementBatchId || row.settlementNo || index}`} className="rounded border border-slate-200 bg-white px-2 py-1">
                          settlement={row.settlementNo || row.settlementBatchId || "-"} ({row.settlementDate || "-"}) | cashTransactionId={row.cashTransactionId || "-"}
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </div>
              ) : null}
            </div>

            <div className="space-y-4">
              <div className="rounded-lg border border-slate-200 p-4">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-700">Draft Actions</h3>
                {editError ? <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{editError}</div> : null}
                {editMessage ? <div className="mt-2 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{editMessage}</div> : null}
                <form className="mt-3 grid gap-2 md:grid-cols-2" onSubmit={handleUpdateDraft}>
                  <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Legal Entity ID<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={editForm.legalEntityId} onChange={(event) => setEditForm((prev) => ({ ...prev, legalEntityId: event.target.value }))} disabled={!canEditOrCancelSelected || editSaving} /></label>
                  <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Counterparty ID<input type="number" min="1" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={editForm.counterpartyId} onChange={(event) => setEditForm((prev) => ({ ...prev, counterpartyId: event.target.value }))} disabled={!canEditOrCancelSelected || editSaving} /></label>
                  <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Document Type<select className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={editForm.documentType} onChange={(event) => setEditForm((prev) => ({ ...prev, documentType: event.target.value }))} disabled={!canEditOrCancelSelected || editSaving}>{DOCUMENT_TYPES.map((documentType) => <option key={`edit-document-type-${documentType}`} value={documentType}>{documentType}</option>)}</select></label>
                  <label className="text-xs font-semibold uppercase tracking-wide text-slate-600">Due Date<input type="date" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={editForm.dueDate} onChange={(event) => setEditForm((prev) => ({ ...prev, dueDate: event.target.value }))} disabled={!canEditOrCancelSelected || editSaving} required={requiresDueDate(editForm.documentType)} /></label>
                  <button type="submit" className="rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50" disabled={!canEditOrCancelSelected || editSaving}>{editSaving ? "Saving..." : "Update Draft Document"}</button>
                  <button type="button" className="rounded-md border border-rose-300 px-4 py-2 text-sm font-semibold text-rose-700 disabled:opacity-50" onClick={handleCancelDraft} disabled={!canEditOrCancelSelected || cancelSaving}>{cancelSaving ? "Cancelling..." : "Cancel Draft"}</button>
                </form>
                {cancelError ? <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{cancelError}</div> : null}
              </div>

              <div className="rounded-lg border border-slate-200 p-4">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-700">Post / Reverse</h3>
                {cariPostingNotReady ? (
                  <div className="mt-3 rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-900">
                    <p className="font-semibold">Setup incomplete (CARI posting)</p>
                    <p className="mt-1">
                      Posting is disabled for legalEntityId={selectedDocumentLegalEntityId}.
                    </p>
                    {Array.isArray(selectedCariPostingReadiness?.missingPurposeCodes) &&
                    selectedCariPostingReadiness.missingPurposeCodes.length > 0 ? (
                      <p className="mt-1">
                        Missing purpose codes:{" "}
                        {selectedCariPostingReadiness.missingPurposeCodes.join(", ")}
                      </p>
                    ) : null}
                    {Array.isArray(selectedCariPostingReadiness?.invalidMappings) &&
                    selectedCariPostingReadiness.invalidMappings.length > 0 ? (
                      <ul className="mt-2 list-disc pl-5">
                        {selectedCariPostingReadiness.invalidMappings.map((row, index) => (
                          <li key={`cari-readiness-invalid-${index}`}>
                            {String(row?.purposeCode || "-")}:{" "}
                            {formatReadinessReason(row?.reason)}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                    <div className="mt-2 flex flex-wrap gap-2">
                      <Link
                        to="/app/ayarlar/hesap-plani-ayarlari#manual-purpose-mappings"
                        className="rounded border border-amber-300 bg-white px-2.5 py-1 text-xs font-semibold text-amber-900"
                      >
                        Fix manually
                      </Link>
                      <Link
                        to="/app/ayarlar/hesap-plani-ayarlari#template-wizard"
                        className="rounded border border-amber-300 bg-white px-2.5 py-1 text-xs font-semibold text-amber-900"
                      >
                        Use template
                      </Link>
                    </div>
                  </div>
                ) : null}
                <label className="mt-2 flex items-center gap-2 text-sm text-slate-700"><input type="checkbox" checked={postForm.useFxOverride} onChange={(event) => setPostForm((prev) => ({ ...prev, useFxOverride: event.target.checked }))} disabled={!canPostSelected || postSaving} />useFxOverride</label>
                <input type="text" className="mt-2 w-full rounded-md border border-slate-300 px-3 py-2 text-sm" placeholder="fxOverrideReason" value={postForm.fxOverrideReason} onChange={(event) => setPostForm((prev) => ({ ...prev, fxOverrideReason: event.target.value }))} disabled={!canPostSelected || postSaving} />
                {postForm.useFxOverride && !canFxOverride ? <p className="mt-2 text-sm text-amber-700">You cannot post with FX override. Missing permission: `cari.fx.override`.</p> : null}
                <button type="button" className="mt-2 rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50" onClick={handlePostDraft} disabled={!canPostSelected || postSaving}>{postSaving ? "Posting..." : "Post Draft"}</button>
                {postError ? <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{postError}</div> : null}
                {postMessage ? <div className="mt-2 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{postMessage}</div> : null}

                <label className="mt-3 block text-xs font-semibold uppercase tracking-wide text-slate-600">reverse reason<input type="text" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={reverseForm.reason} onChange={(event) => setReverseForm((prev) => ({ ...prev, reason: event.target.value }))} disabled={!canReverseSelected || reverseSaving} /></label>
                <label className="mt-2 block text-xs font-semibold uppercase tracking-wide text-slate-600">reversalDate<input type="date" className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 text-sm font-normal" value={reverseForm.reversalDate} onChange={(event) => setReverseForm((prev) => ({ ...prev, reversalDate: event.target.value }))} disabled={!canReverseSelected || reverseSaving} /></label>
                <button type="button" className="mt-2 rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50" onClick={handleReversePosted} disabled={!canReverseSelected || reverseSaving}>{reverseSaving ? "Reversing..." : "Reverse Posted Document"}</button>
                {reverseError ? <div className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{reverseError}</div> : null}
                {reverseMessage ? <div className="mt-2 rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">{reverseMessage}</div> : null}
              </div>
            </div>
          </div>
        ) : null}
      </section>
    </div>
  );
}
