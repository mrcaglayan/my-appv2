import { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  createJournal,
  listAccounts,
  listBooks,
  postJournal,
} from "../api/glAdmin.js";
import {
  listFiscalPeriods,
  listLegalEntities,
  listOperatingUnits,
  listShareholders,
} from "../api/orgAdmin.js";
import { useAuth } from "../auth/useAuth.js";
import { useI18n } from "../i18n/useI18n.js";
import { useModuleReadiness } from "../readiness/useModuleReadiness.js";

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toAmount(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatAmount(value) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function hasId(rows, id) {
  return rows.some((row) => Number(row.id) === Number(id));
}

function createLine() {
  return {
    id: `line-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    accountId: "",
    operatingUnitId: "",
    subledgerReferenceNo: "",
    counterpartyLegalEntityId: "",
    description: "",
    debitBase: "0",
    creditBase: "0",
  };
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
    default:
      return String(reason || "-");
  }
}

export default function AcilisFisiOlustur() {
  const { hasPermission } = useAuth();
  const { getModuleRow } = useModuleReadiness();
  const { language } = useI18n();
  const isTr = language === "tr";
  const l = useCallback((en, tr) => (isTr ? tr : en), [isTr]);
  const canReadOrgTree = hasPermission("org.tree.read");
  const canReadBooks = hasPermission("gl.book.read");
  const canReadAccounts = hasPermission("gl.account.read");
  const canReadPeriods = hasPermission("org.fiscal_period.read");
  const canCreateJournal = hasPermission("gl.journal.create");
  const canPostJournal = hasPermission("gl.journal.post");

  const today = new Date().toISOString().slice(0, 10);
  const [loadingRefs, setLoadingRefs] = useState(false);
  const [loadingPeriods, setLoadingPeriods] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [result, setResult] = useState(null);

  const [legalEntities, setLegalEntities] = useState([]);
  const [books, setBooks] = useState([]);
  const [accounts, setAccounts] = useState([]);
  const [operatingUnits, setOperatingUnits] = useState([]);
  const [shareholders, setShareholders] = useState([]);
  const [periods, setPeriods] = useState([]);

  const [form, setForm] = useState({
    legalEntityId: "",
    bookId: "",
    fiscalPeriodId: "",
    entryDate: today,
    documentDate: today,
    currencyCode: "USD",
    referenceNo: "",
    description: l("Opening entry", "Acilis fisi"),
    autoPost: true,
  });
  const [lines, setLines] = useState([createLine(), createLine()]);

  const selectedLegalEntityId = toPositiveInt(form.legalEntityId);
  const selectedBookId = toPositiveInt(form.bookId);
  const selectedShareholderCommitmentReadiness = getModuleRow(
    "shareholderCommitment",
    selectedLegalEntityId
  );
  const shareholderCommitmentModuleNotReady = Boolean(
    selectedShareholderCommitmentReadiness &&
      !selectedShareholderCommitmentReadiness.ready
  );
  const unitsById = useMemo(() => {
    const map = new Map();
    for (const unit of operatingUnits) {
      const unitId = toPositiveInt(unit.id);
      if (!unitId) {
        continue;
      }
      map.set(unitId, unit);
    }
    return map;
  }, [operatingUnits]);
  const postableAccounts = useMemo(() => {
    const parentIds = new Set(
      accounts
        .map((row) => toPositiveInt(row.parent_account_id))
        .filter(Boolean)
    );
    return accounts.filter((row) => {
      const accountId = toPositiveInt(row.id);
      if (!accountId) {
        return false;
      }
      const allowPosting = !(
        row.allow_posting === false ||
        row.allow_posting === 0 ||
        row.allow_posting === "0"
      );
      return allowPosting && !parentIds.has(accountId);
    });
  }, [accounts]);

  useEffect(() => {
    let cancelled = false;

    async function loadReferences() {
      if (!canReadOrgTree && !canReadBooks && !canReadAccounts) {
        setLegalEntities([]);
        setBooks([]);
        setAccounts([]);
        setOperatingUnits([]);
        setShareholders([]);
        return;
      }

      setLoadingRefs(true);
      setError("");

      try {
        const [entityRes, bookRes, accountRes, unitRes, shareholdersRes] = await Promise.all([
          canReadOrgTree ? listLegalEntities() : Promise.resolve({ rows: [] }),
          canReadBooks
            ? listBooks(
                selectedLegalEntityId
                  ? { legalEntityId: selectedLegalEntityId }
                  : {}
              )
            : Promise.resolve({ rows: [] }),
          canReadAccounts
            ? listAccounts(
                selectedLegalEntityId
                  ? { legalEntityId: selectedLegalEntityId }
                  : {}
              )
            : Promise.resolve({ rows: [] }),
          canReadOrgTree
            ? listOperatingUnits(
                selectedLegalEntityId
                  ? { legalEntityId: selectedLegalEntityId }
                  : {}
              )
            : Promise.resolve({ rows: [] }),
          canReadOrgTree
            ? listShareholders(
                selectedLegalEntityId
                  ? { legalEntityId: selectedLegalEntityId }
                  : {}
              )
            : Promise.resolve({ rows: [] }),
        ]);

        if (cancelled) {
          return;
        }

        const entityRows = entityRes?.rows || [];
        const bookRows = bookRes?.rows || [];
        const accountRows = accountRes?.rows || [];
        const unitRows = unitRes?.rows || [];
        const shareholderRows = shareholdersRes?.rows || [];

        setLegalEntities(entityRows);
        setBooks(bookRows);
        setAccounts(accountRows);
        setOperatingUnits(unitRows);
        setShareholders(shareholderRows);

        setForm((prev) => {
          const next = { ...prev };

          const currentEntityId = toPositiveInt(prev.legalEntityId);
          if (!currentEntityId || !hasId(entityRows, currentEntityId)) {
            next.legalEntityId = entityRows[0] ? String(entityRows[0].id) : "";
          }

          const currentBookId = toPositiveInt(prev.bookId);
          if (!currentBookId || !hasId(bookRows, currentBookId)) {
            next.bookId = bookRows[0] ? String(bookRows[0].id) : "";
          }

          const nextBookId = toPositiveInt(next.bookId);
          const selectedBook = bookRows.find(
            (row) => Number(row.id) === Number(nextBookId)
          );
          if (selectedBook?.base_currency_code) {
            next.currencyCode = String(selectedBook.base_currency_code).toUpperCase();
          }

          return next;
        });
      } catch (err) {
        if (!cancelled) {
          setError(err?.response?.data?.message || l("Failed to load references.", "Referanslar yuklenemedi."));
        }
      } finally {
        if (!cancelled) {
          setLoadingRefs(false);
        }
      }
    }

    loadReferences();
    return () => {
      cancelled = true;
    };
  }, [canReadOrgTree, canReadBooks, canReadAccounts, selectedLegalEntityId, l]);

  useEffect(() => {
    let cancelled = false;

    async function loadPeriodsByBook() {
      if (!canReadPeriods || !selectedBookId) {
        setPeriods([]);
        return;
      }

      const book = books.find((row) => Number(row.id) === Number(selectedBookId));
      const calendarId = toPositiveInt(book?.calendar_id);
      if (!calendarId) {
        setPeriods([]);
        return;
      }

      setLoadingPeriods(true);
      try {
        const res = await listFiscalPeriods(calendarId);
        if (cancelled) {
          return;
        }

        setPeriods(res?.rows || []);
      } catch (err) {
        if (!cancelled) {
          setError(
            err?.response?.data?.message || l("Failed to load fiscal periods.", "Mali donemler yuklenemedi.")
          );
        }
      } finally {
        if (!cancelled) {
          setLoadingPeriods(false);
        }
      }
    }

    loadPeriodsByBook();
    return () => {
      cancelled = true;
    };
  }, [canReadPeriods, books, selectedBookId, l]);

  useEffect(() => {
    setForm((prev) => {
      const currentPeriodId = toPositiveInt(prev.fiscalPeriodId);
      if (currentPeriodId && hasId(periods, currentPeriodId)) {
        return prev;
      }

      const latestPeriod = periods[periods.length - 1];
      return {
        ...prev,
        fiscalPeriodId: latestPeriod ? String(latestPeriod.id) : "",
      };
    });
  }, [periods]);

  const totals = useMemo(() => {
    return lines.reduce(
      (acc, line) => {
        acc.debit += toAmount(line.debitBase);
        acc.credit += toAmount(line.creditBase);
        return acc;
      },
      { debit: 0, credit: 0 }
    );
  }, [lines]);
  const selectedEntityShareholders = useMemo(
    () =>
      shareholders.filter(
        (row) => Number(row.legal_entity_id) === Number(selectedLegalEntityId)
      ),
    [selectedLegalEntityId, shareholders]
  );
  const selectedEntityShareholdersWithCommittedCapital = useMemo(
    () =>
      selectedEntityShareholders.filter(
        (row) => Number(row.committed_capital || 0) > 0
      ),
    [selectedEntityShareholders]
  );
  const selectedEntityEquityAccounts = useMemo(
    () =>
      accounts.filter((row) => {
        const sameEntity =
          Number(row.legal_entity_id) === Number(selectedLegalEntityId);
        const isActive = Boolean(row.is_active);
        const isEquity = String(row.account_type || "").toUpperCase() === "EQUITY";
        const allowPosting = !(
          row.allow_posting === false ||
          row.allow_posting === 0 ||
          row.allow_posting === "0"
        );
        return sameEntity && isActive && isEquity && allowPosting;
      }),
    [accounts, selectedLegalEntityId]
  );
  const commitmentSetupChecks = useMemo(() => {
    if (!selectedLegalEntityId) {
      return [];
    }
    return [
      {
        key: "shareholderMaster",
        label: l("Shareholder master exists", "Ortak ana verisi mevcut"),
        ready: selectedEntityShareholders.length > 0,
      },
      {
        key: "commitmentSubAccounts",
        label: l(
          "Each committed shareholder has debit and capital sub-accounts",
          "Taahhutu olan her ortak icin borc ve sermaye alt hesabi tanimli"
        ),
        ready: selectedEntityShareholdersWithCommittedCapital.every(
          (row) =>
            Boolean(toPositiveInt(row.commitment_debit_sub_account_id)) &&
            Boolean(toPositiveInt(row.capital_sub_account_id))
        ),
      },
      {
        key: "equitySubAccount",
        label: l(
          "Capital equity sub-account exists",
          "Sermaye icin equity alt hesap mevcut"
        ),
        ready: selectedEntityEquityAccounts.length > 0,
      },
      {
        key: "periods",
        label: l(
          "Fiscal periods are generated",
          "Mali donemler olusturulmus"
        ),
        ready: periods.length > 0,
      },
    ];
  }, [
    l,
    periods.length,
    selectedEntityEquityAccounts.length,
    selectedEntityShareholdersWithCommittedCapital,
    selectedEntityShareholders.length,
    selectedLegalEntityId,
  ]);
  const missingCommitmentSetupChecks = useMemo(
    () => commitmentSetupChecks.filter((check) => !check.ready),
    [commitmentSetupChecks]
  );
  const hasCommitmentSetupWarning =
    missingCommitmentSetupChecks.length > 0 ||
    shareholderCommitmentModuleNotReady;

  const isBalanced =
    Math.abs(totals.debit - totals.credit) < 0.0001 && totals.debit > 0;

  const canSubmit = canCreateJournal && isBalanced && lines.length >= 2 && !submitting;

  function setFormField(field, value) {
    setForm((prev) => ({
      ...prev,
      [field]: value,
    }));
  }

  function addLine() {
    setLines((prev) => [...prev, createLine()]);
  }

  function removeLine(lineId) {
    setLines((prev) => {
      if (prev.length <= 2) {
        return prev;
      }
      return prev.filter((line) => line.id !== lineId);
    });
  }

  function updateLine(lineId, field, value) {
    setLines((prev) =>
      prev.map((line) => (line.id === lineId ? { ...line, [field]: value } : line))
    );
  }

  async function handleSubmit(event) {
    event.preventDefault();

    if (!canCreateJournal) {
      setError(l("Missing permission: gl.journal.create", "Eksik yetki: gl.journal.create"));
      return;
    }

    const legalEntityId = toPositiveInt(form.legalEntityId);
    const bookId = toPositiveInt(form.bookId);
    const fiscalPeriodId = toPositiveInt(form.fiscalPeriodId);
    if (!legalEntityId || !bookId || !fiscalPeriodId) {
      setError(l("Legal entity, book, and fiscal period are required.", "Istirak / bagli ortak, defter ve mali donem zorunludur."));
      return;
    }
    if (lines.length < 2) {
      setError(l("At least two lines are required.", "En az iki satir gereklidir."));
      return;
    }

    const normalizedLines = [];
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const lineLabel = `Line ${i + 1}`;
      const accountId = toPositiveInt(line.accountId);
      if (!accountId) {
        setError(
          l(
            `${lineLabel}: account is required.`,
            `Satir ${i + 1}: hesap zorunludur.`
          )
        );
        return;
      }
      const operatingUnitId = toPositiveInt(line.operatingUnitId);
      if (line.operatingUnitId && !operatingUnitId) {
        setError(
          l(
            `${lineLabel}: operating unit must be a positive integer.`,
            `Satir ${i + 1}: birim pozitif bir tam sayi olmali.`
          )
        );
        return;
      }
      const selectedUnit = operatingUnitId ? unitsById.get(operatingUnitId) || null : null;
      const requiresSubledgerReference = Boolean(selectedUnit?.has_subledger);
      const subledgerReferenceNo = String(line.subledgerReferenceNo || "").trim();
      if (subledgerReferenceNo && !operatingUnitId) {
        setError(
          l(
            `${lineLabel}: subledger reference requires operating unit.`,
            `Satir ${i + 1}: alt defter referansi icin birim secilmelidir.`
          )
        );
        return;
      }
      if (requiresSubledgerReference && !subledgerReferenceNo) {
        setError(
          l(
            `${lineLabel}: subledger reference is required for selected unit.`,
            `Satir ${i + 1}: secilen birim icin alt defter referansi zorunludur.`
          )
        );
        return;
      }
      if (subledgerReferenceNo.length > 100) {
        setError(
          l(
            `${lineLabel}: subledger reference must be at most 100 characters.`,
            `Satir ${i + 1}: alt defter referansi en fazla 100 karakter olabilir.`
          )
        );
        return;
      }

      const debitBase = toAmount(line.debitBase);
      const creditBase = toAmount(line.creditBase);
      if (debitBase < 0 || creditBase < 0) {
        setError(
          l(
            `${lineLabel}: debit/credit cannot be negative.`,
            `Satir ${i + 1}: borc/alacak negatif olamaz.`
          )
        );
        return;
      }
      if ((debitBase === 0 && creditBase === 0) || (debitBase > 0 && creditBase > 0)) {
        setError(
          l(
            `${lineLabel}: enter either debit or credit.`,
            `Satir ${i + 1}: yalnizca borc veya alacak girin.`
          )
        );
        return;
      }

      normalizedLines.push({
        accountId,
        operatingUnitId: operatingUnitId || undefined,
        subledgerReferenceNo: subledgerReferenceNo || undefined,
        counterpartyLegalEntityId:
          toPositiveInt(line.counterpartyLegalEntityId) || undefined,
        description: line.description.trim() || undefined,
        currencyCode: String(form.currencyCode || "USD").toUpperCase(),
        amountTxn: debitBase > 0 ? debitBase : -creditBase,
        debitBase,
        creditBase,
      });
    }

    const totalDebit = normalizedLines.reduce((sum, line) => sum + line.debitBase, 0);
    const totalCredit = normalizedLines.reduce((sum, line) => sum + line.creditBase, 0);
    if (Math.abs(totalDebit - totalCredit) >= 0.0001) {
      setError(l("Entry is not balanced.", "Fis dengede degil."));
      return;
    }

    setSubmitting(true);
    setError("");
    setMessage("");
    setResult(null);

    try {
      const payload = {
        legalEntityId,
        bookId,
        fiscalPeriodId,
        entryDate: form.entryDate,
        documentDate: form.documentDate,
        currencyCode: String(form.currencyCode || "USD").toUpperCase(),
        sourceType: "MANUAL",
        referenceNo: form.referenceNo.trim() || undefined,
        description: form.description.trim() || undefined,
        lines: normalizedLines,
      };

      const createResult = await createJournal(payload);
      const journalEntryId = Number(createResult?.journalEntryId || 0);
      const journalNo = createResult?.journalNo || null;

      let posted = false;
      if (form.autoPost && canPostJournal && journalEntryId) {
        const postResult = await postJournal(journalEntryId);
        posted = Boolean(postResult?.posted);
      }

      setResult({
        journalEntryId,
        journalNo,
        posted,
      });
      if (form.autoPost && canPostJournal) {
        setMessage(
          posted
            ? l("Opening entry created and posted.", "Acilis fisi olusturuldu ve post edildi.")
            : l(
                "Opening entry created; posting did not complete.",
                "Acilis fisi olusturuldu; post islemi tamamlanamadi."
              )
        );
      } else if (form.autoPost && !canPostJournal) {
        setMessage(
          l(
            "Opening entry created as draft (missing gl.journal.post permission).",
            "Acilis fisi taslak olarak olusturuldu (gl.journal.post yetkisi eksik)."
          )
        );
      } else {
        setMessage(l("Opening entry created as draft.", "Acilis fisi taslak olarak olusturuldu."));
      }

      setLines([createLine(), createLine()]);
      setForm((prev) => ({
        ...prev,
        referenceNo: "",
        description: l("Opening entry", "Acilis fisi"),
      }));
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to create opening entry.", "Acilis fisi olusturulamadi."));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="space-y-4">
      <header>
        <h1 className="text-xl font-semibold text-slate-900">{l("Create Opening Voucher", "Acilis Fisi Olustur")}</h1>
        <p className="mt-1 text-sm text-slate-600">
          {l(
            "Create a balanced opening entry with optional immediate posting.",
            "Opsiyonel aninda post secenegi ile dengeli bir acilis fisi olusturun."
          )}
        </p>
      </header>
      {selectedLegalEntityId ? (
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="text-sm font-semibold text-slate-700">
            {l(
              "Capital Commitment Setup Required List",
              "Sermaye Taahhut Kurulum Gereklilik Listesi"
            )}
          </h2>
          <p className="mt-1 text-xs text-slate-600">
            {l(
              "Used when this opening entry is for shareholder capital commitment (sermaye taahhut kaydi).",
              "Bu liste, acilis fisini hissedar sermaye taahhut kaydi icin kullandiginizda gecerlidir."
            )}
          </p>
          <div className="mt-2 grid gap-2 md:grid-cols-2">
            {commitmentSetupChecks.map((check) => (
              <div
                key={check.key}
                className="flex items-center justify-between rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs"
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
          {selectedShareholderCommitmentReadiness ? (
            <div
              className={`mt-2 rounded-lg border px-3 py-2 text-xs ${
                selectedShareholderCommitmentReadiness.ready
                  ? "border-emerald-200 bg-emerald-50 text-emerald-800"
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
            </div>
          ) : null}
          {hasCommitmentSetupWarning ? (
            <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
              <div className="font-semibold">
                {l(
                  "System notice: setup is incomplete for automatic capital commitment journal flow.",
                  "Sistem uyarisi: otomatik sermaye taahhut yevmiye akisi icin kurulum eksik."
                )}
              </div>
              {shareholderCommitmentModuleNotReady ? (
                <div className="mt-2 space-y-1">
                  {Array.isArray(
                    selectedShareholderCommitmentReadiness?.missingPurposeCodes
                  ) &&
                  selectedShareholderCommitmentReadiness.missingPurposeCodes.length > 0 ? (
                    <div>
                      {l("Missing purpose codes:", "Eksik amac kodlari:")}{" "}
                      {selectedShareholderCommitmentReadiness.missingPurposeCodes.join(
                        ", "
                      )}
                    </div>
                  ) : null}
                  {Array.isArray(
                    selectedShareholderCommitmentReadiness?.invalidMappings
                  ) &&
                  selectedShareholderCommitmentReadiness.invalidMappings.length > 0 ? (
                    <ul className="list-disc space-y-0.5 pl-4">
                      {selectedShareholderCommitmentReadiness.invalidMappings.map(
                        (row, index) => (
                          <li key={`opening-readiness-invalid-${index}`}>
                            {String(row?.purposeCode || "-")}:{" "}
                            {formatShareholderReadinessReason(row?.reason, l)}
                          </li>
                        )
                      )}
                    </ul>
                  ) : null}
                </div>
              ) : null}
              <div className="mt-2 flex flex-wrap gap-2">
                <Link
                  to="/app/ayarlar/organizasyon-yonetimi"
                  className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900"
                >
                  {l("Open Organization Management", "Organizasyon Yonetimini Ac")}
                </Link>
                <Link
                  to="/app/ayarlar/hesap-plani-ayarlari"
                  className="rounded border border-amber-300 bg-white px-2.5 py-1 font-semibold text-amber-900"
                >
                  {l("Open GL Setup", "GL Ayarlarini Ac")}
                </Link>
              </div>
            </div>
          ) : (
            <div className="mt-3 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-xs text-emerald-800">
              {l(
                "System notice: setup is complete for capital commitment journal entries.",
                "Sistem bildirimi: sermaye taahhut yevmiye kaydi icin kurulum tamamlandi."
              )}
            </div>
          )}
        </section>
      ) : null}

      {!canCreateJournal && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          {l("Missing permission:", "Eksik yetki:")} <span className="font-mono">gl.journal.create</span>
        </div>
      )}
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
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="mb-3 flex items-center justify-between gap-3">
            <h2 className="text-sm font-semibold text-slate-700">{l("Document Header", "Belge Basligi")}</h2>
            <span className="text-xs text-slate-500">
              {loadingRefs ? l("Loading references...", "Referanslar yukleniyor...") : l("Ready", "Hazir")}
            </span>
          </div>

          <div className="grid gap-2 md:grid-cols-4">
            <select
              value={form.legalEntityId}
              onChange={(event) => setFormField("legalEntityId", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
              disabled={!canReadOrgTree || loadingRefs}
            >
              <option value="">{l("Select legal entity", "Istirak / bagli ortak secin")}</option>
              {legalEntities.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.code} - {row.name}
                </option>
              ))}
            </select>

            <select
              value={form.bookId}
              onChange={(event) => setFormField("bookId", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
              disabled={!canReadBooks || loadingRefs}
            >
              <option value="">{l("Select book", "Defter secin")}</option>
              {books.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.code} - {row.name}
                </option>
              ))}
            </select>

            <select
              value={form.fiscalPeriodId}
              onChange={(event) => setFormField("fiscalPeriodId", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
              disabled={!canReadPeriods || loadingPeriods}
            >
              <option value="">
                {loadingPeriods
                  ? l("Loading periods...", "Donemler yukleniyor...")
                  : l("Select period", "Donem secin")}
              </option>
              {periods.map((row) => (
                <option key={row.id} value={row.id}>
                  FY{row.fiscal_year} P{String(row.period_no).padStart(2, "0")} -{" "}
                  {row.period_name}
                </option>
              ))}
            </select>

            <input
              value={form.currencyCode}
              onChange={(event) =>
                setFormField("currencyCode", event.target.value.toUpperCase())
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              maxLength={3}
              placeholder={l("Currency", "Para birimi")}
              required
            />

            <input
              type="date"
              value={form.entryDate}
              onChange={(event) => setFormField("entryDate", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            />

            <input
              type="date"
              value={form.documentDate}
              onChange={(event) => setFormField("documentDate", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            />

            <input
              value={form.referenceNo}
              onChange={(event) => setFormField("referenceNo", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Reference no", "Referans no")}
            />

            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={form.autoPost}
                onChange={(event) => setFormField("autoPost", event.target.checked)}
                disabled={!canPostJournal}
              />
              {l("Auto-post", "Otomatik post et")}
            </label>

            <input
              value={form.description}
              onChange={(event) => setFormField("description", event.target.value)}
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-4"
              placeholder={l("Description", "Aciklama")}
            />
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white">
          <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
            <h2 className="text-sm font-semibold text-slate-700">{l("Lines", "Satirlar")}</h2>
            <button
              type="button"
              onClick={addLine}
              className="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50"
            >
              {l("Add line", "Satir ekle")}
            </button>
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="px-3 py-2">{l("Account", "Hesap")}</th>
                  <th className="px-3 py-2">{l("Unit", "Birim")}</th>
                  <th className="px-3 py-2">{l("Subledger Ref", "Alt Defter Ref")}</th>
                  <th className="px-3 py-2">{l("Counterparty", "Karsi taraf")}</th>
                  <th className="px-3 py-2">{l("Description", "Aciklama")}</th>
                  <th className="px-3 py-2">{l("Debit", "Borc")}</th>
                  <th className="px-3 py-2">{l("Credit", "Alacak")}</th>
                  <th className="px-3 py-2">{l("Action", "Islem")}</th>
                </tr>
              </thead>
              <tbody>
                {lines.map((line, index) => (
                  <tr key={line.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">
                      <select
                        value={line.accountId}
                        onChange={(event) =>
                          updateLine(line.id, "accountId", event.target.value)
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                        required
                        disabled={!canReadAccounts}
                      >
                        <option value="">{l("Select account", "Hesap secin")}</option>
                        {postableAccounts.map((account) => (
                          <option key={account.id} value={account.id}>
                            {account.code} - {account.name}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="px-3 py-2">
                      <select
                        value={line.operatingUnitId}
                        onChange={(event) =>
                          updateLine(line.id, "operatingUnitId", event.target.value)
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                        disabled={!canReadOrgTree}
                      >
                        <option value="">{l("Optional", "Opsiyonel")}</option>
                        {operatingUnits.map((unit) => (
                          <option key={unit.id} value={unit.id}>
                            {unit.code} - {unit.name}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="px-3 py-2">
                      <input
                        value={line.subledgerReferenceNo || ""}
                        onChange={(event) =>
                          updateLine(line.id, "subledgerReferenceNo", event.target.value)
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                        placeholder={
                          (unitsById.get(toPositiveInt(line.operatingUnitId))?.has_subledger ?? false)
                            ? l("Required", "Zorunlu")
                            : l("Optional", "Opsiyonel")
                        }
                        required={unitsById.get(toPositiveInt(line.operatingUnitId))?.has_subledger ?? false}
                      />
                    </td>
                    <td className="px-3 py-2">
                      <select
                        value={line.counterpartyLegalEntityId}
                        onChange={(event) =>
                          updateLine(
                            line.id,
                            "counterpartyLegalEntityId",
                            event.target.value
                          )
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                        disabled={!canReadOrgTree}
                      >
                        <option value="">{l("Optional", "Opsiyonel")}</option>
                        {legalEntities.map((entity) => (
                          <option key={entity.id} value={entity.id}>
                            {entity.code} - {entity.name}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="px-3 py-2">
                      <input
                        value={line.description}
                        onChange={(event) =>
                          updateLine(line.id, "description", event.target.value)
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-xs"
                        placeholder={l(
                          `Line ${index + 1} description`,
                          `Satir ${index + 1} aciklamasi`
                        )}
                      />
                    </td>
                    <td className="px-3 py-2">
                      <input
                        type="number"
                        min={0}
                        step="0.01"
                        value={line.debitBase}
                        onChange={(event) =>
                          updateLine(line.id, "debitBase", event.target.value)
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-right text-xs"
                        required
                      />
                    </td>
                    <td className="px-3 py-2">
                      <input
                        type="number"
                        min={0}
                        step="0.01"
                        value={line.creditBase}
                        onChange={(event) =>
                          updateLine(line.id, "creditBase", event.target.value)
                        }
                        className="w-full rounded border border-slate-300 px-2 py-1.5 text-right text-xs"
                        required
                      />
                    </td>
                    <td className="px-3 py-2">
                      <button
                        type="button"
                        onClick={() => removeLine(line.id)}
                        disabled={lines.length <= 2}
                        className="rounded border border-rose-200 px-2 py-1 text-xs font-semibold text-rose-700 disabled:opacity-50"
                      >
                        {l("Remove", "Kaldir")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="border-t border-slate-200 bg-slate-50 text-sm font-semibold text-slate-700">
                  <td className="px-3 py-2" colSpan={5}>
                    {l("Totals", "Toplamlar")}
                  </td>
                  <td className="px-3 py-2 text-right">{formatAmount(totals.debit)}</td>
                  <td className="px-3 py-2 text-right">{formatAmount(totals.credit)}</td>
                  <td className="px-3 py-2 text-xs">
                    {isBalanced ? (
                      <span className="rounded bg-emerald-100 px-2 py-0.5 text-emerald-700">
                        {l("Balanced", "Dengeli")}
                      </span>
                    ) : (
                      <span className="rounded bg-amber-100 px-2 py-0.5 text-amber-700">
                        {l("Not balanced", "Dengede degil")}
                      </span>
                    )}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
        </section>

        <div className="flex items-center justify-between gap-3">
          <p className="text-xs text-slate-500">
            {l("Posting requires", "Post islemi icin")}{" "}
            <span className="font-mono">gl.journal.post</span> {l("permission.", "yetkisi gerekir.")}
          </p>
          <button
            type="submit"
            disabled={!canSubmit}
            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {submitting
              ? l("Saving...", "Kaydediliyor...")
              : form.autoPost && canPostJournal
              ? l("Create and Post", "Olustur ve Post Et")
              : l("Create Draft", "Taslak Olustur")}
          </button>
        </div>
      </form>

      {result && (
        <section className="rounded-xl border border-slate-200 bg-white p-4 text-sm">
          <h2 className="mb-2 font-semibold text-slate-800">{l("Last Created Entry", "Son Olusturulan Fis")}</h2>
          <div className="grid gap-1 text-slate-700">
            <div>
              {l("Journal ID:", "Fis ID:")} <span className="font-mono">{result.journalEntryId || "-"}</span>
            </div>
            <div>
              {l("Journal No:", "Fis No:")} <span className="font-mono">{result.journalNo || "-"}</span>
            </div>
            <div>{l("Status:", "Durum:")} {result.posted ? "POSTED" : "DRAFT"}</div>
          </div>
        </section>
      )}
    </div>
  );
}

