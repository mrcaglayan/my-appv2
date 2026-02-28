function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function normalizeOptionalText(value) {
  const text = String(value || "").trim();
  return text || null;
}

function normalizeDate(value) {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toISOString();
}

const LIFECYCLE_DEFINITIONS = {
  cariDocument: {
    id: "cariDocument",
    label: "Cari Document",
    terminalStatuses: ["SETTLED", "CANCELLED", "REVERSED"],
    statuses: [
      { code: "DRAFT", label: "Draft", description: "Document is editable." },
      {
        code: "POSTED",
        label: "Posted",
        description: "Document has posted journal entries.",
      },
      {
        code: "PARTIALLY_SETTLED",
        label: "Partially Settled",
        description: "Document has partial settlement allocations.",
      },
      {
        code: "SETTLED",
        label: "Settled",
        description: "Document is fully settled.",
      },
      {
        code: "CANCELLED",
        label: "Cancelled",
        description: "Draft was cancelled before posting.",
      },
      {
        code: "REVERSED",
        label: "Reversed",
        description: "Posted document was reversed.",
      },
    ],
    transitions: {
      post: { from: ["DRAFT"], to: "POSTED", label: "Post" },
      settlePartial: {
        from: ["POSTED", "PARTIALLY_SETTLED"],
        to: "PARTIALLY_SETTLED",
        label: "Settle (Partial)",
      },
      settleFull: {
        from: ["POSTED", "PARTIALLY_SETTLED"],
        to: "SETTLED",
        label: "Settle (Full)",
      },
      cancel: { from: ["DRAFT"], to: "CANCELLED", label: "Cancel Draft" },
      reverse: { from: ["POSTED"], to: "REVERSED", label: "Reverse" },
    },
  },
  cashTransaction: {
    id: "cashTransaction",
    label: "Cash Transaction",
    terminalStatuses: ["POSTED", "CANCELLED", "REVERSED"],
    statuses: [
      { code: "DRAFT", label: "Draft", description: "Awaiting submit/review." },
      { code: "SUBMITTED", label: "Submitted", description: "Submitted for approval." },
      { code: "APPROVED", label: "Approved", description: "Approved for posting." },
      { code: "POSTED", label: "Posted", description: "Posted to ledger." },
      { code: "REVERSED", label: "Reversed", description: "Reversal posted." },
      { code: "CANCELLED", label: "Cancelled", description: "Cancelled before posting." },
    ],
    transitions: {
      submit: { from: ["DRAFT"], to: "SUBMITTED", label: "Submit" },
      approve: { from: ["SUBMITTED"], to: "APPROVED", label: "Approve" },
      post: {
        from: ["DRAFT", "SUBMITTED", "APPROVED"],
        to: "POSTED",
        label: "Post",
      },
      cancel: {
        from: ["DRAFT", "SUBMITTED"],
        to: "CANCELLED",
        label: "Cancel",
      },
      reverse: { from: ["POSTED"], to: "REVERSED", label: "Reverse" },
    },
  },
  cashSession: {
    id: "cashSession",
    label: "Cash Session",
    terminalStatuses: ["CLOSED"],
    statuses: [
      { code: "OPEN", label: "Open", description: "Session is accepting transactions." },
      { code: "CLOSED", label: "Closed", description: "Session was closed and reconciled." },
    ],
    transitions: {
      close: { from: ["OPEN"], to: "CLOSED", label: "Close Session" },
    },
  },
  payrollRun: {
    id: "payrollRun",
    label: "Payroll Run",
    terminalStatuses: ["FINALIZED"],
    statuses: [
      { code: "DRAFT", label: "Draft", description: "Adjustment shell or pre-import draft." },
      { code: "IMPORTED", label: "Imported", description: "Provider file imported." },
      { code: "REVIEWED", label: "Reviewed", description: "Validated and reviewed." },
      { code: "FINALIZED", label: "Finalized", description: "Finalized for posting/close." },
    ],
    transitions: {
      import: { from: ["DRAFT"], to: "IMPORTED", label: "Import" },
      review: { from: ["IMPORTED"], to: "REVIEWED", label: "Review" },
      finalize: { from: ["REVIEWED"], to: "FINALIZED", label: "Finalize" },
    },
  },
  payrollClose: {
    id: "payrollClose",
    label: "Payroll Close",
    terminalStatuses: ["CLOSED", "REOPENED"],
    statuses: [
      { code: "DRAFT", label: "Draft", description: "Checklist is being prepared." },
      {
        code: "READY",
        label: "Ready",
        description: "Checks passed and ready for request.",
      },
      {
        code: "REQUESTED",
        label: "Requested",
        description: "Awaiting close approval.",
      },
      { code: "CLOSED", label: "Closed", description: "Payroll period is closed." },
      {
        code: "REOPENED",
        label: "Reopened",
        description: "Closed period has been reopened.",
      },
    ],
    transitions: {
      prepare: { from: ["DRAFT"], to: "READY", label: "Prepare" },
      request: { from: ["READY"], to: "REQUESTED", label: "Request Close" },
      approveClose: {
        from: ["REQUESTED"],
        to: "CLOSED",
        label: "Approve & Close",
      },
      reopen: { from: ["CLOSED"], to: "REOPENED", label: "Reopen" },
    },
  },
};

export const LIFECYCLE_ENTITY_TYPES = Object.freeze(Object.keys(LIFECYCLE_DEFINITIONS));

export function getLifecycleDefinition(entityType) {
  const key = String(entityType || "").trim();
  return LIFECYCLE_DEFINITIONS[key] || null;
}

export function getLifecycleStatusMeta(entityType, statusCode) {
  const definition = getLifecycleDefinition(entityType);
  if (!definition) {
    return null;
  }
  const normalizedStatus = toUpper(statusCode);
  return definition.statuses.find((row) => row.code === normalizedStatus) || null;
}

export function getLifecycleAllowedActions(entityType, currentStatus) {
  const definition = getLifecycleDefinition(entityType);
  if (!definition) {
    return [];
  }
  const normalizedStatus = toUpper(currentStatus);
  return Object.entries(definition.transitions)
    .filter(([, transition]) => transition.from.includes(normalizedStatus))
    .map(([action, transition]) => ({
      action,
      label: transition.label,
      toStatus: transition.to,
    }));
}

export function buildLifecycleTimelineSteps(entityType, currentStatus, events = []) {
  const definition = getLifecycleDefinition(entityType);
  if (!definition) {
    return [];
  }

  const normalizedStatus = toUpper(currentStatus);
  const statusIndex = definition.statuses.findIndex((status) => status.code === normalizedStatus);
  const timelineRows = definition.statuses.map((status, index) => {
    let state = "upcoming";
    if (status.code === normalizedStatus) {
      state = "current";
    } else if (statusIndex >= 0 && index < statusIndex) {
      state = "done";
    } else if (statusIndex === -1 && index === 0) {
      state = "current";
    }
    return {
      key: status.code,
      statusCode: status.code,
      label: status.label,
      description: status.description || "",
      state,
      eventAt: null,
      actorName: null,
      note: null,
    };
  });

  const eventMap = new Map();
  for (const event of Array.isArray(events) ? events : []) {
    const eventStatus = toUpper(event?.statusCode || event?.toStatus || event?.status);
    if (!eventStatus) {
      continue;
    }
    const existing = eventMap.get(eventStatus);
    const at = normalizeDate(event?.at || event?.createdAt || event?.created_at);
    const actorName = normalizeOptionalText(
      event?.actorName || event?.actor || event?.performedBy || event?.performed_by
    );
    const note = normalizeOptionalText(event?.note || event?.reason || event?.description);
    if (!existing) {
      eventMap.set(eventStatus, {
        at,
        actorName,
        note,
      });
      continue;
    }
    const existingDate = existing.at ? Date.parse(existing.at) : Number.NaN;
    const incomingDate = at ? Date.parse(at) : Number.NaN;
    if (Number.isFinite(existingDate) && Number.isFinite(incomingDate) && incomingDate > existingDate) {
      eventMap.set(eventStatus, { at, actorName, note });
    }
  }

  return timelineRows.map((row) => {
    const match = eventMap.get(row.statusCode);
    if (!match) {
      return row;
    }
    return {
      ...row,
      eventAt: match.at || null,
      actorName: match.actorName || null,
      note: match.note || null,
    };
  });
}
