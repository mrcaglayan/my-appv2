function normalizeSteps(steps) {
  return Array.isArray(steps) ? steps : [];
}

function stateClasses(state) {
  const normalized = String(state || "").trim().toLowerCase();
  if (normalized === "done") {
    return {
      dot: "border-emerald-300 bg-emerald-500 text-white",
      line: "bg-emerald-200",
      title: "text-emerald-800",
    };
  }
  if (normalized === "current") {
    return {
      dot: "border-cyan-300 bg-cyan-500 text-white",
      line: "bg-cyan-200",
      title: "text-cyan-800",
    };
  }
  return {
    dot: "border-slate-300 bg-white text-slate-500",
    line: "bg-slate-200",
    title: "text-slate-700",
  };
}

function formatDateTime(value) {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return String(value);
  }
  return parsed.toLocaleString();
}

export default function StatusTimeline({
  title = "Status Timeline",
  steps = [],
  emptyText = "No lifecycle data available.",
  className = "",
}) {
  const rows = normalizeSteps(steps);

  return (
    <section className={`rounded-xl border border-slate-200 bg-white p-4 shadow-sm ${className}`}>
      <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-700">{title}</h3>
      {rows.length === 0 ? <p className="mt-2 text-sm text-slate-500">{emptyText}</p> : null}
      {rows.length > 0 ? (
        <ol className="mt-3 space-y-3">
          {rows.map((step, index) => {
            const classes = stateClasses(step?.state);
            const key = String(step?.key || step?.statusCode || index);
            const label = String(step?.label || step?.statusCode || key);
            const description = String(step?.description || "").trim();
            const atText = formatDateTime(step?.eventAt || step?.at);
            const actorText = String(step?.actorName || step?.actor || "").trim();
            const noteText = String(step?.note || "").trim();
            const isLast = index === rows.length - 1;
            return (
              <li key={key} className="relative pl-9">
                {!isLast ? (
                  <span
                    aria-hidden="true"
                    className={`absolute left-[13px] top-6 h-[calc(100%-4px)] w-px ${classes.line}`}
                  />
                ) : null}
                <span
                  aria-hidden="true"
                  className={`absolute left-0 top-0 inline-flex h-7 w-7 items-center justify-center rounded-full border text-xs font-semibold ${classes.dot}`}
                >
                  {index + 1}
                </span>
                <div>
                  <p className={`text-sm font-semibold ${classes.title}`}>{label}</p>
                  {description ? <p className="mt-0.5 text-xs text-slate-600">{description}</p> : null}
                  {atText || actorText || noteText ? (
                    <p className="mt-1 text-xs text-slate-500">
                      {atText ? atText : "-"}
                      {actorText ? ` | ${actorText}` : ""}
                      {noteText ? ` | ${noteText}` : ""}
                    </p>
                  ) : null}
                </div>
              </li>
            );
          })}
        </ol>
      ) : null}
    </section>
  );
}
