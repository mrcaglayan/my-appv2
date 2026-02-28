import { useI18n } from "../i18n/useI18n.js";

export default function ModulePlaceholderPage({ title, path }) {
  const { t } = useI18n();
  const resolvedTitle = t(["sidebar", "byPath", path], title || t("modulePlaceholder.defaultTitle"));

  return (
    <div className="mx-auto max-w-3xl rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
      <h1 className="text-xl font-semibold text-slate-900">{resolvedTitle}</h1>
      <p className="mt-2 text-sm text-slate-600">
        {t("modulePlaceholder.description")}
      </p>
      <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
        {t("modulePlaceholder.routeLabel")}{" "}
        <span className="font-mono text-slate-800">{path}</span>
      </div>
    </div>
  );
}
