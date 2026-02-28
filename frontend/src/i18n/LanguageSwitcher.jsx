import { useI18n } from "./useI18n.js";

export default function LanguageSwitcher({ className = "" }) {
  const { language, setLanguage, t } = useI18n();

  return (
    <div
      className={`inline-flex items-center rounded-lg border border-slate-200 bg-white p-0.5 shadow-sm ${className}`.trim()}
      aria-label={t("language.switchLabel")}
      role="group"
    >
      <button
        type="button"
        onClick={() => setLanguage("tr")}
        className={`rounded-md px-2.5 py-1 text-xs font-semibold transition ${
          language === "tr"
            ? "bg-slate-900 text-white"
            : "text-slate-600 hover:bg-slate-100"
        }`}
      >
        {t("language.tr", "TR")}
      </button>
      <button
        type="button"
        onClick={() => setLanguage("en")}
        className={`rounded-md px-2.5 py-1 text-xs font-semibold transition ${
          language === "en"
            ? "bg-slate-900 text-white"
            : "text-slate-600 hover:bg-slate-100"
        }`}
      >
        {t("language.en", "EN")}
      </button>
    </div>
  );
}
