import { Link } from "react-router-dom";
import { useI18n } from "../i18n/useI18n.js";

export default function NotFound() {
  const { t } = useI18n();

  return (
    <div className="grid min-h-screen place-items-center bg-slate-100 px-4">
      <div className="space-y-2 text-center">
        <h2 className="text-2xl font-semibold text-slate-900">{t("notFound.title")}</h2>
        <Link to="/app" className="text-sky-700 underline hover:text-sky-500">
          {t("notFound.goToApp")}
        </Link>
      </div>
    </div>
  );
}
