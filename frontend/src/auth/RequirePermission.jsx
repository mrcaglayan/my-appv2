import { Navigate, useLocation } from "react-router-dom";
import { useI18n } from "../i18n/useI18n.js";
import { useAuth } from "./useAuth.js";

export default function RequirePermission({ anyOf = [], allOf = [], children }) {
  const { isAuthed, booting, hasAnyPermission, hasAllPermissions } = useAuth();
  const { t } = useI18n();
  const location = useLocation();

  if (booting) {
    return (
      <div className="grid min-h-[40vh] place-items-center">
        <div className="text-slate-600">{t("authGuards.loading")}</div>
      </div>
    );
  }

  if (!isAuthed) {
    return <Navigate to="/login" replace state={{ from: location }} />;
  }

  const allowed =
    hasAnyPermission(anyOf) &&
    hasAllPermissions(allOf);

  if (!allowed) {
    return (
      <div className="mx-auto max-w-2xl rounded-xl border border-amber-200 bg-amber-50 p-5">
        <h2 className="text-lg font-semibold text-amber-900">
          {t("authGuards.accessDeniedTitle")}
        </h2>
        <p className="mt-1 text-sm text-amber-800">
          {t("authGuards.accessDeniedDescription")}
        </p>
      </div>
    );
  }

  return children;
}
