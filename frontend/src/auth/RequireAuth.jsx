import { Navigate, useLocation } from "react-router-dom";
import { useI18n } from "../i18n/useI18n.js";
import { useAuth } from "./useAuth.js";

export default function RequireAuth({ children }) {
  const { isAuthed, booting } = useAuth();
  const { t } = useI18n();
  const location = useLocation();

  if (booting) {
    return (
      <div className="grid min-h-screen place-items-center bg-slate-100">
        <div className="text-slate-600">{t("authGuards.loading")}</div>
      </div>
    );
  }

  if (!isAuthed) {
    return <Navigate to="/login" replace state={{ from: location }} />;
  }

  return children;
}
