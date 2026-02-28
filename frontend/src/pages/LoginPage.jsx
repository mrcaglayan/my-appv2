import { useState } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import AuthLayout from "../layouts/AuthLayout";
import LanguageSwitcher from "../i18n/LanguageSwitcher.jsx";
import { useI18n } from "../i18n/useI18n.js";
import { useAuth } from "../auth/useAuth.js";

export default function LoginPage() {
  const { isAuthed, login } = useAuth();
  const { t } = useI18n();
  const navigate = useNavigate();
  const location = useLocation();

  const from = location.state?.from?.pathname || "/app";
  const providerPanelEnabled =
    import.meta.env.DEV ||
    String(import.meta.env.VITE_PROVIDER_PANEL_ENABLED || "")
      .trim()
      .toLowerCase() === "true" ||
    String(import.meta.env.VITE_PROVIDER_BOOTSTRAP_ENABLED || "")
      .trim()
      .toLowerCase() === "true";

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  if (isAuthed) return <Navigate to="/app" replace />;

  async function onSubmit(e) {
    e.preventDefault();
    setError("");
    setBusy(true);

    try {
      await login(email, password);
      navigate(from, { replace: true });
    } catch (err) {
      setError(err?.response?.data?.message || err.message || t("login.failed"));
    } finally {
      setBusy(false);
    }
  }

  return (
    <AuthLayout>
      <div className="mb-5 flex items-center justify-between gap-3">
        <h2 className="text-2xl font-semibold text-slate-900">{t("login.title")}</h2>
        <LanguageSwitcher />
      </div>

      <form onSubmit={onSubmit} className="grid gap-3">
        <label className="grid gap-1">
          <span>{t("login.email")}</span>
          <input
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="you@example.com"
            autoComplete="username"
            className="rounded-md border border-slate-300 px-3 py-2 text-slate-900 outline-none ring-0 placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-200"
          />
        </label>

        <label className="grid gap-1">
          <span>{t("login.password")}</span>
          <input
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="********"
            type="password"
            autoComplete="current-password"
            className="rounded-md border border-slate-300 px-3 py-2 text-slate-900 outline-none ring-0 placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-200"
          />
        </label>

        {error && <div className="text-sm text-red-600">{error}</div>}

        <button
          disabled={busy}
          type="submit"
          className="mt-2 rounded-md bg-sky-600 px-4 py-2 font-semibold text-white hover:bg-sky-500 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {busy ? t("login.signingIn") : t("login.signIn")}
        </button>

        {providerPanelEnabled ? (
          <button
            type="button"
            onClick={() => navigate("/provider/login")}
            className="rounded-md border border-slate-300 px-4 py-2 font-semibold text-slate-700 hover:bg-slate-50"
          >
            {t("login.providerAdminSignIn")}
          </button>
        ) : null}
      </form>
    </AuthLayout>
  );
}
