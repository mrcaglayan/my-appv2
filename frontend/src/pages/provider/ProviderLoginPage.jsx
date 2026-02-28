import { useState } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import AuthLayout from "../../layouts/AuthLayout.jsx";
import { useI18n } from "../../i18n/useI18n.js";
import { useProviderAuth } from "../../provider/useProviderAuth.js";

export default function ProviderLoginPage() {
  const { isAuthed, login } = useProviderAuth();
  const { t } = useI18n();
  const navigate = useNavigate();
  const location = useLocation();
  const from = location.state?.from?.pathname || "/provider/bootstrap";

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  if (isAuthed) {
    return <Navigate to="/provider/bootstrap" replace />;
  }

  async function onSubmit(event) {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      await login(email, password);
      navigate(from, { replace: true });
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          err?.message ||
          t("providerLogin.failed")
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <AuthLayout>
      <h2 className="text-2xl font-semibold text-slate-900">
        {t("providerLogin.title")}
      </h2>
      <p className="mt-1 text-sm text-slate-600">
        {t("providerLogin.subtitle")}
      </p>

      <form onSubmit={onSubmit} className="mt-4 grid gap-3">
        <label className="grid gap-1">
          <span>{t("providerLogin.email")}</span>
          <input
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            autoComplete="username"
            placeholder={t("providerLogin.emailPlaceholder")}
            className="rounded-md border border-slate-300 px-3 py-2 text-slate-900 outline-none ring-0 placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-200"
            required
          />
        </label>

        <label className="grid gap-1">
          <span>{t("providerLogin.password")}</span>
          <input
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            autoComplete="current-password"
            placeholder={t("providerLogin.passwordPlaceholder")}
            className="rounded-md border border-slate-300 px-3 py-2 text-slate-900 outline-none ring-0 placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-200"
            required
          />
        </label>

        {error ? <div className="text-sm text-rose-700">{error}</div> : null}

        <button
          type="submit"
          disabled={busy}
          className="rounded-md bg-slate-900 px-4 py-2 font-semibold text-white disabled:opacity-60"
        >
          {busy ? t("providerLogin.signingIn") : t("providerLogin.signIn")}
        </button>

        <button
          type="button"
          onClick={() => navigate("/login")}
          className="rounded-md border border-slate-300 px-4 py-2 font-semibold text-slate-700 hover:bg-slate-50"
        >
          {t("providerLogin.backToUserLogin")}
        </button>
      </form>
    </AuthLayout>
  );
}
