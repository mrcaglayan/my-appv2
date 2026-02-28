import { useCallback, useEffect, useMemo, useState } from "react";
import {
  bootstrapTenantBaseline,
  getTenantReadiness,
} from "../api/readiness.js";
import { useAuth } from "../auth/useAuth.js";
import { TenantReadinessContext } from "./tenantReadinessContext.js";

export default function TenantReadinessProvider({ children }) {
  const { isAuthed } = useAuth();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [readiness, setReadiness] = useState(null);
  const [bootstrapping, setBootstrapping] = useState(false);
  const [bootstrapError, setBootstrapError] = useState("");
  const [bootstrapResult, setBootstrapResult] = useState(null);

  const refresh = useCallback(async () => {
    if (!isAuthed) {
      setLoading(false);
      setError("");
      setReadiness(null);
      return null;
    }

    setLoading(true);
    setError("");
    try {
      const data = await getTenantReadiness();
      setReadiness(data || null);
      return data || null;
    } catch (err) {
      setReadiness(null);
      setError(
        err?.response?.data?.message || err?.message || "Failed to load readiness."
      );
      return null;
    } finally {
      setLoading(false);
    }
  }, [isAuthed]);

  const runBaselineBootstrap = useCallback(
    async (payload = {}) => {
      if (!isAuthed) {
        return null;
      }

      setBootstrapping(true);
      setBootstrapError("");
      try {
        const data = await bootstrapTenantBaseline(payload);
        setBootstrapResult(data || null);
        await refresh();
        return data || null;
      } catch (err) {
        setBootstrapError(
          err?.response?.data?.message ||
            err?.message ||
            "Failed to run baseline bootstrap."
        );
        return null;
      } finally {
        setBootstrapping(false);
      }
    },
    [isAuthed, refresh]
  );

  useEffect(() => {
    refresh();
  }, [refresh]);

  const value = useMemo(
    () => ({
      loading,
      error,
      readiness,
      ready: Boolean(readiness?.ready),
      missingChecks: Array.isArray(readiness?.checks)
        ? readiness.checks.filter((check) => !check.ready)
        : [],
      refresh,
      bootstrapping,
      bootstrapError,
      bootstrapResult,
      runBaselineBootstrap,
    }),
    [
      loading,
      error,
      readiness,
      refresh,
      bootstrapping,
      bootstrapError,
      bootstrapResult,
      runBaselineBootstrap,
    ]
  );

  return (
    <TenantReadinessContext.Provider value={value}>
      {children}
    </TenantReadinessContext.Provider>
  );
}
