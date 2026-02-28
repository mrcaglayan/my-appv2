import { useCallback, useEffect, useMemo, useState } from "react";
import {
  getProviderAdminMe,
  loginProviderAdmin,
} from "../api/providerControl.js";
import { ProviderAuthContext } from "./providerAuthContext.js";

const PROVIDER_TOKEN_KEY = "provider_token";

export function ProviderAuthProvider({ children }) {
  const [token, setToken] = useState(null);
  const [providerAdmin, setProviderAdmin] = useState(null);
  const [booting, setBooting] = useState(true);

  const clearSession = useCallback(() => {
    localStorage.removeItem(PROVIDER_TOKEN_KEY);
    setToken(null);
    setProviderAdmin(null);
  }, []);

  useEffect(() => {
    const storedToken = localStorage.getItem(PROVIDER_TOKEN_KEY);
    if (!storedToken) {
      setBooting(false);
      return;
    }

    setToken(storedToken);

    (async () => {
      try {
        const me = await getProviderAdminMe(storedToken);
        setProviderAdmin(me || null);
      } catch {
        clearSession();
      } finally {
        setBooting(false);
      }
    })();
  }, [clearSession]);

  const login = useCallback(
    async (email, password) => {
      const response = await loginProviderAdmin(email, password);
      const nextToken = response?.token;
      if (!nextToken) {
        throw new Error("Provider login response did not include token");
      }

      localStorage.setItem(PROVIDER_TOKEN_KEY, nextToken);
      setToken(nextToken);

      const me = await getProviderAdminMe(nextToken);
      setProviderAdmin(me || null);
    },
    []
  );

  const logout = useCallback(() => {
    clearSession();
  }, [clearSession]);

  const value = useMemo(
    () => ({
      token,
      providerAdmin,
      booting,
      isAuthed: Boolean(token),
      login,
      logout,
      clearSession,
    }),
    [token, providerAdmin, booting, login, logout, clearSession]
  );

  return (
    <ProviderAuthContext.Provider value={value}>
      {children}
    </ProviderAuthContext.Provider>
  );
}
