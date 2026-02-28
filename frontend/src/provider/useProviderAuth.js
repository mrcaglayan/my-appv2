import { useContext } from "react";
import { ProviderAuthContext } from "./providerAuthContext.js";

export function useProviderAuth() {
  const context = useContext(ProviderAuthContext);
  if (!context) {
    throw new Error("useProviderAuth must be used within ProviderAuthProvider");
  }
  return context;
}
