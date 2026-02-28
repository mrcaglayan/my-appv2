import { useContext } from "react";
import { TenantReadinessContext } from "./tenantReadinessContext.js";

export function useTenantReadiness() {
  const value = useContext(TenantReadinessContext);
  if (!value) {
    throw new Error(
      "useTenantReadiness must be used within TenantReadinessProvider"
    );
  }
  return value;
}
