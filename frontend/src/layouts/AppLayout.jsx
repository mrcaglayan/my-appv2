import { useEffect, useMemo, useRef, useState } from "react";
import { Link, NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/useAuth.js";
import LanguageSwitcher from "../i18n/LanguageSwitcher.jsx";
import { useI18n } from "../i18n/useI18n.js";
import { useTenantReadiness } from "../readiness/useTenantReadiness.js";
import { toastError, toastSuccess } from "../toast/toastBus.js";
import SidebarSection from "./SidebarSection.jsx";
import { sidebarItems } from "./sidebarConfig.js";
import WorkingContextBar from "./WorkingContextBar.jsx";

const MODULE_PREVIEW_ADMIN_PERMISSIONS = [
  "security.role.upsert",
  "security.role_permissions.assign",
];
const TENANT_SETUP_ROUTE = "/app/ayarlar/sirket-ayarlari";
const TENANT_SETUP_ROUTES = [
  {
    to: "/app/ayarlar/sirket-ayarlari",
    fallback: "Company setup",
  },
  {
    to: "/app/ayarlar/organizasyon-yonetimi",
    fallback: "Organization setup",
  },
  {
    to: "/app/ayarlar/hesap-plani-ayarlari",
    fallback: "GL setup",
  },
];

function resolveReadinessChip(loading, error, ready, t) {
  if (loading) {
    return {
      label: t("layout.readinessChecking", "Readiness: Checking"),
      classes: "border-slate-300 bg-slate-100 text-slate-700",
    };
  }

  if (error) {
    return {
      label: t("layout.readinessError", "Readiness: Error"),
      classes: "border-amber-300 bg-amber-100 text-amber-900",
    };
  }

  if (ready) {
    return {
      label: t("layout.readinessReady", "Readiness: Ready"),
      classes: "border-emerald-300 bg-emerald-100 text-emerald-900",
    };
  }

  return {
    label: t("layout.readinessSetupRequired", "Readiness: Setup Required"),
    classes: "border-rose-300 bg-rose-100 text-rose-900 hover:bg-rose-200",
  };
}

function getReadinessCheckLabel(t, check) {
  return t(
    ["readinessChecklist", "checkLabels", check?.key],
    check?.label || check?.key || ""
  );
}

function Icon({ name, className = "h-4 w-4" }) {
  switch (name) {
    case "dashboard":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M3.5 3.5h5.5v5.5H3.5V3.5zm7.5 0h5.5v3.5H11V3.5zM3.5 11h3.5v5.5H3.5V11zm5.5 2h7.5v3.5H9V13z"
            stroke="currentColor"
            strokeWidth="1.6"
          />
        </svg>
      );
    case "spark":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M10 2.5l1.7 3.8 3.9 1.7-3.9 1.7L10 13.5l-1.7-3.8-3.9-1.7 3.9-1.7L10 2.5zM4 12.5l.9 1.9 1.9.9-1.9.8L4 18l-.8-1.9-1.9-.8 1.9-.9L4 12.5zm12.2-.9l.7 1.5 1.5.7-1.5.7-.7 1.5-.7-1.5-1.5-.7 1.5-.7.7-1.5z"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "journal":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M5 3.5h9a1.5 1.5 0 011.5 1.5v10A1.5 1.5 0 0114 16.5H5A1.5 1.5 0 013.5 15V5A1.5 1.5 0 015 3.5zm2.5 3h5m-5 3h5m-5 3h3.5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </svg>
      );
    case "bank":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M10 3.5l7 3v1H3v-1l7-3zm-5 4v7m3.8-7v7m3.8-7v7m3.8-7v7M3 16.5h14"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "company":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M3.5 16.5h13M5.5 16.5V8.5L10 6l4.5 2.5v8m-7-6h1.8m1.4 0h1.8m-5 2.8h1.8m1.4 0h1.8"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "box":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M10 2.8l6 3.2v8L10 17.2 4 14V6l6-3.2zm0 0v6.3m6-3.1l-6 3.1-6-3.1"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "inventory":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M4 6.5h12M6 6.5v9.5m8-9.5v9.5M4 16h12M5.5 3.5h9"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "calendar":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M5 4.5h10A1.5 1.5 0 0116.5 6v9A1.5 1.5 0 0115 16.5H5A1.5 1.5 0 013.5 15V6A1.5 1.5 0 015 4.5zm0 3h10M7 3.5v2m6-2v2"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "report":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M5 3.5h10A1.5 1.5 0 0116.5 5v10A1.5 1.5 0 0115 16.5H5A1.5 1.5 0 013.5 15V5A1.5 1.5 0 015 3.5zm2.2 9h5.6m-5.6-3h5.6m-5.6-3h3.2"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "settings":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M10 6.9a3.1 3.1 0 100 6.2 3.1 3.1 0 000-6.2zm0-3.4l.7 1.8a5 5 0 011.7.7l1.8-.7 1.1 1.9-1.3 1.4c.2.5.3 1 .3 1.5s-.1 1-.3 1.5l1.3 1.4-1.1 1.9-1.8-.7a5 5 0 01-1.7.7l-.7 1.8H8.6l-.7-1.8a5 5 0 01-1.7-.7l-1.8.7-1.1-1.9 1.3-1.4a5.3 5.3 0 010-3l-1.3-1.4 1.1-1.9 1.8.7a5 5 0 011.7-.7l.7-1.8H10z"
            stroke="currentColor"
            strokeWidth="1.3"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "logout":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path
            d="M8 3.5h-3A1.5 1.5 0 003.5 5v10A1.5 1.5 0 005 16.5h3m3-9l3 2.5-3 2.5m-5-2.5h8"
            stroke="currentColor"
            strokeWidth="1.6"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "menu":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path d="M3.5 5.5h13M3.5 10h13M3.5 14.5h13" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" />
        </svg>
      );
    case "chevron-left":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path d="M12.5 4.5L7 10l5.5 5.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "chevron-right":
      return (
        <svg viewBox="0 0 20 20" className={className} fill="none" aria-hidden="true">
          <path d="M7.5 4.5L13 10l-5.5 5.5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    default:
      return <span className={className} />;
  }
}

const BUILTIN_ICON_NAMES = new Set([
  "dashboard",
  "spark",
  "journal",
  "bank",
  "company",
  "box",
  "inventory",
  "calendar",
  "report",
  "settings",
  "logout",
  "menu",
  "chevron-left",
  "chevron-right",
]);

const BUILTIN_ICON_EMOJI = {
  dashboard: "🏠",
  spark: "✨",
  journal: "📘",
  bank: "🏦",
  company: "🏢",
  box: "📦",
  inventory: "🗂️",
  calendar: "🗓️",
  report: "📊",
  settings: "⚙️",
  logout: "🚪",
};

const SIDEBAR_ICON_RULES = [
  { pattern: /(dashboard|anasayfa)/, icon: "🏠" },
  { pattern: /(employee|kullanici|user|personel)/, icon: "👥" },
  { pattern: /(permission|yetki|rbac|scope|audit|rol)/, icon: "🛡️" },
  { pattern: /(bank)/, icon: "🏦" },
  { pattern: /(stok|inventory|item)/, icon: "📦" },
  { pattern: /(demirbas|asset|amortisman)/, icon: "🧰" },
  { pattern: /(donem|calendar|month|year|kapanis|acilis)/, icon: "🗓️" },
  { pattern: /(report|rapor|mizan|bilanco|gelir)/, icon: "📊" },
  { pattern: /(ayar|setup|settings|kurulumu)/, icon: "⚙️" },
  { pattern: /(kur|fx|rate)/, icon: "💱" },
  { pattern: /(konsolidasyon|consolidation)/, icon: "🧩" },
  { pattern: /(tediye|tahsilat|mahsup|yevmiye|journal)/, icon: "📒" },
  { pattern: /(organizasyon|organization|sirket|company)/, icon: "🏢" },
  { pattern: /(discount|indirim)/, icon: "🏷️" },
  { pattern: /(request|talep)/, icon: "📝" },
  { pattern: /(approve|onay)/, icon: "✅" },
];

function deriveSidebarEmoji(item) {
  const rawIcon = typeof item?.icon === "string" ? item.icon.trim() : "";
  if (rawIcon) {
    if (BUILTIN_ICON_EMOJI[rawIcon]) return BUILTIN_ICON_EMOJI[rawIcon];
    return rawIcon;
  }

  const haystack = `${item?.label || ""} ${item?.title || ""} ${item?.to || ""} ${item?.matchPrefix || ""}`.toLowerCase();
  for (const rule of SIDEBAR_ICON_RULES) {
    if (rule.pattern.test(haystack)) return rule.icon;
  }

  return isSectionItem(item) ? "📁" : "📄";
}

function renderSidebarIcon(item, options = {}) {
  const { svgClass = "h-4 w-4", emojiClass = "text-[16px]" } = options;
  const rawIcon = typeof item?.icon === "string" ? item.icon.trim() : "";

  if (rawIcon && BUILTIN_ICON_NAMES.has(rawIcon) && !BUILTIN_ICON_EMOJI[rawIcon]) {
    return <Icon name={rawIcon} className={svgClass} />;
  }

  const emoji = deriveSidebarEmoji(item);
  return (
    <span className={`inline-flex items-center justify-center leading-none ${emojiClass}`} aria-hidden="true">
      {emoji}
    </span>
  );
}

function mainLinkClass({ isActive }, collapsed) {
  return `group flex items-center text-sm font-semibold transition-colors ${collapsed ? "mx-1 h-10 w-[calc(100%-0.5rem)] justify-center rounded-lg p-0" : "w-full gap-2 rounded-lg pl-4 pr-0 py-1.5"
    } ${isActive
      ? "bg-gray-100 text-[#143c62]"
      : "text-[#143c62] hover:bg-gray-100 hover:text-[#143c62]"
    }`;
}

function subLinkClass(isActive, nested = false) {
  return `flex w-full items-start gap-2 rounded-sm ${nested ? "pl-2 pr-2 py-2" : "pl-2 pr-2 py-1.5"} text-sm font-medium transition-colors ${isActive
    ? "bg-gray-100 text-black"
    : "text-gray-700 hover:bg-gray-100 hover:text-black"
    }`;
}

function formatSegmentLabel(segment) {
  return segment
    .split("-")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function toSidebarTitleKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function isSectionItem(item) {
  return item?.type === "section" || Array.isArray(item?.items);
}

function getPathWithoutQueryOrHash(target) {
  return String(target || "").replace(/[?#].*$/, "");
}

function getHashFragment(target) {
  const value = String(target || "");
  const hashIndex = value.indexOf("#");
  if (hashIndex < 0) {
    return "";
  }
  return value.slice(hashIndex + 1);
}

function isSidebarEntryActive(entry, pathname, hash) {
  const targetPath = getPathWithoutQueryOrHash(entry?.to);
  if (!targetPath) {
    return false;
  }

  const pathMatches = entry?.end
    ? pathname === targetPath
    : pathname.startsWith(targetPath);
  if (!pathMatches) {
    return false;
  }

  const targetHash = getHashFragment(entry.to);
  if (!targetHash) {
    return true;
  }

  return hash === `#${targetHash}`;
}

function hasActiveChildPath(items, pathname, hash) {
  if (!Array.isArray(items)) return false;

  return items.some((entry) => {
    if (isSectionItem(entry)) {
      if (entry.matchPrefix && pathname.startsWith(entry.matchPrefix)) {
        return true;
      }
      return hasActiveChildPath(entry.items, pathname, hash);
    }

    return isSidebarEntryActive(entry, pathname, hash);
  });
}

function findActiveTopSectionKey(items, pathname, hash) {
  if (!Array.isArray(items)) {
    return null;
  }

  for (const item of items) {
    if (!isSectionItem(item)) {
      continue;
    }

    const isActive =
      (item.matchPrefix && pathname.startsWith(item.matchPrefix)) ||
      hasActiveChildPath(item.items, pathname, hash);
    if (isActive) {
      return item.matchPrefix || item.title || null;
    }
  }

  return null;
}

function hasRequiredPermissions(item, hasAnyPermission) {
  const requiredPermissions = Array.isArray(item?.requiredPermissions)
    ? item.requiredPermissions.map((value) => String(value || "").trim()).filter(Boolean)
    : [];

  if (requiredPermissions.length === 0) {
    return true;
  }

  return hasAnyPermission(requiredPermissions);
}

function annotateSidebarItemsWithAccess(
  items,
  hasAnyPermission,
  includeUnimplemented,
  t
) {
  if (!Array.isArray(items)) {
    return [];
  }

  const visible = [];
  for (const item of items) {
    const requiredPermissions = Array.isArray(item?.requiredPermissions)
      ? item.requiredPermissions.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    const hasAccess = hasRequiredPermissions(item, hasAnyPermission);
    const lockedReason =
      !hasAccess && requiredPermissions.length > 0
        ? `${t("layout.permissionRequired", "Permission required")}: ${requiredPermissions.join(", ")}`
        : "";

    if (!isSectionItem(item)) {
      if (!includeUnimplemented && item.implemented !== true) {
        continue;
      }
      visible.push({
        ...item,
        requiredPermissions,
        isLocked: !hasAccess,
        lockedReason,
      });
      continue;
    }

    const children = annotateSidebarItemsWithAccess(
      item.items,
      hasAnyPermission,
      includeUnimplemented,
      t
    );
    if (children.length === 0) {
      continue;
    }

    visible.push({
      ...item,
      requiredPermissions,
      isLocked: !hasAccess,
      lockedReason,
      items: children,
    });
  }

  return visible;
}

export default function AppLayout() {
  const { user, isAuthed, logout, hasAnyPermission, hasAllPermissions } = useAuth();
  const { t } = useI18n();
  const {
    loading: readinessLoading,
    error: readinessError,
    ready: tenantReady,
    missingChecks,
    refresh: refreshReadiness,
  } = useTenantReadiness();
  const navigate = useNavigate();
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [readinessMenuPathname, setReadinessMenuPathname] = useState(null);
  const [openTopSectionKey, setOpenTopSectionKey] = useState(() =>
    findActiveTopSectionKey(sidebarItems, location.pathname, location.hash)
  );
  const readinessMenuRef = useRef(null);
  const canViewUnimplementedModules = hasAllPermissions(
    MODULE_PREVIEW_ADMIN_PERMISSIONS
  );
  const readinessMenuOpen = readinessMenuPathname === location.pathname;

  function getItemDisplayText(item, type) {
    const fallback = type === "title" ? item?.title : item?.label;
    const pathKey = item?.to || item?.matchPrefix;
    if (!pathKey) {
      const titleKey = toSidebarTitleKey(fallback);
      return t(["sidebar", "titles", titleKey], fallback);
    }
    return t(["sidebar", "byPath", pathKey], fallback);
  }

  const breadcrumbs = useMemo(() => {
    const segments = location.pathname.split("/").filter(Boolean);

    return segments.map((segment, index) => {
      const builtPath = `/${segments.slice(0, index + 1).join("/")}`;
      const explicitLabel = t(["breadcrumbs", "byPath", builtPath], null);
      const sidebarLabel = t(["sidebar", "byPath", builtPath], null);
      return {
        to: builtPath,
        label: explicitLabel || sidebarLabel || formatSegmentLabel(segment),
        isLast: index === segments.length - 1,
      };
    });
  }, [location.pathname, t]);

  const visibleSidebarItems = useMemo(
    () =>
      annotateSidebarItemsWithAccess(
        sidebarItems,
        hasAnyPermission,
        canViewUnimplementedModules,
        t
      ),
    [hasAnyPermission, canViewUnimplementedModules, t]
  );
  const readinessChip = useMemo(
    () =>
      resolveReadinessChip(readinessLoading, readinessError, tenantReady, t),
    [readinessLoading, readinessError, tenantReady, t]
  );

  const closeMobileSidebar = () => setMobileOpen(false);
  const closeReadinessMenu = () => setReadinessMenuPathname(null);
  const handleLogout = () => {
    logout();
    closeMobileSidebar();
    navigate("/login", { replace: true });
  };

  async function handleCopyAccessRequest(item) {
    const requiredPermissions = Array.isArray(item?.requiredPermissions)
      ? item.requiredPermissions
      : [];
    if (requiredPermissions.length === 0) {
      return;
    }

    const label = getItemDisplayText(item, "label") || getItemDisplayText(item, "title") || item?.to || "Route";
    const text = [
      "Access request",
      `User: ${user?.email || user?.name || "-"}`,
      `Route: ${label}`,
      `Path: ${item?.to || "-"}`,
      `Required permissions: ${requiredPermissions.join(", ")}`,
    ].join("\n");

    if (!navigator?.clipboard?.writeText) {
      toastError(t("layout.copyAccessRequestUnsupported", "Clipboard is not available in this browser."));
      return;
    }

    try {
      await navigator.clipboard.writeText(text);
      toastSuccess(t("layout.copyAccessRequestSuccess", "Access request copied."));
    } catch {
      toastError(t("layout.copyAccessRequestFailed", "Failed to copy access request."));
    }
  }

  useEffect(() => {
    if (!readinessMenuOpen) return undefined;

    function handlePointerDown(event) {
      if (!readinessMenuRef.current?.contains(event.target)) {
        closeReadinessMenu();
      }
    }

    function handleKeyDown(event) {
      if (event.key === "Escape") {
        closeReadinessMenu();
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, [readinessMenuOpen]);

  function renderSectionChildren(items, depth = 0) {
    if (!Array.isArray(items)) return null;

    return items.map((subItem, index) => {
      if (isSectionItem(subItem)) {
        const nestedItems = Array.isArray(subItem.items) ? subItem.items : [];
        const nestedSectionActive =
          (subItem.matchPrefix && location.pathname.startsWith(subItem.matchPrefix)) ||
          hasActiveChildPath(
            nestedItems,
            location.pathname,
            location.hash
          );

        return (
          <div
            key={subItem.title || `section-${depth}-${index}`}
            className="border-l-2 border-gray-300"
          >
            <SidebarSection
              title={getItemDisplayText(subItem, "title") || "Section"}
              icon={renderSidebarIcon(subItem, {
                svgClass: "h-4 w-4",
                emojiClass: "text-[16px]",
              })}
              badge={subItem.badge}
              collapsed={collapsed}
              nested
              flyoutNested
              defaultOpen={nestedSectionActive}
              active={nestedSectionActive}
            >
              {renderSectionChildren(nestedItems, depth + 1)}
            </SidebarSection>
          </div>
        );
      }

      return (
        <div
          key={subItem.to || `${subItem.label}-${depth}-${index}`}
          className="border-l-2 border-gray-300"
        >
          {subItem.isLocked ? (
            <div
              className={`flex w-full items-start gap-2 rounded-sm border border-amber-200 bg-amber-50/70 text-sm font-medium text-amber-900 ${
                depth > 0 ? "pl-2 pr-2 py-2" : "pl-2 pr-2 py-1.5"
              }`}
              title={subItem.lockedReason || t("layout.lockedMenu", "Access restricted")}
              aria-disabled="true"
            >
              <span className="inline-flex h-5 w-5 shrink-0 items-center justify-center text-amber-800">
                {renderSidebarIcon(subItem, {
                  svgClass: "h-4 w-4",
                  emojiClass: "text-[15px]",
                })}
              </span>
              <span className="flex min-w-0 flex-1 items-start justify-between gap-2">
                <span className="whitespace-normal break-words leading-5">
                  {getItemDisplayText(subItem, "label")}
                </span>
                <button
                  type="button"
                  onClick={() => handleCopyAccessRequest(subItem)}
                  className="shrink-0 rounded border border-amber-300 bg-white px-1.5 py-0.5 text-[10px] font-semibold text-amber-900 hover:bg-amber-100"
                >
                  {t("layout.copyAccessRequest", "Copy request")}
                </button>
              </span>
            </div>
          ) : (
            <NavLink
              to={subItem.to}
              end={subItem.end}
              className={() =>
                subLinkClass(
                  isSidebarEntryActive(subItem, location.pathname, location.hash),
                  depth > 0
                )
              }
              onClick={closeMobileSidebar}
            >
              <span className="inline-flex h-5 w-5 shrink-0 items-center justify-center text-[#143c62]">
                {renderSidebarIcon(subItem, {
                  svgClass: "h-4 w-4",
                  emojiClass: "text-[15px]",
                })}
              </span>
              <span className="flex min-w-0 flex-1 items-start justify-between gap-2">
                <span className="whitespace-normal break-words leading-5">{getItemDisplayText(subItem, "label")}</span>
                {subItem.implemented !== true && (
                  <span className="inline-flex h-4 min-w-4 items-center justify-center rounded-full border border-slate-300 px-1 text-[9px] font-semibold uppercase leading-none text-slate-500">
                    S
                  </span>
                )}
              </span>
            </NavLink>
          )}
        </div>
      );
    });
  }

  return (
    <div className="relative flex h-dvh overflow-hidden bg-slate-100 text-slate-900 font-['Trebuchet_MS','Lucida_Sans_Unicode','Segoe_UI',sans-serif]">
      <div
        className={`absolute inset-0 z-30 bg-slate-950/55 backdrop-blur-[1px] transition-opacity md:hidden ${mobileOpen ? "opacity-100" : "pointer-events-none opacity-0"
          }`}
        onClick={closeMobileSidebar}
      />

      <aside
        className={`absolute inset-y-0 left-0 z-40 flex flex-col border-r border-slate-200 bg-white text-slate-900 shadow-xl transition-all duration-300 md:static md:translate-x-0 lg:rounded-br-3xl ${collapsed ? "w-[52px]" : "w-[229px]"
          } ${mobileOpen ? "translate-x-0" : "-translate-x-full md:translate-x-0"}`}
      >
        <div
          className={`shrink-0 border-b border-slate-200 ${
            collapsed ? "px-2 py-2" : "px-3 py-3"
          }`}
        >
          <div
            className={`flex items-center overflow-hidden ${
              collapsed ? "gap-0" : "gap-2"
            }`}
          >
            <button
              type="button"
              onClick={() => setCollapsed((value) => !value)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-300 bg-white text-slate-600 transition-colors hover:bg-slate-100 hover:text-slate-900"
              aria-label={collapsed ? t("layout.expandSidebar") : t("layout.collapseSidebar")}
            >
              <Icon
                name={collapsed ? "chevron-right" : "chevron-left"}
                className="h-4 w-4"
              />
            </button>
            <div
              className={`min-w-0 overflow-hidden transition-all duration-200 ease-out ${collapsed ? "max-w-0 opacity-0" : "max-w-[12rem] opacity-100"
                }`}
              aria-hidden={collapsed}
            >
              <p className="truncate whitespace-nowrap text-[10px] uppercase tracking-[0.2em] text-slate-500">
                {t("layout.financeConsole")}
              </p>
              <h3 className="truncate whitespace-nowrap text-sm font-semibold text-[#143c62]">
                {t("layout.proSidebar")}
              </h3>
            </div>
          </div>
        </div>

        <div className="flex-1 min-h-0">
          <nav
            className={`h-full space-y-0.5 pl-0 pr-0 py-3 ${collapsed
                ? "overflow-visible"
                : "overflow-y-scroll overflow-x-hidden [scrollbar-width:thin] [scrollbar-color:#cbd5e1_transparent] [&::-webkit-scrollbar]:w-2 [&::-webkit-scrollbar-track]:bg-transparent [&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-300 hover:[&::-webkit-scrollbar-thumb]:bg-slate-400"
              }`}
          >
            {visibleSidebarItems.map((item) => {
              if (item.type === "link") {
                if (item.isLocked) {
                  return (
                    <div
                      key={item.to}
                      title={item.lockedReason || t("layout.lockedMenu", "Access restricted")}
                      className={`group flex items-center text-sm font-semibold transition-colors ${
                        collapsed
                          ? "mx-1 h-10 w-[calc(100%-0.5rem)] justify-center rounded-lg border border-amber-300 bg-amber-50/80 p-0 text-amber-900"
                          : "w-full gap-2 rounded-lg border border-amber-300 bg-amber-50/80 pl-4 pr-2 py-1.5 text-amber-900"
                      }`}
                      aria-disabled="true"
                    >
                      <span className="inline-flex h-8 w-8 shrink-0 items-center justify-center text-amber-800">
                        {renderSidebarIcon(item, {
                          svgClass: "h-4 w-4",
                          emojiClass: "text-[18px]",
                        })}
                      </span>
                      {!collapsed && (
                        <span className="min-w-0 flex-1 truncate whitespace-nowrap leading-5">
                          {getItemDisplayText(item, "label")}
                        </span>
                      )}
                      {!collapsed && (
                        <button
                          type="button"
                          onClick={() => handleCopyAccessRequest(item)}
                          className="ml-auto rounded border border-amber-300 bg-white px-1.5 py-0.5 text-[10px] font-semibold text-amber-900 hover:bg-amber-100"
                        >
                          {t("layout.copyAccessRequest", "Copy request")}
                        </button>
                      )}
                    </div>
                  );
                }

                return (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.end}
                    title={collapsed ? getItemDisplayText(item, "label") : undefined}
                    className={(state) => mainLinkClass(state, collapsed)}
                    onClick={closeMobileSidebar}
                  >
                    {({ isActive }) => (
                      <>
                        <span
                          className={`inline-flex h-8 w-8 shrink-0 items-center justify-center transition-colors ${isActive
                              ? "text-[#143c62]"
                              : "text-[#143c62]"
                            }`}
                        >
                          {renderSidebarIcon(item, {
                            svgClass: "h-4 w-4",
                            emojiClass: "text-[18px]",
                          })}
                        </span>
                        {!collapsed && (
                          <span className="min-w-0 flex-1 truncate whitespace-nowrap leading-5">
                            {getItemDisplayText(item, "label")}
                          </span>
                        )}
                        {!collapsed && item.badge && (
                          <span className="ml-auto rounded-full bg-slate-200 px-2 py-0.5 text-[10px] font-semibold tracking-wide text-slate-700">
                            {item.badge}
                          </span>
                        )}
                      </>
                    )}
                  </NavLink>
                );
              }

              const isSectionActive =
                (item.matchPrefix && location.pathname.startsWith(item.matchPrefix)) ||
                hasActiveChildPath(
                  item.items,
                  location.pathname,
                  location.hash
                );
              const sectionKey = item.matchPrefix || item.title;
              const isSectionOpen = openTopSectionKey === sectionKey;

              return (
              <SidebarSection
                key={item.title}
                title={getItemDisplayText(item, "title")}
                icon={renderSidebarIcon(item, {
                  svgClass: "h-5 w-5",
                  emojiClass: "text-[18px]",
                })}
                badge={item.badge}
                collapsed={collapsed}
                open={isSectionOpen}
                  active={isSectionActive}
                  onToggle={() =>
                    setOpenTopSectionKey((current) =>
                      current === sectionKey ? null : sectionKey
                    )
                  }
                >
                  {renderSectionChildren(item.items)}
                </SidebarSection>
              );
            })}
          </nav>
        </div>

        <div
          className={`shrink-0 border-t border-slate-200 bg-white overflow-hidden ${
            collapsed ? "p-1" : "p-3"
          }`}
        >
          <div
            className={`mx-auto flex items-center rounded-lg transition-all duration-300 ${collapsed
                ? "h-10 w-full justify-center gap-0 p-0"
                : "w-full justify-between gap-3 border border-slate-200 bg-slate-50 px-3 py-2"
              }`}
          >
            <div
              className={`min-w-0 transition-all duration-200 ${collapsed ? "max-w-0 overflow-hidden opacity-0" : "max-w-[11rem] opacity-100"
                }`}
              aria-hidden={collapsed}
            >
              <p className="truncate whitespace-nowrap text-[10px] uppercase tracking-[0.16em] text-slate-500">
                {t("layout.myAccount")}
              </p>
              <p className="mt-0.5 truncate whitespace-nowrap text-sm font-semibold text-[#143c62]">
                {user?.name || t("layout.loggedInUser")}
              </p>
            </div>

            <button
              type="button"
              onClick={handleLogout}
              title={collapsed ? t("layout.logout") : undefined}
              className={`inline-flex shrink-0 items-center transition-all duration-200 ${collapsed
                  ? "h-10 w-full justify-center rounded-lg border border-slate-300 bg-white text-[#143c62] hover:bg-gray-100"
                  : "gap-1.5 rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-semibold text-[#143c62] hover:bg-gray-100"
                }`}
            >
              <Icon name="logout" className={collapsed ? "h-4 w-4" : "h-3.5 w-3.5"} />
              <span
                className={`overflow-hidden whitespace-nowrap transition-all duration-200 ${collapsed ? "max-w-0 opacity-0" : "max-w-16 opacity-100"
                  }`}
                aria-hidden={collapsed}
              >
                {t("layout.logout")}
              </span>
            </button>
          </div>
        </div>
      </aside>

      <main className="relative flex min-w-0 flex-1 flex-col overflow-hidden">
        <div className="flex items-center justify-between border-b border-slate-200 bg-white/85 px-4 py-3 backdrop-blur">
          <div className="flex min-w-0 items-center gap-2">
            <button
              type="button"
              onClick={() => setMobileOpen(true)}
              className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white text-slate-700 shadow-sm transition hover:bg-slate-50 md:hidden"
              aria-label={t("layout.openSidebar")}
            >
              <Icon name="menu" className="h-4 w-4" />
            </button>
            <div className="min-w-0">
              <p className="text-[10px] uppercase tracking-[0.18em] text-slate-500">
                {t("layout.workspace")}
              </p>
              <nav
                aria-label={t("layout.breadcrumbAria")}
                className="mt-0.5 flex items-center gap-1 overflow-x-auto text-xs text-slate-500"
              >
                {breadcrumbs.map((crumb, index) => (
                  <span
                    key={crumb.to}
                    className="inline-flex items-center gap-1 whitespace-nowrap"
                  >
                    {crumb.isLast ? (
                      <span className="font-semibold text-slate-700">{crumb.label}</span>
                    ) : (
                      <Link
                        to={crumb.to}
                        className="transition-colors hover:text-slate-700"
                      >
                        {crumb.label}
                      </Link>
                    )}
                    {index < breadcrumbs.length - 1 && <span>/</span>}
                  </span>
                ))}
              </nav>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <div className="relative" ref={readinessMenuRef}>
              <button
                type="button"
                onClick={() =>
                  setReadinessMenuPathname((currentPathname) =>
                    currentPathname === location.pathname ? null : location.pathname
                  )
                }
                className={`inline-flex items-center gap-1 rounded-full border px-2 py-1 text-[11px] font-semibold tracking-wide transition-colors ${readinessChip.classes}`}
                aria-haspopup="menu"
                aria-expanded={readinessMenuOpen}
                aria-label={t("layout.readinessChecklist", "Readiness checklist")}
              >
                <span>{readinessChip.label}</span>
                <svg
                  viewBox="0 0 20 20"
                  className={`h-3 w-3 transition-transform ${readinessMenuOpen ? "rotate-180" : ""}`}
                  fill="none"
                  aria-hidden="true"
                >
                  <path
                    d="M5 7.5L10 12.5l5-5"
                    stroke="currentColor"
                    strokeWidth="1.6"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </button>

              {readinessMenuOpen && (
                <div
                  className="absolute right-0 top-[calc(100%+0.45rem)] z-50 w-80 max-w-[85vw] rounded-xl border border-slate-200 bg-white p-3 shadow-xl"
                  role="menu"
                >
                  <p className="text-[10px] uppercase tracking-[0.16em] text-slate-500">
                    {t("layout.readinessChecklist", "Readiness checklist")}
                  </p>

                  {readinessLoading && (
                    <p className="mt-2 text-sm text-slate-600">
                      {t("layout.readinessChecking", "Readiness: Checking")}
                    </p>
                  )}

                  {!readinessLoading && readinessError && (
                    <div className="mt-2 rounded-lg border border-amber-200 bg-amber-50 p-2">
                      <p className="text-xs font-medium text-amber-900">
                        {t("layout.readinessError", "Readiness: Error")}
                      </p>
                      <p className="mt-1 text-xs text-amber-800">{readinessError}</p>
                    </div>
                  )}

                  {!readinessLoading && !readinessError && tenantReady && (
                    <p className="mt-2 text-sm text-emerald-700">
                      {t(
                        "layout.readinessAllSet",
                        "All required setup items are complete."
                      )}
                    </p>
                  )}

                  {!readinessLoading && !readinessError && !tenantReady && (
                    <div className="mt-2">
                      <p className="text-xs font-semibold text-slate-700">
                        {t("layout.readinessMissingItems", "Missing items")}
                      </p>
                      <ul className="mt-2 space-y-1">
                        {missingChecks.map((check) => (
                          <li
                            key={check.key}
                            className="flex items-center justify-between rounded-md border border-rose-200 bg-rose-50 px-2 py-1.5"
                          >
                            <span className="text-xs text-rose-900">
                              {getReadinessCheckLabel(t, check)}
                            </span>
                            <span className="text-[11px] font-semibold text-rose-700">
                              {check.count}/{check.minimum}
                            </span>
                          </li>
                        ))}
                      </ul>

                      <div className="mt-2 grid gap-1">
                        {TENANT_SETUP_ROUTES.map((route) => (
                          <Link
                            key={route.to}
                            to={route.to}
                            className="rounded-md border border-slate-200 px-2 py-1 text-xs font-medium text-slate-700 transition-colors hover:bg-slate-50"
                            onClick={closeReadinessMenu}
                          >
                            {t(["sidebar", "byPath", route.to], route.fallback)}
                          </Link>
                        ))}
                      </div>
                    </div>
                  )}

                  <div className="mt-3 flex items-center justify-between gap-2 border-t border-slate-200 pt-2">
                    <button
                      type="button"
                      onClick={() => {
                        refreshReadiness();
                      }}
                      className="rounded-md border border-slate-300 bg-white px-2 py-1 text-xs font-semibold text-slate-700 transition-colors hover:bg-slate-50"
                    >
                      {t("layout.readinessRefresh", "Refresh")}
                    </button>
                    {!tenantReady && (
                      <Link
                        to={TENANT_SETUP_ROUTE}
                        className="text-xs font-semibold text-cyan-700 hover:text-cyan-800"
                        onClick={closeReadinessMenu}
                      >
                        {t("layout.readinessOpenSetup", "Open setup")}
                      </Link>
                    )}
                  </div>
                </div>
              )}
            </div>
            <LanguageSwitcher />
            <p className="truncate text-sm font-medium text-slate-700">
              {user?.name || t("layout.userFallback")}
            </p>
          </div>
        </div>
        {isAuthed ? (
          <div className="border-b border-slate-200 bg-white px-4 py-2">
            <WorkingContextBar />
          </div>
        ) : null}

        <div className="flex-1 min-h-0 p-4 md:p-6 overflow-auto">
          <Outlet />
        </div>

        <footer className="border-t border-slate-200 bg-white/70 px-4 py-3 text-xs text-slate-500">
          <small>
            &copy; {new Date().getFullYear()} {t("layout.madeWithLoveBy")}{" "}
            <a
              target="_blank"
              rel="noopener noreferrer"
              href="https://granada.com.gt/es/"
              className="font-semibold text-slate-700 hover:text-slate-900"
            >
              Fabrica Granada
            </a>
          </small>
        </footer>
      </main>
    </div>
  );
}
