import { useCallback, useEffect, useId, useRef, useState } from "react";

export default function SidebarSection({
  title,
  icon,
  badge,
  children,
  collapsed = false,
  nested = false,
  flyoutNested = false,
  defaultOpen = false,
  open,
  active = false,
  onToggle,
}) {
  const isControlled = typeof open === "boolean";
  const [internalOpen, setInternalOpen] = useState(defaultOpen);
  const [flyoutOpen, setFlyoutOpen] = useState(false);
  const [flyoutTop, setFlyoutTop] = useState(0);
  const closeTimerRef = useRef(null);
  const sectionRef = useRef(null);
  const flyoutRef = useRef(null);
  const panelId = useId();
  const collapseTopLevel = collapsed && !nested;
  const nestedFlyout = collapsed && nested && flyoutNested;
  const flyoutEnabled = collapseTopLevel || nestedFlyout;

  useEffect(
    () => () => {
      if (closeTimerRef.current) {
        clearTimeout(closeTimerRef.current);
      }
    },
    []
  );

  function clearCloseTimer() {
    if (closeTimerRef.current) {
      clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }

  function openFlyout() {
    if (!flyoutEnabled) return;
    clearCloseTimer();
    setFlyoutTop(0);
    setFlyoutOpen(true);
  }

  function closeFlyoutSoon() {
    if (!flyoutEnabled) return;
    clearCloseTimer();
    closeTimerRef.current = setTimeout(() => {
      setFlyoutOpen(false);
      setFlyoutTop(0);
      closeTimerRef.current = null;
    }, 120);
  }

  const sectionOpen = isControlled ? open : internalOpen;
  const showChildren = !collapseTopLevel && !nestedFlyout && sectionOpen;
  const showFlyout = flyoutEnabled && flyoutOpen;
  const sectionHighlighted = active || sectionOpen || showFlyout;
  const indicatorExpanded = nestedFlyout ? showFlyout : sectionOpen;

  const updateFlyoutPosition = useCallback(() => {
    if (!flyoutEnabled || !flyoutOpen) return;

    const anchorEl = sectionRef.current;
    const flyoutEl = flyoutRef.current;
    if (!anchorEl || !flyoutEl) return;

    const anchorRect = anchorEl.getBoundingClientRect();
    const flyoutHeight = flyoutEl.offsetHeight;
    const viewportHeight = window.innerHeight;
    const margin = 8;

    // Keep the flyout in viewport: shift up near bottom, shift down near top.
    const minTop = margin - anchorRect.top;
    const maxTop = viewportHeight - margin - flyoutHeight - anchorRect.top;

    let nextTop = 0;
    if (nextTop < minTop) nextTop = minTop;
    if (nextTop > maxTop) nextTop = maxTop;
    if (maxTop < minTop) nextTop = minTop;

    const roundedTop = Math.round(nextTop);
    setFlyoutTop((currentTop) => (currentTop === roundedTop ? currentTop : roundedTop));
  }, [flyoutEnabled, flyoutOpen]);

  useEffect(() => {
    if (!flyoutEnabled || !flyoutOpen) return undefined;

    const rafId = window.requestAnimationFrame(updateFlyoutPosition);
    const handleViewportChange = () => updateFlyoutPosition();

    window.addEventListener("resize", handleViewportChange);
    window.addEventListener("scroll", handleViewportChange, true);

    let resizeObserver;
    if (typeof ResizeObserver !== "undefined" && flyoutRef.current) {
      resizeObserver = new ResizeObserver(() => updateFlyoutPosition());
      resizeObserver.observe(flyoutRef.current);
    }

    return () => {
      window.cancelAnimationFrame(rafId);
      window.removeEventListener("resize", handleViewportChange);
      window.removeEventListener("scroll", handleViewportChange, true);
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
    };
  }, [flyoutEnabled, flyoutOpen, updateFlyoutPosition]);

  return (
    <div
      ref={sectionRef}
      className="relative"
      onMouseEnter={openFlyout}
      onMouseLeave={closeFlyoutSoon}
      onFocusCapture={openFlyout}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) {
          closeFlyoutSoon();
        }
      }}
    >
      <button
        type="button"
        aria-expanded={nestedFlyout ? showFlyout : sectionOpen}
        aria-controls={panelId}
        aria-haspopup={flyoutEnabled ? "menu" : undefined}
        title={collapseTopLevel ? title : undefined}
        onClick={() => {
          if (nestedFlyout) {
            clearCloseTimer();
            setFlyoutTop(0);
            setFlyoutOpen((value) => !value);
            return;
          }
          if (!collapseTopLevel) {
            if (onToggle) {
              onToggle();
              return;
            }
            setInternalOpen((value) => !value);
          }
        }}
        className={`group flex items-center text-left text-sm font-semibold transition-colors ${
          nested
            ? `w-full gap-2 rounded-sm pl-2 pr-2 py-2 ${
                sectionHighlighted
                  ? "bg-gray-100 text-black"
                  : "text-gray-700 hover:bg-gray-100 hover:text-black"
              }`
            : `${collapseTopLevel ? "mx-1 h-10 w-[calc(100%-0.5rem)] justify-center rounded-lg p-0" : "w-full gap-2 rounded-lg pl-4 pr-0 py-1.5"} ${
                sectionHighlighted
                  ? "bg-gray-100 text-[#143c62]"
                  : "text-[#143c62] hover:bg-gray-100 hover:text-[#143c62]"
              }`
        }`}
      >
        <span
          className={`inline-flex shrink-0 items-center justify-center transition-colors ${
            nested ? "h-5 w-5 text-[#143c62]" : "h-6 w-6 text-[#143c62]"
          }`}
        >
          {icon}
        </span>
        {!collapseTopLevel && (
          <span
            className={`min-w-0 flex-1 ${nested
              ? "whitespace-normal break-words leading-5"
              : "truncate whitespace-nowrap leading-5"
              }`}
          >
            {title}
          </span>
        )}
        {!collapseTopLevel && badge && (
          <span className="rounded-full bg-slate-200 px-2 py-0.5 text-[10px] font-semibold tracking-wide text-slate-700">
            {badge}
          </span>
        )}
        {!collapseTopLevel && (
          <svg
            viewBox="0 0 20 20"
            aria-hidden="true"
            className={`ml-auto h-4 w-4 text-gray-500 transition-transform ${indicatorExpanded ? "rotate-90" : ""}`}
          >
            <path
              d="M7.5 4.5L13 10l-5.5 5.5"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        )}
      </button>

      <div
        id={panelId}
        className={`grid overflow-hidden transition-all duration-200 ${
          showChildren ? "grid-rows-[1fr]" : "grid-rows-[0fr]"
        }`}
      >
        <div className={`min-h-0 ${nested ? "pl-4 pr-0" : "pl-6 pr-0"}`}>
          <div className="grid gap-0 pt-0">{children}</div>
        </div>
      </div>

      {flyoutEnabled && (
        <div
          ref={flyoutRef}
          className={`absolute left-full z-50 w-[22rem] max-w-[calc(100vw-6rem)] overflow-visible rounded-xl border border-slate-200 bg-white px-2 py-2 shadow-2xl transition duration-150 ${
            showFlyout
              ? "pointer-events-auto translate-x-0 opacity-100"
              : "pointer-events-none -translate-x-1 opacity-0"
          }`}
          style={{ top: `${flyoutTop}px` }}
          role="menu"
          aria-label={title}
        >
          <p className="px-2 pb-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">
            {title}
          </p>
          <div className="grid gap-0">{children}</div>
        </div>
      )}
    </div>
  );
}
