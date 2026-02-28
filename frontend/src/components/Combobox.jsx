import { useEffect, useId, useMemo, useRef, useState } from "react";

function normalizeText(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function defaultGetOptionValue(option, index) {
  if (option && typeof option === "object") {
    if (option.value !== undefined && option.value !== null) return option.value;
    if (option.id !== undefined && option.id !== null) return option.id;
    if (option.key !== undefined && option.key !== null) return option.key;
  }
  if (option === null || option === undefined || option === "") {
    return `option-${index}`;
  }
  return option;
}

function defaultGetOptionLabel(option, index) {
  if (option && typeof option === "object") {
    if (option.label !== undefined && option.label !== null) return option.label;
    if (option.name !== undefined && option.name !== null) return option.name;
    if (option.title !== undefined && option.title !== null) return option.title;
  }
  return String(defaultGetOptionValue(option, index));
}

function defaultGetOptionDescription(option) {
  if (option && typeof option === "object" && option.description !== undefined && option.description !== null) {
    return option.description;
  }
  return "";
}

function defaultGetOptionDisabled(option) {
  if (option && typeof option === "object") {
    return Boolean(option.disabled);
  }
  return false;
}

function isSameValue(left, right) {
  if (left === right) return true;
  if (left === null || left === undefined || right === null || right === undefined) return false;
  return String(left) === String(right);
}

function buildOptionId(listId, value, index) {
  const raw = normalizeText(value, `option-${index}`).replace(/[^a-zA-Z0-9_-]/g, "_");
  return `${listId}-${raw}-${index}`;
}

function findFirstEnabledIndex(options) {
  for (let i = 0; i < options.length; i += 1) {
    if (!options[i]?.disabled) return i;
  }
  return -1;
}

function findNextEnabledIndex(options, startIndex, direction) {
  if (!Array.isArray(options) || options.length === 0) return -1;
  const step = direction >= 0 ? 1 : -1;
  let index = startIndex;
  for (let i = 0; i < options.length; i += 1) {
    index += step;
    if (index >= options.length) index = 0;
    if (index < 0) index = options.length - 1;
    if (!options[index]?.disabled) return index;
  }
  return -1;
}

function findInitialHighlightedIndex(options, selectedValue) {
  if (!Array.isArray(options) || options.length === 0) return -1;
  const selectedIndex = options.findIndex(
    (option) => !option.disabled && isSameValue(option.value, selectedValue)
  );
  if (selectedIndex >= 0) return selectedIndex;
  return findFirstEnabledIndex(options);
}

export default function Combobox({
  id = "",
  name = "",
  value = null,
  options = [],
  onChange,
  inputValue,
  onInputChange,
  placeholder = "",
  disabled = false,
  readOnly = false,
  loading = false,
  clearable = true,
  openOnFocus = true,
  noOptionsText = "No options found.",
  loadingText = "Loading...",
  listMaxHeightClassName = "max-h-64",
  className = "",
  inputClassName = "",
  listClassName = "",
  optionClassName = "",
  getOptionValue = defaultGetOptionValue,
  getOptionLabel = defaultGetOptionLabel,
  getOptionDescription = defaultGetOptionDescription,
  getOptionDisabled = defaultGetOptionDisabled,
  renderOption = null,
  filterOptions = true,
  onOpenChange,
}) {
  const reactId = useId();
  const inputId = normalizeText(id, `combobox-${reactId}`);
  const listId = `${inputId}-listbox`;
  const rootRef = useRef(null);
  const inputRef = useRef(null);
  const isInputControlled = inputValue !== undefined;

  const normalizedOptions = useMemo(
    () =>
      (Array.isArray(options) ? options : []).map((option, index) => {
        const optionValue = getOptionValue(option, index);
        const optionLabel = normalizeText(getOptionLabel(option, index), `Option ${index + 1}`);
        const optionDescription = normalizeText(getOptionDescription(option, index));
        const optionDisabled = Boolean(getOptionDisabled(option, index));
        return {
          id: buildOptionId(listId, optionValue, index),
          value: optionValue,
          label: optionLabel,
          description: optionDescription,
          disabled: optionDisabled,
          raw: option,
        };
      }),
    [getOptionDescription, getOptionDisabled, getOptionLabel, getOptionValue, listId, options]
  );

  const selectedOption = useMemo(
    () => normalizedOptions.find((option) => isSameValue(option.value, value)) || null,
    [normalizedOptions, value]
  );

  const [localInputValue, setLocalInputValue] = useState(() => normalizeText(selectedOption?.label));
  const [isOpen, setIsOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const query = isInputControlled
    ? normalizeText(inputValue)
    : isOpen
      ? localInputValue
      : normalizeText(selectedOption?.label, localInputValue);
  const filteredOptions = useMemo(() => {
    if (!filterOptions) return normalizedOptions;
    const normalizedQuery = normalizeText(query).toLowerCase();
    if (!normalizedQuery) return normalizedOptions;
    return normalizedOptions.filter((option) =>
      `${option.label} ${option.description}`.toLowerCase().includes(normalizedQuery)
    );
  }, [filterOptions, normalizedOptions, query]);

  const fallbackHighlightedIndex = useMemo(
    () => findInitialHighlightedIndex(filteredOptions, value),
    [filteredOptions, value]
  );
  const activeHighlightedIndex =
    highlightedIndex >= 0 &&
    highlightedIndex < filteredOptions.length &&
    !filteredOptions[highlightedIndex]?.disabled
      ? highlightedIndex
      : fallbackHighlightedIndex;
  const highlightedOption =
    activeHighlightedIndex >= 0 && activeHighlightedIndex < filteredOptions.length
      ? filteredOptions[activeHighlightedIndex]
      : null;

  function emitOpenChange(nextOpen) {
    setIsOpen(nextOpen);
    if (typeof onOpenChange === "function") {
      onOpenChange(nextOpen);
    }
  }

  function setInputText(nextValue, reason, option = null) {
    const text = String(nextValue ?? "");
    if (!isInputControlled) {
      setLocalInputValue(text);
    }
    if (typeof onInputChange === "function") {
      onInputChange(text, { reason, option });
    }
  }

  function handleSelect(option) {
    if (!option || option.disabled) return;
    setInputText(option.label, "select", option.raw);
    setHighlightedIndex(-1);
    emitOpenChange(false);
    if (typeof onChange === "function") {
      onChange(option.value, option.raw);
    }
  }

  function handleClear() {
    if (disabled || readOnly) return;
    setInputText("", "clear");
    setHighlightedIndex(-1);
    emitOpenChange(false);
    if (typeof onChange === "function") {
      onChange(null, null);
    }
    inputRef.current?.focus();
  }

  function handleInputChange(event) {
    const nextValue = String(event?.target?.value ?? "");
    setInputText(nextValue, "input");
    emitOpenChange(true);
  }

  function handleInputFocus() {
    if (openOnFocus && !disabled && !readOnly) {
      emitOpenChange(true);
    }
  }

  function handleToggle() {
    if (disabled || readOnly) return;
    emitOpenChange(!isOpen);
    inputRef.current?.focus();
  }

  function handleInputKeyDown(event) {
    if (disabled || readOnly) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      emitOpenChange(true);
      setHighlightedIndex(
        findNextEnabledIndex(filteredOptions, activeHighlightedIndex >= 0 ? activeHighlightedIndex : -1, 1)
      );
      return;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      emitOpenChange(true);
      const start = activeHighlightedIndex >= 0 ? activeHighlightedIndex : filteredOptions.length;
      setHighlightedIndex(findNextEnabledIndex(filteredOptions, start, -1));
      return;
    }
    if (event.key === "Enter") {
      if (isOpen && highlightedOption && !highlightedOption.disabled) {
        event.preventDefault();
        handleSelect(highlightedOption);
      }
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      setHighlightedIndex(-1);
      emitOpenChange(false);
      return;
    }
    if (event.key === "Tab") {
      emitOpenChange(false);
    }
  }

  useEffect(() => {
    function handlePointerDown(event) {
      if (!rootRef.current) return;
      if (rootRef.current.contains(event.target)) return;
      setIsOpen(false);
      if (typeof onOpenChange === "function") {
        onOpenChange(false);
      }
      setHighlightedIndex(-1);
    }

    window.addEventListener("mousedown", handlePointerDown);
    window.addEventListener("touchstart", handlePointerDown);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
      window.removeEventListener("touchstart", handlePointerDown);
    };
  }, [onOpenChange]);

  const clearSourceText = isInputControlled ? inputValue : localInputValue;
  const showClear =
    clearable && !disabled && !readOnly && Boolean(normalizeText(clearSourceText) || selectedOption);
  const listVisible = isOpen && !disabled && !readOnly;
  const hasOptions = filteredOptions.length > 0;

  return (
    <div ref={rootRef} className={`relative ${className}`}>
      <div className="relative">
        <input
          id={inputId}
          ref={inputRef}
          name={name || undefined}
          type="text"
          role="combobox"
          autoComplete="off"
          spellCheck={false}
          value={query}
          placeholder={placeholder}
          disabled={disabled}
          readOnly={readOnly}
          aria-expanded={listVisible}
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={highlightedOption?.id || undefined}
          className={`w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 outline-none focus:border-cyan-400 focus:ring-2 focus:ring-cyan-100 disabled:bg-slate-100 disabled:text-slate-500 ${inputClassName}`}
          onChange={handleInputChange}
          onFocus={handleInputFocus}
          onKeyDown={handleInputKeyDown}
        />
        <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-2">
          {loading ? <span className="text-xs text-slate-500">{loadingText}</span> : null}
        </div>
        <div className="absolute inset-y-0 right-2 flex items-center gap-1">
          {showClear ? (
            <button
              type="button"
              className="rounded border border-slate-300 bg-white px-1.5 py-0.5 text-xs text-slate-600 hover:bg-slate-50"
              onClick={handleClear}
              aria-label="Clear selection"
            >
              x
            </button>
          ) : null}
          <button
            type="button"
            className="rounded border border-slate-300 bg-white px-1.5 py-0.5 text-xs text-slate-600 hover:bg-slate-50 disabled:opacity-60"
            onClick={handleToggle}
            disabled={disabled || readOnly}
            aria-label={listVisible ? "Close options" : "Open options"}
            aria-controls={listId}
            aria-expanded={listVisible}
          >
            v
          </button>
        </div>
      </div>

      {listVisible ? (
        <ul
          id={listId}
          role="listbox"
          className={`absolute z-40 mt-1 w-full overflow-auto rounded border border-slate-200 bg-white p-1 shadow-lg ${listMaxHeightClassName} ${listClassName}`}
        >
          {!hasOptions ? (
            <li className="px-2 py-2 text-sm text-slate-500">{noOptionsText}</li>
          ) : (
            filteredOptions.map((option, index) => {
              const isHighlighted = index === highlightedIndex;
              const isSelected = isSameValue(option.value, value);
              if (typeof renderOption === "function") {
                return (
                  <li
                    key={option.id}
                    id={option.id}
                    role="option"
                    aria-selected={isSelected}
                    aria-disabled={option.disabled}
                    className={option.disabled ? "opacity-60" : ""}
                    onMouseEnter={() => setHighlightedIndex(index)}
                    onMouseDown={(event) => event.preventDefault()}
                    onClick={() => handleSelect(option)}
                  >
                    {renderOption({
                      option: option.raw,
                      isHighlighted,
                      isSelected,
                      disabled: option.disabled,
                    })}
                  </li>
                );
              }
              return (
                <li
                  key={option.id}
                  id={option.id}
                  role="option"
                  aria-selected={isSelected}
                  aria-disabled={option.disabled}
                  className={`cursor-pointer rounded px-2 py-2 text-sm ${
                    option.disabled
                      ? "cursor-not-allowed text-slate-400"
                      : isHighlighted
                        ? "bg-cyan-50 text-cyan-900"
                        : isSelected
                          ? "bg-slate-100 text-slate-900"
                          : "text-slate-700 hover:bg-slate-50"
                  } ${optionClassName}`}
                  onMouseEnter={() => setHighlightedIndex(index)}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => handleSelect(option)}
                >
                  <div className="flex items-center gap-2">
                    <span className="truncate">{option.label}</span>
                    {isSelected ? <span className="ml-auto text-xs text-cyan-700">Selected</span> : null}
                  </div>
                  {option.description ? (
                    <div className="mt-0.5 truncate text-xs text-slate-500">{option.description}</div>
                  ) : null}
                </li>
              );
            })
          )}
        </ul>
      ) : null}
    </div>
  );
}
