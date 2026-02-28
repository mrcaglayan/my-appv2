export function buildAutoAllocatePreview(openItems = [], incomingAmountTxn = 0) {
  const sorted = [...openItems].sort((a, b) => {
    const aDue = String(a?.dueDate || "");
    const bDue = String(b?.dueDate || "");
    if (aDue !== bDue) return aDue.localeCompare(bDue);
    return Number(a?.openItemId || 0) - Number(b?.openItemId || 0);
  });

  let remaining = Number(incomingAmountTxn || 0);
  return sorted.map((item) => {
    const openTxn = Number(item?.residualAmountTxnAsOf || 0);
    const applyTxn = Math.max(0, Math.min(openTxn, remaining));
    remaining = Math.max(0, remaining - applyTxn);
    return {
      openItemId: item?.openItemId || null,
      documentNo: item?.documentNo || null,
      dueDate: item?.dueDate || null,
      direction: item?.direction || null,
      openAmountTxn: openTxn,
      expectedApplyTxn: applyTxn,
      expectedResidualTxn: Math.max(0, openTxn - applyTxn),
    };
  });
}

export function buildSettlementApplyPayload(form) {
  const payload = {
    legalEntityId: Number(form.legalEntityId),
    counterpartyId: Number(form.counterpartyId),
    direction: form.direction || undefined,
    settlementDate: form.settlementDate,
    currencyCode: form.currencyCode,
    incomingAmountTxn: Number(form.incomingAmountTxn || 0),
    idempotencyKey: String(form.idempotencyKey || "").trim(),
    autoAllocate: Boolean(form.autoAllocate),
    useUnappliedCash: Boolean(form.useUnappliedCash),
    allocations: Array.isArray(form.allocations) ? form.allocations : [],
    fxRate: form.fxRate || undefined,
    note: form.note || undefined,
  };

  if (form.paymentChannel) {
    payload.paymentChannel = String(form.paymentChannel).trim().toUpperCase();
  }
  if (form.linkedCashTransaction) {
    payload.linkedCashTransaction = form.linkedCashTransaction;
  }

  return payload;
}
