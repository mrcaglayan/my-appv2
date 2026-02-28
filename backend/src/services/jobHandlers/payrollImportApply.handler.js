import { parsePositiveInt } from "../../routes/_utils.js";
import { applyPayrollProviderImport } from "../payroll.providers.service.js";

function noopScopeAccess() {
  return true;
}

const payrollImportApplyHandler = {
  async run({ job, payload }) {
    const tenantId = parsePositiveInt(job?.tenant_id);
    const importJobId = parsePositiveInt(payload?.import_job_id);
    const actingUserId = parsePositiveInt(payload?.acting_user_id) || null;

    if (!tenantId) {
      const err = new Error("PAYROLL_IMPORT_APPLY job is missing tenant_id");
      err.status = 400;
      err.errorCode = "JOB_PAYROLL_IMPORT_MISSING_TENANT";
      err.retryable = false;
      throw err;
    }
    if (!importJobId) {
      const err = new Error("PAYROLL_IMPORT_APPLY payload.import_job_id is required");
      err.status = 400;
      err.errorCode = "JOB_PAYROLL_IMPORT_MISSING_IMPORT_JOB_ID";
      err.retryable = false;
      throw err;
    }

    const result = await applyPayrollProviderImport({
      req: null,
      tenantId,
      userId: actingUserId,
      importJobId,
      input: {
        applyIdempotencyKey: `JOB:${job.id}|PAYROLL_IMPORT_APPLY`,
        note: payload?.apply_note || "Async payroll provider import apply",
        allowSameUserApply: Boolean(payload?.allow_same_user_apply),
      },
      assertScopeAccess: noopScopeAccess,
    });

    return {
      ok: true,
      import_job_id: importJobId,
      applied_payroll_run_id: parsePositiveInt(result?.job?.applied_payroll_run_id) || null,
      provider_code: result?.job?.provider_code || null,
      payroll_period: result?.job?.payroll_period || null,
    };
  },
};

export default payrollImportApplyHandler;
