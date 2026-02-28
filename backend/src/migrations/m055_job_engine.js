const migration055JobEngine = {
  key: "m055_job_engine",
  description: "Background jobs and retry engine backbone (H02)",
  async up(connection) {
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS app_jobs (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_id BIGINT UNSIGNED NOT NULL,

        queue_name VARCHAR(60) NOT NULL,
        module_code VARCHAR(40) NOT NULL,
        job_type VARCHAR(60) NOT NULL,

        status VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
        priority INT NOT NULL DEFAULT 100,
        run_after_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

        idempotency_key VARCHAR(190) NULL,
        payload_json JSON NULL,
        payload_hash VARCHAR(128) NULL,

        attempt_count INT NOT NULL DEFAULT 0,
        max_attempts INT NOT NULL DEFAULT 5,

        locked_by VARCHAR(120) NULL,
        locked_at DATETIME NULL,

        started_at DATETIME NULL,
        finished_at DATETIME NULL,

        last_error_code VARCHAR(80) NULL,
        last_error_message VARCHAR(500) NULL,
        last_error_json JSON NULL,

        result_json JSON NULL,

        created_by INT NULL,
        cancelled_by INT NULL,
        cancelled_at DATETIME NULL,

        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

        PRIMARY KEY (id),
        UNIQUE KEY uq_app_jobs_tenant_queue_idem (tenant_id, queue_name, idempotency_key),
        KEY ix_app_jobs_sched (tenant_id, status, run_after_at, priority, id),
        KEY ix_app_jobs_sched_global (status, run_after_at, priority, id),
        KEY ix_app_jobs_type (tenant_id, module_code, job_type),
        KEY ix_app_jobs_locked (status, locked_at),
        KEY ix_app_jobs_tenant_created_by (tenant_id, created_by),
        KEY ix_app_jobs_tenant_cancelled_by (tenant_id, cancelled_by),

        CONSTRAINT fk_app_jobs_tenant
          FOREIGN KEY (tenant_id) REFERENCES tenants(id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT fk_app_jobs_created_by
          FOREIGN KEY (tenant_id, created_by) REFERENCES users(tenant_id, id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT fk_app_jobs_cancelled_by
          FOREIGN KEY (tenant_id, cancelled_by) REFERENCES users(tenant_id, id)
          ON UPDATE RESTRICT ON DELETE RESTRICT
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    await connection.execute(`
      CREATE TABLE IF NOT EXISTS app_job_attempts (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        tenant_id BIGINT UNSIGNED NOT NULL,
        app_job_id BIGINT UNSIGNED NOT NULL,

        attempt_no INT NOT NULL,
        worker_id VARCHAR(120) NOT NULL,

        status VARCHAR(20) NOT NULL,
        started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        finished_at DATETIME NULL,

        error_code VARCHAR(80) NULL,
        error_message VARCHAR(500) NULL,
        error_json JSON NULL,

        result_json JSON NULL,

        PRIMARY KEY (id),
        UNIQUE KEY uq_app_job_attempt_no (tenant_id, app_job_id, attempt_no),
        KEY ix_app_job_attempts_job (tenant_id, app_job_id),
        KEY ix_app_job_attempts_status (tenant_id, status),

        CONSTRAINT fk_app_job_attempts_tenant
          FOREIGN KEY (tenant_id) REFERENCES tenants(id)
          ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT fk_app_job_attempts_job
          FOREIGN KEY (app_job_id) REFERENCES app_jobs(id)
          ON UPDATE RESTRICT ON DELETE CASCADE
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
  },

  async down(connection) {
    await connection.execute(`DROP TABLE IF EXISTS app_job_attempts`);
    await connection.execute(`DROP TABLE IF EXISTS app_jobs`);
  },
};

export default migration055JobEngine;
