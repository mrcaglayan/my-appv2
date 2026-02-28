const migration066BankReconAutopostTaxModes = {
  key: "m066_bank_recon_autopost_tax_modes",
  description: "Enable INCLUDED tax split mode for bank reconciliation auto-post templates",
  async up(connection) {
    await connection.execute(
      `ALTER TABLE bank_reconciliation_posting_templates
       MODIFY COLUMN tax_mode ENUM('NONE','INCLUDED') NOT NULL DEFAULT 'NONE'`
    );
  },
};

export default migration066BankReconAutopostTaxModes;
