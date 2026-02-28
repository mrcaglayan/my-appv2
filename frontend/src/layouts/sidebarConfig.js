const ROLE_PERMISSIONS_PAGE_PERMISSIONS = [
  "security.role.read",
  "security.permission.read",
  "security.role.upsert",
  "security.role_permissions.assign",
];

const USER_ASSIGNMENTS_PAGE_PERMISSIONS = [
  "security.role_assignment.read",
  "security.role_assignment.upsert",
];

const SCOPE_ASSIGNMENTS_PAGE_PERMISSIONS = [
  "security.data_scope.read",
  "security.data_scope.upsert",
  "security.role_assignment.read",
];

const AUDIT_LOGS_PAGE_PERMISSIONS = ["security.audit.read"];
const SENSITIVE_DATA_AUDIT_PAGE_PERMISSIONS = [
  "security.sensitive_data.audit.read",
];
const OPS_DASHBOARD_PAGE_PERMISSIONS = ["ops.dashboard.read"];
const OPS_EXCEPTION_WORKBENCH_PAGE_PERMISSIONS = ["ops.exceptions.read"];
const OPS_RETENTION_PAGE_PERMISSIONS = [
  "ops.retention.read",
  "ops.export_snapshot.read",
];
const COMPANY_SETTINGS_PAGE_PERMISSIONS = ["onboarding.company.setup"];
const ORG_SETTINGS_PAGE_PERMISSIONS = ["org.tree.read", "org.fiscal_calendar.read"];
const GL_SETUP_PAGE_PERMISSIONS = [
  "gl.book.read",
  "gl.coa.read",
  "gl.account.read",
  "gl.book.upsert",
  "gl.coa.upsert",
  "gl.account.upsert",
  "gl.account_mapping.upsert",
];
const RECLASS_PAGE_PERMISSIONS = [
  "org.tree.read",
  "gl.book.read",
  "gl.account.read",
  "org.fiscal_period.read",
  "gl.trial_balance.read",
  "gl.journal.create",
  "gl.journal.read",
];
const JOURNAL_PAGE_PERMISSIONS = [
  "gl.journal.read",
  "gl.journal.create",
  "gl.journal.post",
  "gl.journal.reverse",
  "gl.trial_balance.read",
  "gl.period.close",
];
const INTERCOMPANY_RECONCILIATION_PAGE_PERMISSIONS = [
  "intercompany.reconcile.run",
];
const CONSOLIDATION_REPORT_PAGE_PERMISSIONS = [
  "consolidation.run.read",
  "consolidation.report.balance_sheet.read",
  "consolidation.report.income_statement.read",
];
const FX_RATE_PAGE_PERMISSIONS = ["fx.rate.read", "fx.rate.bulk_upsert"];
const CONSOLIDATION_SETUP_PAGE_PERMISSIONS = [
  "consolidation.group.read",
  "consolidation.group.upsert",
  "consolidation.group_member.upsert",
  "consolidation.coa_mapping.read",
  "consolidation.coa_mapping.upsert",
  "consolidation.elimination_placeholder.read",
  "consolidation.elimination_placeholder.upsert",
  "consolidation.run.read",
  "consolidation.run.create",
  "consolidation.run.execute",
  "consolidation.run.finalize",
];

export const sidebarItems = [
  {
    type: "link",
    label: "Dashboard",
    to: "/app",
    end: true,
    icon: "dashboard",
    implemented: true,
  },
  {
    type: "section",
    title: "Donem Islemleri",
    icon: "spark",
    matchPrefix: "/app/donem-islemleri",
    items: [
      {
        label: "Acilis Fisi Olustur",
        to: "/app/acilis-fisi",
        implemented: true,
      },
    ],
  },
  {
    type: "section",
    title: "Yevmiye Kayitlari",
    icon: "journal",
    matchPrefix: "/app/yevmiye-kayitlari",
    items: [
      {
        type: "section",
        title: "Kasa Hazirlik ve Oturum",
        icon: "settings",
        items: [
          {
            label: "Kasa Tanimlari",
            to: "/app/kasa-tanimlari",
            requiredPermissions: ["cash.register.read"],
            implemented: true,
          },
          {
            label: "Kasa Oturumlari",
            to: "/app/kasa-oturumlari",
            requiredPermissions: ["cash.register.read"],
            implemented: true,
          },
        ],
      },
      {
        type: "section",
        title: "Gunluk Nakit Islemleri",
        icon: "journal",
        items: [
          {
            label: "Kasa Islemleri",
            to: "/app/kasa-islemleri",
            requiredPermissions: ["cash.txn.read"],
            implemented: true,
          },
          {
            label: "Kasa Transit Transferleri",
            to: "/app/kasa-transit-transferleri",
            requiredPermissions: ["cash.txn.read"],
            implemented: true,
          },
          {
            label: "Tahsilat",
            to: "/app/tahsilat-islemleri",
            requiredPermissions: ["cash.txn.read"],
            implemented: true,
          },
          {
            label: "Tediye",
            to: "/app/tediye-islemleri",
            requiredPermissions: ["cash.txn.read"],
            implemented: true,
          },
        ],
      },
      {
        type: "section",
        title: "Kontrol ve Mahsup",
        icon: "report",
        items: [
          {
            label: "Kasa Istisnalari",
            to: "/app/kasa-istisnalari",
            requiredPermissions: ["cash.report.read"],
            implemented: true,
          },
          {
            label: "Mahsup",
            to: "/app/mahsup-islemleri",
            requiredPermissions: JOURNAL_PAGE_PERMISSIONS,
            implemented: true,
          },
        ],
      },
    ],
  },
  {
    type: "section",
    title: "Banka Islemleri",
    icon: "bank",
    matchPrefix: "/app/banka-islemleri",
    items: [
      {
        label: "Banka Tanimla",
        to: "/app/banka-tanimla",
        requiredPermissions: ["bank.accounts.read"],
        implemented: true,
      },
      {
        label: "Banka Ekstre Ice Aktar",
        to: "/app/banka-ekstre-ice-aktar",
        requiredPermissions: ["bank.statements.import"],
        implemented: true,
      },
      {
        label: "Banka Ekstre Kuyrugu",
        to: "/app/banka-ekstre-kuyrugu",
        requiredPermissions: ["bank.statements.read"],
        implemented: true,
      },
      {
        label: "Banka Mutabakat",
        to: "/app/banka-mutabakat",
        requiredPermissions: ["bank.reconcile.read"],
        implemented: true,
      },
      {
        label: "Banka Onaylari",
        to: "/app/banka-onaylar",
        requiredPermissions: [
          "bank.approvals.policies.read",
          "bank.approvals.requests.read",
        ],
        implemented: true,
      },
      {
        label: "Banka Islemleri",
        to: "/app/banka-islemleri",
        requiredPermissions: ["bank.statements.read"],
        implemented: true,
      },
    ],
  },
  {
    type: "section",
    title: "Cari Islemler",
    icon: "company",
    matchPrefix: "/app/cari-islemler",
    items: [
      {
        type: "section",
        title: "Cari Kartlar",
        icon: "company",
        items: [
          {
            label: "Alici Karti Olustur",
            to: "/app/alici-kart-olustur",
            requiredPermissions: ["cari.card.upsert"],
            implemented: true,
          },
          {
            label: "Alici Karti Listesi",
            to: "/app/alici-kart-listesi",
            requiredPermissions: ["cari.card.read"],
            implemented: true,
          },
          {
            label: "Satici Karti Olustur",
            to: "/app/satici-kart-olustur",
            requiredPermissions: ["cari.card.upsert"],
            implemented: true,
          },
          {
            label: "Satici Karti Listesi",
            to: "/app/satici-kart-listesi",
            requiredPermissions: ["cari.card.read"],
            implemented: true,
          },
        ],
      },
      {
        type: "section",
        title: "Cari Belge ve Mutabakat",
        icon: "journal",
        items: [
          {
            label: "Cari Belgeler",
            to: "/app/cari-belgeler",
            requiredPermissions: ["cari.doc.read"],
            implemented: true,
          },
          {
            label: "Cari Mahsuplastirma / Tahsilat-Odeme",
            to: "/app/cari-settlements",
            requiredPermissions: [
              "cari.settlement.apply",
              "cari.settlement.reverse",
              "cari.bank.attach",
              "cari.bank.apply",
            ],
            implemented: true,
          },
        ],
      },
      {
        type: "section",
        title: "Cari Rapor ve Denetim",
        icon: "report",
        items: [
          {
            label: "Cari Denetim Izleri",
            to: "/app/cari-audit",
            requiredPermissions: ["cari.audit.read"],
            implemented: true,
          },
          {
            label: "Cari Raporlari",
            to: "/app/cari-raporlari",
            requiredPermissions: ["cari.report.read"],
            implemented: true,
          },
        ],
      },
      {
        type: "section",
        title: "Sozlesme ve Gelir",
        icon: "report",
        items: [
          {
            label: "Contracts",
            to: "/app/contracts",
            requiredPermissions: ["contract.read"],
            implemented: true,
          },
          {
            label: "Donemsellik ve Tahakkuklar",
            to: "/app/gelecek-yillar-gelirleri",
            requiredPermissions: [
              "revenue.schedule.read",
              "revenue.run.read",
              "revenue.report.read",
            ],
            implemented: true,
          },
        ],
      },
    ],
  },
  {
    type: "section",
    title: "Odeme Islemleri",
    icon: "bank",
    matchPrefix: "/app/odeme-batchleri",
    items: [
      {
        label: "Odeme Batchleri",
        to: "/app/odeme-batchleri",
        requiredPermissions: ["payments.batch.read"],
        implemented: true,
      },
    ],
  },
  {
    type: "section",
    title: "Bordro Islemleri",
    icon: "company",
    matchPrefix: "/app/payroll",
    items: [
      {
        label: "Bordro Runlari",
        to: "/app/payroll-runs",
        requiredPermissions: ["payroll.runs.read"],
        implemented: true,
      },
      {
        label: "Bordro Import",
        to: "/app/payroll-runs/import",
        requiredPermissions: ["payroll.runs.import"],
        implemented: true,
      },
      {
        label: "Bordro Mappingleri",
        to: "/app/payroll-mappings",
        requiredPermissions: ["payroll.mappings.read"],
        implemented: true,
      },
      {
        label: "Bordro Liabilities",
        to: "/app/payroll-liabilities",
        requiredPermissions: ["payroll.liabilities.read"],
        implemented: true,
      },
      {
        label: "Bordro Beneficiaries",
        to: "/app/payroll-beneficiaries",
        requiredPermissions: ["payroll.beneficiary.read"],
        implemented: true,
      },
      {
        label: "Bordro Kapanis Kontrolleri",
        to: "/app/payroll-close-controls",
        requiredPermissions: ["payroll.close.read"],
        implemented: true,
      },
    ],
  },
  {
    type: "section",
    title: "Stoklar",
    icon: "box",
    matchPrefix: "/app/stoklar",
    items: [
      {
        label: "Stok Karti Olustur",
        to: "/app/stok-karti-olustur",
      },
      {
        label: "Stok Yansitma Islemleri",
        to: "/app/stok-yansitma-islemleri",
      },
      {
        label: "Stok Karti Listesi",
        to: "/app/stok-karti-listesi",
      },
    ],
  },
  {
    type: "section",
    title: "Demirbaslar",
    icon: "inventory",
    matchPrefix: "/app/demirbaslar",
    items: [
      {
        label: "Demirbas Karti Olustur",
        to: "/app/demirbas-karti-olustur",
      },
      {
        label: "Demirbas Alim Islemleri",
        to: "/app/demirbas-alim-islemleri",
      },
      {
        label: "Demirbas Satis Islemleri",
        to: "/app/demirbas-satis-islemleri",
      },
      {
        label: "Amortisman Ayarlari",
        to: "/app/demirbas-amortisman-ayarlar",
      },
    ],
  },
  {
    type: "section",
    title: "Donem Sonu Islemler",
    icon: "calendar",
    matchPrefix: "/app/donem-sonu-islemler",
    items: [
      {
        type: "section",
        title: "Aysonu İşlemler",
        icon: "calendar",
        matchPrefix: "/app/donem-sonu-islemler/aylik",
        items: [
          {
            label: "Değerleme İşlemleri",
            to: "/app/donem-sonu-islemler/aylik/degerleme-islemleri",
          },
          {
            label: "Amortisman Islemleri",
            to: "/app/donem-sonu-islemler/aylik/amortisman-islemleri",
          },
          {
            label: "Beyanname Islemleri",
            to: "/app/donem-sonu-islemler/aylik/beyanname-islemleri",
          },
          {
            label: "Intercompany Mutabakat",
            to: "/app/donem-sonu-islemler/aylik/intercompany-mutabakat",
            requiredPermissions: INTERCOMPANY_RECONCILIATION_PAGE_PERMISSIONS,
            implemented: true,
          },
        ],
      },
      {
        type: "section",
        title: "Yılsonu İşlemler",
        icon: "calendar",
        matchPrefix: "/app/donem-sonu-islemler/yillik",
        items: [
          {
            label: "Envanter Islemleri",
            to: "/app/donem-sonu-islemler/yillik/envanter-islemleri",
          },
          {
            label: "Kapanis Islemleri",
            to: "/app/donem-sonu-islemler/yillik/kapanis-islemleri",
          },
          {
            label: "Yansitma Islemleri",
            to: "/app/donem-sonu-islemler/yillik/yansitma-islemleri",
          },
          {
            label: "Konsolidasyon Raporlari",
            to: "/app/donem-sonu-islemler/yillik/konsolidasyon-raporlari",
            requiredPermissions: CONSOLIDATION_REPORT_PAGE_PERMISSIONS,
            implemented: true,
          },
        ],
      },
    ],
  },
  {
    type: "section",
    title: "Raporlar",
    icon: "report",
    matchPrefix: "/app/raporlar",
    items: [
      {
        label: "Defter-i Kebir",
        to: "/app/defter-i-kebir",
      },
      {
        label: "Bilanco",
        to: "/app/bilanco",
      },
      {
        label: "Gelir Tablosu",
        to: "/app/gelir-tablosu",
      },
      {
        label: "Stok Raporu",
        to: "/app/stok-raporu",
      },
      {
        label: "Demirbas Raporu",
        to: "/app/demirbas-raporu",
      },
      {
        label: "Mizan Raporu",
        to: "/app/mizan-raporu",
      },
    ],
  },
  {
    type: "section",
    title: "Ayarlar",
    icon: "settings",
    matchPrefix: "/app/ayarlar",
    items: [
      {
        label: "Kullanici Yonetimi",
        to: "/app/ayarlar/kullanici-yonetimi",
      },
      {
        label: "Roller ve Yetkiler",
        to: "/app/ayarlar/rbac/roles-permissions",
        requiredPermissions: ROLE_PERMISSIONS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Kullanici Rol Atamalari",
        to: "/app/ayarlar/rbac/user-assignments",
        requiredPermissions: USER_ASSIGNMENTS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Scope Atamalari",
        to: "/app/ayarlar/rbac/scope-assignments",
        requiredPermissions: SCOPE_ASSIGNMENTS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "RBAC Denetim Loglari",
        to: "/app/ayarlar/rbac/audit-logs",
        requiredPermissions: AUDIT_LOGS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Hassas Veri Denetim Kayitlari",
        to: "/app/ayarlar/rbac/sensitive-data-audit",
        requiredPermissions: SENSITIVE_DATA_AUDIT_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Operasyon Dashboard",
        to: "/app/ayarlar/operasyon-dashboard",
        requiredPermissions: OPS_DASHBOARD_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Exception Workbench",
        to: "/app/ayarlar/exception-workbench",
        requiredPermissions: OPS_EXCEPTION_WORKBENCH_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Veri Saklama ve Snapshot",
        to: "/app/ayarlar/veri-saklama-snapshot",
        requiredPermissions: OPS_RETENTION_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Sirket Ayarlari",
        to: "/app/ayarlar/sirket-ayarlari",
        requiredPermissions: COMPANY_SETTINGS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Organizasyon Yonetimi",
        to: "/app/ayarlar/organizasyon-yonetimi",
        requiredPermissions: ORG_SETTINGS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Hesap Plani Olustur",
        to: "/app/ayarlar/hesap-plani-olustur",
        implemented: true,
      },
      {
        label: "Hesap Plani Ayarlari",
        to: "/app/ayarlar/hesap-plani-ayarlari",
        requiredPermissions: GL_SETUP_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Hesap Yeniden Siniflandirma",
        to: "/app/ayarlar/hesap-yeniden-siniflandirma",
        requiredPermissions: RECLASS_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Kur Yonetimi",
        to: "/app/ayarlar/kur-yonetimi",
        requiredPermissions: FX_RATE_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Konsolidasyon Kurulumu",
        to: "/app/ayarlar/konsolidasyon-kurulumu",
        requiredPermissions: CONSOLIDATION_SETUP_PAGE_PERMISSIONS,
        implemented: true,
      },
      {
        label: "Stok Ayarlari",
        to: "/app/ayarlar/stok-ayarlari",
      },
      {
        label: "Demirbas Ayarlari",
        to: "/app/ayarlar/demirbas-ayarlari",
      },
    ],
  },
];

function isSectionItem(item) {
  return item?.type === "section" || Array.isArray(item?.items);
}

export function collectSidebarLinks(items = sidebarItems) {
  const byPath = new Map();

  function walk(nodes) {
    if (!Array.isArray(nodes)) {
      return;
    }

    for (const node of nodes) {
      if (isSectionItem(node)) {
        walk(node.items);
        continue;
      }

      if (node?.to && !byPath.has(node.to)) {
        byPath.set(node.to, node);
      }
    }
  }

  walk(items);
  return Array.from(byPath.values());
}
