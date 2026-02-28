import { useEffect, useMemo, useState } from "react";
import {
  listAccounts,
  listBooks,
  listCoas,
  upsertAccount,
  upsertAccountMapping,
  upsertBook,
  upsertCoa,
} from "../../api/glAdmin.js";
import {
  listCountries,
  listFiscalCalendars,
  listLegalEntities,
  listShareholderJournalConfigs,
  upsertShareholderJournalConfig,
} from "../../api/orgAdmin.js";
import {
  listJournalPurposeAccounts,
  upsertJournalPurposeAccount,
} from "../../api/glPurposeMappings.js";
import {
  applyPolicyPack,
  listPolicyPacks,
  resolvePolicyPack,
} from "../../api/policyPacks.js";
import { useAuth } from "../../auth/useAuth.js";
import { useI18n } from "../../i18n/useI18n.js";
import { useModuleReadiness } from "../../readiness/useModuleReadiness.js";
import TenantReadinessChecklist from "../../readiness/TenantReadinessChecklist.jsx";

const BOOK_TYPES = ["LOCAL", "GROUP"];
const COA_SCOPES = ["LEGAL_ENTITY", "GROUP"];
const ACCOUNT_TYPES = ["ASSET", "LIABILITY", "EQUITY", "REVENUE", "EXPENSE"];
const NORMAL_SIDES = ["DEBIT", "CREDIT"];
const TURKISH_DEFAULT_COA_ACCOUNTS = [
  { code: "100", name: "Kasa", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "101", name: "Alinan Cekler", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "102", name: "Bankalar", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "103",
    name: "Verilen Cekler ve Odeme Emirleri (-)",
    accountType: "ASSET",
    normalSide: "CREDIT",
  },
  { code: "108", name: "Diger Hazir Degerler", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "110", name: "Hisse Senetleri", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "111",
    name: "Ozel Kesim Tahvil Senet ve Bonolari",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  {
    code: "112",
    name: "Kamu Kesimi Tahvil Senet ve Bonolari",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "118", name: "Diger Menkul Kiymetler", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "119",
    name: "Menkul Kiymetler Deger Dusuklugu Karsiligi (-)",
    accountType: "ASSET",
    normalSide: "CREDIT",
  },
  { code: "120", name: "Alicilar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "121", name: "Alacak Senetleri", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "122",
    name: "Alacak Senetleri Reeskontu (-)",
    accountType: "ASSET",
    normalSide: "CREDIT",
  },
  {
    code: "126",
    name: "Verilen Depozito ve Teminatlar",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "127", name: "Diger Ticari Alacaklar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "128", name: "Supheli Ticari Alacaklar", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "129",
    name: "Supheli Ticari Alacaklar Karsiligi (-)",
    accountType: "ASSET",
    normalSide: "CREDIT",
  },
  { code: "131", name: "Ortaklardan Alacaklar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "136", name: "Diger Cesitli Alacaklar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "153", name: "Ticari Mallar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "157", name: "Diger Stoklar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "159", name: "Verilen Siparis Avanslari", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "180",
    name: "Gelecek Aylara Ait Giderler",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "181", name: "Gelir Tahakkuklari", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "191", name: "Indirilecek KDV", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "193",
    name: "Pesin Odenen Vergi ve Fonlar",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "240", name: "Bagli Menkul Kiymetler", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "242", name: "Istirakler", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "245", name: "Bagli Ortakliklar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "250", name: "Arazi ve Arsalar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "252", name: "Binalar", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "253",
    name: "Tesis Makine ve Cihazlar",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "254", name: "Tasitlar", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "255", name: "Demirbaslar", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "257",
    name: "Birikmis Amortismanlar (-)",
    accountType: "ASSET",
    normalSide: "CREDIT",
  },
  {
    code: "258",
    name: "Yapilmakta Olan Yatirimlar",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "260", name: "Haklar", accountType: "ASSET", normalSide: "DEBIT" },
  {
    code: "262",
    name: "Kurulus ve Orgutlenme Giderleri",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  {
    code: "263",
    name: "Arastirma ve Gelistirme Giderleri",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  {
    code: "267",
    name: "Diger Maddi Olmayan Duran Varliklar",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  {
    code: "268",
    name: "Birikmis Amortismanlar (-)",
    accountType: "ASSET",
    normalSide: "CREDIT",
  },
  {
    code: "280",
    name: "Gelecek Yillara Ait Giderler",
    accountType: "ASSET",
    normalSide: "DEBIT",
  },
  { code: "281", name: "Gelir Tahakkuklari", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "300", name: "Banka Kredileri", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "320", name: "Saticilar", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "321", name: "Borc Senetleri", accountType: "LIABILITY", normalSide: "CREDIT" },
  {
    code: "326",
    name: "Alinan Depozito ve Teminatlar",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  { code: "329", name: "Diger Ticari Borclar", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "331", name: "Ortaklara Borclar", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "335", name: "Personele Borclar", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "340", name: "Alinan Siparis Avanslari", accountType: "LIABILITY", normalSide: "CREDIT" },
  {
    code: "360",
    name: "Odenecek Vergi ve Fonlar",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "361",
    name: "Odenecek Sosyal Guvenlik Kesintileri",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "368",
    name: "Vadesi Gecmis Vergi ve Diger Yukumlulukler",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "370",
    name: "Donem Kari Vergi ve Diger Yasal Yukumluluk Karsiliklari",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "380",
    name: "Gelecek Aylara Ait Gelirler",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  { code: "381", name: "Gider Tahakkuklari", accountType: "LIABILITY", normalSide: "CREDIT" },
  {
    code: "391",
    name: "Hesaplanan KDV",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "400",
    name: "Banka Kredileri",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  { code: "420", name: "Saticilar", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "421", name: "Borc Senetleri", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "431", name: "Ortaklara Borclar", accountType: "LIABILITY", normalSide: "CREDIT" },
  {
    code: "472",
    name: "Kidem Tazminati Karsiligi",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  {
    code: "480",
    name: "Gelecek Yillara Ait Gelirler",
    accountType: "LIABILITY",
    normalSide: "CREDIT",
  },
  { code: "481", name: "Gider Tahakkuklari", accountType: "LIABILITY", normalSide: "CREDIT" },
  {
    code: "500",
    name: "Sermaye",
    accountType: "EQUITY",
    normalSide: "CREDIT",
    allowPosting: false,
  },
  {
    code: "501",
    name: "Odenmemis Sermaye (-)",
    accountType: "EQUITY",
    normalSide: "DEBIT",
    allowPosting: false,
  },
  {
    code: "520",
    name: "Hisse Senedi Ihrac Primleri",
    accountType: "EQUITY",
    normalSide: "CREDIT",
  },
  {
    code: "529",
    name: "Diger Sermaye Yedekleri",
    accountType: "EQUITY",
    normalSide: "CREDIT",
  },
  { code: "540", name: "Yasal Yedekler", accountType: "EQUITY", normalSide: "CREDIT" },
  {
    code: "542",
    name: "Olaganustu Yedekler",
    accountType: "EQUITY",
    normalSide: "CREDIT",
  },
  {
    code: "570",
    name: "Gecmis Yillar Karlari",
    accountType: "EQUITY",
    normalSide: "CREDIT",
  },
  {
    code: "580",
    name: "Gecmis Yillar Zararlari (-)",
    accountType: "EQUITY",
    normalSide: "DEBIT",
  },
  { code: "590", name: "Donem Net Kari", accountType: "EQUITY", normalSide: "CREDIT" },
  {
    code: "591",
    name: "Donem Net Zarari (-)",
    accountType: "EQUITY",
    normalSide: "DEBIT",
  },
  { code: "600", name: "Yurtici Satislar", accountType: "REVENUE", normalSide: "CREDIT" },
  { code: "601", name: "Yurtdisi Satislar", accountType: "REVENUE", normalSide: "CREDIT" },
  { code: "602", name: "Diger Gelirler", accountType: "REVENUE", normalSide: "CREDIT" },
  {
    code: "610",
    name: "Satislardan Iadeler (-)",
    accountType: "REVENUE",
    normalSide: "DEBIT",
  },
  {
    code: "611",
    name: "Satis Iskontolari (-)",
    accountType: "REVENUE",
    normalSide: "DEBIT",
  },
  {
    code: "612",
    name: "Diger Indirimler (-)",
    accountType: "REVENUE",
    normalSide: "DEBIT",
  },
  {
    code: "620",
    name: "Satilan Mallar Maliyeti (-)",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "621",
    name: "Satilan Ticari Mallar Maliyeti (-)",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "622",
    name: "Satilan Hizmet Maliyeti (-)",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "630",
    name: "Arastirma ve Gelistirme Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "631",
    name: "Pazarlama Satis ve Dagitim Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "632",
    name: "Genel Yonetim Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "640",
    name: "Istiraklerden Temettu Gelirleri",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  },
  { code: "642", name: "Faiz Gelirleri", accountType: "REVENUE", normalSide: "CREDIT" },
  { code: "646", name: "Kambiyo Karlari", accountType: "REVENUE", normalSide: "CREDIT" },
  {
    code: "649",
    name: "Diger Olagan Gelir ve Karlar",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  },
  {
    code: "654",
    name: "Karsilik Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  { code: "656", name: "Kambiyo Zararlari", accountType: "EXPENSE", normalSide: "DEBIT" },
  {
    code: "659",
    name: "Diger Olagan Gider ve Zararlar",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "660",
    name: "Kisa Vadeli Borclanma Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "671",
    name: "Onceki Donem Gider ve Zararlari",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "679",
    name: "Diger Olagandisi Gelir ve Karlar",
    accountType: "REVENUE",
    normalSide: "CREDIT",
  },
  {
    code: "689",
    name: "Diger Olagandisi Gider ve Zararlar",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  {
    code: "700",
    name: "Direkt Ilk Madde ve Malzeme Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  { code: "710", name: "Direkt Iscilik Giderleri", accountType: "EXPENSE", normalSide: "DEBIT" },
  { code: "720", name: "Genel Uretim Giderleri", accountType: "EXPENSE", normalSide: "DEBIT" },
  { code: "740", name: "Hizmet Uretim Maliyeti", accountType: "EXPENSE", normalSide: "DEBIT" },
  {
    code: "760",
    name: "Pazarlama Satis ve Dagitim Giderleri",
    accountType: "EXPENSE",
    normalSide: "DEBIT",
  },
  { code: "770", name: "Genel Yonetim Giderleri", accountType: "EXPENSE", normalSide: "DEBIT" },
  { code: "780", name: "Finansman Giderleri", accountType: "EXPENSE", normalSide: "DEBIT" },
];
const USA_DEFAULT_COA_ACCOUNTS = [
  { code: "1000", name: "Cash and Cash Equivalents", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "1100", name: "Accounts Receivable", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "1200", name: "Inventory", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "1300", name: "Prepaid Expenses", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "1500", name: "Property Plant and Equipment", accountType: "ASSET", normalSide: "DEBIT" },
  { code: "1590", name: "Accumulated Depreciation", accountType: "ASSET", normalSide: "CREDIT" },
  { code: "2000", name: "Accounts Payable", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "2100", name: "Accrued Expenses", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "2200", name: "Taxes Payable", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "2300", name: "Deferred Revenue", accountType: "LIABILITY", normalSide: "CREDIT" },
  { code: "3000", name: "Retained Earnings", accountType: "EQUITY", normalSide: "CREDIT" },
  {
    code: "3100",
    name: "Capital Stock Parent",
    accountType: "EQUITY",
    normalSide: "CREDIT",
    allowPosting: false,
  },
  { code: "3101", name: "Common Stock Class A", accountType: "EQUITY", normalSide: "CREDIT" },
  {
    code: "3110",
    name: "Capital Commitment Parent",
    accountType: "EQUITY",
    normalSide: "DEBIT",
    allowPosting: false,
  },
  {
    code: "3111",
    name: "Capital Commitment Receivable",
    accountType: "EQUITY",
    normalSide: "DEBIT",
  },
  { code: "4000", name: "Sales Revenue", accountType: "REVENUE", normalSide: "CREDIT" },
  { code: "4100", name: "Service Revenue", accountType: "REVENUE", normalSide: "CREDIT" },
  { code: "5000", name: "Cost of Goods Sold", accountType: "EXPENSE", normalSide: "DEBIT" },
  { code: "6100", name: "Operating Expenses", accountType: "EXPENSE", normalSide: "DEBIT" },
  { code: "6200", name: "General and Administrative Expense", accountType: "EXPENSE", normalSide: "DEBIT" },
  { code: "7000", name: "Interest Expense", accountType: "EXPENSE", normalSide: "DEBIT" },
];
const CARI_REQUIRED_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL",
  "CARI_AR_OFFSET",
  "CARI_AP_CONTROL",
  "CARI_AP_OFFSET",
]);
const CARI_OPTIONAL_CONTEXT_PURPOSE_CODES = Object.freeze([
  "CARI_AR_CONTROL_CASH",
  "CARI_AR_OFFSET_CASH",
  "CARI_AP_CONTROL_CASH",
  "CARI_AP_OFFSET_CASH",
  "CARI_AR_CONTROL_MANUAL",
  "CARI_AR_OFFSET_MANUAL",
  "CARI_AP_CONTROL_MANUAL",
  "CARI_AP_OFFSET_MANUAL",
  "CARI_AR_CONTROL_ON_ACCOUNT",
  "CARI_AR_OFFSET_ON_ACCOUNT",
  "CARI_AP_CONTROL_ON_ACCOUNT",
  "CARI_AP_OFFSET_ON_ACCOUNT",
]);
const CARI_MANUAL_PURPOSE_CODES = Object.freeze([
  ...CARI_REQUIRED_PURPOSE_CODES,
  ...CARI_OPTIONAL_CONTEXT_PURPOSE_CODES,
]);
const CARI_REQUIRED_PURPOSE_CODE_SET = new Set(CARI_REQUIRED_PURPOSE_CODES);
const CARI_OPTIONAL_PURPOSE_CODE_SET = new Set(CARI_OPTIONAL_CONTEXT_PURPOSE_CODES);
const CARI_PURPOSE_UI_META = Object.freeze({
  CARI_AR_CONTROL: Object.freeze({
    en: "AR control account (customer balance account).",
    tr: "AR kontrol hesabi (musteri bakiye hesabi).",
    exampleEn: "Example: AR invoice -> Dr 120, Cr 600",
    exampleTr: "Ornek: AR fatura -> Borc 120, Alacak 600",
  }),
  CARI_AR_OFFSET: Object.freeze({
    en: "AR document offset account (usually revenue).",
    tr: "AR belge karsi hesabi (genelde gelir hesabi).",
    exampleEn: "Example: AR invoice sales side -> 600/601/602",
    exampleTr: "Ornek: AR fatura satis tarafi -> 600/601/602",
  }),
  CARI_AP_CONTROL: Object.freeze({
    en: "AP control account (vendor balance account).",
    tr: "AP kontrol hesabi (satici bakiye hesabi).",
    exampleEn: "Example: AP invoice -> Dr 770, Cr 320",
    exampleTr: "Ornek: AP fatura -> Borc 770, Alacak 320",
  }),
  CARI_AP_OFFSET: Object.freeze({
    en: "AP document offset account (usually expense/cost).",
    tr: "AP belge karsi hesabi (genelde gider/maliyet hesabi).",
    exampleEn: "Example: AP expense side -> 770 or 632",
    exampleTr: "Ornek: AP gider tarafi -> 770 veya 632",
  }),
  CARI_AR_CONTROL_CASH: Object.freeze({
    en: "Optional AR control override for CASH settlement context.",
    tr: "CASH settlement baglami icin opsiyonel AR kontrol override.",
    exampleEn: "Used only in cash-linked settlement; else fallback to CARI_AR_CONTROL.",
    exampleTr: "Sadece kasa/banka bagli settlement'ta kullanilir; yoksa CARI_AR_CONTROL fallback olur.",
  }),
  CARI_AR_OFFSET_CASH: Object.freeze({
    en: "Optional AR cash offset (cash/bank account).",
    tr: "Opsiyonel AR nakit karsi hesabi (kasa/banka hesabi).",
    exampleEn: "Example: cash collection apply -> Dr 102, Cr 120",
    exampleTr: "Ornek: nakit tahsilat apply -> Borc 102, Alacak 120",
  }),
  CARI_AP_CONTROL_CASH: Object.freeze({
    en: "Optional AP control override for CASH settlement context.",
    tr: "CASH settlement baglami icin opsiyonel AP kontrol override.",
    exampleEn: "Used only in cash-linked settlement; else fallback to CARI_AP_CONTROL.",
    exampleTr: "Sadece kasa/banka bagli settlement'ta kullanilir; yoksa CARI_AP_CONTROL fallback olur.",
  }),
  CARI_AP_OFFSET_CASH: Object.freeze({
    en: "Optional AP cash offset (cash/bank account).",
    tr: "Opsiyonel AP nakit karsi hesabi (kasa/banka hesabi).",
    exampleEn: "Example: vendor payout apply -> Dr 320, Cr 102",
    exampleTr: "Ornek: satici odeme apply -> Borc 320, Alacak 102",
  }),
  CARI_AR_CONTROL_MANUAL: Object.freeze({
    en: "Optional AR control override for MANUAL settlement context.",
    tr: "MANUAL settlement baglami icin opsiyonel AR kontrol override.",
    exampleEn: "Used in manual settlement without cash transaction link.",
    exampleTr: "Kasa islemi baglantisi olmayan manuel settlement'ta kullanilir.",
  }),
  CARI_AR_OFFSET_MANUAL: Object.freeze({
    en: "Optional AR offset for MANUAL settlement context.",
    tr: "MANUAL settlement baglami icin opsiyonel AR karsi hesap.",
    exampleEn: "Example: manual collection settlement -> usually 100/102.",
    exampleTr: "Ornek: manuel tahsilat settlement -> genelde 100/102.",
  }),
  CARI_AP_CONTROL_MANUAL: Object.freeze({
    en: "Optional AP control override for MANUAL settlement context.",
    tr: "MANUAL settlement baglami icin opsiyonel AP kontrol override.",
    exampleEn: "Used in manual settlement without cash transaction link.",
    exampleTr: "Kasa islemi baglantisi olmayan manuel settlement'ta kullanilir.",
  }),
  CARI_AP_OFFSET_MANUAL: Object.freeze({
    en: "Optional AP offset for MANUAL settlement context.",
    tr: "MANUAL settlement baglami icin opsiyonel AP karsi hesap.",
    exampleEn: "Example: manual payout settlement -> usually 100/102.",
    exampleTr: "Ornek: manuel odeme settlement -> genelde 100/102.",
  }),
  CARI_AR_CONTROL_ON_ACCOUNT: Object.freeze({
    en: "Optional AR control override for ON_ACCOUNT apply context.",
    tr: "ON_ACCOUNT apply baglami icin opsiyonel AR kontrol override.",
    exampleEn: "Used when settlement consumes/relieves on-account balances.",
    exampleTr: "Settlement on-account bakiyeleri tukettiginde/cozdugunde kullanilir.",
  }),
  CARI_AR_OFFSET_ON_ACCOUNT: Object.freeze({
    en: "Optional AR on-account offset (customer advances liability).",
    tr: "Opsiyonel AR on-account karsi hesabi (alinan siparis avansi yukumlulugu).",
    exampleEn: "Example: clear customer advance -> Dr 340, Cr 120",
    exampleTr: "Ornek: musteri avans kapama -> Borc 340, Alacak 120",
  }),
  CARI_AP_CONTROL_ON_ACCOUNT: Object.freeze({
    en: "Optional AP control override for ON_ACCOUNT apply context.",
    tr: "ON_ACCOUNT apply baglami icin opsiyonel AP kontrol override.",
    exampleEn: "Used when settlement consumes/relieves on-account balances.",
    exampleTr: "Settlement on-account bakiyeleri tukettiginde/cozdugunde kullanilir.",
  }),
  CARI_AP_OFFSET_ON_ACCOUNT: Object.freeze({
    en: "Optional AP on-account offset (vendor advances asset).",
    tr: "Opsiyonel AP on-account karsi hesabi (verilen siparis avansi varligi).",
    exampleEn: "Example: clear vendor advance -> Dr 320, Cr 159",
    exampleTr: "Ornek: satici avans kapama -> Borc 320, Alacak 159",
  }),
});
const SHAREHOLDER_REQUIRED_PURPOSE_CODES = Object.freeze([
  "SHAREHOLDER_CAPITAL_CREDIT_PARENT",
  "SHAREHOLDER_COMMITMENT_DEBIT_PARENT",
]);

function toPositiveInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toUpper(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function getCariPurposeUiMeta(purposeCode) {
  const normalized = String(purposeCode || "")
    .trim()
    .toUpperCase();
  return CARI_PURPOSE_UI_META[normalized] || null;
}

function toBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value === 1;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "1" || normalized === "true";
  }
  return false;
}

function toQueryMapByPurpose(rows) {
  const byPurpose = {};
  for (const row of Array.isArray(rows) ? rows : []) {
    const purposeCode = toUpper(row?.purposeCode || row?.purpose_code);
    if (!purposeCode) {
      continue;
    }
    byPurpose[purposeCode] = row;
  }
  return byPurpose;
}

function buildAccountLabel(account) {
  const code = String(account?.code || "").trim();
  const name = String(account?.name || "").trim();
  const accountType = String(account?.account_type || account?.accountType || "")
    .trim()
    .toUpperCase();
  const posting = toBoolean(account?.allow_posting ?? account?.allowPosting)
    ? "Post"
    : "No Post";
  if (!code && !name) {
    return String(account?.id || "");
  }
  return `${code} - ${name} (${accountType || "N/A"}, ${posting})`;
}

export default function GlSetupPage() {
  const { hasPermission } = useAuth();
  const { language } = useI18n();
  const { getModuleRow, refreshLegalEntity } = useModuleReadiness();
  const isTr = language === "tr";
  const l = (en, tr) => (isTr ? tr : en);
  const canReadLegalEntities = hasPermission("org.tree.read");
  const canReadCalendars = hasPermission("org.fiscal_calendar.read");
  const canReadBooks = hasPermission("gl.book.read");
  const canReadCoas = hasPermission("gl.coa.read");
  const canReadAccounts = hasPermission("gl.account.read");
  const canUpsertBooks = hasPermission("gl.book.upsert");
  const canUpsertCoas = hasPermission("gl.coa.upsert");
  const canUpsertAccounts = hasPermission("gl.account.upsert");
  const canUpsertMappings = hasPermission("gl.account_mapping.upsert");
  const canUpsertShareholderParentMappings = hasPermission("org.legal_entity.upsert");

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState("");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [updatingAccountId, setUpdatingAccountId] = useState(null);

  const [legalEntities, setLegalEntities] = useState([]);
  const [countries, setCountries] = useState([]);
  const [calendars, setCalendars] = useState([]);
  const [books, setBooks] = useState([]);
  const [coas, setCoas] = useState([]);
  const [accounts, setAccounts] = useState([]);
  const [policyPacks, setPolicyPacks] = useState([]);

  const [bookForm, setBookForm] = useState({
    legalEntityId: "",
    calendarId: "",
    code: "",
    name: "",
    bookType: "LOCAL",
    baseCurrencyCode: "USD",
  });
  const [coaForm, setCoaForm] = useState({
    scope: "LEGAL_ENTITY",
    legalEntityId: "",
    code: "",
    name: "",
  });
  const [accountForm, setAccountForm] = useState({
    coaId: "",
    code: "",
    name: "",
    accountType: "ASSET",
    normalSide: "DEBIT",
    allowPosting: true,
    parentAccountId: "",
  });
  const [mappingForm, setMappingForm] = useState({
    sourceAccountId: "",
    targetAccountId: "",
    mappingType: "LOCAL_TO_GROUP",
  });
  const [templateWizardForm, setTemplateWizardForm] = useState({
    legalEntityId: "",
    packId: "",
    mode: "MERGE",
  });
  const [templatePreviewRows, setTemplatePreviewRows] = useState([]);
  const [templateOverridesByPurpose, setTemplateOverridesByPurpose] = useState({});
  const [templateApplyResult, setTemplateApplyResult] = useState(null);
  const [manualMappingsForm, setManualMappingsForm] = useState({
    legalEntityId: "",
    capitalCreditParentAccountId: "",
    commitmentDebitParentAccountId: "",
  });
  const [manualCariMappingsByPurpose, setManualCariMappingsByPurpose] = useState({});
  const [showOptionalCariMappings, setShowOptionalCariMappings] = useState(false);
  const [loadingManualMappings, setLoadingManualMappings] = useState(false);
  const parentAccountIds = new Set(
    accounts.map((row) => toPositiveInt(row.parent_account_id)).filter(Boolean)
  );
  const countryIso2ById = useMemo(() => {
    const byId = new Map();
    for (const country of countries) {
      const id = toPositiveInt(country?.id);
      const iso2 = toUpper(country?.iso2);
      if (!id || !iso2) {
        continue;
      }
      byId.set(id, iso2);
    }
    return byId;
  }, [countries]);
  const accountsByLegalEntityId = useMemo(() => {
    const byEntity = new Map();
    for (const account of accounts) {
      const legalEntityId = toPositiveInt(account?.legal_entity_id);
      if (!legalEntityId) {
        continue;
      }
      if (!byEntity.has(legalEntityId)) {
        byEntity.set(legalEntityId, []);
      }
      byEntity.get(legalEntityId).push(account);
    }
    for (const rows of byEntity.values()) {
      rows.sort((left, right) =>
        String(left?.code || "").localeCompare(String(right?.code || ""))
      );
    }
    return byEntity;
  }, [accounts]);
  const selectedTemplateLegalEntityId = toPositiveInt(templateWizardForm.legalEntityId);
  const selectedManualLegalEntityId = toPositiveInt(manualMappingsForm.legalEntityId);
  const selectedTemplateEntity = legalEntities.find(
    (entity) => toPositiveInt(entity?.id) === selectedTemplateLegalEntityId
  );
  const selectedTemplateEntityCountryIso2 = toUpper(
    countryIso2ById.get(toPositiveInt(selectedTemplateEntity?.country_id))
  );
  const availableTemplatePacks = useMemo(() => {
    if (!selectedTemplateEntityCountryIso2) {
      return policyPacks;
    }
    const filtered = policyPacks.filter(
      (pack) => toUpper(pack?.countryIso2) === selectedTemplateEntityCountryIso2
    );
    return filtered.length > 0 ? filtered : policyPacks;
  }, [policyPacks, selectedTemplateEntityCountryIso2]);
  const templatePackIdSet = useMemo(
    () => new Set(availableTemplatePacks.map((pack) => String(pack?.packId || "").trim())),
    [availableTemplatePacks]
  );
  const templateEntityAccounts =
    accountsByLegalEntityId.get(selectedTemplateLegalEntityId) || [];
  const templateOverrideAccountOptions = templateEntityAccounts.filter((account) =>
    toBoolean(account?.is_active)
  );
  const manualEntityAccounts =
    accountsByLegalEntityId.get(selectedManualLegalEntityId) || [];
  const manualCariAccountOptions = manualEntityAccounts.filter(
    (account) => toBoolean(account?.is_active) && toBoolean(account?.allow_posting)
  );
  const manualShareholderAccountOptions = manualEntityAccounts.filter(
    (account) =>
      toBoolean(account?.is_active) &&
      !toBoolean(account?.allow_posting) &&
      toUpper(account?.account_type) === "EQUITY"
  );
  const selectedManualCariReadiness = getModuleRow(
    "cariPosting",
    selectedManualLegalEntityId
  );
  const selectedManualShareholderReadiness = getModuleRow(
    "shareholderCommitment",
    selectedManualLegalEntityId
  );
  const visibleCariPurposeCodes = showOptionalCariMappings
    ? CARI_MANUAL_PURPOSE_CODES
    : CARI_REQUIRED_PURPOSE_CODES;

  async function loadData() {
    setLoading(true);
    setError("");

    const updates = {
      legalEntities,
      countries,
      calendars,
      books,
      coas,
      accounts,
      policyPacks,
    };

    try {
      const tasks = [];

      if (canReadLegalEntities) {
        tasks.push(
          listLegalEntities().then((response) => {
            updates.legalEntities = response?.rows || [];
          })
        );
        tasks.push(
          listCountries().then((response) => {
            updates.countries = response?.rows || [];
          })
        );
        tasks.push(
          listPolicyPacks().then((response) => {
            updates.policyPacks = response?.rows || [];
          })
        );
      }

      if (canReadCalendars) {
        tasks.push(
          listFiscalCalendars().then((response) => {
            updates.calendars = response?.rows || [];
          })
        );
      }

      if (canReadBooks) {
        tasks.push(
          listBooks().then((response) => {
            updates.books = response?.rows || [];
          })
        );
      }

      if (canReadCoas) {
        tasks.push(
          listCoas().then((response) => {
            updates.coas = response?.rows || [];
          })
        );
      }

      if (canReadAccounts) {
        tasks.push(
          listAccounts({ includeInactive: true }).then((response) => {
            updates.accounts = response?.rows || [];
          })
        );
      }

      await Promise.all(tasks);

      setLegalEntities(updates.legalEntities);
      setCountries(updates.countries);
      setCalendars(updates.calendars);
      setBooks(updates.books);
      setCoas(updates.coas);
      setAccounts(updates.accounts);
      setPolicyPacks(updates.policyPacks);

      setBookForm((prev) => ({
        ...prev,
        legalEntityId: prev.legalEntityId || String(updates.legalEntities[0]?.id || ""),
        calendarId: prev.calendarId || String(updates.calendars[0]?.id || ""),
      }));
      setCoaForm((prev) => ({
        ...prev,
        legalEntityId: prev.legalEntityId || String(updates.legalEntities[0]?.id || ""),
      }));
      setAccountForm((prev) => ({
        ...prev,
        coaId: prev.coaId || String(updates.coas[0]?.id || ""),
      }));
      setMappingForm((prev) => ({
        ...prev,
        sourceAccountId:
          prev.sourceAccountId || String(updates.accounts[0]?.id || ""),
        targetAccountId:
          prev.targetAccountId || String(updates.accounts[1]?.id || updates.accounts[0]?.id || ""),
      }));
      setTemplateWizardForm((prev) => ({
        ...prev,
        legalEntityId:
          prev.legalEntityId || String(updates.legalEntities[0]?.id || ""),
        packId: prev.packId || String(updates.policyPacks[0]?.packId || ""),
      }));
      setManualMappingsForm((prev) => ({
        ...prev,
        legalEntityId:
          prev.legalEntityId || String(updates.legalEntities[0]?.id || ""),
      }));
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to load GL setup data.", "GL kurulum verileri yuklenemedi."));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    canReadLegalEntities,
    canReadCalendars,
    canReadBooks,
    canReadCoas,
    canReadAccounts,
  ]);

  useEffect(() => {
    const currentPackId = String(templateWizardForm.packId || "").trim();
    if (currentPackId && templatePackIdSet.has(currentPackId)) {
      return;
    }
    const fallbackPackId = String(availableTemplatePacks[0]?.packId || "").trim();
    if (!fallbackPackId || fallbackPackId === currentPackId) {
      return;
    }
    setTemplateWizardForm((prev) => ({
      ...prev,
      packId: fallbackPackId,
    }));
  }, [availableTemplatePacks, templatePackIdSet, templateWizardForm.packId]);

  useEffect(() => {
    setTemplatePreviewRows([]);
    setTemplateOverridesByPurpose({});
    setTemplateApplyResult(null);
  }, [templateWizardForm.legalEntityId, templateWizardForm.packId]);

  async function loadManualMappings(legalEntityIdInput) {
    const legalEntityId = toPositiveInt(
      legalEntityIdInput ?? manualMappingsForm.legalEntityId
    );
    if (!legalEntityId || !canReadLegalEntities || !canReadAccounts) {
      setManualCariMappingsByPurpose({});
      setManualMappingsForm((prev) => ({
        ...prev,
        capitalCreditParentAccountId: "",
        commitmentDebitParentAccountId: "",
      }));
      return;
    }

    setLoadingManualMappings(true);
    try {
      const [cariResponse, shareholderResponse] = await Promise.all([
        listJournalPurposeAccounts({ legalEntityId }),
        listShareholderJournalConfigs({ legalEntityId }),
      ]);

      setManualCariMappingsByPurpose(toQueryMapByPurpose(cariResponse?.rows || []));
      const shareholderRows = Array.isArray(shareholderResponse?.rows)
        ? shareholderResponse.rows
        : [];
      const shareholderRow =
        shareholderRows.find(
          (row) => toPositiveInt(row?.legal_entity_id) === legalEntityId
        ) || null;

      setManualMappingsForm((prev) => ({
        ...prev,
        legalEntityId: String(legalEntityId),
        capitalCreditParentAccountId: String(
          toPositiveInt(shareholderRow?.capital_credit_parent_account_id) || ""
        ),
        commitmentDebitParentAccountId: String(
          toPositiveInt(shareholderRow?.commitment_debit_parent_account_id) || ""
        ),
      }));
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to load manual purpose mappings.",
            "Manuel amac eslemeleri yuklenemedi."
          )
      );
    } finally {
      setLoadingManualMappings(false);
    }
  }

  useEffect(() => {
    if (!selectedManualLegalEntityId) {
      return;
    }
    loadManualMappings(selectedManualLegalEntityId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedManualLegalEntityId, canReadLegalEntities, canReadAccounts]);

  function getPurposeReadinessStatus(readinessRow, purposeCode) {
    if (!readinessRow) {
      return {
        label: l("Unknown", "Bilinmiyor"),
        className: "bg-slate-100 text-slate-700",
        detail: "",
      };
    }

    const normalizedPurposeCode = toUpper(purposeCode);
    const missingPurposeCodes = new Set(
      (readinessRow?.missingPurposeCodes || []).map((code) => toUpper(code))
    );
    if (missingPurposeCodes.has(normalizedPurposeCode)) {
      return {
        label: l("Missing", "Eksik"),
        className: "bg-rose-100 text-rose-700",
        detail: "",
      };
    }

    const invalid = (readinessRow?.invalidMappings || []).filter(
      (row) => toUpper(row?.purposeCode) === normalizedPurposeCode
    );
    if (invalid.length > 0) {
      const reasons = Array.from(
        new Set(invalid.map((row) => toUpper(row?.reason)).filter(Boolean))
      );
      return {
        label: l("Invalid", "Gecersiz"),
        className: "bg-amber-100 text-amber-800",
        detail: reasons.join(", "),
      };
    }

    return {
      label: l("OK", "Tamam"),
      className: "bg-emerald-100 text-emerald-700",
      detail: "",
    };
  }

  function handleTemplateOverrideChange(purposeCode, nextAccountId) {
    const normalizedPurposeCode = toUpper(purposeCode);
    if (!normalizedPurposeCode) {
      return;
    }
    setTemplateOverridesByPurpose((prev) => ({
      ...prev,
      [normalizedPurposeCode]: nextAccountId,
    }));
  }

  async function handleTemplatePreview() {
    if (!canReadLegalEntities) {
      setError(l("Missing permission: org.tree.read", "Eksik yetki: org.tree.read"));
      return;
    }

    const legalEntityId = toPositiveInt(templateWizardForm.legalEntityId);
    const packId = String(templateWizardForm.packId || "").trim();
    if (!legalEntityId || !packId) {
      setError(
        l(
          "Select legal entity and policy pack first.",
          "Once legal entity ve politika paketi secin."
        )
      );
      return;
    }

    setSaving("policy-pack-resolve");
    setError("");
    setMessage("");
    setTemplateApplyResult(null);
    try {
      const response = await resolvePolicyPack(packId, { legalEntityId });
      const rows = Array.isArray(response?.rows) ? response.rows : [];
      setTemplatePreviewRows(rows);
      setTemplateOverridesByPurpose({});
      setMessage(
        l(
          "Template preview prepared. Review rows and confirm apply to write mappings.",
          "Sablon onizlemesi hazirlandi. Satirlari kontrol edin ve yazmak icin onaylayin."
        )
      );
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to resolve policy pack.", "Politika paketi onizlemesi alinamadi.")
      );
    } finally {
      setSaving("");
    }
  }

  async function handleTemplateApply() {
    if (!canUpsertAccounts) {
      setError(l("Missing permission: gl.account.upsert", "Eksik yetki: gl.account.upsert"));
      return;
    }

    const legalEntityId = toPositiveInt(templateWizardForm.legalEntityId);
    const packId = String(templateWizardForm.packId || "").trim();
    if (!legalEntityId || !packId) {
      setError(
        l(
          "Select legal entity and policy pack first.",
          "Once legal entity ve politika paketi secin."
        )
      );
      return;
    }
    if (templatePreviewRows.length === 0) {
      setError(
        l(
          "Run template preview before confirm apply.",
          "Onaylamadan once sablon onizlemesi calistirin."
        )
      );
      return;
    }

    const rows = [];
    for (const previewRow of templatePreviewRows) {
      const purposeCode = toUpper(previewRow?.purposeCode);
      if (!purposeCode) {
        continue;
      }
      const resolvedAccountId = toPositiveInt(previewRow?.accountId);
      const overrideAccountId = toPositiveInt(
        templateOverridesByPurpose[purposeCode]
      );
      const effectiveAccountId = previewRow?.missing
        ? overrideAccountId
        : resolvedAccountId;
      if (!effectiveAccountId) {
        setError(
          l(
            `Select account override for missing purpose ${purposeCode} before apply.`,
            `Uygulamadan once eksik ${purposeCode} amaci icin hesap secin.`
          )
        );
        return;
      }
      rows.push({
        purposeCode,
        accountId: effectiveAccountId,
      });
    }

    setSaving("policy-pack-apply");
    setError("");
    setMessage("");
    try {
      const response = await applyPolicyPack(packId, {
        legalEntityId,
        mode: toUpper(templateWizardForm.mode || "MERGE"),
        rows,
      });
      setTemplateApplyResult({
        packId: String(response?.packId || packId),
        mode: String(response?.mode || templateWizardForm.mode || "MERGE"),
        appliedAt: response?.metadata?.appliedAt || null,
      });
      setMessage(
        l(
          "Template applied successfully. Module readiness refreshed.",
          "Sablon basariyla uygulandi. Modul hazirlik bilgisi yenilendi."
        )
      );
      await Promise.all([
        refreshLegalEntity(legalEntityId),
        loadManualMappings(legalEntityId),
      ]);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l("Failed to apply policy pack.", "Politika paketi uygulanamadi.")
      );
    } finally {
      setSaving("");
    }
  }

  function handleManualCariPurposeAccountChange(purposeCode, nextAccountId) {
    const normalizedPurposeCode = toUpper(purposeCode);
    if (!normalizedPurposeCode) {
      return;
    }
    setManualCariMappingsByPurpose((prev) => ({
      ...prev,
      [normalizedPurposeCode]: {
        ...(prev[normalizedPurposeCode] || {}),
        purposeCode: normalizedPurposeCode,
        accountId: nextAccountId,
      },
    }));
  }

  async function handleSaveManualCariMappings() {
    if (!canUpsertAccounts) {
      setError(l("Missing permission: gl.account.upsert", "Eksik yetki: gl.account.upsert"));
      return;
    }

    const legalEntityId = selectedManualLegalEntityId;
    if (!legalEntityId) {
      setError(l("Select legal entity first.", "Once legal entity secin."));
      return;
    }

    const payloadRows = [];
    for (const purposeCode of CARI_REQUIRED_PURPOSE_CODES) {
      const accountId = toPositiveInt(
        manualCariMappingsByPurpose[purposeCode]?.accountId
      );
      if (!accountId) {
        setError(
          l(
            `Select account for ${purposeCode} before saving.`,
            `Kaydetmeden once ${purposeCode} icin hesap secin.`
          )
        );
        return;
      }
      payloadRows.push({ purposeCode, accountId });
    }
    for (const purposeCode of CARI_OPTIONAL_CONTEXT_PURPOSE_CODES) {
      const accountId = toPositiveInt(
        manualCariMappingsByPurpose[purposeCode]?.accountId
      );
      if (!accountId) {
        continue;
      }
      payloadRows.push({ purposeCode, accountId });
    }

    setSaving("manual-cari-purpose-mappings");
    setError("");
    setMessage("");
    try {
      for (const row of payloadRows) {
        // eslint-disable-next-line no-await-in-loop
        await upsertJournalPurposeAccount({
          legalEntityId,
          purposeCode: row.purposeCode,
          accountId: row.accountId,
        });
      }

      setMessage(
        l(
          "Manual CARI purpose mappings saved.",
          "Manuel CARI amac eslemeleri kaydedildi."
        )
      );
      await Promise.all([refreshLegalEntity(legalEntityId), loadManualMappings(legalEntityId)]);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to save manual CARI purpose mappings.",
            "Manuel CARI amac eslemeleri kaydedilemedi."
          )
      );
    } finally {
      setSaving("");
    }
  }

  async function handleSaveManualShareholderMappings() {
    if (!canUpsertShareholderParentMappings) {
      setError(
        l(
          "Missing permission: org.legal_entity.upsert",
          "Eksik yetki: org.legal_entity.upsert"
        )
      );
      return;
    }

    const legalEntityId = selectedManualLegalEntityId;
    const capitalCreditParentAccountId = toPositiveInt(
      manualMappingsForm.capitalCreditParentAccountId
    );
    const commitmentDebitParentAccountId = toPositiveInt(
      manualMappingsForm.commitmentDebitParentAccountId
    );

    if (!legalEntityId || !capitalCreditParentAccountId || !commitmentDebitParentAccountId) {
      setError(
        l(
          "Both shareholder parent accounts are required.",
          "Iki ortak parent hesap secimi zorunludur."
        )
      );
      return;
    }
    if (capitalCreditParentAccountId === commitmentDebitParentAccountId) {
      setError(
        l(
          "Shareholder parent accounts must be different.",
          "Ortak parent hesaplari farkli olmali."
        )
      );
      return;
    }

    setSaving("manual-shareholder-purpose-mappings");
    setError("");
    setMessage("");
    try {
      await upsertShareholderJournalConfig({
        legalEntityId,
        capitalCreditParentAccountId,
        commitmentDebitParentAccountId,
      });
      setMessage(
        l(
          "Manual shareholder parent mappings saved.",
          "Manuel ortak parent eslemeleri kaydedildi."
        )
      );
      await Promise.all([refreshLegalEntity(legalEntityId), loadManualMappings(legalEntityId)]);
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to save shareholder parent mappings.",
            "Ortak parent eslemeleri kaydedilemedi."
          )
      );
    } finally {
      setSaving("");
    }
  }

  async function handleBookSubmit(event) {
    event.preventDefault();
    if (!canUpsertBooks) {
      setError(l("Missing permission: gl.book.upsert", "Eksik yetki: gl.book.upsert"));
      return;
    }

    const legalEntityId = toPositiveInt(bookForm.legalEntityId);
    const calendarId = toPositiveInt(bookForm.calendarId);
    if (!legalEntityId || !calendarId) {
      setError(l("legalEntityId and calendarId are required.", "legalEntityId ve calendarId zorunludur."));
      return;
    }

    setSaving("book");
    setError("");
    setMessage("");
    try {
      await upsertBook({
        legalEntityId,
        calendarId,
        code: bookForm.code.trim(),
        name: bookForm.name.trim(),
        bookType: bookForm.bookType,
        baseCurrencyCode: bookForm.baseCurrencyCode.trim().toUpperCase(),
      });
      setBookForm((prev) => ({ ...prev, code: "", name: "" }));
      setMessage(l("Book saved.", "Defter kaydedildi."));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save book.", "Defter kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleCoaSubmit(event) {
    event.preventDefault();
    if (!canUpsertCoas) {
      setError(l("Missing permission: gl.coa.upsert", "Eksik yetki: gl.coa.upsert"));
      return;
    }

    const legalEntityId = toPositiveInt(coaForm.legalEntityId);
    if (coaForm.scope === "LEGAL_ENTITY" && !legalEntityId) {
      setError(l("legalEntityId is required when scope is LEGAL_ENTITY.", "scope LEGAL_ENTITY iken legalEntityId zorunludur."));
      return;
    }

    setSaving("coa");
    setError("");
    setMessage("");
    try {
      await upsertCoa({
        scope: coaForm.scope,
        legalEntityId: coaForm.scope === "LEGAL_ENTITY" ? legalEntityId : undefined,
        code: coaForm.code.trim(),
        name: coaForm.name.trim(),
      });
      setCoaForm((prev) => ({ ...prev, code: "", name: "" }));
      setMessage(l("Chart of accounts saved.", "Hesap plani kaydedildi."));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save CoA.", "Hesap plani kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleAccountSubmit(event) {
    event.preventDefault();
    if (!canUpsertAccounts) {
      setError(l("Missing permission: gl.account.upsert", "Eksik yetki: gl.account.upsert"));
      return;
    }

    const coaId = toPositiveInt(accountForm.coaId);
    if (!coaId) {
      setError(l("coaId is required.", "coaId zorunludur."));
      return;
    }

    setSaving("account");
    setError("");
    setMessage("");
    try {
      await upsertAccount({
        coaId,
        code: accountForm.code.trim(),
        name: accountForm.name.trim(),
        accountType: accountForm.accountType,
        normalSide: accountForm.normalSide,
        allowPosting: Boolean(accountForm.allowPosting),
        parentAccountId: toPositiveInt(accountForm.parentAccountId) || undefined,
      });
      setAccountForm((prev) => ({ ...prev, code: "", name: "", parentAccountId: "" }));
      setMessage(l("Account saved.", "Hesap kaydedildi."));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save account.", "Hesap kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  async function handleAccountPostingChange(account, nextAllowPosting) {
    if (!canUpsertAccounts) {
      setError(l("Missing permission: gl.account.upsert", "Eksik yetki: gl.account.upsert"));
      return;
    }

    const accountId = toPositiveInt(account?.id);
    const coaId = toPositiveInt(account?.coa_id);
    if (!accountId || !coaId) {
      setError(l("Invalid account row.", "Gecersiz hesap satiri."));
      return;
    }

    const hasChildren = parentAccountIds.has(accountId);
    if (hasChildren && nextAllowPosting) {
      setError(
        l(
          "Header account with children cannot be set to posting.",
          "Alt hesabi olan ust hesap post edilebilir yapilamaz."
        )
      );
      return;
    }

    setUpdatingAccountId(accountId);
    setError("");
    setMessage("");
    try {
      const response = await upsertAccount({
        coaId,
        code: String(account.code || "").trim(),
        name: String(account.name || "").trim(),
        accountType: String(account.account_type || "").toUpperCase(),
        normalSide: String(account.normal_side || "").toUpperCase(),
        allowPosting: Boolean(nextAllowPosting),
        parentAccountId: toPositiveInt(account.parent_account_id) || undefined,
      });

      if (response?.enforcedNonPosting) {
        setMessage(
          l(
            "Account has child rows; posting was kept OFF by rule.",
            "Hesabin alt satirlari oldugu icin post secenegi kural geregi kapali tutuldu."
          )
        );
      } else {
        setMessage(
          l(
            `Posting option updated for account ${account.code || accountId}.`,
            `${account.code || accountId} hesap icin post secenegi guncellendi.`
          )
        );
      }
      await loadData();
    } catch (err) {
      setError(
        err?.response?.data?.message ||
          l(
            "Failed to update account posting option.",
            "Hesap post secenegi guncellenemedi."
          )
      );
    } finally {
      setUpdatingAccountId(null);
    }
  }

  async function loadDefaultCoaAccounts({
    accountsToLoad,
    savingKey,
    confirmMessage,
    successMessage,
    failureMessage,
  }) {
    if (!canUpsertAccounts) {
      setError(l("Missing permission: gl.account.upsert", "Eksik yetki: gl.account.upsert"));
      return;
    }

    const coaId = toPositiveInt(accountForm.coaId);
    if (!coaId) {
      setError(
        l(
          "Select a CoA first, then run default account loader.",
          "Once bir hesap plani secin, sonra varsayilan hesap yukleyiciyi calistirin."
        )
      );
      return;
    }

    if (!window.confirm(confirmMessage)) {
      return;
    }

    setSaving(savingKey);
    setError("");
    setMessage("");

    try {
      let processed = 0;
      for (const account of accountsToLoad) {
        // Keep explicit non-postable defaults intact instead of forcing posting=true.
        await upsertAccount({
          coaId,
          code: account.code,
          name: account.name,
          accountType: account.accountType,
          normalSide: account.normalSide,
          allowPosting: account.allowPosting ?? true,
        });
        processed += 1;
      }
      setMessage(successMessage(processed));
      await loadData();
    } catch (err) {
      setError(err?.response?.data?.message || failureMessage);
    } finally {
      setSaving("");
    }
  }

  async function handleLoadTurkishDefaultAccounts() {
    await loadDefaultCoaAccounts({
      accountsToLoad: TURKISH_DEFAULT_COA_ACCOUNTS,
      savingKey: "turkish-default-accounts",
      confirmMessage: l(
        `Load ${TURKISH_DEFAULT_COA_ACCOUNTS.length} Turkish default accounts into selected CoA?`,
        `Secili hesap planina ${TURKISH_DEFAULT_COA_ACCOUNTS.length} adet varsayilan Turk hesap plani hesabi yuklensin mi?`
      ),
      successMessage: (processed) =>
        l(
          `Turkish default CoA loaded. Processed ${processed} accounts.`,
          `Varsayilan Turk hesap plani yuklendi. ${processed} hesap isleme alindi.`
        ),
      failureMessage: l(
        "Failed to load Turkish default CoA accounts.",
        "Varsayilan Turk hesap plani hesaplari yuklenemedi."
      ),
    });
  }

  async function handleLoadUsaDefaultAccounts() {
    await loadDefaultCoaAccounts({
      accountsToLoad: USA_DEFAULT_COA_ACCOUNTS,
      savingKey: "usa-default-accounts",
      confirmMessage: l(
        `Load ${USA_DEFAULT_COA_ACCOUNTS.length} USA default accounts into selected CoA?`,
        `Secili hesap planina ${USA_DEFAULT_COA_ACCOUNTS.length} adet varsayilan ABD hesap plani hesabi yuklensin mi?`
      ),
      successMessage: (processed) =>
        l(
          `USA default CoA loaded. Processed ${processed} accounts.`,
          `Varsayilan ABD hesap plani yuklendi. ${processed} hesap isleme alindi.`
        ),
      failureMessage: l(
        "Failed to load USA default CoA accounts.",
        "Varsayilan ABD hesap plani hesaplari yuklenemedi."
      ),
    });
  }

  async function handleMappingSubmit(event) {
    event.preventDefault();
    if (!canUpsertMappings) {
      setError(l("Missing permission: gl.account_mapping.upsert", "Eksik yetki: gl.account_mapping.upsert"));
      return;
    }

    const sourceAccountId = toPositiveInt(mappingForm.sourceAccountId);
    const targetAccountId = toPositiveInt(mappingForm.targetAccountId);
    if (!sourceAccountId || !targetAccountId) {
      setError(l("sourceAccountId and targetAccountId are required.", "sourceAccountId ve targetAccountId zorunludur."));
      return;
    }

    setSaving("mapping");
    setError("");
    setMessage("");
    try {
      await upsertAccountMapping({
        sourceAccountId,
        targetAccountId,
        mappingType: mappingForm.mappingType,
      });
      setMessage(l("Account mapping saved.", "Hesap eslemesi kaydedildi."));
    } catch (err) {
      setError(err?.response?.data?.message || l("Failed to save mapping.", "Hesap eslemesi kaydedilemedi."));
    } finally {
      setSaving("");
    }
  }

  if (!canReadBooks && !canReadCoas && !canReadAccounts) {
    return (
      <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        {l(
          "You need GL read permissions (`gl.book.read`, `gl.coa.read`, `gl.account.read`) to use this page.",
          "Bu sayfayi kullanmak icin GL okuma yetkileri (`gl.book.read`, `gl.coa.read`, `gl.account.read`) gerekir."
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <TenantReadinessChecklist />

      <div>
        <h1 className="text-xl font-semibold text-slate-900">{l("GL Setup", "GL Ayarlari")}</h1>
        <p className="mt-1 text-sm text-slate-600">
          {l(
            "Manage books, charts of accounts, accounts, and account mappings.",
            "Defterleri, hesap planlarini, hesaplari ve hesap eslemelerini yonetin."
          )}
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
          {error}
        </div>
      )}
      {message && (
        <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700">
          {message}
        </div>
      )}

      <section
        id="template-wizard"
        className="rounded-xl border border-emerald-200 bg-emerald-50/40 p-4"
      >
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <h2 className="text-sm font-semibold text-emerald-900">
              {l("Recommended: Template Wizard", "Onerilen: Sablon Sihirbazi")}
            </h2>
            <p className="mt-1 text-xs text-emerald-800">
              {l(
                "Preview first (no write), then confirm apply to write selected purpose mappings.",
                "Once onizleme yapin (yazmaz), sonra secili amac eslemelerini yazmak icin onaylayin."
              )}
            </p>
          </div>
          <span className="rounded-full border border-emerald-300 bg-white px-2 py-1 text-[11px] font-semibold text-emerald-800">
            {l("No silent writes", "Sessiz yazim yok")}
          </span>
        </div>

        <div className="grid gap-2 md:grid-cols-4">
          <select
            value={templateWizardForm.legalEntityId}
            onChange={(event) =>
              setTemplateWizardForm((prev) => ({
                ...prev,
                legalEntityId: event.target.value,
              }))
            }
            className="rounded-lg border border-emerald-300 bg-white px-3 py-2 text-sm"
          >
            <option value="">{l("Select legal entity", "Legal entity secin")}</option>
            {legalEntities.map((entity) => (
              <option key={entity.id} value={entity.id}>
                {entity.code} - {entity.name}
              </option>
            ))}
          </select>

          <select
            value={templateWizardForm.packId}
            onChange={(event) =>
              setTemplateWizardForm((prev) => ({
                ...prev,
                packId: event.target.value,
              }))
            }
            className="rounded-lg border border-emerald-300 bg-white px-3 py-2 text-sm"
          >
            <option value="">{l("Select policy pack", "Politika paketi secin")}</option>
            {availableTemplatePacks.map((pack) => (
              <option key={pack.packId} value={pack.packId}>
                {pack.packId} - {pack.label} ({pack.countryIso2})
              </option>
            ))}
          </select>

          <select
            value={templateWizardForm.mode}
            onChange={(event) =>
              setTemplateWizardForm((prev) => ({
                ...prev,
                mode: event.target.value,
              }))
            }
            className="rounded-lg border border-emerald-300 bg-white px-3 py-2 text-sm"
          >
            <option value="MERGE">MERGE</option>
            <option value="OVERWRITE">OVERWRITE</option>
          </select>

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={handleTemplatePreview}
              disabled={saving === "policy-pack-resolve"}
              className="rounded-lg border border-emerald-400 bg-white px-3 py-2 text-sm font-semibold text-emerald-800 disabled:opacity-60"
            >
              {saving === "policy-pack-resolve"
                ? l("Previewing...", "Onizleniyor...")
                : l("Preview template", "Sablonu onizle")}
            </button>
            <button
              type="button"
              onClick={handleTemplateApply}
              disabled={
                saving === "policy-pack-apply" || templatePreviewRows.length === 0
              }
              className="rounded-lg bg-emerald-700 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "policy-pack-apply"
                ? l("Applying...", "Uygulaniyor...")
                : l("Confirm apply", "Uygulamayi onayla")}
            </button>
          </div>
        </div>

        {templateApplyResult ? (
          <div className="mt-3 rounded-lg border border-emerald-300 bg-white px-3 py-2 text-xs text-emerald-900">
            {l("Applied pack", "Uygulanan paket")}: {templateApplyResult.packId} |{" "}
            {l("Mode", "Mod")}: {templateApplyResult.mode} |{" "}
            {l("Applied at", "Uygulama zamani")}:{" "}
            {templateApplyResult.appliedAt || l("n/a", "yok")}
          </div>
        ) : null}

        {templatePreviewRows.length > 0 ? (
          <div className="mt-3 overflow-x-auto rounded-lg border border-emerald-200 bg-white">
            <table className="min-w-full text-sm">
              <thead className="bg-emerald-50 text-left text-emerald-900">
                <tr>
                  <th className="px-3 py-2">{l("Purpose code", "Amac kodu")}</th>
                  <th className="px-3 py-2">{l("Module", "Modul")}</th>
                  <th className="px-3 py-2">{l("Proposed account", "Onerilen hesap")}</th>
                  <th className="px-3 py-2">{l("Status", "Durum")}</th>
                </tr>
              </thead>
              <tbody>
                {templatePreviewRows.map((row) => {
                  const purposeCode = toUpper(row?.purposeCode);
                  const missing = Boolean(row?.missing);
                  const overrideValue = String(
                    templateOverridesByPurpose[purposeCode] || ""
                  );
                  return (
                    <tr key={purposeCode} className="border-t border-slate-100">
                      <td className="px-3 py-2 align-top font-medium text-slate-800">
                        {purposeCode}
                      </td>
                      <td className="px-3 py-2 align-top">
                        {String(row?.moduleKey || "-")}
                      </td>
                      <td className="px-3 py-2 align-top">
                        {!missing ? (
                          <div className="text-slate-800">
                            {String(row?.accountCode || "")}{" "}
                            <span className="text-xs text-slate-500">
                              (#{toPositiveInt(row?.accountId) || "-"})
                            </span>
                          </div>
                        ) : (
                          <div className="space-y-1">
                            <div className="text-xs text-rose-700">
                              {l("Missing reason", "Eksik nedeni")}:{" "}
                              {String(row?.reason || "no_match")}
                            </div>
                            <select
                              value={overrideValue}
                              onChange={(event) =>
                                handleTemplateOverrideChange(
                                  purposeCode,
                                  event.target.value
                                )
                              }
                              className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-xs"
                            >
                              <option value="">
                                {l("Select override account", "Override hesap secin")}
                              </option>
                              {templateOverrideAccountOptions.map((account) => (
                                <option key={account.id} value={account.id}>
                                  {buildAccountLabel(account)}
                                </option>
                              ))}
                            </select>
                          </div>
                        )}
                      </td>
                      <td className="px-3 py-2 align-top">
                        {!missing ? (
                          <span className="rounded-full bg-emerald-100 px-2 py-0.5 text-xs font-semibold text-emerald-700">
                            {String(row?.confidence || "HIGH")}
                          </span>
                        ) : (
                          <span className="rounded-full bg-rose-100 px-2 py-0.5 text-xs font-semibold text-rose-700">
                            {l("Missing", "Eksik")}
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="mt-3 text-xs text-emerald-900">
            {l(
              "No preview rows yet. Select legal entity + pack and run preview.",
              "Henuz onizleme satiri yok. Legal entity + paket secip onizleme calistirin."
            )}
          </p>
        )}
      </section>

      <section
        id="manual-purpose-mappings"
        className="rounded-xl border border-slate-200 bg-white p-4"
      >
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div>
            <h2 className="text-sm font-semibold text-slate-800">
              {l(
                "Advanced: Manual Purpose Mappings",
                "Gelismis: Manuel Amac Eslemeleri"
              )}
            </h2>
            <p className="mt-1 text-xs text-slate-600">
              {l(
                "Manual path is fully supported without templates.",
                "Manuel yol, sablon kullanmadan tamamen desteklenir."
              )}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <select
              value={manualMappingsForm.legalEntityId}
              onChange={(event) =>
                setManualMappingsForm((prev) => ({
                  ...prev,
                  legalEntityId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              <option value="">{l("Select legal entity", "Legal entity secin")}</option>
              {legalEntities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.code} - {entity.name}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => loadManualMappings(selectedManualLegalEntityId)}
              disabled={loadingManualMappings || !selectedManualLegalEntityId}
              className="rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs font-semibold text-slate-700 disabled:opacity-60"
            >
              {loadingManualMappings ? l("Loading...", "Yukleniyor...") : l("Reload", "Yenile")}
            </button>
          </div>
        </div>

        <div className="mb-3 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
          {l("Required CARI purpose codes:", "Zorunlu CARI amac kodlari:")}{" "}
          {CARI_REQUIRED_PURPOSE_CODES.join(", ")} <br />
          {l(
            "Optional CARI settlement context purpose codes:",
            "Opsiyonel CARI settlement baglam amac kodlari:"
          )}{" "}
          {CARI_OPTIONAL_CONTEXT_PURPOSE_CODES.length}{" "}
          {l(
            "(hidden by default; use Show optional button below).",
            "(varsayilan gizli; asagidaki Opsiyonelleri goster butonunu kullanin)."
          )}{" "}
          <br />
          {l(
            "Required shareholder parent purpose codes:",
            "Zorunlu ortak parent amac kodlari:"
          )}{" "}
          {SHAREHOLDER_REQUIRED_PURPOSE_CODES.join(", ")}
        </div>

        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-600">
            {l("CARI mappings", "CARI eslemeleri")}
          </h3>
          <button
            type="button"
            onClick={() => setShowOptionalCariMappings((prev) => !prev)}
            className="rounded-lg border border-slate-300 bg-white px-2 py-1 text-[11px] font-semibold text-slate-700"
          >
            {showOptionalCariMappings
              ? l("Hide optional context mappings", "Opsiyonel baglam eslemelerini gizle")
              : l(
                  `Show optional context mappings (${CARI_OPTIONAL_CONTEXT_PURPOSE_CODES.length})`,
                  `Opsiyonel baglam eslemelerini goster (${CARI_OPTIONAL_CONTEXT_PURPOSE_CODES.length})`
                )}
          </button>
        </div>
        <p className="mb-2 text-xs text-slate-500">
          {l(
            "Start with 4 required rows. Optional context rows only override settlement behavior for CASH, MANUAL, or ON_ACCOUNT.",
            "Ilk olarak 4 zorunlu satiri doldurun. Opsiyonel baglam satirlari sadece CASH, MANUAL veya ON_ACCOUNT settlement davranisini override eder."
          )}
        </p>
        <div className="overflow-x-auto rounded-lg border border-slate-200">
          <table className="min-w-full text-sm">
            <thead className="bg-slate-50 text-left text-slate-600">
              <tr>
                <th className="px-3 py-2">{l("Purpose code", "Amac kodu")}</th>
                <th className="px-3 py-2">{l("Account", "Hesap")}</th>
                <th className="px-3 py-2">{l("Readiness", "Hazirlik")}</th>
              </tr>
            </thead>
            <tbody>
              {visibleCariPurposeCodes.map((purposeCode) => {
                const row = manualCariMappingsByPurpose[purposeCode] || null;
                const selectedAccountId = String(toPositiveInt(row?.accountId) || "");
                const isRequiredPurpose = CARI_REQUIRED_PURPOSE_CODE_SET.has(purposeCode);
                const isOptionalPurpose = CARI_OPTIONAL_PURPOSE_CODE_SET.has(purposeCode);
                const purposeMeta = getCariPurposeUiMeta(purposeCode);
                const readinessStatus = isRequiredPurpose
                  ? getPurposeReadinessStatus(selectedManualCariReadiness, purposeCode)
                  : {
                      label: l("Optional", "Opsiyonel"),
                      className: "bg-slate-100 text-slate-700",
                      detail: l(
                        "Optional override; if empty fallback uses base mapping.",
                        "Opsiyonel override; bos ise fallback temel mapping'i kullanir."
                      ),
                    };
                return (
                  <tr key={purposeCode} className="border-t border-slate-100">
                    <td className="px-3 py-2">
                      <div className="font-medium text-slate-800">{purposeCode}</div>
                      {purposeMeta ? (
                        <p className="mt-1 text-[11px] text-slate-500">
                          {l(purposeMeta.en, purposeMeta.tr)}
                        </p>
                      ) : null}
                      {purposeMeta ? (
                        <p className="text-[11px] text-slate-400">
                          {l(purposeMeta.exampleEn, purposeMeta.exampleTr)}
                        </p>
                      ) : null}
                      {isOptionalPurpose ? (
                        <span className="mt-1 inline-block rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-700">
                          {l("Context override", "Baglam override")}
                        </span>
                      ) : null}
                    </td>
                    <td className="px-3 py-2">
                      <select
                        value={selectedAccountId}
                        onChange={(event) =>
                          handleManualCariPurposeAccountChange(
                            purposeCode,
                            event.target.value
                          )
                        }
                        className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-xs"
                      >
                        <option value="">{l("Select account", "Hesap secin")}</option>
                        {manualCariAccountOptions.map((account) => (
                          <option key={account.id} value={account.id}>
                            {buildAccountLabel(account)}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="px-3 py-2">
                      <span
                        className={`rounded-full px-2 py-0.5 text-xs font-semibold ${readinessStatus.className}`}
                      >
                        {readinessStatus.label}
                      </span>
                      {readinessStatus.detail ? (
                        <p className="mt-1 text-[11px] text-slate-500">
                          {readinessStatus.detail}
                        </p>
                      ) : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="mt-2">
          <button
            type="button"
            onClick={handleSaveManualCariMappings}
            disabled={saving === "manual-cari-purpose-mappings" || !selectedManualLegalEntityId}
            className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {saving === "manual-cari-purpose-mappings"
              ? l("Saving CARI mappings...", "CARI eslemeleri kaydediliyor...")
              : l("Save CARI mappings", "CARI eslemelerini kaydet")}
          </button>
        </div>

        <h3 className="mb-2 mt-4 text-xs font-semibold uppercase tracking-wide text-slate-600">
          {l("Shareholder parent mappings", "Ortak parent eslemeleri")}
        </h3>
        <div className="grid gap-2 md:grid-cols-2">
          <div className="rounded-lg border border-slate-200 p-3">
            <label className="mb-1 block text-xs font-semibold text-slate-700">
              SHAREHOLDER_CAPITAL_CREDIT_PARENT
            </label>
            <select
              value={manualMappingsForm.capitalCreditParentAccountId}
              onChange={(event) =>
                setManualMappingsForm((prev) => ({
                  ...prev,
                  capitalCreditParentAccountId: event.target.value,
                }))
              }
              className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-xs"
            >
              <option value="">{l("Select account", "Hesap secin")}</option>
              {manualShareholderAccountOptions.map((account) => (
                <option key={account.id} value={account.id}>
                  {buildAccountLabel(account)}
                </option>
              ))}
            </select>
            {(() => {
              const status = getPurposeReadinessStatus(
                selectedManualShareholderReadiness,
                "SHAREHOLDER_CAPITAL_CREDIT_PARENT"
              );
              return (
                <div className="mt-2">
                  <span
                    className={`rounded-full px-2 py-0.5 text-xs font-semibold ${status.className}`}
                  >
                    {status.label}
                  </span>
                  {status.detail ? (
                    <p className="mt-1 text-[11px] text-slate-500">{status.detail}</p>
                  ) : null}
                </div>
              );
            })()}
          </div>

          <div className="rounded-lg border border-slate-200 p-3">
            <label className="mb-1 block text-xs font-semibold text-slate-700">
              SHAREHOLDER_COMMITMENT_DEBIT_PARENT
            </label>
            <select
              value={manualMappingsForm.commitmentDebitParentAccountId}
              onChange={(event) =>
                setManualMappingsForm((prev) => ({
                  ...prev,
                  commitmentDebitParentAccountId: event.target.value,
                }))
              }
              className="w-full rounded-lg border border-slate-300 px-2 py-1.5 text-xs"
            >
              <option value="">{l("Select account", "Hesap secin")}</option>
              {manualShareholderAccountOptions.map((account) => (
                <option key={account.id} value={account.id}>
                  {buildAccountLabel(account)}
                </option>
              ))}
            </select>
            {(() => {
              const status = getPurposeReadinessStatus(
                selectedManualShareholderReadiness,
                "SHAREHOLDER_COMMITMENT_DEBIT_PARENT"
              );
              return (
                <div className="mt-2">
                  <span
                    className={`rounded-full px-2 py-0.5 text-xs font-semibold ${status.className}`}
                  >
                    {status.label}
                  </span>
                  {status.detail ? (
                    <p className="mt-1 text-[11px] text-slate-500">{status.detail}</p>
                  ) : null}
                </div>
              );
            })()}
          </div>
        </div>
        <div className="mt-2">
          <button
            type="button"
            onClick={handleSaveManualShareholderMappings}
            disabled={
              saving === "manual-shareholder-purpose-mappings" ||
              !selectedManualLegalEntityId
            }
            className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
          >
            {saving === "manual-shareholder-purpose-mappings"
              ? l(
                  "Saving shareholder parents...",
                  "Ortak parent eslemeleri kaydediliyor..."
                )
              : l(
                  "Save shareholder parent mappings",
                  "Ortak parent eslemelerini kaydet"
                )}
          </button>
        </div>
      </section>

      <div className="grid gap-4 xl:grid-cols-2">
        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">{l("Books", "Defterler")}</h2>
          <form onSubmit={handleBookSubmit} className="grid gap-2 md:grid-cols-3">
            <select
              value={bookForm.legalEntityId}
              onChange={(event) =>
                setBookForm((prev) => ({ ...prev, legalEntityId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select legal entity", "Istirak / bagli ortak secin")}</option>
              {legalEntities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.code} - {entity.name}
                </option>
              ))}
            </select>
            <select
              value={bookForm.calendarId}
              onChange={(event) =>
                setBookForm((prev) => ({ ...prev, calendarId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select calendar", "Takvim secin")}</option>
              {calendars.map((calendar) => (
                <option key={calendar.id} value={calendar.id}>
                  {calendar.code} - {calendar.name}
                </option>
              ))}
            </select>
            <select
              value={bookForm.bookType}
              onChange={(event) =>
                setBookForm((prev) => ({ ...prev, bookType: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {BOOK_TYPES.map((bookType) => (
                <option key={bookType} value={bookType}>
                  {bookType}
                </option>
              ))}
            </select>
            <input
              value={bookForm.code}
              onChange={(event) =>
                setBookForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Book code", "Defter kodu")}
              required
            />
            <input
              value={bookForm.name}
              onChange={(event) =>
                setBookForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Book name", "Defter adi")}
              required
            />
            <input
              value={bookForm.baseCurrencyCode}
              onChange={(event) =>
                setBookForm((prev) => ({
                  ...prev,
                  baseCurrencyCode: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Base currency (e.g. USD)", "Ana para birimi (orn. USD)")}
              maxLength={3}
              required
            />
            <button
              type="submit"
              disabled={saving === "book" || !canUpsertBooks}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60 md:col-span-3"
            >
              {saving === "book" ? l("Saving...", "Kaydediliyor...") : l("Save Book", "Defteri Kaydet")}
            </button>
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Entity", "Birim")}</th>
                  <th className="px-3 py-2">{l("Calendar", "Takvim")}</th>
                </tr>
              </thead>
              <tbody>
                {books.map((book) => (
                  <tr key={book.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{book.id}</td>
                    <td className="px-3 py-2">{book.code}</td>
                    <td className="px-3 py-2">{book.name}</td>
                    <td className="px-3 py-2">{book.legal_entity_id}</td>
                    <td className="px-3 py-2">{book.calendar_id}</td>
                  </tr>
                ))}
                {books.length === 0 && !loading && (
                  <tr>
                    <td colSpan={5} className="px-3 py-3 text-slate-500">
                      {l("No books found.", "Defter bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Charts of Accounts", "Hesap Planlari")}
          </h2>
          <form onSubmit={handleCoaSubmit} className="grid gap-2 md:grid-cols-3">
            <select
              value={coaForm.scope}
              onChange={(event) =>
                setCoaForm((prev) => ({ ...prev, scope: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {COA_SCOPES.map((scope) => (
                <option key={scope} value={scope}>
                  {scope}
                </option>
              ))}
            </select>
            <select
              value={coaForm.legalEntityId}
              onChange={(event) =>
                setCoaForm((prev) => ({ ...prev, legalEntityId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              disabled={coaForm.scope !== "LEGAL_ENTITY"}
            >
              <option value="">{l("Select legal entity", "Istirak / bagli ortak secin")}</option>
              {legalEntities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.code} - {entity.name}
                </option>
              ))}
            </select>
            <input
              value={coaForm.code}
              onChange={(event) =>
                setCoaForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("CoA code", "Hesap plani kodu")}
              required
            />
            <input
              value={coaForm.name}
              onChange={(event) =>
                setCoaForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              placeholder={l("CoA name", "Hesap plani adi")}
              required
            />
            <button
              type="submit"
              disabled={saving === "coa" || !canUpsertCoas}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "coa" ? l("Saving...", "Kaydediliyor...") : l("Save CoA", "Hesap Planini Kaydet")}
            </button>
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Scope", "Kapsam")}</th>
                  <th className="px-3 py-2">{l("Entity", "Birim")}</th>
                </tr>
              </thead>
              <tbody>
                {coas.map((coa) => (
                  <tr key={coa.id} className="border-t border-slate-100">
                    <td className="px-3 py-2">{coa.id}</td>
                    <td className="px-3 py-2">{coa.code}</td>
                    <td className="px-3 py-2">{coa.name}</td>
                    <td className="px-3 py-2">{coa.scope}</td>
                    <td className="px-3 py-2">{coa.legal_entity_id || "-"}</td>
                  </tr>
                ))}
                {coas.length === 0 && !loading && (
                  <tr>
                    <td colSpan={5} className="px-3 py-3 text-slate-500">
                      {l("No CoA rows found.", "Hesap plani satiri bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-sm font-semibold text-slate-700">{l("Accounts", "Hesaplar")}</h2>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={handleLoadTurkishDefaultAccounts}
                disabled={saving === "turkish-default-accounts" || !canUpsertAccounts}
                className="rounded-lg border border-cyan-300 bg-cyan-50 px-3 py-1.5 text-xs font-semibold text-cyan-800 disabled:opacity-60"
              >
                {saving === "turkish-default-accounts"
                  ? l("Loading Turkish CoA...", "Turk hesap plani yukleniyor...")
                  : l("Load Turkish Default CoA", "Varsayilan Turk Hesap Planini Yukle")}
              </button>
              <button
                type="button"
                onClick={handleLoadUsaDefaultAccounts}
                disabled={saving === "usa-default-accounts" || !canUpsertAccounts}
                className="rounded-lg border border-indigo-300 bg-indigo-50 px-3 py-1.5 text-xs font-semibold text-indigo-800 disabled:opacity-60"
              >
                {saving === "usa-default-accounts"
                  ? l("Loading USA CoA...", "ABD hesap plani yukleniyor...")
                  : l("Load USA defaults", "Varsayilan ABD Hesap Planini Yukle")}
              </button>
            </div>
          </div>
          <div className="mb-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
            {l(
              "Hint: For shareholder commitment parent mappings (e.g. 500/501), save those parent equity accounts with Allow posting turned off.",
              "Ipucu: Ortak sermaye taahhut parent eslemesinde (ornegin 500/501), parent ozkaynak hesaplarini Post edilmeye izin ver kapali olarak kaydedin."
            )}
          </div>
          <form onSubmit={handleAccountSubmit} className="grid gap-2 md:grid-cols-4">
            <select
              value={accountForm.coaId}
              onChange={(event) =>
                setAccountForm((prev) => ({ ...prev, coaId: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              required
            >
              <option value="">{l("Select CoA", "Hesap plani secin")}</option>
              {coas.map((coa) => (
                <option key={coa.id} value={coa.id}>
                  {coa.code} - {coa.name}
                </option>
              ))}
            </select>
            <input
              value={accountForm.code}
              onChange={(event) =>
                setAccountForm((prev) => ({ ...prev, code: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Account code", "Hesap kodu")}
              required
            />
            <input
              value={accountForm.name}
              onChange={(event) =>
                setAccountForm((prev) => ({ ...prev, name: event.target.value }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Account name", "Hesap adi")}
              required
            />
            <select
              value={accountForm.accountType}
              onChange={(event) =>
                setAccountForm((prev) => ({
                  ...prev,
                  accountType: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {ACCOUNT_TYPES.map((accountType) => (
                <option key={accountType} value={accountType}>
                  {accountType}
                </option>
              ))}
            </select>
            <select
              value={accountForm.normalSide}
              onChange={(event) =>
                setAccountForm((prev) => ({
                  ...prev,
                  normalSide: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
            >
              {NORMAL_SIDES.map((normalSide) => (
                <option key={normalSide} value={normalSide}>
                  {normalSide}
                </option>
              ))}
            </select>
            <input
              type="number"
              min={1}
              value={accountForm.parentAccountId}
              onChange={(event) =>
                setAccountForm((prev) => ({
                  ...prev,
                  parentAccountId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Parent account ID (optional)", "Ust hesap ID (opsiyonel)")}
            />
            <label className="inline-flex items-center gap-2 rounded-lg border border-slate-300 px-3 py-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={accountForm.allowPosting}
                onChange={(event) =>
                  setAccountForm((prev) => ({
                    ...prev,
                    allowPosting: event.target.checked,
                  }))
                }
              />
              {l("Allow posting", "Post edilmeye izin ver")}
            </label>
            <button
              type="submit"
              disabled={saving === "account" || !canUpsertAccounts}
              className="rounded-lg bg-slate-900 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "account" ? l("Saving...", "Kaydediliyor...") : l("Save Account", "Hesabi Kaydet")}
            </button>
          </form>

          <div className="mt-3 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-slate-50 text-left text-slate-600">
                <tr>
                  <th className="px-3 py-2">ID</th>
                  <th className="px-3 py-2">CoA</th>
                  <th className="px-3 py-2">{l("Code", "Kod")}</th>
                  <th className="px-3 py-2">{l("Name", "Ad")}</th>
                  <th className="px-3 py-2">{l("Type", "Tur")}</th>
                  <th className="px-3 py-2">{l("Side", "Yon")}</th>
                  <th className="px-3 py-2">{l("Posting", "Post")}</th>
                  <th className="px-3 py-2">{l("Action", "Islem")}</th>
                </tr>
              </thead>
              <tbody>
                {accounts.map((account) => {
                  const accountId = toPositiveInt(account.id);
                  const hasChildren = parentAccountIds.has(accountId);
                  const rowUpdating = updatingAccountId === accountId;
                  const postingAllowed = toBoolean(account.allow_posting);
                  return (
                    <tr key={account.id} className="border-t border-slate-100">
                      <td className="px-3 py-2">{account.id}</td>
                      <td className="px-3 py-2">{account.coa_id}</td>
                      <td className="px-3 py-2">{account.code}</td>
                      <td className="px-3 py-2">{account.name}</td>
                      <td className="px-3 py-2">{account.account_type}</td>
                      <td className="px-3 py-2">{account.normal_side}</td>
                      <td className="px-3 py-2">
                        <div className="flex flex-col gap-1">
                          <span
                            className={`inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${
                              postingAllowed
                                ? "bg-emerald-100 text-emerald-700"
                                : "bg-amber-100 text-amber-700"
                            }`}
                          >
                            {postingAllowed
                              ? l("Leaf (Post)", "Alt hesap (Post)")
                              : l("Header (No Post)", "Ust hesap (Post yok)")}
                          </span>
                          {hasChildren ? (
                            <span className="text-[11px] text-slate-500">
                              {l("Has child accounts", "Alt hesabi var")}
                            </span>
                          ) : null}
                        </div>
                      </td>
                      <td className="px-3 py-2">
                        <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                          <input
                            type="checkbox"
                            checked={postingAllowed}
                            disabled={
                              !canUpsertAccounts ||
                              rowUpdating ||
                              (hasChildren && !postingAllowed)
                            }
                            onChange={(event) =>
                              handleAccountPostingChange(account, event.target.checked)
                            }
                          />
                          {rowUpdating
                            ? l("Saving...", "Kaydediliyor...")
                            : l("Allow posting", "Post etmeye izin ver")}
                        </label>
                      </td>
                    </tr>
                  );
                })}
                {accounts.length === 0 && !loading && (
                  <tr>
                    <td colSpan={8} className="px-3 py-3 text-slate-500">
                      {l("No accounts found.", "Hesap bulunamadi.")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-4">
          <h2 className="mb-3 text-sm font-semibold text-slate-700">
            {l("Account Mapping", "Hesap Esleme")}
          </h2>
          <form onSubmit={handleMappingSubmit} className="grid gap-2 md:grid-cols-4">
            <select
              value={mappingForm.sourceAccountId}
              onChange={(event) =>
                setMappingForm((prev) => ({
                  ...prev,
                  sourceAccountId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              required
            >
              <option value="">{l("Select source account", "Kaynak hesap secin")}</option>
              {accounts.map((account) => (
                <option key={account.id} value={account.id}>
                  {account.code} - {account.name}
                </option>
              ))}
            </select>
            <select
              value={mappingForm.targetAccountId}
              onChange={(event) =>
                setMappingForm((prev) => ({
                  ...prev,
                  targetAccountId: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm md:col-span-2"
              required
            >
              <option value="">{l("Select target account", "Hedef hesap secin")}</option>
              {accounts.map((account) => (
                <option key={account.id} value={account.id}>
                  {account.code} - {account.name}
                </option>
              ))}
            </select>
            <input
              value={mappingForm.mappingType}
              onChange={(event) =>
                setMappingForm((prev) => ({
                  ...prev,
                  mappingType: event.target.value,
                }))
              }
              className="rounded-lg border border-slate-300 px-3 py-2 text-sm"
              placeholder={l("Mapping type", "Esleme tipi")}
            />
            <button
              type="submit"
              disabled={saving === "mapping" || !canUpsertMappings}
              className="rounded-lg bg-cyan-700 px-3 py-2 text-sm font-semibold text-white disabled:opacity-60"
            >
              {saving === "mapping" ? l("Saving...", "Kaydediliyor...") : l("Save Mapping", "Eslemeyi Kaydet")}
            </button>
          </form>
          <p className="mt-3 text-xs text-slate-500">
            {l(
              "Backend currently provides upsert for mappings. Listing mappings is not exposed yet.",
              "Backend su an yalnizca esleme upsert islemini saglar. Esleme listeleme henuz acik degildir."
            )}
          </p>
        </section>
      </div>
    </div>
  );
}

