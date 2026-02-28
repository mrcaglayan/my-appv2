# KULLANIM_KILAVUZU_BANKA_BORDRO.md

## SAAP Banka ve Bordro Modulleri Kullanim Kilavuzu (Teknik Olmayan Kullanicilar Icin)

Surum: v1  
Tarih: 2026-02-27  
Kapsam:
- `/app/banka-tanimla`
- `/app/banka-ekstre-ice-aktar`
- `/app/banka-ekstre-kuyrugu`
- `/app/banka-mutabakat`
- `/app/banka-onaylar`
- `/app/odeme-batchleri`
- `/app/odeme-batchleri/:batchId`
- `/app/payroll-runs`
- `/app/payroll-runs/import`
- `/app/payroll-runs/:runId`
- `/app/payroll-mappings`
- `/app/payroll-liabilities`
- `/app/payroll-runs/:runId/liabilities`
- `/app/payroll-beneficiaries`
- `/app/payroll-close-controls`
- `/app/ayarlar/operasyon-dashboard`
- `/app/ayarlar/exception-workbench`

Bu kilavuz, kod bilmeyen finans, muhasebe, IK-bordro, hazine ve operasyon ekipleri icin yazildi.
Amac:
- Hangi ekranda hangi adim izlenecek
- Hangi secenek ne zaman secilecek
- Hangi secenek secilirse ne olur, secilmezse ne olur
- Hata durumunda ne yapilacak

---

## 1. Bu iki modul birlikte neyi cozer?

Banka + Bordro akisi birlikte su sorunu cozer:
- Bordro verisini sisteme alirsin
- Bordro tahakkugunu (accrual) olusturursun
- Odeme batch hazirlarsin
- Bankaya dosya cikarirsin
- Bankadan ack/sonuc alirsin
- Mutabakat ve settlement durumunu guncellersin
- Donem kapanisini checklist + maker-checker ile guvenli sekilde yaparsin

Kisa ozet:
- Bordro modulu "kime ne kadar borc var" ve "hangi muhasebe kaydi olusmali" sorusunu cozer.
- Banka modulu "banka hareketi gercekte ne oldu" ve "odeme tamamlandi mi" sorusunu cozer.

---

## 2. Baslamadan Once Kontrol Listesi

1. Legal entity yapisi hazir olmali.
2. Banka GL hesaplari ve bordro GL hesaplari tanimli olmali.
3. Banka hesaplari `Banka Tanimla` ekraninda olusturulmus olmali.
4. Bordro component mappingleri bos olmamali.
5. Yetkiler dogru atanmis olmali.

### 2.1 Kritik Yetkiler (Ozet)

Bank:
- `bank.accounts.read`, `bank.accounts.write`
- `bank.statements.import`, `bank.statements.read`
- `bank.reconcile.read`, `bank.reconcile.write`
- `bank.reconcile.auto.run`
- `bank.reconcile.templates.read`, `bank.reconcile.templates.write`
- `bank.reconcile.diffprofiles.read`, `bank.reconcile.diffprofiles.write`
- `bank.reconcile.exceptions.read`, `bank.reconcile.exceptions.write`
- `bank.payments.returns.read`, `bank.payments.returns.write`
- `payments.batch.read`, `payments.batch.create`, `payments.batch.approve`, `payments.batch.export`, `payments.batch.post`, `payments.batch.cancel`
- `bank.payments.export.create`, `bank.payments.ack.import`, `bank.payments.ack.read`
- `bank.approvals.policies.read/create/update`
- `bank.approvals.requests.read/approve/reject`

Payroll:
- `payroll.runs.read`, `payroll.runs.import`, `payroll.runs.review`, `payroll.runs.finalize`
- `payroll.mappings.read`, `payroll.mappings.write`
- `payroll.liabilities.read`, `payroll.liabilities.build`
- `payroll.payment.prepare`, `payroll.payment.sync.read`, `payroll.payment.sync.apply`
- `payroll.settlement.override.read/request/approve`
- `payroll.beneficiary.read/write/set_primary/snapshot.read`
- `payroll.close.read/prepare/request/approve/reopen`

---

## 3. Banka Modulu Kullanim Rehberi

## 3.1 Ekran: Banka Tanimla (`/app/banka-tanimla`)

Bu ekranin amaci:
- Banka hesap masterini olusturmak
- Banka hesabini GL hesap ile baglamak
- Connector test/sync yapmak (B05)

### 3.1.1 Alanlar ve secim etkileri

`Legal Entity`
- Ne zaman secilir: Her zaman zorunlu.
- Yanlis secim sonucu: Yanlis sirket adina hesap acilir, mutabakat ve odeme akisi yanlis entityde calisir.

`Code`
- Oneri: Kurum standart kodu kullan (ornek `BANK_TRY_MAIN`).
- Yanlis secim sonucu: Listeleme/raporlama karisir.

`Currency`
- Ne zaman secilir: Gercek banka hesabinin para birimi.
- Yanlis secim sonucu: Export/import ve settlement eslesmeleri bozulur.

`GL Account`
- Ekran yalniz ACTIVE + postable + LEGAL_ENTITY + ASSET hesaplari listeler.
- Ne zaman secilir: O bankanin ana bilanco hesabi.
- Yanlis secim sonucu: Muhasebe postlamasi yanlis hesaba gider.

`Bank Name`, `Branch Name`, `IBAN`, `Account No`
- Isletme kalite verisidir.
- Bos birakilabilir ama dolu olmasi operasyonu rahatlatir.

`Active`
- `ACTIVE`: Hesap kullanilir.
- `INACTIVE`: Yeni islemde kullanilmaz.

### 3.1.2 Connector (B05) butonlari

`Test`
- Ne yapar: Connector baglanti sagligini test eder.
- Ne zaman kullan: Ilk kurulumda, sifre/token degistiginde, sync oncesi.

`Sync Now`
- Ne yapar: Bagli bankadan statement sync surecini tetikler.
- Ne zaman kullan: Zamanlanmis sync beklenmeden manuel cekim gerektiginde.

---

## 3.2 Ekran: Banka Ekstre Ice Aktar (`/app/banka-ekstre-ice-aktar`)

Bu ekran PR-B02 CSV import temelidir.

### 3.2.1 CSV minimum kolonlar

Beklenen kolon seti:
- `txn_date`
- `value_date`
- `description`
- `reference_no`
- `amount`
- `currency_code`
- `balance_after`

### 3.2.2 Seceneklerin anlami

`Bank Account`
- Import satirlari hangi banka hesabina baglanacak.

`Original Filename`
- Denetim izi icin dosya adi.

`CSV Icerik`
- Dosya yukleyebilir veya metin olarak yapistirabilirsin.

### 3.2.3 Duplicate mantigi

Bu import idempotent ve duplicate kontrolludur:
- Ayni satir tekrar gelirse "duplicate" sayilir.
- Yeni satirlar "inserted" sayilir.

Sonuc panelinde:
- `Toplam`
- `Eklenen`
- `Duplicate`

---

## 3.3 Ekran: Banka Ekstre Kuyrugu (`/app/banka-ekstre-kuyrugu`)

Bu ekran statement importlari ve line queue izleme ekranidir.

### 3.3.1 Import status filtreleri

- `IMPORTED`: Basarili import
- `FAILED`: Hata alan import

### 3.3.2 Recon status filtreleri

- `UNMATCHED`: Henuz eslesmemis
- `PARTIAL`: Kismi eslesmis
- `MATCHED`: Tam eslesmis
- `IGNORED`: Bilincli olarak dislanmis

Kural:
- Operasyon hedefi, acik satirlari (`UNMATCHED`/`PARTIAL`) azaltmaktir.

---

## 3.4 Ekran: Banka Mutabakat (`/app/banka-mutabakat`)

Bu ekran B03 + B07 + B08A + B08B akislarini toplar.

### 3.4.1 Queue filtreleri

`Bank Account`
- Tek hesap bazli mutabakat calismasi yaparsin.

`Recon Status`
- Onceliklendirme icin kullanilir.

`Description / ref (q)`
- Satir bazli hizli arama.

### 3.4.2 Manuel aksiyonlar

`Match`
- Secili statement line bir veya daha fazla aday ile eslestirilir.
- Sonuc: line `MATCHED` veya `PARTIAL` olur.

`Unmatch`
- Daha once yapilan eslesmeyi bozar.
- Sonuc: line tekrar acik duruma doner.

`Ignore`
- Bu satir bilerek mutabakat kapsami disina alinir.
- Sonuc: line `IGNORED` olur.

Not:
- `IGNORED` satira tekrar operasyon sinirlidir; geri almak icin uygun aksiyon akisi gerekir.

### 3.4.3 Auto-run (B07/B08)

`Preview Auto-Run`
- Ne yapar: Kurallar ve template'lere gore ne olacagini gosterir.
- Ne zaman: Her zaman once preview.

`Apply Auto-Run`
- Ne yapar: Eslestirme/istisna/auto-post aksiyonlarini uygular.
- Ne zaman: Preview sonucu beklenen ise.

### 3.4.4 Posting Templates (B08-A)

Amac: Banka line'larindan otomatik journal olusturmak.

Kritik alanlar:

`scopeType`
- `LEGAL_ENTITY`: Entity geneli
- `BANK_ACCOUNT`: Tek hesap ozeli
- `GLOBAL`: Genis kapsam (yine LE baglamiyla kullan)

Secim onerisi:
- Standart kurallar: `LEGAL_ENTITY`
- Ozel hesap kurali: `BANK_ACCOUNT`
- `GLOBAL` sadece kurumsal merkez politikasi netse kullan.

`status`
- `ACTIVE`: Kural calisir
- `PAUSED`: Gecici durdur
- `DISABLED`: Kalici kapat

`directionPolicy`
- `BOTH`: Giris+cikis
- `OUTFLOW_ONLY`: Sadece para cikisi
- `INFLOW_ONLY`: Sadece para girisi

`counterAccountId`
- Zorunlu. Auto-post'ta karsi hesap.

`descriptionMode`
- `USE_STATEMENT_TEXT`: Ekstre aciklamasini kullan
- `FIXED_TEXT`: Sabit metin (fixedDescription gerekli)
- `PREFIXED`: Prefix + ekstre metni (descriptionPrefix gerekli)

Secim rehberi:
- Operasyon okunabilirligi onemliyse `PREFIXED`
- Banka aciklamasi kaliteliyse `USE_STATEMENT_TEXT`
- Tam standart metin istenirse `FIXED_TEXT`

### 3.4.5 Difference Profiles (B08-B)

Amac: Fark tiplerini kontrollu islemek.

`differenceType`
- `FEE`: Banka masrafi farklari
- `FX`: Kur farki

`directionPolicy`
- `BOTH`
- `INCREASE_ONLY`
- `DECREASE_ONLY`

`maxAbsDifference`
- Bu tolerans altindaki farklar profile gore islenir.

Hesap alanlari:
- `FEE` icin `expenseAccountId` gerekir.
- `FX` icin `fxGainAccountId` + `fxLossAccountId` gerekir.

### 3.4.6 Return Events (B08-B)

`eventType`
- `PAYMENT_RETURNED`
- `PAYMENT_REJECTED`
- `PAYMENT_REVERSAL`

Ne zaman kullanilir:
- Banka bir odeme satirini iade/reddettiginde.

Etki:
- Payment line durumu ve ilgili mutabakat/istisna akisina sinyal uretir.

### 3.4.7 Exception Queue (B07)

`status`
- `OPEN`
- `ASSIGNED`
- `RESOLVED`
- `IGNORED`

Aksiyonlar:
- `Assign to me`
- `Resolve`
- `Ignore`
- `Retry`

Operasyon kurali:
- `OPEN/ASSIGNED` satirlar aktif takip ister.
- `RESOLVED/IGNORED` satirlar kapatilmis kabul edilir.

---

## 3.5 Ekranlar: Odeme Batchleri (`/app/odeme-batchleri`, `/app/odeme-batchleri/:batchId`)

Bu ekran PR-B04 + B06 + B09 operasyon merkezidir.

### 3.5.1 Status akisi

Batch statuslari:
- `DRAFT`
- `APPROVED`
- `EXPORTED`
- `POSTED`
- `FAILED`
- `CANCELLED`

Tipik yol:
`DRAFT -> APPROVED -> EXPORTED -> POSTED`

### 3.5.2 Butonlar ve ne zaman kullanilir

`Onayla`
- Ne zaman: Batch icerigi tamam, gonderime hazir.
- Durum etkisi: `DRAFT -> APPROVED`

`Bank Export (B06)` veya `CSV Export`
- Ne zaman: Bankaya dosya gondermeden hemen once.
- Durum etkisi: Export kaydi olusur, batch export asamasina gecer.
- B09 etkisi: Policy varsa export yerine onay talebi acilabilir.

`Ack Import`
- Ne zaman: Bankadan geri bildirim dosyasi geldiginde.
- Etki: Line bazli bank execution/ack durumlari guncellenir.

`Post`
- Ne zaman: Odeme sonucu muhasebe kesinlestirilecekse.
- Durum etkisi: Muhasebe post islemi tamamlanir.

`Iptal`
- Ne zaman: Batch operasyonel olarak iptal edilecekse.
- Durum etkisi: `CANCELLED`.

---

## 3.6 Ekran: Banka Onaylari (`/app/banka-onaylar`)

Bu ekran B09 governance ekranidir.

Iki bolum vardir:
- Policy Listesi / Policy Olustur
- Onay Kuyrugu (`PENDING`)

### 3.6.1 Policy alanlari

`targetType`
- Hangi nesne icin policy (or. `PAYMENT_BATCH`).

`actionType`
- Hangi aksiyonda policy devreye girer (or. `SUBMIT_EXPORT`).

`scopeType`
- `GLOBAL`, `LEGAL_ENTITY`, `BANK_ACCOUNT`

`requiredApprovals`
- Kac onay gerekir.

`makerCheckerRequired`
- Talebi acan kisi ayni talebi onaylayamaz.

`autoExecuteOnFinalApproval`
- Son onaydan sonra hedef islem otomatik devam eder.

### 3.6.2 Onay kuyrugu nasil dolar?

Kuyruk genelde su durumda dolar:
- Batch export adiminda policy kosulu saglanir
- Sistem dogrudan export yapmaz
- `PENDING` onay talebi olusturur

Kuyrukta:
- `Approve`: Islem onaylanir, policyye gore export devam eder.
- `Reject`: Talep red edilir, islem beklemede kalir veya durur.

---

## 4. Bordro Modulu Kullanim Rehberi

## 4.1 Ekran: Bordro Import (`/app/payroll-runs/import`)

Amac: Provider CSV verisinden payroll run olusturmak veya correction shell'e veri aktarmak.

### 4.1.1 Alanlar ve secim etkileri

`Target Run ID (opsiyonel)`
- Doluysa: Yeni run acmaz, mevcut DRAFT correction shell icine import eder.
- Bos ise: Yeni payroll run olusur.

`Legal Entity`
- Zorunlu (target run yoksa).

`Provider Code`
- Entegrasyon kaynagi.
- Oneri: Kurumda standardize tek kod seti kullan.

`Payroll Period` ve `Pay Date`
- Raporlama ve close kontrollerinde temel tarih alanlari.

`Currency`
- Bordro para birimi.

`Source Batch Ref`
- Dis sistem batch referansi (audit icin faydali).

`CSV Icerik`
- Satir bazli calisan bordro verisi.

### 4.1.2 Sonuc paneli ne anlatir?

- Run no
- Status
- Employee sayisi
- Gross / Net toplamlar
- Inserted / duplicate satirlar

---

## 4.2 Ekran: Bordro Runlari (`/app/payroll-runs`)

Bu ekran toplu izleme ekranidir.

Filtreler:
- `Provider`
- `Payroll Period`
- `Status` (`IMPORTED`, `REVIEWED`, `FINALIZED`, `DRAFT`)
- Arama

Yorum:
- `IMPORTED`: Veri alindi, tahakkuk oncesi kontrol asamasi.
- `REVIEWED`: Incelendi, finalize adimina hazir.
- `FINALIZED`: Tahakkuk post edilmis final run.
- `DRAFT`: Genelde correction shell veya ara durum.

---

## 4.3 Ekran: Bordro Run Detay (`/app/payroll-runs/:runId`)

Bu ekran 3 kritik isi birlestirir:
- Run detay/audit
- Corrections/Reversal (PR-P05)
- Tahakkuk preview + finalize (PR-P02)

### 4.3.1 Corrections/Reversal secenekleri

`Create OFF_CYCLE Shell`
- Normal donem disi ekstra odemeler icin.

`Create RETRO Shell`
- Gecmis doneme duzeltme icin.

`Reverse Run`
- Yalniz FINALIZED run icin.
- Reversal reason zorunludur.

### 4.3.2 Tahakkuk Preview ve Finalize

Preview alaninda:
- `Debit Total`
- `Credit Total`
- `Missing Mappings`

Karar kurali:
- Preview dengeli degilse veya mapping eksikse finalize etme.

`Mark Reviewed`
- Inceleme tamam isareti.

`Finalize + Post Accrual`
- Tahakkuk jurnalini olusturur ve run'i final yapar.

---

## 4.4 Ekran: Bordro Mappingleri (`/app/payroll-mappings`)

Amac:
- Bordro componentlerini GL hesaplara etkili tarih araligiyla baglamak.

### 4.4.1 Component secim rehberi

DEBIT tarafi gider:
- `BASE_SALARY_EXPENSE`
- `OVERTIME_EXPENSE`
- `BONUS_EXPENSE`
- `ALLOWANCES_EXPENSE`
- `EMPLOYER_TAX_EXPENSE`
- `EMPLOYER_SOCIAL_SECURITY_EXPENSE`

CREDIT tarafi borc/yukumluluk:
- `PAYROLL_NET_PAYABLE`
- `EMPLOYEE_TAX_PAYABLE`
- `EMPLOYEE_SOCIAL_SECURITY_PAYABLE`
- `EMPLOYER_TAX_PAYABLE`
- `EMPLOYER_SOCIAL_SECURITY_PAYABLE`
- `OTHER_DEDUCTIONS_PAYABLE`

### 4.4.2 Provider bazli veya fallback?

`providerCode` dolu:
- Sadece o provider icin mapping gecerlidir.

`providerCode` bos:
- Fallback mapping olur.

Oneri:
- Tek provider varsa fallback yeterli olabilir.
- Coklu provider varsa provider bazli net ayirim yap.

---

## 4.5 Ekran: Bordro Liabilities (`/app/payroll-liabilities` veya `/app/payroll-runs/:runId/liabilities`)

Bu ekran PR-P03 ve PR-P04 merkezi ekranidir.

### 4.5.1 Scope secenekleri

`NET_PAY`
- Calisan net odemeleri.

`STATUTORY`
- Vergi/SGK gibi yasal yukumluluk odemeleri.

`ALL`
- Ikisini birden.

Secim rehberi:
- Maas odemesi cikariyorsan `NET_PAY`.
- Kurum odemeleri (vergi/SGK) icin `STATUTORY`.
- Toplu analizde `ALL`.

### 4.5.2 Build ve Payment Prep

`Build Liabilities`
- Run'dan liability satirlari uretir.

`Create Payment Batch`
- Liability satirlarindan odeme batch olusturur.
- `bankAccountId` zorunlu.
- `idempotencyKey` tekrar calistirma guvenligi icin onerilir.

### 4.5.3 Payment Sync (B04 + B03 kaniti)

`Sync Scope`: `ALL`, `NET_PAY`, `STATUTORY`

`allowB04OnlySettlement` secenegi:
- Acik: B03 mutabakat kaniti olmadan, sadece B04 odeme durumuna dayanarak settlement kabul edilir.
- Kapali: Daha siki kontrol, bank mutabakat kaniti beklersin.

Oneri:
- Uretimde normalde kapali tut.
- Gecici operasyonel ihtiyac varsa kontrollu ac.

### 4.5.4 Manual Settlement Overrides (PR-P06)

Maker-checker mantigi:
- `Request` olusturan kisi ayri
- `Approve/Reject` eden kisi ayri

Ne zaman kullan:
- Banka kaniti gec geliyor ama odemenin gerceklestigi teyitli.

Risk:
- Yanlis kullanilirsa audit riski artar.

### 4.5.5 Beneficiary Snapshot goruntuleme

Amac:
- Odeme hazirlandigi andaki immutable beneficiary verisini gormek.
- Sonradan beneficiary master degisse bile o odeme icin eski snapshot korunur.

---

## 4.6 Ekran: Bordro Beneficiaries (`/app/payroll-beneficiaries`)

Amac:
- Calisan bazli banka hesap masteri
- Primary hesap yonetimi

### 4.6.1 Lookup kurali

Listelemek icin:
- `legalEntityId` + `employeeCode` zorunlu.

### 4.6.2 Secenekler

`status`
- `ACTIVE`: Odeme icin kullanilabilir.
- `INACTIVE`: Yeni odemede kullanma.

`verificationStatus`
- `UNVERIFIED`
- `VERIFIED`

`isPrimary`
- Ayni calisan/para birimi icin birincil hesap.

Secim rehberi:
- Operasyonda kullanilacak hesaplari `ACTIVE + VERIFIED` tut.
- Eski hesaplari silmek yerine `INACTIVE` yap.

---

## 4.7 Ekran: Payroll Close Controls (`/app/payroll-close-controls`)

Bu ekran PR-P08 donem kapanis kontrol ekranidir.

### 4.7.1 Status akisi

- `DRAFT`
- `READY`
- `REQUESTED`
- `CLOSED`
- `REOPENED`

Kural:
- `READY` olmadan request-close yapilmaz.
- `REQUESTED` olmadan approve-close yapilmaz.
- `CLOSED` olmayan donem reopen edilemez.

Maker-checker:
- Request eden kisi ayni donemi approve edemez.

### 4.7.2 Prepare Checklist lock secenekleri

`lock_run_changes`
- Aciksa run degisiklikleri kapanis doneminde bloke edilir.

`lock_manual_settlements`
- Aciksa manual settlement override gibi islemler bloke edilir.

`lock_payment_prep`
- Aciksa yeni payment prep bloke edilir.

Secim rehberi:
- Kapanis yaklasinca:
  - run ve manual settlement lock acik olmali.
  - payment prep lock, odemeler tamamlaninca acilmali.

### 4.7.3 Islem butonlari

`Request`
- Kapanis talebini acar.

`Approve + Close`
- Donemi resmi olarak kapatir.

`Reopen`
- Zorunlu duzeltme icin kapanmis donemi geri acar.
- `reason` zorunludur.

---

## 4.8 Ekran: Operasyon Dashboard (`/app/ayarlar/operasyon-dashboard`)

Bu ekran H05 operasyon saglik ekranidir.

Amac:
- Banka ve bordro sureclerini tek yerden izlemek
- Birikmis is, geciken is, hata yogunlugu gibi sinyalleri erken yakalamak

### 4.8.1 Filtreler ve ne zaman hangisi secilir

`Legal entity ID`
- Tek sirket performansi izlenecekse doldur.
- Grup geneli izlenecekse bos birak.

`Bank account ID`
- Tek banka hesabi kaynakli problem araniyorsa doldur.
- Genel saglik kontrolunde bos birak.

`Date from` / `Date to`
- Net bir tarih araligi incelemek istiyorsan kullan.
- Ornek: "Bu ayin 1'i ile 15'i arasi gecikmeler."

`Days fallback`
- Tarih vermediysen son kac gunun gorulecegini belirler.
- Oneri: Gunluk takipte `30`, haftalik trend icin `60-90`.

`Jobs module code`
- Sadece belirli is kuyrugunu incelemek icin (or. BANK, PAYROLL).
- Genel bakista bos birak.

`Jobs queue name`
- Tek queue odakli incelemede doldur.
- Genel bakista bos birak.

### 4.8.2 Kartlar nasil yorumlanir

`Bank Reconciliation Summary`
- `UNMATCHED` veya acik exception artiyorsa mutabakat birikiyor demektir.

`Bank Payment Batches Health`
- `awaiting_ack` ve `awaiting_ack_gt_24h` yuksekse banka geri bildirimleri gecikiyor olabilir.

`Payroll Import Health`
- `failed_jobs` veya `oldest_pending_or_failed_hours` yukseliyorsa bordro import akisinda sorun vardir.

`Payroll Close Status`
- `requested_gt_24h` artiyorsa kapanis onaylari beklemede birikiyor olabilir.

`Jobs Health`
- `queued_due_now` ve `retries_due_now` artiyorsa islem kuyruklari yetismiyor olabilir.

Not:
- Ekranda metrikler JSON bloklari olarak gosterilebilir; bu normaldir.
- Operasyon yorumu yaparken trend degisimine bak: bugun dunden daha iyi mi daha kotu mu?

### 4.8.3 Pratik kullanim rutini

Gun basi (10 dk):
1. Filter bos -> genel durum bak.
2. `awaiting_ack_gt_24h`, `failed_jobs`, `unmatched_open_total` kontrol et.
3. Kritik birikim varsa ilgili ekip ve ekrana in.

Gun sonu (10 dk):
1. Ayni filtreyle tekrar bak.
2. Acilan issue'larin azaldigini teyit et.

---

## 4.9 Ekran: Exception Workbench (`/app/ayarlar/exception-workbench`)

Bu ekran H06 merkezi istisna yonetim ekranidir.

Amac:
- Banka ve bordro istisnalarini tek kuyrukta toplamak
- Atama, cozum, ignore ve reopen adimlarini standardize etmek
- Denetim izi ile kapanis kalitesini artirmak

### 4.9.1 Filtreler ve secim rehberi

`Module`
- `BANK`: Sadece banka kaynakli istisnalar
- `PAYROLL`: Sadece bordro kaynakli istisnalar

`Status`
- `OPEN`: Yeni/acik
- `IN_REVIEW`: Uzerinde calisiliyor
- `RESOLVED`: Cozuldu
- `IGNORED`: Bilincli olarak kapsam disi

`Severity`
- `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`
- Gunluk takipte once `CRITICAL/HIGH` filtrele.

`Legal entity ID`
- Tek sirket sorununu ayirmak icin kullan.

`Search`
- Baslik, kaynak key, not ile hizli arama.

`Days`
- Son kac gunun exception kaydi taranacak.
- Oneri: Operasyonda 30-60, denetim bakisinda 180.

`Auto-refresh sources on list`
- Aciksa liste cagrilarinda kaynaklar da tazelenir.
- Hizli manuel analizde kapatip sadece "Apply Filters" kullanabilirsin.

### 4.9.2 Aksiyonlar ve etkileri

`Claim`
- Istisnayi sorumlu kisiye alir.
- Operasyonel sahiplik saglar.

`Resolve`
- Istisnayi cozulmus duruma ceker.
- Cozum notu girilmesi onerilir.

`Ignore`
- Istisnayi bilincli sekilde dislar.
- Neden ignore edildigi nota yazilmalidir.

`Reopen`
- Yanlis kapatilmis veya tekrar acilan problemde yeniden acilir.

### 4.9.3 Hangi durumda ne secilmeli?

Durum 1: Gercek problem ve cozum var
- `Claim` -> cozum uygula -> `Resolve`

Durum 2: False-positive veya kapsama disi
- `Claim` -> neden kaydi -> `Ignore`

Durum 3: Resolve edilmis ama tekrarlandi
- `Reopen` -> yeni kok neden analizi

### 4.9.4 Kisa operasyon standardi

1. Her `CRITICAL/HIGH` kayit icerisinde bir owner olsun (`Claim`).
2. `Resolve/Ignore` adiminda not bos birakilmasin.
3. Haftalik kontrolde uzun suredir acik kalanlar (`OPEN/IN_REVIEW`) raporlansin.

---

## 5. Uc Uca Ornek Senaryolar

## 5.1 Senaryo A: Aylik standart bordro odemesi

1. `Bordro Import` ile run olustur.
2. `Bordro Mappingleri` eksiklerini tamamla.
3. `Run Detay` ekraninda preview kontrol et.
4. `Finalize + Post Accrual` yap.
5. `Liabilities` ekraninda `Build` sonra `Create Payment Batch` yap.
6. `Odeme Batch Detay` ekraninda `Onayla` ve `Bank Export` yap.
7. Bankadan ack geldikten sonra `Ack Import` yap.
8. `Payment Sync Apply` ile liability durumlarini guncelle.
9. `Payroll Close Controls` ile prepare -> request -> approve close tamamla.

## 5.2 Senaryo B: Banka iade/reddedilen odeme

1. `Banka Mutabakat` ekraninda `Return Event` ekle (`PAYMENT_REJECTED` veya `PAYMENT_RETURNED`).
2. Exception queue'yu kontrol et.
3. Gerekirse `Retry` veya `Resolve` aksiyonu uygula.
4. `Liabilities` ekraninda sync preview/apply ile etkisini gor.

## 5.3 Senaryo C: Gecmis donem bordro duzeltmesi

1. `Run Detay` ekraninda `Create RETRO Shell`.
2. `Bordro Import` ekraninda `Target Run ID` ile shell'e import yap.
3. Preview + finalize adimlarini shell run icin tekrarla.
4. Gerekliyse yeni payment batch olustur.

## 5.4 Senaryo D: Acil manuel settlement (istisna)

1. `Liabilities` tablosunda ilgili satira `Manual Settlement Request` ac.
2. Ayri yetkili kullanici `Approve/Apply` yapar.
3. Audit kaydini ve reason notunu doldur.
4. Sonraki bank kaniti geldiginde mutabakat tarafini da kapat.

---

## 6. Hangi Secenegi Ne Zaman Secmeliyim? (Kisa Karar Matrisi)

`Mutabakat directionPolicy`
- Cikis hareketleri icin `OUTFLOW_ONLY`
- Giris hareketleri icin `INFLOW_ONLY`
- Karisik hesapta `BOTH`

`Description Mode`
- Bank aciklamasi temizse `USE_STATEMENT_TEXT`
- Standart muhasebe metni istenirse `FIXED_TEXT`
- Hem standart hem kaynak metin istenirse `PREFIXED`

`Liability Scope`
- Maas odemesi: `NET_PAY`
- Vergi/SGK: `STATUTORY`
- Tum borclar: `ALL`

`Close lock_payment_prep`
- Odemeler bitmediyse `false`
- Odemeler tamamlandiysa `true`

`allowB04OnlySettlement`
- Normalde `false`
- Gecici operasyonel zorunlulukta kontrollu `true`

---

## 7. Sik Hatalar ve Cozumler

`Hata: missing permission`
- Cozum: RBAC yetkisi eklemeden ekran/buton calismaz.

`Hata: counterAccountId is required`
- Cozum: Template veya islemde karsi GL hesap sec.

`Hata: mapping eksik / preview not balanced`
- Cozum: `Bordro Mappingleri` ekraninda eksik componentleri tamamla.

`Hata: period close state conflict`
- Cozum: Durum akisini takip et (`READY -> REQUESTED -> CLOSED`).

`Hata: maker-checker violation`
- Cozum: Talebi acan ve onaylayan kullanici farkli olmali.

---

## 8. Gunluk ve Aylik Operasyon Onerisi

Gunluk:
1. Banka ekstre import/sync kontrolu
2. Mutabakat queue ve exception takibi
3. Odeme batch ack import kontrolu
4. Liabilities sync preview kontrolu

Aylik:
1. Bordro run import + finalize
2. Liabilities build + payment prep
3. Settlement sync apply
4. Payroll close checklist + onay
5. Gerekirse retention/snapshot kontrolleri (`/app/ayarlar/veri-saklama-snapshot`)

---

## 9. Son Not

Bu kilavuz operasyonel kullanim icindir.
Kod seviyesinde teknik degisiklik gerektiren talepler icin teknik ekip ile ilerleyin.
Kullanici tarafinda temel kural:
- Once preview/kontrol
- Sonra uygulama/post
- Her kritik adimda audit notu ve idempotency key kullanimi


