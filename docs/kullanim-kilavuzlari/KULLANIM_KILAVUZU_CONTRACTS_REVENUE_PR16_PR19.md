# KULLANIM_KILAVUZU_CONTRACTS_REVENUE_PR16_PR19.md

## SAAP Contracts + Periodization + Counterparty Mapping Kilavuzu (PR-16..24)

Surum: v1  
Tarih: 2026-02-25  
Kapsam:
- `/app/contracts`
- `/app/contracts-and-revenue` (alias, `/app/contracts`e yonlenir)
- `/app/gelecek-yillar-gelirleri`
- Counterparty ekranlarindaki AR/AP hesap esleme alanlari (`Alici/Satici kart` ekranlari)

Bu kilavuz finans, muhasebe, operasyon ve denetim ekipleri icin yazildi.  
Amac: "Hangi secenek ne icin var, secmezsem ne olur, hata olursa nasil okumaliyim?" sorularina net cevap vermek.

---

## 1. PR-16..24 neyi cozer?

- PR-16: Sozlesme (contract) omurgasi, durum yonetimi, belge baglama.
- PR-17: Gelecek donem gelir/gider dagitim motoru (DEFREV, PREPAID, ACCRUAL).
- PR-18: Contracts + Revenue ekranlarinin operasyonel UI akisi ve yetkiye gore fetch-gating.
- PR-19: Cari kart bazli AR/AP kontrol hesap esleme ve postingte cozum sirasi.
- PR-21: Contracttan otomatik cari belge uretimi (invoice/advance/adjustment) + otomatik link.
- PR-22: Contract satirlarindan otomatik RevRec schedule uretimi.
- PR-23: RevRec postingte satir bazli hesap cozumleme (deferred/revenue account override).
- PR-24: Contract detayinda finansal rollup KPI'lari (billed/collected/deferred/recognized/open).

Kisa is etkisi:
- Sozlesme bazli tahakkuk/erteleme surecleri izlenebilir olur.
- Subledger -> GL baglantisi denetlenebilir olur.
- Yetki yoksa sistem gereksiz fetch yapmaz, kontrolsuz erisim azalir.
- Musteri/tedarikciye ozel kontrol hesaplari ile daha dogru muhasebe dagitimi yapilir.
- Contract ekranindan belgeleme + revrec uretimi tek akista yonetilir.
- Finans, "faturaladim mi/tahsil ettim mi/tanidim mi?" sorusunu tek ekranda KPI ile gorur.

---

## 2. Yetki matrisi (kim ne yapar?)

Route seviyesinde:
- `contract.read`: Contracts ekranina giris/listeleme
- Revenue route acilisi: `revenue.schedule.read` veya `revenue.run.read` veya `revenue.report.read`

Action seviyesinde:
- Contracts:
  - `contract.upsert`
  - `contract.activate`
  - `contract.suspend`
  - `contract.close`
  - `contract.cancel`
  - `contract.link_document` (link + adjust/unlink + `generate-billing`)
- Revenue:
  - `revenue.schedule.generate` (`generate-revrec`)
  - `revenue.run.create`
  - `revenue.run.post`
  - `revenue.run.reverse`
- Picker bagimli read izinleri:
  - Counterparty picker: `cari.card.read`
  - Hesap picker: `gl.account.read`
  - Belge picker (contract-scoped): `contract.link_document`

Not:
- Izin yoksa ilgili buton ya pasif olur ya da section gizlenir.
- PR-18 kurali geregi section gizliyse arka planda unauthorized fetch yapilmaz.

---

## 3. PR-16 Contracts Kilavuzu

## 3.1 Contract durumlari ve gecisleri

Durumlar:
- `DRAFT`
- `ACTIVE`
- `SUSPENDED`
- `CLOSED`
- `CANCELLED`

Izinli gecisler:
- `DRAFT -> ACTIVE` (Activate)
- `SUSPENDED -> ACTIVE` (Activate)
- `ACTIVE -> SUSPENDED` (Suspend)
- `ACTIVE -> CLOSED` (Close)
- `SUSPENDED -> CLOSED` (Close)
- `DRAFT -> CANCELLED` (Cancel)

Ne olur?
- Uygun olmayan durumda buton pasif olur.
- API tarafinda da "Cannot <action> contract from status <x>" hatasi ile bloklanir.

Gercek hayat:
- Satis sozlesmesi once `DRAFT` acilir, hukuki onaydan sonra `ACTIVE` yapilir.
- Muvakkat durdurma varsa `SUSPENDED`, tamamen biterse `CLOSED`.

## 3.2 Contract olusturma/guncelleme alanlari

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `legalEntityId` | Sozlesmenin bagli oldugu sirket birimi | Kayit bloklanir (`required`) |
| `counterpartyId` | Musteri/tedarikci baglantisi | Kayit bloklanir (`required`) |
| `contractNo` | Operasyonel tekil sozlesme no | Kayit bloklanir; ayni legal entity icinde duplicate olamaz |
| `contractType` (`CUSTOMER`,`VENDOR`) | Akis tipi (AR/AP yonu) | Kayit bloklanir (`required`) |
| `currencyCode` | Sozlesme para birimi | Kayit bloklanir; sistemde tanimli doviz olmalidir |
| `startDate` | Baslangic tarihi | Kayit bloklanir (`required`) |
| `endDate` | Bitis tarihi | Bos olabilir; doluysa `startDate <= endDate` olmalidir |
| `notes` | Operasyon notu | Bos olabilir |
| `lines[]` | Sozlesme satirlari | Dizi zorunlu; satir icerigi kurala aykiriysa kayit bloklanir |

Satir alanlari:

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `description` | Satir aciklamasi | Kayit bloklanir |
| `lineAmountTxn` / `lineAmountBase` | Tutar | `0` olamaz; eksi tutar (credit/adjustment) kabul edilir |
| `recognitionMethod` (`STRAIGHT_LINE`,`MILESTONE`,`MANUAL`) | Dagitim metodu | Bossa `STRAIGHT_LINE` kabul edilir |
| `recognitionStartDate` / `recognitionEndDate` | Donemleme araligi | `STRAIGHT_LINE`: ikisi de zorunlu; `MILESTONE`: ikisi de zorunlu ve ayni tarih; `MANUAL`: ikisi de bos olmali |
| `deferredAccountId` | Erteleme hesabi | Opsiyonel; girilirse tip/scope/aktif/postable kontrolu yapilir |
| `revenueAccountId` | Gelir/gider hesabi | Opsiyonel; girilirse tip/scope/aktif/postable kontrolu yapilir |
| `status` (`ACTIVE`,`INACTIVE`) | Satir aktifligi | Bossa `ACTIVE` kabul edilir |

Onemli davranis:
- `lineNo` kullanici tarafinda belirleyici degildir; backend 1..N sirali atar.
- `PUT /contracts/{id}` satir setini "tam degistirir" (partial patch degil).
- `DRAFT` disindaki sozlesme guncellenemez.

## 3.3 `contractType` secenegi gercekte neyi degistirir?

`CUSTOMER`:
- Linklenecek cari belge yonu `AR` olmak zorunda.
- `deferredAccountId` beklenen tip: `LIABILITY`
- `revenueAccountId` beklenen tip: `REVENUE`

`VENDOR`:
- Linklenecek cari belge yonu `AP` olmak zorunda.
- `deferredAccountId` beklenen tip: `ASSET`
- `revenueAccountId` beklenen tip: `EXPENSE`

Yanlis secim etkisi:
- Kayit veya link islemi backend tarafinda bloklanir.

## 3.4 Belge baglama (link-document) secenekleri

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `cariDocumentId` | Hangi cari belge baglanacak | Kayit bloklanir |
| `linkType` (`BILLING`,`ADVANCE`,`ADJUSTMENT`) | Baglama amaci | UI default `BILLING`; API'de invalid deger blok |
| `linkedAmountTxn` / `linkedAmountBase` | Baglanan tutar | `>0` zorunlu, bos/0/eksi blok |
| `linkFxRate` (opsiyonel) | Cross-currency baglamada link-level FX snapshot override | Bos birakirsan belge `fx_rate` (yoksa `linkedAmountBase/linkedAmountTxn`, ayni currency ise `1`) kullanilir |

Ek kontroller:
- Contract status sadece `DRAFT` veya `ACTIVE` ise linklenebilir.
- Belge status sadece `POSTED`, `PARTIALLY_SETTLED`, `SETTLED` ise linklenebilir.
- Sozlesme ve belge currency ayni olmak zorunda degildir (cross-currency desteklenir).
- Link satirinda `contractCurrencyCodeSnapshot`, `documentCurrencyCodeSnapshot`, `linkFxRateSnapshot` saklanir.
- Ayni tuple (`contract_id`,`cari_document_id`,`link_type`) tekrar insert edilemez.
- Kumulatif linked tutar belge tutar cap'ini gecemez.

Gercek hayat ornegi:
- Yillik yazilim aboneligi sozlesmesine kesilmis faturayi `BILLING` olarak baglarsiniz.
- Pesin avans senaryosunda ayni belgeyi `ADVANCE` tipiyle ayri izlersiniz.

---

## 4. PR-17 Periodization Kilavuzu (17A/17B/17C/17D)

## 4.1 Account family secenekleri

| Account Family | Is anlami | Tipik hesap ailesi |
|---|---|---|
| `DEFREV` | Gelecek ay/yil gelirleri (ertelenmis gelir) | 380/480 + gelir hesabi |
| `PREPAID_EXPENSE` | Gelecek ay/yil giderleri (pesin odeme) | 180/280 + gider hesabi |
| `ACCRUED_REVENUE` | Gelir tahakkuku | 181/281 + gelir hesabi |
| `ACCRUED_EXPENSE` | Gider tahakkuku | 381/481 + gider hesabi |

## 4.2 Schedule Generate ekrani

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `legalEntityId` | Hangi entity icin schedule | Kayit bloklanir |
| `fiscalPeriodId` | Donem baglami | Kayit bloklanir |
| `accountFamily` | Is akisi ailesi | Kayit bloklanir |
| `maturityBucket` (`SHORT_TERM`,`LONG_TERM`) | Kisa/uzun vade sinifi | Kayit bloklanir |
| `maturityDate` | Vade tarihi | Kayit bloklanir |
| `reclassRequired` | Uzundan kisaya reclass yapilsin mi | UI default `true`; false ise reclass satiri uretilmez |
| `currencyCode` | Islem para birimi | Kayit bloklanir (3 harf) |
| `fxRate` | Kur | Bos olabilir; girilirse `>0` olmali |
| `amountTxn` / `amountBase` | Tutar | Bos olamaz; sayisal olmak zorunda |
| `sourceEventUid` | Kaynak event kimligi | Bossa backend deterministik uid uretir |

Not:
- Teknik olarak amount alanlari 0 kabul edebilir; operasyonel olarak 0 schedule anlamsizdir.

## 4.3 Run Create ekrani

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `legalEntityId`, `fiscalPeriodId`, `accountFamily`, `maturityBucket`, `maturityDate`, `currencyCode`, `totalAmountTxn`, `totalAmountBase` | Run olusturma cekirdegi | Bos/invalid ise kayit bloklanir |
| `scheduleId` | Run'i bir schedule'a baglamak | Bos olursa bagimsiz run acilir |
| `runNo` | Operator run numarasi | Bos olursa backend otomatik `RRUN-*` run no uretir |
| `sourceRunUid` | Kaynak run kimligi | Bos olursa backend deterministik uid uretir |
| `fxRate` | Kur | Bos olabilir; girilirse `>0` olmali |
| `reclassRequired` | Reclass uretilsin mi | Default true; false ise reclass entry olusmaz |

## 4.4 Run/Post/Reverse aksiyonlari

Post:
- Sadece `DRAFT` veya `READY` run post edilebilir.
- `settlementPeriodId` opsiyoneldir.
- Vermezseniz run'in kendi period bilgisi/fallback period kullanilir.

Reverse:
- Sadece `POSTED` run reverse edilebilir.
- `reversalPeriodId` opsiyoneldir.
- `reason` bos ise varsayilan "Manual reversal" kullanilir.

Accrual aksiyonlari:
- `accruals/generate` sadece `ACCRUED_REVENUE` veya `ACCRUED_EXPENSE` kabul eder.
- `accruals/:id/settle` ve `accruals/:id/reverse` run permission setini kullanir.

## 4.5 `maturityBucket` + `reclassRequired` kombinasyonu

- `LONG_TERM + reclassRequired=true`:
  - Reclass satiri da uretilir.
  - DEFREV icin 480->380 gorunurlugu.
  - PREPAID icin 280->180 gorunurlugu.
  - ACCR_REV icin 281->181 gorunurlugu.
  - ACCR_EXP icin 481->381 gorunurlugu.

- `LONG_TERM + reclassRequired=false`:
  - Sadece recognition/accrual entrysi olusur, reclass olusmaz.

- `SHORT_TERM`:
  - Reclass flag true olsa bile fiilen reclass ihtiyaci yoktur.

## 4.6 Setup eksikse ne olur?

Posting onkosulu:
- Ilgili purpose kodlari `journal_purpose_accounts` icinde map edilmis olmali.
- Hesaplar `LEGAL_ENTITY` scope, aktif ve postable olmali.

Eksik setup etkisi:
- Post/settle/reverse adimi "Setup required: configure journal_purpose_accounts ..." hatasi ile durur.

Donem kilidi etkisi:
- Donem `OPEN` degilse post/reverse bloklanir.

Gercek hayat ornekleri:
- DEFREV: 12 aylik lisans gelirini aylik tanima.
- PREPAID: yillik sigorta giderini aylik amortize etme.
- ACCRUED_REVENUE: hizmet verildi ama fatura henuz kesilmedi.
- ACCRUED_EXPENSE: hizmet alindi ama fatura henuz gelmedi.

---

## 5. PR-18 UI davranislari (secmezsen / yetki yoksa)

Contracts sayfasi:
- `contract.read` yoksa sayfa verisi acilmaz.
- Counterparty picker icin `cari.card.read` yoksa:
  - Picker fetch yapilmaz.
  - Operatorden manual ID girmesi beklenir.
- Hesap picker icin `gl.account.read` yoksa:
  - Picker fetch yapilmaz.
  - Manual `deferredAccountId/revenueAccountId` girilebilir.
- Belge picker icin `contract.link_document` yoksa:
  - Picker fetch yapilmaz.
  - Manual `cariDocumentId` girilebilir.

Revenue sayfasi:
- `revenue.schedule.read` yoksa Schedules bolumu gizlenir.
- `revenue.run.read` yoksa Runs bolumu gizlenir.
- `revenue.report.read` yoksa Reports bolumu gizlenir.
- Gizli bolumler icin fetch call yapilmaz (403 noise azalir).

Action butonlari:
- Yetki yoksa disabled/hata mesaji.
- Status uygun degilse Post/Reverse gibi aksiyonlar engellenir.

---

## 6. PR-19 Counterparty AR/AP Mapping Kilavuzu

## 6.1 AR/AP mapping ne icin var?

Amac:
- Genel control hesap yerine, belirli cari kartlar icin ozel control hesap kullanmak.
- Ornek: "Stratejik Musteri A" tum AR hareketleri ozel alt hesapta izlensin.

Alanlar:
- `arAccountId` (customer icin)
- `apAccountId` (vendor icin)

## 6.2 Kurallar

- `arAccountId` ancak `isCustomer=true` iken atanabilir.
- `apAccountId` ancak `isVendor=true` iken atanabilir.
- Hesap:
  - Ayni tenant ve legal entity kapsaminda olmali.
  - CoA scope `LEGAL_ENTITY` olmali.
  - Aktif ve postable olmali.
  - Tip uyumu:
    - AR mapping icin `ASSET`
    - AP mapping icin `LIABILITY`

## 6.3 "Secmezsem ne olur?" (kritik semantik)

Create:
- `arAccountId/apAccountId` vermezseniz `null` kaydedilir.

Update:
- Alan hic gonderilmezse:
  - Mevcut deger korunur.
- Alan acikca `null` gonderilirse:
  - Mevcut mapping temizlenir.

Bu fark operasyonel olarak cok onemlidir:
- "Dokunma" ile "temizle" ayni sey degildir.

## 6.4 Postingte hesap cozum sirasi

Cari document ve settlement postingte:
1. Counterparty mapping varsa ve gecerliyse onu kullanir.
2. Mapping yoksa `journal_purpose_accounts` fallback kullanir.
3. Mapping var ama posting aninda gecersiz hale geldiyse (pasif, postable degil, yanlis tip/scope):
   - Posting acik hata ile bloklanir.

Ek not:
- Counterparty yoksa settlement tarafinda override lookup yapilmaz, purpose-account fallback kullanilir.

Gercek hayat ornekleri:
- Musteri bazli risk takibi:
  - Buyuk musteriler icin ayri AR control hesaplari kullanip bakiye analizi yaparsiniz.
- Tedarikci bazli raporlama:
  - Kritik tedarikcilere ait AP borclarini ayri control hesapta izlersiniz.

---

## 7. Sik hata senaryolari ve cozum

| Hata/Semptom | Anlami | Operasyon aksiyonu |
|---|---|---|
| `Only DRAFT contracts can be updated` | Active/closed contract edit edilmeye calisiliyor | Once lifecycle stratejisini netlestir; gerekli ise yeni contract/revision akisi kullan |
| `Direction mismatch` | Contract type ile belge yonu uyumsuz | CUSTOMER->AR, VENDOR->AP kuraliyla belgeyi kontrol et |
| `Currency mismatch` | Contract ve belge para birimi farkli | Ayni para birimli belge sec veya sozlesme setup'ini duzelt |
| `Setup required: configure journal_purpose_accounts ...` | Posting mapping eksik | Ilgili purpose kodlarina hesap bagla |
| `Period is CLOSED...` | Kapali doneme post/reverse denendi | Uygun acik donem sec veya donem yonetimi ile ilerle |
| `arAccountId requires isCustomer=true` | Rol-map uyumsuz | Cari kart rolunu duzelt veya alanı temizle |
| `...must reference an ACTIVE/postable account` | Mapping hesabinin durumu gecersiz | Hesabi aktif/postable yap veya yeni hesap sec |

---

## 8. Gercek hayat uygulama akislari

## Akis A - SaaS abonelik geliri (DEFREV)
1. Contract `CUSTOMER` olarak acilir.
2. Fatura belgeye donusur ve contract'a `BILLING` ile linklenir.
3. Revenue schedule/run olusturulur (`DEFREV`, genelde `LONG_TERM`, reclass true).
4. Ay sonu run post edilir, raporlarda short/long dagilim izlenir.

## Akis B - Pesin gider (PREPAID_EXPENSE)
1. Vendor contract/odeme baglami acilir.
2. Run `PREPAID_EXPENSE` ile olusturulur.
3. Donemsel amortizasyon post edilir.
4. 280->180 reclass gorunurlugu rapordan takip edilir.

## Akis C - Tahakkuk (ACCRUED_REVENUE / ACCRUED_EXPENSE)
1. Fatura zamanlamasi ile hizmet/teslim zamani farkliysa accrual run olusturulur.
2. `accrual settle` ile olgunlasan kisim kapatilir.
3. Gerekirse `accrual reverse` ile ters cevrilir.

## Akis D - Counterparty ozel control hesap yonetimi
1. Stratejik cari kartta `arAccountId` veya `apAccountId` atanir.
2. Postinglerde once bu mapping kullanilir.
3. Mapping temizlenirse sistem fallback purpose hesabina doner.

---

## 9. Canliya cikis kontrol listesi (PR-16..24)

1. Contracts lifecycle aksiyonlari rol bazinda test edildi.
2. Link-document senaryolarinda direction/currency/status kontrolleri test edildi.
3. Revenue icin required purpose kodlari map edildi.
4. Post/reverse donem acik/kapali testleri yapildi.
5. Revenue report panellerinde split + reconciliation degerleri kontrol edildi.
6. PR-18 fetch-gating davranisi (izin yoksa fetch yok) smoke testten gecti.
7. Counterparty AR/AP mapping:
   - omit ve explicit null semantigi dogrulandi.
   - posting aninda hesap gecerlilik kontrolleri dogrulandi.
8. `generate-billing`:
   - `FULL` / `PARTIAL` / `MILESTONE` stratejileri test edildi.
   - Ayni `idempotencyKey` ile replay davranisi dogrulandi.
9. `generate-revrec`:
   - `BY_CONTRACT_LINE` ve `BY_LINKED_DOCUMENT` modlari test edildi.
   - `regenerateMissingOnly=true` ile duplicate olusmadigi dogrulandi.
10. RevRec posting account precedence:
    - satir hesabi doluysa satir hesabi,
    - satir hesabi bossa purpose mapping fallback.
11. Contract detail `financialRollup` KPI'lari:
    - billed/collected/deferred/recognized/open degerleri
    - bagli cari/revrec kayitlariyla mutabik kontrol edildi.

Bu kontrol listesi yesil olmadan uretim kullanimi onerilmez.

---

## 10. PR-21 Contract -> Cari Auto-Billing Kilavuzu

Endpoint:
- `POST /api/v1/contracts/:contractId/generate-billing`

UI:
- `/app/contracts` icindeki **Generate Billing** paneli

### 10.1 Alanlar ve secim etkileri

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `docType` (`INVOICE`,`ADVANCE`,`ADJUSTMENT`) | Uretilecek cari belge tipi | Invalid deger bloklanir |
| `amountStrategy` (`FULL`,`PARTIAL`,`MILESTONE`) | Tutarin nasil hesaplanacagi | Bos ise `FULL` kabul edilir |
| `billingDate` | Belge tarihi / donemleme capasi | Bossa islem bloklanir |
| `dueDate` | Vade tarihi | Bos olabilir; doluysa `dueDate >= billingDate` olmali |
| `selectedLineIds[]` | Hangi contract satirlari faturalanacak | Bos birakirsaniz tum `ACTIVE` satirlar kullanilir |
| `amountTxn` + `amountBase` | `PARTIAL/MILESTONE` icin hedef tutar | Ikisi birlikte verilmelidir; `FULL`te ikisi de bos olmali |
| `idempotencyKey` | Tekrarda duplicate belgeyi engeller | Zorunlu; bos olursa islem gitmez |
| `integrationEventUid` | Cross-module takip kimligi | Bos olabilir |
| `note` / `referenceNo` | Operasyon aciklamasi | Bos olabilir |

Sonuc:
- `billingBatch`
- uretilen `document`
- otomatik olusan `link`
- `idempotentReplay` bayragi

### 10.2 Idempotency davranisi

- Ayni `idempotencyKey` + ayni payload:
  - Yeni batch olusmaz, onceki sonuc replay edilir.
- Ayni `idempotencyKey` + farkli kritik alan:
  - Islem bloklanir (fingerprint uyusmazligi).

### 10.3 Gercek hayat ornekleri

Ornek A - Full invoice:
1. `docType=INVOICE`, `amountStrategy=FULL`
2. `selectedLineIds` bos birakilir
3. Tum aktif satirlar icin tek belge + otomatik link olusur

Ornek B - Milestone billing:
1. `docType=INVOICE`, `amountStrategy=MILESTONE`
2. Milestone satirlari secilir
3. `amountTxn/amountBase` girilir, sadece secili satirlar icin belge uretilir

Ornek C - Duplicate click:
1. Ayni form ikinci kez ayni `idempotencyKey` ile gonderilir
2. `idempotentReplay=true` doner, ikinci belge acilmaz

---

## 11. PR-22 Contract -> RevRec Auto Schedule Kilavuzu

Endpoint:
- `POST /api/v1/contracts/:contractId/generate-revrec`

UI:
- `/app/contracts` icindeki **Generate RevRec Schedule** paneli

### 11.1 Alanlar ve secim etkileri

| Alan | Ne icin kullanilir | Secmezsen ne olur |
|---|---|---|
| `fiscalPeriodId` | Uretim donemi baglami | Bossa islem bloklanir |
| `generationMode` (`BY_CONTRACT_LINE`,`BY_LINKED_DOCUMENT`) | Uretim kaynagi | Bossa `BY_CONTRACT_LINE` |
| `contractLineIds[]` | Uretime dahil satirlar | Bos birakirsaniz tum `ACTIVE` satirlar kullanilir |
| `sourceCariDocumentId` | Belge bazli uretimde kaynak belge | `BY_LINKED_DOCUMENT` modunda zorunlu |
| `regenerateMissingOnly` | Sadece eksik schedule satiri uret | Default `true` (onerilen) |

Sonuc:
- `generatedScheduleCount`
- `generatedLineCount`
- `skippedLineCount`
- `rows[]` (uretilen schedule ID listesi)
- `idempotentReplay`

### 11.2 Operasyon notlari

- `MANUAL` recognition method satirlari auto-generation disinda kalabilir.
- `regenerateMissingOnly=true` tekrarda duplicate satir riskini azaltir.
- `BY_LINKED_DOCUMENT` modu, contract-cari baglantisinin dogru kurulmus olmasina baglidir.

---

## 12. PR-23 RevRec Postingte Hesap Cozum Sirasi

Sistem postingte hesaplari su sirayla cozer:

1. `contract_line.deferred_account_id` / `contract_line.revenue_account_id`
2. Fallback: `journal_purpose_accounts` purpose mapping

Ne olur?
- Satir hesabi dolu ve gecerliyse (aktif, postable, legalEntity scope) o kullanilir.
- Satir hesabi bossa fallback purpose hesabi kullanilir.
- Satir hesabi dolu ama gecersizse posting acik hata ile bloklanir.
- Ikisi de yoksa setup hatasi ile bloklanir.

Tipik hata sinyalleri:
- `contractLineId=... deferred_account_id=... is not an active posting account ...`
- `Setup required: configure journal_purpose_accounts ...`

Bu davranis, line-level muhasebe niyetini gercek postinge tasir.

---

## 13. PR-24 Contract Financial Rollup KPI Kilavuzu

Contract detay API'si artik `financialRollup` alani dondurur.
UI'da KPI kartlari ve progress bar'lar buradan beslenir.

### 13.1 KPI alanlari (is anlami)

| KPI | Kaynak mantik | Yorum |
|---|---|---|
| `billedAmount*` | Contracta bagli cari belge linkleri | Ne kadar faturalandi |
| `collectedAmount*` | Bagli belgelerde tahsil/odeme etkisi | Ne kadar tahsil/odendi |
| `uncollectedAmount*` | `billed - collected` | Kalan tahsilat/odeme |
| `revrecScheduledAmount*` | Schedule line toplami | Planlanan tanima |
| `recognizedToDate*` | `POSTED` run line toplami | Gerceklesen tanima |
| `deferredBalance*` | `scheduled - recognized` | Henuz taninmayan bakiye |
| `openReceivable*` / `openPayable*` | Contract type'a gore acik kalan | CUSTOMER'da AR, VENDOR'da AP bakisi |
| `collectedCoveragePct` | `collected / billed` | Tahsilat/odeme kapsama yuzdesi |
| `recognizedCoveragePct` | `recognized / scheduled` | Tanima ilerleme yuzdesi |

### 13.2 Null/partial durum okuma

- Hic link yoksa KPI'lar sifir-donusludur (null degil, operasyonel okunabilir).
- Faturalama var ama tahsilat yoksa: `billed > 0`, `collected = 0`.
- Faturalama var, RevRec henuz yoksa: `revrecScheduled = 0`, `recognized = 0`.
- Kismi tanimada: progress bar'lar 0-100 arasi ilerleme gosterir.

### 13.3 Gercek hayat mutabakat ornegi

1. Contractta 1.000.000 TRY billing olustu.
2. 400.000 TRY tahsil edildi.
3. RevRec schedule 1.000.000, posted recognition 250.000.
4. Beklenen KPI:
   - billed: 1.000.000
   - collected: 400.000
   - uncollected/open receivable: 600.000
   - recognized: 250.000
   - deferred: 750.000

---

## 14. Ekran Bazli Kart/Buton Rehberi (Contracts + Revenue)

Bu bolumde iki ana ekranin (Contracts ve Gelecek Yillar Gelirleri) kullanici aksiyonlari kart/buton bazinda ozetlenir.

### 14.1 `/app/contracts` (ContractsPage)

Kartlar:
- Ust filtre karti
- Contract liste tablosu
- Contract form karti (`Create Draft` / `Edit Draft` / `Amend Active/Suspended`)
- `Lifecycle` karti
- `Financial Rollups` KPI karti
- `Generate Billing` karti
- `Generate RevRec Schedule` karti
- `Link Document` karti
  - Link listesi
  - `Adjust Link`
  - `Unlink Link`

Ana butonlar:
- Liste: `Refresh`, `New`, `Edit Selected`, satirda `Select`
- Form: `Create Draft` / `Update Draft` / `Apply Amendment`, `Reset`
- Satir bazinda: `Patch Line`, `Add line`, `Remove`
- Lifecycle: `Activate`, `Suspend`, `Close`, `Cancel`
- Billing: `Generate Billing`, `Select Active`, `Clear`
- RevRec: `Generate RevRec`, `Select Active`, `Clear`
- Link: `Link`, satirda `Adjust`, `Unlink`, formlarda `Apply Adjust`, `Apply Unlink`

Kritik davranis:
- `Generate Billing` sonucu: batch + document + link + replay bilgisi.
- `Generate RevRec` sonucu: generated/skipped line sayilari + replay bilgisi.
- `Financial Rollups` karti contract secimi olmadan bos gorunur; secim yapinca KPI/progress dolar.
- Link adjust/unlink butonlari satir durumuna gore acilir/kapanir (state guard).

Gercek hayat ornegi A (sozlesmeden fatura ve link):
1. Contract satirlari secilir.
2. `Generate Billing` calistirilir.
3. Sonuc kartinda `Batch`, `Document`, `LinkId` dogrulanir.
4. `Financial Rollups`ta billed artisi gorulur.

Gercek hayat ornegi B (kismi tanima takibi):
1. Ayni contractta `Generate RevRec` calistirilir.
2. Sonraki run postinglerinden sonra tekrar detay acilir.
3. `Recognition Progress` barinda artis teyit edilir.

### 14.2 `/app/gelecek-yillar-gelirleri` (FutureYearRevenuePage)

Kartlar:
- `Schedules` karti
  - query filtreleri + liste + `Generate Schedule` formu
- `Runs` karti
  - query filtreleri + run listesi + `Create Run` formu + selected run action paneli
- `Reports and Split Panels` karti
  - report filtreleri
  - KPI kutulari (deferred/accrual/prepaid/reclass)
  - reconciliation summary
  - rollforward + split tablolari

Ana butonlar:
- `Schedules`: `Refresh`, `Generate Schedule`
- `Runs`: `Refresh`, `Create Run`, satirda `Select`, aksiyonda `Post` / `Reverse`
- `Reports`: `Load Reports`

Kritik davranis:
- Okuma izinleri yoksa ilgili section "hidden/missing permission" notuyla kapanir.
- `Post` sadece secili run `DRAFT/READY` iken acik olur.
- `Reverse` sadece secili run `POSTED` iken acik olur.
- Rapor panelindeki reconciliation tablosu subledger-vs-GL farkini hizli gosterir.

Gercek hayat ornegi:
1. Ay kapanisinda `Load Reports` ile split panelleri acilir.
2. `Reclass Visibility` ve reconciliation summary kontrol edilir.
3. Fark varsa ilgili run/schedule secilip post veya reverse aksiyonu planlanir.

