# KULLANIM_KILAVUZU_KASA_MODULU.md

## SAAP Kasa Modulu Kullanım Kilavuzu (Teknik Olmayan Kullanicilar Icin)

Surum: v1  
Tarih: 2026-02-22  
Kapsam: `/app/kasa-tanimlari`, `/app/kasa-oturumlari`, `/app/tediye-islemleri`, `/app/tahsilat-islemleri`, `/app/kasa-islemleri`, `/app/kasa-istisnalari`

Bu kilavuz, kod bilmeyen operasyon, muhasebe, finans ve denetim ekipleri icin yazildi.  
Amac: "Hangi ekranda ne yapmaliyim, secersem/secmesem ne olur, hatada ne yapmaliyim" sorularina net cevap vermek.

---

## 1. Bu modul neyi cozer?

Kasa modulu, nakit islemlerini kontrol altina alir.  
Yani:
- Kasayi kim acmis/kapatmis, ne kadar para girmis/cikmis gorulur.
  (Buradaki "acma/kapama", fiziksel kasanin kilidini acma degil; sistemde oturum baslatma/bitirme kaydidir.)
- Islemler belli adimlardan gecer (olustur, post et, gerekirse ters kayit).
- Yetkisiz veya kurala aykiri hareketler engellenir.
- Istisnalar (zorunlu kapama, yuksek fark, override vb.) tek ekranda izlenir.

Kisa mantik:
- **GL (Defteri Kebir / Genel Ledger)** kayit dogru kaynak olarak kalir.
- **Kasa operasyonu**, GL (Defteri Kebir)'e gitmeden once kontrollu bir is akisindan gecer.

---

## 2. Temel kavramlar (teknik olmayan dille)

- **Kasa Register**: Fiziksel/operasyonel para noktasi. (Ornek: Magaza kasa cekmecesi, sube kasasi, merkez kasa)
- **Oturum (Session)**: Kasa acilis-kapanis periyodu. (Ornek: 08:00 acildi, 18:00 kapandi)
- **Islem (Transaction)**: Tek bir para hareketi. (Tahsilat, odeme, bankaya yatirma vb.)
- **Post etmek**: Islemi resmi muhasebe kaydina cevirme.
- **Iptal (Cancel)**: Henuz post edilmemis islemi gecersiz kilma.
- **Ters kayit (Reverse)**: Post edilmis islemi geri alan yeni ve bagli bir kayit uretme.
- **Varyans (Fark)**: Kasada beklenen para ile sayilan para arasindaki fark.
- **Override**: Normalde engellenen bir post islemini, ozel yetki + zorunlu gerekce ile yapma.
- **Istisna**: Riskli/inceleme gerektiren olay. (Yuksek fark, forced close, unposted islem vb.)

### 2.1 "Kasayi acmak/kapatmak" tam olarak ne demek?

- **Kasayi acmak (oturum acmak)**:
  - Sistem kaydi olarak "bu register su anda su kisi sorumlulugunda kullanima basladi" demektir.
  - Genelde fiziksel teslim/tesellum (kasa devri) sonrasi yapilir.
- **Kasayi kapatmak (oturum kapatmak)**:
  - Sistem kaydi olarak "bu register icin operasyon bitti, sayim yapildi, fark hesaplandi" demektir.
  - Fiziksel kilit kapatma operasyonu sirket prosedurudur; sistem bunun muhasebe/denetim kaydini tutar.

Ozet:
- Bu moduldaki acma/kapama, **fiziksel anahtar hareketinden cok operasyonel sorumluluk kaydi**dir.

---

## 3. Menude nereye girilir?

Sol menu > **Yevmiye Kayitlari**:
- **Kasa Tanimlari**: Register acma/guncelleme
- **Kasa Oturumlari**: Oturum acma/kapama
- **Tahsilat**: Sadece RECEIPT tipi islemler
- **Tediye**: Sadece PAYOUT tipi islemler
- **Kasa Islemleri**: Tum islem tipleri
- **Kasa Transit Transferleri**: Farkli OU registerlar arasi transferin baslat/receive/iptal takibi
- **Kasa Istisnalari**: Risk/denetim paneli

Not:
- Tahsilat ve Tediye ekranlari, Kasa Islemleri ekraninin hazir filtreli versiyonudur.

---

## 4. Yetki modeli (kim ne yapabilir?)

Bu yetkiler yoksa butonlar gorunmez veya ekran erisimi engellenir.

Temel yetkiler:
- `cash.register.read`: Kasa tanimlarini gorur
- `cash.register.upsert`: Kasa tanimi olusturur/gunceller, aktif-pasif yapar
- `cash.session.open`: Oturum acar
- `cash.session.close`: Oturum kapatir
- `cash.variance.approve`: Esik ustu varyansi onaylayarak kapatir
- `cash.txn.read`: Islem listelerini gorur
- `cash.txn.create`: Yeni islem olusturur
- `cash.txn.post`: Islem post eder
- `cash.txn.cancel`: Post edilmemis islemi iptal eder
- `cash.txn.reverse`: Post edilmis islemi ters kayit eder
- `cash.override.post`: Override ile post yapar
- `cash.report.read`: Istisna ekranini gorur

Gercek hayat ornek:
- Kasiyer: create + session open/close
- Supervisor: session close + variance approve + istisna izleme
- Finans: post + reverse
- Finans admin: register setup + override

---

## 5. Kasa kontrol modu banner'i (OFF / WARN / ENFORCE)

Ekran ustunde "Kasa kontrol modu" gorursunuz.

- **OFF**:
  - Direkt GL (Defteri Kebir) kasa kontrolu kapali.
  - Risk daha yuksek; pilot/discovery asamasi icin.
- **WARN**:
  - Direkt GL (Defteri Kebir) kaydi durmaz, ama uyari/denetim izi olusur.
  - Gecis donemi icin ideal.
- **ENFORCE**:
  - Kural disi direkt GL (Defteri Kebir) girisi engellenir.
  - Uretim ve denetim icin onerilen mod.
  - Pratikte:
    - `is_cash_controlled=true` hesaplara, normal manuel GL fisinden dogrudan satir yazmak bloke olur.
    - Ayni hareket, kasa modulu akisiyla (`source_type=CASH`) yapiliyorsa izinli olur.
    - Acil istisna durumunda sadece yetkili kullanici `override` + zorunlu gerekce ile ilerleyebilir.
  - Sonuc:
    - Yanlis kanaldan kasa hesabi oynanmasi zorlasir.
    - Denetimde "neden bu hesapta bu hareket var" sorusunun cevabi netlesir.

Secim etkisi:
- ENFORCE secmezseniz operasyon rahatlar ama kontrol riski artar.
- ENFORCE secerseniz disiplin artar, kisa vadede kullanici hata mesaji daha cok gorebilir.

---

## 6. Isletmeye almadan once kontrol listesi

1. Kasa registerlari tanimli mi?
2. Her register uygun GL (Defteri Kebir) hesaba bagli mi?
3. Register para birimi dogru mu?
4. Session mode dogru mu? (`REQUIRED/OPTIONAL/NONE`)
5. Varyans kazanc/kayip hesaplari tanimli mi?
6. Ekipte yetki dagilimi net mi?
7. Kasa kontrol modu beklendigi gibi mi? (WARN ya da ENFORCE)

---

## 7. Kasa Tanimlari ekrani (adim adim)

Ekran: `/app/kasa-tanimlari`

### 7.1 Ne yaparsiniz?
- Yeni kasa tanimi acarsiniz
- Var olani guncellersiniz
- Aktif/Pasif durumunu degistirirsiniz

### 7.2 Alanlar ve secim etkileri

- **code / name**
  - Bos birakilamaz.
  - Gercek hayat: `TILL-01`, `Sube Cekmece-1`

- **registerType** (`VAULT`, `DRAWER`, `TILL`)
  - Operasyon tipi secimidir.
  - Oneri:
    - TILL: POS kasasi
    - DRAWER: sube petty cash
    - VAULT: merkez kasa

- **sessionMode** (`REQUIRED`, `OPTIONAL`, `NONE`)
  - `REQUIRED`: Oturum olmadan islem akisi bloke olabilir (ozellikle create/post)
  - `OPTIONAL`: Oturum var/yok esnek
  - `NONE`: Oturum acma kapali
  - Detayli isletme yorumu:
    - `REQUIRED`: Kisiden kisiye devir ve vardiya disiplini isteyen noktalarda
    - `OPTIONAL`: Bazi gunler vardiya acilip bazi gunler acilmayan ara yapilarda
    - `NONE`: Session takip ihtiyaci olmayan, daha cok merkez kasa/ozel nokta akislari

- **legalEntity / operatingUnit / account**
  - Kasa muhasebe baglamini belirler.
  - Yanlis baglanti olursa kayit reddedilir.

- **currencyCode**
  - 3 harfli olmalidir (USD, TRY vb.)
  - Islem para birimi register para birimiyle ayni olmalidir.

- **allowNegative**
  - `Hayir` (onerilen): Eksi kasaya izin verilmez
  - `Evet`: Eksi bakiye operasyonel olarak mumkun olur ama risk artar
  - Gercek hayat etkisi:
    - "Evet" oldugunda kasa bakiyesi 0 iken de odeme fisleri gecici olarak gecebilir.
    - Bu, "kasada olmayan parayi sistemde cikis gostermek" riskini dogurur.

- **maxTxnAmount**
  - Tek islem ust limitidir
  - Asilinca backend islemi reddeder

- **requiresApprovalOverAmount**
  - Bu deger ustu hareketlerde ek onay disiplini uygulanir
  - `maxTxnAmount`'i gecemez

- **varianceGainAccountId / varianceLossAccountId**
  - Kasa sayim farklarinin hangi hesaplara post edilecegini belirler
  - Eksikse session close sirasinda varyans olursa islem durur

- **status** (`ACTIVE`, `INACTIVE`)
  - INACTIVE register ile operasyon yapilamaz
  - Acik oturum varken pasife alma engellenir

### 7.3 Secersen / secmezsen ne olur?

- SessionMode = `REQUIRED` secerseniz:
  - Avantaj: Denetim izi guclenir
  - Dezavantaj: Oturum unutulursa islem bloke olur
  - Ornek: Magaza kasasi icin dogru secim

- SessionMode = `NONE` secerseniz:
  - Avantaj: Hizli is akis
  - Dezavantaj: Gun sonu kasa-kisi izlenebilirligi zayiflar
  - Ornek: Merkez kasa, shift takibi gerekmeyen yerde tercih edilebilir

- allowNegative = `Evet` secerseniz:
  - Avantaj: Acil odemede operasyon durmaz
  - Dezavantaj: Kasa disiplini ve suistimal riski artar
  - Gercek hayat ornek:
    - Kasa bakiyesi 300 TL iken 1.200 TL acil kargo odemesi cikar.
    - `allowNegative=true` ise islem gecebilir ve kasa -900'e duser.
    - Ayni gun icinde tahsilatla kapatilmazsa, gun sonu sayim/fark ve denetim riski buyur.
  - Yonetim onerisi:
    - `allowNegative=true` sadece istisnai registerlarda kullanin.
    - Bu registerlar icin dusuk `maxTxnAmount` ve istisna paneli takibi zorunlu olsun.

---

## 8. Kasa Oturumlari ekrani

Ekran: `/app/kasa-oturumlari`

### 8.1 Oturum acma

Gerekli alanlar:
- registerId (zorunlu)
- openingAmount (opsiyonel, bos ise 0)

Kurallar:
- Ayni register icin ayni anda tek OPEN oturum olur
- Register `ACTIVE` olmali
- Register `session_mode=NONE` ise oturum acilamaz

Gercek hayat:
- Sabah kasiyer kasayi teslim alip 500 TL acilisla oturum acar.

500 TL nereden gelir?
- Bu tutar normalde **fiziksel devir sayimindan** gelir (eldeki para gercekten sayilir).
- Sistem bu tutari otomatik "uretmez"; kullanici/prosedur girer.
- Bazi firmalarda onceki gunun kapanis tutari referans alinir, ama yine fiziki sayimla teyit edilmesi gerekir.

Acilista kayitli tutar ile fiziksel tutar uyusmazsa ne yapilir?
- Ornek:
  - Onceki kapanis kaydi 5.000 TL gorunuyor, fiziksel sayim 4.800 TL cikti.
- Mevcut backend davranisi:
  - Sistem, acilista "onceki kapanisla birebir eslesme" kontrolu ile oturum acmayi zorunlu kilmaz.
  - Yani 4.800 TL ile oturum acabilirsiniz.
- Onerilen dogru operasyon:
  - Acilisi **fiziksel sayilan gercek tutarla** acin (rakam gizlemeyin).
  - Farki ayni anda supervisor/finans ekibine bildirin.
  - Sirket prosedurune gore devir-fark tutanagi acin.
  - Gerekirse ilk islemde referans/aciklama ile olay numarasini baglayin.
- Neden boyle?
  - Yanlis rakamla (5.000) acmak, sorunu sadece gecici olarak saklar.
  - Gercek tutarla acmak, gun sonu fark analizini ve denetim izini dogru tutar.

Real-world SaaS uygulamalari bu problemi nasil yonetir?
- Cogu sistem acilista iki ayri deger tutar:
  - `expected_opening` (kayitli devir)
  - `counted_opening` (fiziksel sayim)
- Sistem farki otomatik hesaplar:
  - `opening_variance = counted_opening - expected_opening`
- Esik politikalari uygulanir:
  - kucuk fark -> izin + uyari
  - orta fark -> supervisor onayi
  - buyuk fark -> acilisi gecici bloke et / yeniden sayim iste
- Neden kodu ve aciklama zorunlu tutulur.
- Olay denetim izine ve istisna paneline duser.
- Tekrarlayan farklar icin register/kullanici bazli izleme yapilir.

SAAP icin ileride uygulanabilecek cozum adayi (backlog):
- `openMismatchMode`: `OFF` | `WARN` | `ENFORCE`
- `openMismatchTolerance`: otomatik izin esigi
- `openMismatchApprovalThreshold`: onay gerektiren esik
- Session alanlari:
  - `opening_expected_amount`
  - `opening_counted_amount`
  - `opening_variance_amount`
  - `opening_variance_reason`
- Kurgu:
  - `WARN`: acilis izinli + istisna kaydi
  - `ENFORCE`: esik ustu farkta onay olmadan acilis yok

Oturum acmak her yerde zorunlu mu?
- Hayir, register ayarina baglidir:
  - `REQUIRED`: Pratikte zorunlu kabul edilir (acmadan ilerlemek engellenebilir)
  - `OPTIONAL`: Tercihe bagli
  - `NONE`: Oturum acma zaten kapali

### 8.2 Oturum kapama

Gerekli alanlar:
- sessionId (acik oturum)
- countedClosingAmount (zorunlu)

Opsiyonel/kurala bagli alanlar:
- closedReason: `END_SHIFT`, `FORCED_CLOSE`, `COUNT_CORRECTION`
- closeNote: bazi durumlarda zorunlu
- approveVariance: esik ustu fark onayi icin

Kritik kurallar:
- Sadece OPEN session kapanir
- Sessionda post edilmemis islem varsa kapama bloklanir
- `FORCED_CLOSE` secildi ise closeNote zorunlu
- Varyans esigi asildiysa:
  - closeNote zorunlu
  - `approveVariance=true` ve yetki gerekir (`cash.variance.approve`)

### 8.3 Beklenen / Sayilan / Fark

- **Opening**: Oturum acilis tutari
- **Expected**: Beklenen kapanis (genelde kapanista kesinlesir)
- **Counted**: Fiziki sayilan tutar
- **Variance**: Counted - Expected

Not:
- Sistemde `expected_closing_amount` kapanista session satirina guvenilir sekilde yazilir.
- Acik oturumda canli expected her zaman ayrica endpoint ile gelmeyebilir.

### 8.4 Varyans olursa ne olur?

- Sayilan < Beklenen: "short" (eksik)
- Sayilan > Beklenen: "over" (fazla)
- Sistem, uygun hesaplara varyans kaydi uretebilir/post eder

Ornek:
- Beklenen 10.000, sayilan 9.940 -> -60 fark
- Esik 50 ise bu fark esik ustu olabilir -> onay + not istenir

---

## 9. Kasa Islemleri / Tahsilat / Tediye

Ekranlar:
- `/app/tahsilat-islemleri` -> RECEIPT sabit
- `/app/tediye-islemleri` -> PAYOUT sabit
- `/app/kasa-islemleri` -> tum tipler

### 9.1 Islem olusturma (Create)

Temel alanlar:
- registerId
- txnType
- txnDatetime
- bookDate
- amount (>0)
- currencyCode (register ile ayni olmali)

Ek alanlar (duruma gore):
- counterAccountId (banka yonlu tiplerde zorunlu)
- counterCashRegisterId (transfer tiplerinde zorunlu)
- cashSessionId (register politikasi gerektirirse)
- referenceNo, description vb.

Sistem tarafi kritik:
- Her create isteginde idempotency key zorunlu
- Cift tiklama/yeniden denemede ayni islem ikinci kez uretilmez
- `idempotentReplay=true` donerse bu hata degil, "zaten islenmisti" bilgisidir

### 9.2 Islem tipi bazli zorunluluklar

- `TRANSFER_IN` / `TRANSFER_OUT`:
  - `counterCashRegisterId` zorunlu
  - Ayni OU transferde direkt register->register kaydi calisir.
  - Farkli OU transferde transit akisi (`/app/kasa-transit-transferleri`) kullanilir; `transitAccountId` zorunludur.

- `DEPOSIT_TO_BANK` / `WITHDRAWAL_FROM_BANK`:
  - `counterAccountId` zorunlu

- `VARIANCE`:
  - Manuel olusturulamaz (sistem olusturur)

### 9.3 Durum akisi (state machine)

Durumlar:
- `DRAFT`
- `SUBMITTED`
- `APPROVED`
- `POSTED`
- `REVERSED`
- `CANCELLED`

Kurallar:
- Cancel: sadece `DRAFT`/`SUBMITTED`
- Post: `DRAFT`/`SUBMITTED`/`APPROVED`
- Reverse: sadece `POSTED` orijinal kayit
- Reversal satiri tekrar reverse edilemez
- POSTED kayit edit/cancel edilemez (immutability)

### 9.4 Post ederken override

Normalde sistem kurala aykiri postu engeller.  
Override icin:
- `overrideCashControl=true`
- `overrideReason` dolu
- kullanicida `cash.override.post` yetkisi

Secersen/secmesen:
- Override secmezseniz: guvenli ama bazen acil durumda bloke olabilirsiniz
- Override secerseniz: is devam eder ama denetimde sorumluluk artar

Gercek hayat:
- Aylik kapanisa 10 dk kala kritik bir duzeltme lazim.
- Finans admin override reason yazarak post eder.
- Sonra denetimde bu olay "istisna" olarak gorunur.

---

## 10. Kasa Istisnalari ekrani (denetim paneli)

Ekran: `/app/kasa-istisnalari`

Bu ekranda 5 ana bolum vardir:
1. Yuksek farkli oturumlar
2. Forced close oturumlari
3. Override kullanilan islemler
4. Post edilmemis islemler
5. Direkt GL cash-control olaylari (warn/override)

Filtreler:
- Legal entity
- Operating unit
- Register
- Tarih araligi
- Minimum mutlak fark

Ne zaman bakilmali?
- Gun sonu kapanista (operasyon)
- Hafta sonu risk taramasinda (supervisor)
- Ay sonu denetimde (finans/denetim)

---

## 11. "Secim" rehberi (hizli karar tablosu)

### 11.1 Session mode secimi

- `REQUIRED` sec:
  - POS/TILL gibi vardiyali yerde
  - Kisi bazli sorumluluk istiyorsan
  - Ornek:
    - AVM magazasi, sabah kasiyer A aciyor, aksam kasiyer B kapatiyor
    - "kimde kac saat acik kaldi, kapanista fark var mi" net izlenir

- `OPTIONAL` sec:
  - Sube kasasi ama vardiya disiplini kismi ise
  - Ornek:
    - Muhasebe ofisi haftada 2 gun kasa kullaniyor
    - Kullanilan gunlerde oturum aciliyor, diger gunlerde islem yok

- `NONE` sec:
  - Oturum takip ihtiyaci yoksa (nadir)
  - Ornek:
    - Sadece yonetici kontrollu, gun ici kisiler arasi devir olmayan merkez kasa noktasi
  - Dikkat:
    - Denetimde "hangi kullanici hangi vardiyada kapatti" izi session seviyesinde olmaz

### 11.2 Kapanis nedeni secimi

- `END_SHIFT`: normal vardiya kapanisi
- `FORCED_CLOSE`: elektrik kesintisi, sistem arizasi, acil durum
  - closeNote zorunlu
- `COUNT_CORRECTION`: sayim tekrar duzeltmesi

### 11.3 approveVariance secimi

- Isaretlemezsen:
  - Esik ustu farkta kapama reddedilebilir
- Isaretlersen:
  - Yetkin varsa kapama ilerler
  - Denetimde "onayli varyans" izi kalir

### 11.4 allowNegative secimi

- `false` (onerilen):
  - Kasa disiplini yuksek
  - Islem daha erken bloke olabilir
  - Ornek:
    - Kasada 400 TL var, 700 TL odeme girilmek isteniyor -> sistem durdurur

- `true`:
  - Operasyon durmaz
  - Yanlis kullanim riski artar
  - Ornek:
    - Kasada 400 TL var, 700 TL odeme geciyor -> kasa -300 olur
    - Sonradan kapatilacak denirse de, gecikirse uyumsuzluk ve aciklama ihtiyaci artar

---

## 12. Gercek hayat senaryolari

### Senaryo A - Magaza gunluk akisi (ideal)

1. Kasiyer sabah oturum acar (opening 1.000)
2. Gun boyu tahsilat/tediye girer
3. Supervisor gun sonu sayim alir
4. Sayilan ve beklenen uyusur
5. Oturum `END_SHIFT` ile kapanir
6. Istisna ekraninda sorun gorulmez

### Senaryo B - Varyansli kapanis

1. Beklenen 20.000, sayilan 19.930
2. Esik 50 ise fark 70 -> esik ustu
3. closeNote yazilir
4. approveVariance + yetkili kullanici ile kapanir
5. Sistem varyans kaydini olusturur ve kayit izi birakir

### Senaryo C - Cift tiklama / internet kopmasi

1. Kullanici "Olustur" butonuna iki kere basar
2. Sistem ayni idempotency anahtariyla ikinciyi tekrar kaydetmez
3. Ekranda "Bu istek daha once islenmis" bilgisi gorunur
4. Muhasebede duplicate olusmaz

### Senaryo D - Yanlis post edildi

1. Islem POSTED oldugu icin duzenlenemez/silinemez
2. Reverse yapilir (gerekce zorunlu)
3. Gerekirse dogru islem yeni kayit olarak girilir
4. Denetimde zincir net gorulur

### Senaryo E - Acil override

1. Normal post kuraldan dolayi bloklanir
2. Finans admin override secip gerekce girer
3. Post tamamlanir
4. Olay istisna paneline duser, denetimde izlenir

---

## 13. Sik gorulen hata mesajlari ve cozum

- "registerId is required"
  - Register secmeden devam edilmis

- "Cash register is not ACTIVE"
  - Register pasif; once aktiflestirin

- "An OPEN session already exists for this register"
  - Ayni kasada ikinci oturum acilmaya calisildi

- "countedClosingAmount is required"
  - Kapanista sayilan tutar girilmedi

- "closeNote is required when closedReason is FORCED_CLOSE"
  - Forced close secip not yazilmadi

- "Variance exceeds configured threshold"
  - Esik ustu fark var; approveVariance + yetki gerekir

- "amount exceeds register max_txn_amount"
  - Islem tutari register limitini asti

- "Transaction currency must match register currency"
  - Islem para birimi register para birimiyle ayni degil

- "Only POSTED transactions can be reversed"
  - Reverse icin once islem POSTED olmali

- "overrideReason is required when overrideCashControl=true"
  - Override secili ama gerekce bos

Not:
- Hata kutusunda "Talep ID" gorurseniz, destek ekibine bu ID'yi iletin.

---

## 14. Gun sonu operasyon proseduru (onerilen)

1. Acik oturum listesi kontrol et
2. Tum bekleyen islemleri gozden gecir
3. Gerekliyse post/cancel islemlerini tamamla
4. Fiziki sayim yap
5. Oturumu uygun kapanis nedeni ile kapat
6. Istisna panelini kontrol et:
   - yuksek fark
   - forced close
   - override
   - unposted
7. Gerekli aciklamalari ayni gun gir

---

## 15. Haftalik/aylik kontrol proseduru (finans/supervisor)

Haftalik:
- En cok varyans olusan registerlari incele
- Tekrar eden forced close nedenlerini takip et
- Override kullanimi artis trendini kontrol et

Aylik:
- Unposted islemleri sifirla
- Varyans gain/loss hesap etkisini raporla
- Denetim icin orneklem sec (requestId + aciklama + onay izi)

---

## 16. "Neden bu kadar kisit var?" (isletme mantigi)

Bu kisitlarin amaci operasyonu zorlastirmak degil, su riskleri azaltmaktir:
- Cift kayit
- Yetkisiz post
- Kapanis sonrasi sessiz veri degisikligi
- Kasa-fiziki para uyumsuzlugu
- Denetimde aciklanamayan hareketler

Kisa cevap:
- Hız + kontrol dengesini korumak icin.

---

## 17. Hangi durumda neyi secmeliyim? (tek sayfada)

- Magaza kasasi -> `TILL + REQUIRED + allowNegative=false`
- Subede esnek petty cash -> `DRAWER + OPTIONAL`
- Merkez kasa -> `VAULT + OPTIONAL/NONE` (politika ile)
- Riskli gecis donemi -> cash control mode `WARN`
- Stabil uretim -> cash control mode `ENFORCE`

---

## 18. Son notlar

- Post edilmis kayitlar bilerek degistirilemez. Bu denetim guvencesidir.
- Silmek yerine ters kayit tercih edilir.
- Yetkiniz yoksa sistem bunu gizler/engeller; bu hata degil kontrol mekanizmasidir.
- Istisna ekrani sadece "problem listesi" degil, iyilestirme rehberidir.

---

## 19. Ekip ici hizli egitim plani (onerilir)

1. 30 dk: Kasa Tanimlari + Session mode egitimi
2. 30 dk: Kasa Oturumu ac/kapat canli deneme
3. 45 dk: Islem olustur-post-cancel-reverse senaryolari
4. 15 dk: Istisna paneli ve gun sonu checklist
5. 15 dk: Soru-cevap + yetki matrisi teyidi

Toplam: yaklasik 2 saat

---

## 20. Destek isterken ne gondermeliyim?

Sorun bildirirken su bilgileri ekleyin:
- Hangi ekran/rota
- Hangi adimda hata alindi
- Hata metni
- Talep ID (requestId)
- Islem ID / Session ID / Register ID (varsa)
- Kisa is aciklamasi (ornek: "Gun sonu kapama, forced close")

Bu bilgiler, teknik ekibin sorunu cok daha hizli cozmesini saglar.

---

## 21. Ek A - Kasa Modulu Teknik Karar Ozeti (ADR'den Isletmeye Cevrilmis)

Bu bolum, sistemde gercekten calisan kurallarin is diline cevrilmis ozetidir.

1. Register modeli
- Her kasa register tek bir GL (Defteri Kebir) hesabina baglidir.
- Register hesabi: aktif, postable, leaf ve ayni legal entity olmalidir.

2. Register tipleri ve oturum modu
- Tipler: `VAULT`, `DRAWER`, `TILL`
- Oturum modlari: `REQUIRED`, `OPTIONAL`, `NONE`
- Sistem defaultlari:
  - `registerType`: `DRAWER`
  - `sessionMode`: `REQUIRED`
- Not:
  - Tipe gore otomatik mode atamasi (ornegin TILL->REQUIRED) politika olarak onerilir, kodda otomatik bagli degildir.

3. Para birimi kurali
- Register tek para birimiyle calisir.
- Islem para birimi register para birimiyle ayni olmadan kayit gecmez.

4. Kasa kontrollu hesap kurali
- Kasa akisiyla baglanan hesaplar `is_cash_controlled` olur.
- Direkt GL (Defteri Kebir) kaydinda cash-control modu `ENFORCE` ise kural disi giris bloklanir.

5. Islem degistirilemezligi
- `POSTED` islem sonradan duzenlenmez/silinmez.
- Duzeltme yolu: `REVERSE` + gerekirse yeni dogru kayit.

6. Oturum kurallari
- Ayni register icin ayni anda tek acik oturum.
- `REQUIRED` modda open session olmadan create/post akisi bloklanabilir.
- Session kapamada expected/counted/variance hesaplanir.

7. Guvenilirlik kurallari
- Create icin idempotency key zorunlu.
- Cift tiklama/yeniden denemede replay korumasi vardir.
- `txn_no` legal entity + yil bazli deterministik gider.

8. Transfer kapsam siniri (guncel)
- Registerlar arasi direkt transfer ayni legal entity + ayni operating unit icin calismaya devam eder.
- Cross-OU transfer artik CASH_IN_TRANSIT akisiyla aktif desteklenir:
  - Ayni legal entity zorunlu
  - Kaynak ve hedef register farkli operating unitte olmali
  - Transfer-out + transfer-in cift kayit zinciri korunur (tek tarafli transfer olusmaz)

---

## 22. Ek B - Islem Tipine Gore Muhasebe Kaydi Matrisi (Uygulamadaki Guncel Davranis)

| Islem Tipi | Borc | Alacak | Pratik Not |
|---|---|---|---|
| `RECEIPT` | Register Kasa | Karsi Hesap | Tahsilat |
| `PAYOUT` | Karsi Hesap | Register Kasa | Odeme |
| `DEPOSIT_TO_BANK` | Karsi Hesap (banka vb.) | Register Kasa | Kasadan bankaya cikis |
| `WITHDRAWAL_FROM_BANK` | Register Kasa | Karsi Hesap (banka vb.) | Bankadan kasaya giris |
| `TRANSFER_OUT` | Hedef Register Kasa (direkt) veya CASH_IN_TRANSIT hesabi (`counterAccountId`) | Kaynak Register Kasa (`registerId`) | Ayni LE zorunlu. Ayni OU: direkt transfer. Farkli OU: transit akisi (`transitAccountId` zorunlu). |
| `TRANSFER_IN` | Hedef Register Kasa (`registerId`) | Kaynak Register Kasa (direkt) veya CASH_IN_TRANSIT hesabi (`counterAccountId`) | Ayni LE zorunlu. Transit receive icin transfer-out kaydi once `POSTED` olmalidir. |
| `VARIANCE` (eksik) | Varyans Zarar Hesabi | Register Kasa | Counted < Expected |
| `VARIANCE` (fazla) | Register Kasa | Varyans Kazanc Hesabi | Counted > Expected |
| `OPENING_FLOAT` | Register Kasa | Karsi Hesap | Opsiyonel acilis hareketi |
| `CLOSING_ADJUSTMENT` | Karsi Hesap | Register Kasa | Kontrollu ve aciklamali kullanim |

Ek teknik kontroller:
- Fis dengesi zorunlu (borc = alacak).
- Period OPEN degilse post olmaz.
- Satir scope kontrolunden gecmeyen kayit post olmaz.
- Sistem kaynagi `source_type = CASH` olarak yazilir.

---

## 23. Ek C - Yetki ve Gorev Ayrimi (SoD) - Mevcut Sistem

### 23.1 Mevcut aktif kasa yetkileri
- `cash.register.read`
- `cash.register.upsert`
- `cash.session.open`
- `cash.session.close`
- `cash.txn.read`
- `cash.txn.create`
- `cash.txn.cancel`
- `cash.txn.post`
- `cash.txn.reverse`
- `cash.override.post`
- `cash.variance.approve`
- `cash.report.read`

Not:
- `cash.txn.submit` ve `cash.txn.approve` su an aktif API akisinin parcasi degildir.

### 23.2 Sistemde fiilen zorunlu olan SoD kontrolleri
- Override ile post:
  - `overrideCashControl=true`
  - `overrideReason` dolu
  - `cash.override.post` yetkisi
- Esik ustu varyans kapama:
  - `approveVariance=true`
  - `cash.variance.approve` yetkisi
  - `closeNote` zorunlu
- Durum gecis kurallari:
  - cancel: `DRAFT`/`SUBMITTED`
  - post: `DRAFT`/`SUBMITTED`/`APPROVED`
  - reverse: sadece `POSTED`
  - reversal satiri tekrar reverse edilemez

### 23.3 Henuz zorunlu olmayan (gelecek iyilestirme adayi)
- \"Olusturan kisi post edemez\" gibi kisi-bazli ayrim
- Ayrik submit/approve endpointleri

Bu nedenle organizasyonel SoD (rol ayrimi) halen onemlidir:
- Operator agirlikli rollerde `post/reverse/override` verilmemelidir.

---

## 24. Kasa Transit Transferleri (Cross-OU Is Akisi)

Bu bolum, farkli operating unit (OU) registerlari arasinda para tasima akisinin nasil calistigini anlatir.

### 24.1 Ne zaman transit kullanilir?

- Ayni legal entity altinda,
- Kaynak ve hedef register farkli OU'da ise,
- Fiziksel para transferini iki adimli ve denetlenebilir yapmak istiyorsaniz.

Not:
- Cross-legal-entity transit desteklenmez.
- Register para birimleri ayni olmadan transit baslatilamaz.

### 24.2 Durumlar (state machine)

- `INITIATED`: Transit kaydi olustu, transfer-out henuz kapanmadi.
- `IN_TRANSIT`: Transfer-out `POSTED`, para yolda.
- `RECEIVED`: Hedef registerda transfer-in olustu ve post edildi.
- `CANCELED`: Sadece `INITIATED` durumunda iptal edildi.
- `REVERSED`: Transfer zinciri reversal ile geri alindi.

### 24.3 Ekran ve adimlar

Ekran:
- `/app/kasa-transit-transferleri` (alias: `/app/cash-transit-transfers`)

Adim 1 - Transit baslat:
- Kaynak register (`registerId`)
- Hedef register (`targetRegisterId`)
- Transit hesabi (`transitAccountId`)
- Tutar, para birimi, tarih
- `idempotencyKey`

Adim 2 - Transfer-out post:
- Transfer-out kaydi post edilince durum `IN_TRANSIT` olur.

Adim 3 - Receive:
- Hedef tarafta receive aksiyonu calisir.
- Sistem `TRANSFER_IN` kaydini olusturur ve post eder.
- Basarili durumda durum `RECEIVED` olur.

### 24.4 Kritik kurallar (operasyonel)

- Ayni registera kaynak+hedef secilemez.
- Kaynak/hedef registerlar farkli OU'da olmali.
- Receive icin transfer-out kaydi `POSTED` olmali.
- Ayni transit kaydina ikinci kez receive denemesi idempotent veya bloklu doner; cift kapanis olmaz.
- Sadece `INITIATED` transit iptal edilebilir.
- `RECEIVED` olduktan sonra transfer-out reversal'i dogrudan yapilamaz; once transfer-in reversal gerekir.

### 24.5 Gercek hayat ornegi

Ornek: OU-1 Ana Kasa -> OU-2 Sube Kasasi, 25.000 TRY
1. Operasyon, transit kaydini baslatir (`INITIATED`).
2. Kaynak kasada transfer-out post edilir (`IN_TRANSIT`).
3. Sube kasasi teslim aldiginda receive yapar (`RECEIVED`).
4. Hata varsa reversal sureci tek tek izlenir; denetim izi korunur.

---

## 25. Ekran Bazli Kartlar ve Butonlar (Tam Islev Rehberi)

Bu bolumde her kasa ekranindaki kart/bolum, buton ve tipik kullanimi bir arada verilir.

### 25.1 `/app/kasa-tanimlari` (CashRegistersPage)

Kartlar:
- Register olustur/guncelle karti
- Register listesi karti

Ana butonlar:
- `Create` / `Update`
- `Cancel Edit`
- Liste satirinda `Edit`
- Liste satirinda `Activate` / `Deactivate`
- Liste ustunde `Refresh`

Ne zaman hangi buton?
- Yeni kasa aciliyorsa: formu doldurup `Create`.
- Yanlis register secildiysa: `Cancel Edit` ile formu temizle.
- Gecici kapanacak kasada: satirdan `Deactivate`.
- Donem basi kontrolunde: `Refresh` ile durumlari tazele.

Gercek hayat ornegi:
1. Yeni sube acildi, yeni register acilacak.
2. Register hesap, currency, session mode set edilir.
3. `Create` ile kayit acilir.
4. Operasyon baslamadan once listeden durum `ACTIVE` teyit edilir.

### 25.2 `/app/kasa-oturumlari` (CashSessionsPage)

Kartlar:
- Oturum Ac (`Open`) karti
- Oturum Kapat (`Close`) karti
- Acik oturumlar tablosu
- Gecmis oturumlar tablosu

Ana butonlar:
- `Open`
- `Close`
- Acik satirda `Use for Close`
- Tablo ustunde `Refresh`
- (Yetki varsa) `approveVariance` secimi

Ne zaman hangi buton?
- Vardiya basinda: `Open`.
- Vardiya sonunda sayimla: `Close`.
- Kapatilacak oturumu hizli secmek icin: `Use for Close`.
- Beklenen/sayilan farki esik uzeri ise: `approveVariance` + acik `closeNote`.

Gercek hayat ornegi:
1. Kasiyer sabah `Open` yapar.
2. Aksam sayimda fark cikarsa aciklama girer.
3. Supervisor, yetkisi varsa `approveVariance` ile kapatir.
4. History tablosundan ay sonu denetim izi kontrol edilir.

### 25.3 `/app/kasa-islemleri`, `/app/tahsilat-islemleri`, `/app/tediye-islemleri` (CashTransactionsPage)

Kartlar:
- Filtre karti
- Islem olusturma karti
- Aksiyon hazirlama karti (`prepare post/cancel/reverse`, `receive transit`, `apply cari`)
- Islem listesi karti

Ana butonlar:
- Filtrede `Apply Filters`, `Clear Filters`, `Refresh`
- Create formunda `Create`
- Satir bazinda:
  - `Prepare Post`
  - `Prepare Cancel`
  - `Prepare Reverse`
  - `Receive Transit` (uygunsa)
  - `Apply Cari` (RECEIPT/PAYOUT icin)
- Aksiyon formunda `Submit Action`, `Cancel Action`

Link badge'leri:
- `Transit #...`
- `Settlement #...`
- `Unapplied #...`
- `Linked`

Ne zaman hangi buton?
- Liste kirli/uzunsa: once filtre kartini kullan.
- Yanlis draft fiş: `Prepare Cancel`.
- Post edilmis hatali fiş: `Prepare Reverse`.
- Nakit tahsilati hemen cariye baglamak icin: `Apply Cari`.
- Cross-OU transfer hedefe ulasmis ise: `Receive Transit`.

Gercek hayat ornegi A (Tahsilat + cari uygulama):
1. `Tahsilat` ekranindan RECEIPT olusturulur.
2. Satirdan `Apply Cari` secilir.
3. Open documents picker'da faturalar secilip tutar dagitilir.
4. Sonucta satirda `Settlement #...` veya `Unapplied #...` badge'i gorulur.

Gercek hayat ornegi B (Yanlis post duzeltme):
1. Hatali POSTED fiş bulunur.
2. `Prepare Reverse` ile ters kayit olusturulur.
3. Denetim zinciri korunur, orijinal fiş silinmez.

### 25.4 `/app/kasa-transit-transferleri` (CashTransitTransfersPage)

Kartlar:
- Transit filtre karti
- Transit listesi karti
- Aksiyon karti (Receive veya Cancel)

Ana butonlar:
- Filtrede `Apply filters`, `Clear`, `Refresh`
- Satir bazinda `Receive`, `Cancel`
- Aksiyon kartinda `Receive Transit` veya `Cancel Transit`

Ne zaman hangi buton?
- Yoldaki transferleri bulmak icin status=`IN_TRANSIT` filtrele.
- Hedef teslim aldiysa satirdan `Receive`.
- Henuz yola cikmamis transfer iptal edilecekse `Cancel` (yalniz `INITIATED`).

Gercek hayat ornegi:
1. Merkez kasadan subeye transfer baslatildi.
2. Status `IN_TRANSIT` oldugunda sube sorumlusu satirdan `Receive` yapar.
3. Cift tiklamada ikinci receive duplicate yaratmaz; idempotent koruma vardir.

### 25.5 `/app/kasa-istisnalari` (CashExceptionsPage)

Kartlar:
- Filtre karti
- Ozet KPI kartlari:
  - High variance
  - Forced close
  - Override usage
  - Unposted
  - GL cash-control events
- Her KPI icin detay tablolar

Ana butonlar:
- Filtrede `Apply`, `Clear`, `Refresh`

Ne zaman hangi kart?
- Gun sonu kontrol: once KPI kartlarini tara.
- `Forced close` artisi varsa vardiya disiplinini incele.
- `Override usage` artisi varsa cash-control politika ihlali riski vardir.
- `Unposted` yuksekse muhasebe kapanisinda gecikme riski vardir.

Gercek hayat ornegi:
1. Haftalik kontrolde `High variance` ve `Forced close` kartlari yuksek cikti.
2. Detay tablolardan ilgili register ve kullanici bulunur.
3. Ayni hafta icinde duzeltici aksiyon plani (egitim + rol/yetki gozden gecirme) acilir.
