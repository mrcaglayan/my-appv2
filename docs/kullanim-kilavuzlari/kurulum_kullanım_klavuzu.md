# SaaP Kullanim Kilavuzu (Is Birimi Odakli)

Bu dokuman teknik olmayan kullanicilar icin yazildi.
Amac: sistemi sifirdan dogru kurmak, gunluk muhasebe akislarini hatasiz ilerletmek ve intercompany islemlerini otomatik hale getirmek.

Bu kilavuzda her adim icin su 3 soruya cevap verilir:
1. Bu adim ne ise yarar?
2. Ne doldurmaliyim?
3. Bu adimi yapmazsam ne olur?

---

## 1) Kisa Ozet: Bu Sistemde Ana Mantik Nedir?

Sistem 4 temel blokta calisir:
1. Organizasyon yapisi kurulur.
2. Defter ve hesap plani kurulumu yapilir.
3. Fisler olusturulur, post edilir, donem kontrolu yapilir.
4. Intercompany mutabakat ve konsolidasyon raporlari alinır.

Gercek hayat benzetmesi:
- Organizasyon yapisi = Sirketin iskeleti.
- Hesap plani = Finans dili.
- Fis = Her islemin resmi kaydi.
- Mutabakat = Iki sirketin birbirine ayni tutari gormesi.
- Konsolidasyon = Tum sirketlerin tek tablo gibi gosterilmesi.

Terminoloji notu:
- Bu kilavuzda standart ifade `Legal Entity (Bagli Ortak)` olarak kullanilir.

---

## 2) Sifirdan Baslangic Senaryosu (Schema Silindikten Sonra)

Schema silindiyse once teknik hazirlik gerekir. Bu bolumu genelde IT/teknik ekip yapar.

### 2.1 Teknik Hazirlik (IT Ekibi)

Yapilacaklar:
1. Veritabani tablolarini yeniden olusturmak (migration).
2. Temel sistem verilerini yuklemek (seed).
3. Ilk admin kullanicisini hazirlamak.

Yapilmazsa ne olur:
1. Login ekranina girseniz bile kullanici dogrulanamaz.
2. Sayfalar bos gelir veya "not found" tipinde hatalar gorursunuz.
3. Kaydetme butonlari calissa bile alt tarafta listeler dolmaz.

### 2.2 Kullanici Kontrolu

Kullanici olarak kontrol edin:
1. Login olabiliyor muyum?
2. Sol menude "Ayarlar" altinda sayfalar gorunuyor mu?
3. "Organizasyon Yonetimi" aciliyor mu?

Bunlardan biri calismiyorsa teknik kurulum bitmemistir.

---

## 3) Kurulum Yolu: Hangi Sirayi Izlemeliyim?

Iki yol var:
1. Hizli kurulum: `Sirket Ayarlari` (Company bootstrap)
2. Manuel kurulum: `Organizasyon Yonetimi` + `Hesap Plani Ayarlari`

Bu kilavuz manuel yola odaklidir (sizin seciminiz).

Neden manuel yol?
1. Her adimi kontrol ederek ilerlersiniz.
2. Yanlis kurgu riskini erken gorursunuz.
3. Buyuk organizasyonlarda daha guvenli olur.

Not (guncel sistem davranisi):
1. Readiness kontrol listesi artik su ek kalemleri de denetler:
   - `Open book periods`
   - `Shareholders`
   - `Shareholder commitment debit mappings`
2. Bu kalemler eksikse sistem kurulum adimlarina yonlendirir.

---

## 4) Adim Adim Manuel Kurulum

## Adim 1 - Grup Sirketini Olustur

Ekran:
- `Ayarlar > Organizasyon Yonetimi > Group Companies`

Ne doldurulur:
1. `Code` (ornek: `TMV`)
2. `Name` (ornek: `Turkish Maarif Foundation`)
3. `Save`

Amac:
- Tum `Legal Entity (Bagli Ortak)` kayitlarini tek bir cati grup altinda toplamak.

Yapilmazsa:
- `Legal Entity (Bagli Ortak)` olustururken baglanacak grup olmaz.
- Sonraki konsolidasyon kurulumunda problem yasarsiniz.

---

## Adim 2 - Legal Entity (Bagli Ortak) Olustur

Ekran:
- `Ayarlar > Organizasyon Yonetimi > Legal Entities`

Ne doldurulur:
1. Group company secimi
2. Entity code, entity name
3. Country, currency
4. Opsiyonel: tax id
5. Isaret kutulari:
   - `Intercompany enabled`
   - `Partner required`
   - `Auto-create defaults` (isterseniz)

### `Auto-create defaults` ne yapar?

Bu kutu aciksa, `Save` aninda sistem otomatik olarak sunlari olusturmayi dener:
1. Fiscal calendar: yoksa `MAIN`
2. Fiscal periods: secilen yil icin 12 donem (Ocak-Aralik akisi)
3. CoA: `Legal Entity (Bagli Ortak)` icin `COA-<EntityCode>` tipinde hesap plani
4. Book: `Legal Entity (Bagli Ortak)` icin `BOOK-<EntityCode>` tipinde LOCAL defter
5. Temel hesaplar: CoA tamamen bossa su 6 hesap:
   - `1000` Cash and Cash Equivalents
   - `1100` Accounts Receivable
   - `2000` Accounts Payable
   - `3000` Retained Earnings
   - `4000` Revenue
   - `5000` Operating Expense

### `Auto-create defaults` ne yapmaz?

1. Turk detay hesap planini (genis hesap listesi) yuklemez.
2. Var olan hesaplari topluca silip sifirdan kurmaz.
3. CoA icinde zaten hesap varsa, yukaridaki 6 temel hesabi yeniden eklemez.

Not:
- Turk hesap plani istiyorsaniz ayrica
  `Ayarlar > Hesap Plani Ayarlari > Load Turkish Default CoA`
  adimini kullanin.

Amac:
- Sirket bazli muhasebe kayit alanini acmak.

Yapilmazsa:
- Defter, hesap plani, fis gibi hicbir finans adimi baslamaz.

Gercek hayat ornegi:
- `AMF` (Afghanistan `Legal Entity (Bagli Ortak)`) tek basina bir muhasebe defteri tutar.
- `TMV` grubuna bagli oldugu icin sonra toplu rapora girebilir.

### Intercompany Kutularinin Anlami (Bugun Itibariyla)

`Intercompany enabled`:
- Aciksa: bu `Legal Entity (Bagli Ortak)` intercompany karsi tarafli satir kullanabilir.
- Kapaliysa: karsi tarafli satir ve INTERCOMPANY kaynakli fis engellenir.

`Partner required`:
- Aciksa ve kaynak tipi `INTERCOMPANY` secilmisse:
  tum satirlarda karsi taraf entity secimi zorunlu olur.

Yapilmazsa veya yanlis secilirse:
1. Fis kaydi sirasinda policy hatalari alirsiniz.
2. Karsi tarafi eksik birakir, mutabakatta sapma olusturursunuz.

---

## Adim 3 - Sube / Operasyon Birimi Tanimla

Ekran:
- `Ayarlar > Organizasyon Yonetimi > Operating Units / Branches`

Ne doldurulur:
1. `Legal Entity (Bagli Ortak)` sec
2. Sube kodu ve adi
3. `Save`

### `Has subledger` (Alt defter var) kutusu ne anlama gelir?

Bu kutu, subenin satir bazli alt referans zorunlulugu olup olmadigini belirler.

Ne zaman isaretlenmeli?
1. Sube bazinda detay takip istiyorsaniz (ornek: sube bazli alici/satici, stok, ogrenci/veli gibi alt detaylar).
2. "Bu sube kendi operasyon detayini ayri izlemeli" diyorsaniz.

Ne zaman isaretlenmemeli?
1. Sube sadece genel gider merkezi gibi kullaniliyorsa.
2. Tum detay takip merkezde yapiliyorsa, subede ayri alt detay acilmayacaksa.

Isaretlersem ne olur?
1. Sube kaydinda `Subledger = Yes` olur.
2. Journal satirinda bu sube secildiginde `Subledger Ref` alani zorunlu olur.
3. `Subledger Ref` girilmeden fis kaydi alinmaz (validasyon hatasi verir).
4. Tek basina ekstra hesap, defter veya otomatik fis olusturmaz.

Isaretlemezsem ne olur?
1. Sube kaydinda `Subledger = No` olur.
2. Bu sube secildiginde `Subledger Ref` alani opsiyonel kalir.
3. Kayit acisindan engel olmaz; sube normal calismaya devam eder.

Pratik ornekler:
1. Buyuk kampus subesi: kendi ogrenci/veli alacak takibi var -> `Has subledger = Evet`, satira `Subledger Ref` olarak ogrenci/veli referansi girilir.
2. Sadece idari temsilcilik: sadece merkezden butce aliyor -> `Has subledger = Hayir`, `Subledger Ref` bos birakilabilir.
3. Tum muhasebe merkezde tutuluyor, subeler sadece operasyon noktasi -> genelde `Hayir`.

Amac:
- Ayni `Legal Entity (Bagli Ortak)` icinde sube bazli takip yapmak.

Onemli not:
- Sube kendi basina ayri hesap plani kullanmaz.
- Sube, bagli oldugu `Legal Entity (Bagli Ortak)` defter ve hesap planini kullanir.

Yapilmazsa:
- Sube bazli rapor kirilimlari zayif kalir.

---

## Adim 4 - Mali Takvim Kaydet

Ekran:
- `Ayarlar > Organizasyon Yonetimi > Fiscal Calendars and Periods`

Ne doldurulur:
1. Calendar code
2. Calendar name
3. Start month/day
4. `Save Calendar`

Amac:
- Hangi gunlerde hangi mali donemin gececegini belirlemek.

Yapilmazsa:
- Donem olusmaz.
- Fis post etme ve donem kapama islemleri calismaz.

---

## Adim 5 - Donemleri Uret ve Dogru Filtreyle Listele

Ekran:
- Ayni bolumde `Generate 12 Periods`

Ne yapilir:
1. Takvimi sec.
2. Mali yili gir (ornek: `2026`).
3. `Generate 12 Periods`.
4. Sonra `Reload Periods` bas.

Neden bazen "No periods found" gorunur?
1. Yanlis takvim secili olabilir.
2. Yanlis mali yil filtresi olabilir.
3. Liste yenilenmemis olabilir.

Yapilmazsa:
- Fis kaydinda period secemezsiniz.
- Donem acik/kapali kontrolu dogru calismaz.

---

## Adim 6 - Defter (Book) Olustur

Ekran:
- `Ayarlar > Hesap Plani Ayarlari > Books`

Ne doldurulur:
1. `Legal Entity (Bagli Ortak)`
2. Calendar
3. Book type (`LOCAL` genelde)
4. Book code, book name
5. Base currency

Book ne ise yarar?
- Ayni `Legal Entity (Bagli Ortak)` icinde farkli kayit amaclari icin ayri defter acabilirsiniz.
- En temel kullanim: yerel resmi kayit defteri.

Yapilmazsa:
- Fis acamazsiniz (fis book ister).

---

## Adim 7 - Hesap Plani (CoA) Olustur

Ekran:
- `Ayarlar > Hesap Plani Ayarlari > Charts of Accounts`

Ne doldurulur:
1. Scope (`LEGAL_ENTITY` veya `GROUP`)
2. Code, name
3. `Legal Entity (Bagli Ortak)` secimi (LEGAL_ENTITY ise)

Amac:
- Hangi hesap kodlariyla calisacaginizi belirlemek.

Yapilmazsa:
- Hesap olusturamazsiniz.
- Fis satiri hesap secimi bos kalir.

---

## Adim 8 - Hesaplari Yukle (Turkish Default CoA)

Ekran:
- `Ayarlar > Hesap Plani Ayarlari > Accounts`
- Buton: `Load Turkish Default CoA`

Amac:
- Turk hesap plani iskeletini hizli yuklemek.

Tekrar basarsam ne olur?
1. Ayni kodlu hesaplar guncellenir/korunur.
2. Tum tabloyu sifirlayip bastan silmez.
3. Ozel eklediginiz farkli kodlu hesaplar genelde kalir.

Yapilmazsa:
- Tum hesaplari tek tek acmaniz gerekir.
- Kurulum suresi uzar.

---

## Adim 9 - Ortaklar ve Sermaye Taahhut Kurulumu

Ekran:
- `Ayarlar > Organizasyon Yonetimi > Shareholders`

Bu adim 3 parcadan olusur:
1. `Parent mapping` (legal entity bazli ust hesap eslesmesi)
2. `Shareholder` (ortak) + ortak bazli alt hesap baglantisi
3. Kuyruktan `tek bir toplu taahhut taslak yevmiyesi` olusturma

### Sermaye taahhut yevmiyesi icin "Setup Required List" (zorunlu kontrol listesi)

Sistem, secili `Legal Entity (Bagli Ortak)` icin su 4 kalemi kontrol eder:
1. En az 1 ortak tanimli mi?
2. Taahhutu olan ortaklar icin borc ve sermaye alt hesaplari tanimli mi?
3. Kullanilabilir equity (sermaye) alt hesap var mi?
4. Mali donemler olusturulmus mu?

Sistem nasil bildirir?
1. `Organizasyon Yonetimi > Shareholders` bolumunde "Setup Required List" kutusunda kalemleri `OK / Eksik` olarak gosterir.
2. Eksik varsa kullaniciyi dogrudan yonlendirir:
   - `Go to shareholder form`
   - `Open GL setup`
3. `Acilis Fisi Olustur` ekraninda da ayni kontrol listesi gosterilir (sermaye taahhut fisine yonelik uyarili panel).

### Sermaye taahhudu nasil calisir?

Sistem mantigi:
1. Ortak bazli iki alt hesap birlikte calisir:
   - `Commitment debit sub-account` (tipik `501.xx`, DEBIT/EQUITY)
   - `Capital sub-account` (tipik `500.xx`, CREDIT/EQUITY)
2. Kayitta girilen taahhut mantigi `artis` uzerindendir:
   - formdaki artis tutari mevcut toplam taahhude eklenir
3. Toplu fis onizlemesinde sistem her ortak icin `delta` hesaplar:
   - `delta = committed_capital - already_journaled_amount`
4. Sadece `delta > 0` olan ortaklar toplu taahhut fisine dahil edilir.
5. Olusan taslak yevmiyede her ortak icin 2 satir vardir:
   - Borc: `commitment debit sub-account`
   - Alacak: `capital sub-account`
6. Fisleme kaydi audit tablosuna yazilir; ayni tutarin tekrar fislenmesi engellenir.
7. Journal post edilirken sistem, uygun satir kombinasyonlarini tespit ederse ortak taahhut toplamini ve sahiplik yuzdesini (ownership %) tekrar senkronize eder.

### Uctan uca adimlar (onerilen is akis)

1. `Shareholder parent account mapping` formunda:
   - `Capital credit parent` (tipik `500`)
   - `Commitment debit parent` (tipik `501`)
   secilip `Save Parent Mapping` yapilir.
2. Ortak formunda su alanlar doldurulur:
   - code, name, shareholder type, commitment date
   - commitment debit sub-account
   - capital sub-account
   - taahhut artisi (bu kayit)
3. `Save Shareholder` ile kayit alin.
4. Artis > 0 ise ortak otomatik olarak `Toplu taahhut yevmiye kuyrugu`na eklenir.
5. Gerekirse `Sermaye taahhut arttirimi` modali ile mevcut ortak icin ek artis girip `Kaydet ve kuyruga ekle` yapin.
6. Kuyruktan `Tek bir toplu taahhut yevmiyesi olustur` aksiyonunu acin.
7. Onizlemede kontrol edin:
   - Blocking validation errors
   - Included rows / Skipped rows
   - Toplam borc / toplam alacak / para birimi
8. `Create batch journal` ile tek bir taslak yevmiye olusturun.
9. Journal Workbench uzerinden taslagi gozden gecirip post edin.
10. Post sonrasi kontrol:
   - ilgili ortagin committed capital/ownership degerleri
   - olusan journal no, book ve fiscal period bilgileri

Yapilmazsa:
1. Taahhut akisinda manuel is yukunuz artar.
2. Delta takibi zorlasir ve ayni tutarin tekrar fislenme riski artar.
3. Kaydetme aninda sistem su tip hata/uyariyi verir:
   - "commitmentDebitSubAccountId is required..."
   - "capitalSubAccountId is required..."

---

## Adim 10 - Ilk Test Fisini Olustur

Ekran:
- `Yevmiye Kayitlari > Mahsup Islemleri` (Journal Workbench)

Asgari gerekli alanlar:
1. `Legal Entity (Bagli Ortak)`
2. Book
3. Fiscal period
4. Currency
5. En az 2 satir
6. Borc ve alacak esitligi
7. Eger satirdaki birimde `Has subledger = Evet` ise `Subledger Ref` alani

Amac:
- Sistemin temel muhasebe omurgasini dogrulamak.

Yapilmazsa:
- Sonraki intercompany veya konsolidasyon adiminda kok neden bulmak zorlasir.

---

## Adim 11 - Intercompany Ciftlerini (Pair) Dogru Hazirla

Intercompany duzende cift yon gerekir:
1. Kaynak -> Partner aktif pair
2. Partner -> Kaynak aktif pair

Neden iki yon?
- Kaynaktan partnera kayit var.
- Otomatik mirror olustururken partner tarafin da kurallari gerekir.

Yapilmazsa:
- INTERCOMPANY fis kaydinda "pair mapping" hatasi alirsiniz.
- Otomatik mirror akisi calismaz.

Not:
- Journal Workbench icindeki compliance alani, eksik pair icin hizli duzeltme aksiyonu sunar.

---

## Adim 12 - Intercompany Fisini Otomatik Partner Mirror Ile Calistir

Ekran:
- Journal Workbench > Create Draft Journal

Ne secilir:
1. `SourceType = INTERCOMPANY`
2. `Auto-create partner mirror draft journal(s)` kutusunu acik birak
3. Satirlarda `Counterparty LE` doldur

Onemli kural:
- Otomatik mirror modunda tum satirlarda karsi taraf secimi olmali.

Sistem ne yapar?
1. Kaynak `Legal Entity (Bagli Ortak)` uzerinde taslak fis olusturur.
2. Partner `Legal Entity (Bagli Ortak)` uzerinde bagli mirror taslak fis(ler) olusturur.
3. Ekranda mirror fis IDlerini gosterir.

Yapilmazsa:
- Partner kaydi elle acmak zorunda kalirsiniz.
- Zaman kaybi ve hata riski artar.

Gercek hayat ornegi:
- A sirketi B sirketine 100 birim hizmet faturaladi.
- A'da alacak ve gelir fis satirlari girilir.
- Sistem B'de buna karsi gider + borc mirror taslagi acabilir.

---

## Adim 13 - Post Asamasinda Bagli Mirrorlari Birlikte Post Et

Ekran:
- Journal Workbench > Post Journal

Ne yapilir:
1. Source fis ID gir.
2. `Post linked intercompany mirrors` kutusunu isaretle.
3. `Post` bas.

Sistem ne yapar?
1. Kaynak fis + bagli mirror taslaklari birlikte post etmeyi dener.
2. Her fisin donemi acik mi kontrol eder.
3. Birlikte post sonucu mesajda listelenir.

Yapilmazsa:
- Kaynak post olur, partner mirror taslakta kalabilir.
- Mutabakatta gecici fark gorebilirsiniz.

---

## Adim 14 - Intercompany Uyumluluk Kontrolu (Compliance)

Ekran:
- Journal Workbench icindeki `Intercompany Compliance` bolumu

Ne ise yarar?
- Politika ihlallerini toplu gosterir:
1. Intercompany kapali entityde karsi tarafli satir
2. Partner zorunlu oldugu halde eksik karsi taraf
3. Eksik aktif pair

Ekrandaki duzeltme aksiyonlari:
1. Entity icin intercompany ac
2. Partner required kuralini kapat
3. Eksik active pair olustur

Yapilmazsa:
- Ay sonu mutabakatta cok sayida elle duzeltme gerekir.

---

## Adim 15 - Intercompany Mutabakat Raporunu Calistir

Ekran:
- `Donem Sonu Islemler > Aylik > Intercompany Mutabakat`

Ne secilir:
1. Calendar
2. Fiscal period
3. Gerekirse from/to `Legal Entity (Bagli Ortak)` filtreleri
4. Tolerance

Amac:
- Iki `Legal Entity (Bagli Ortak)` kaydinin birbirini ayni tutarda gorup gormedigini kontrol etmek.

Yapilmazsa:
- Konsolidasyon oncesi farklar yakalanmaz.
- Raporlara yanlis bakiye tasinir.

---

## Adim 16 - Konsolidasyon Kurulumu ve Raporlari

Kurulum ekrani:
- `Ayarlar > Konsolidasyon Kurulumu`

Rapor ekrani:
- `Donem Sonu Islemler > Yillik > Konsolidasyon Raporlari`

Asgari gerekli kurulum:
1. Consolidation group
2. Group memberlar (`Legal Entity (Bagli Ortak)` ekleme)
3. Gerekirse CoA mapping
4. Run olusturma ve calistirma

Yapilmazsa:
- Bilanco ve gelir tablosu konsolide alinmaz.
- Grup resmi raporlama eksik kalir.

---

## Adim 17 - Hesap Yeniden Siniflandirma (Bakiye Dagitimi / Islem Bazli)

Ekran:
- `Ayarlar > Hesap Yeniden Siniflandirma`
- URL: `/app/ayarlar/hesap-yeniden-siniflandirma`

Bu ekran 2 akis sunar:
1. `Bakiye Dagitimi Olustur` (hesap bakiyesini alt hesaplara dagitma)
2. `Islem Bazli Yeniden Siniflandirma` (tek tek fis satiri esleme)

### A) Bakiye Dagitimi Olustur

Ne doldurulur?
1. `Legal Entity (Bagli Ortak)` secin.
2. `Book` secin.
3. `Fiscal period` secin.
4. `Source account (direct != 0)` secin.
5. Dagitim tipini secin:
   - `Yuzdeye gore (PERCENT)`
   - `Tutara gore (AMOUNT)`
6. En az 1 `Hedef hesap` satiri ekleyin.
7. Dagitim degerlerini girin:
   - PERCENT modunda toplam yuzde = `100` olmali.
   - AMOUNT modunda toplam tutar = `Dagitilacak tutar` olmali.
8. `Entry date`, `Document date`, `Currency` alanlarini kontrol edin.
9. Gerekirse `Aciklama`, `Referans no`, `Run notu` girin.
10. `Yeniden siniflandirma taslagi olustur` butonuna basin.

Sistem ne yapar?
1. Kaynak bakiyeyi tersleyip hedef hesaplara dagitan tek bir taslak yevmiye olusturur.
2. Islemi `Son Yeniden Siniflandirma Runlari` listesine kaydeder.
3. Olusan `Journal No / Journal Id` bilgisini listede gosterir.

### B) Islem Bazli Yeniden Siniflandirma

Ne doldurulur?
1. Ust kisimda yine `Legal Entity`, `Book`, `Fiscal period`, `Source account` secili olmali.
2. Gerekirse `dateFrom`, `dateTo`, `limit` filtrelerini girin.
3. `Kaynak satirlari yukle` butonuna basin.
4. Yeniden siniflandirilacak satirlari secin.
5. Her secili satir icin bir `Hedef hesap` secin.
6. `Secili satir` ve `Eslenen` sayisi esit oldugunda
   `Islem bazli yeniden siniflandirma taslagi olustur` butonuna basin.

Sistem ne yapar?
1. Secilen her kaynak satiri tersleyip secilen hedef hesapta yeni satirlar olusturur.
2. Taslak yevmiye olusturur ve run kaydina ekler.

Kontrol listesi:
1. `Son Yeniden Siniflandirma Runlari` alaninda yeni kayit gorunuyor mu?
2. `Journal Workbench` ekraninda ilgili taslak fis aciliyor mu?
3. Gerekli inceleme sonrasi fis post edildi mi?

Yapilmazsa:
1. Hesaplar arasi bakiye dagitimi manuel fisle yapilir.
2. Manuel dagitimda hata ve atlanan satir riski artar.

---

## 5) En Cok Karsilasilan Durumlar ve Cozumler

Durum:
- `No periods found for selected filters`

Cozum:
1. Takvim dogru mu kontrol et.
2. Mali yil dogru mu kontrol et.
3. `Reload Periods` bas.

Durum:
- `Intercompany disabled` hatasi

Cozum:
1. `Legal Entity (Bagli Ortak)` kaydinda `Intercompany enabled` acik olmali.
2. Kapaliysa policy geregi engellenir.

Durum:
- `Partner required` hatasi

Cozum:
1. SourceType `INTERCOMPANY` ise tum satirlarda Counterparty LE doldur.

Durum:
- `subledgerReferenceNo is required` hatasi

Cozum:
1. Satirda secilen birim `Has subledger = Evet` ise `Subledger Ref` girin.
2. `Subledger Ref` girip birim secmediyseniz once birim secin.

Durum:
- `commitmentDebitSubAccountId is required when committedCapital is greater than 0` hatasi

Cozum:
1. `Organizasyon Yonetimi > Shareholders` ekraninda ortak kartinda `Commitment debit sub-account` secin.
2. Bu hesap equity tipinde, aktif ve post edilebilir bir alt hesap olmali (tipik TR: `501.xx`).
3. Ortak kaydini tekrar kaydedin.

Durum:
- `capitalSubAccountId is required when committedCapital is greater than 0` hatasi

Cozum:
1. Ortak kartinda `Committed capital` 0'dan buyukse `Capital sub-account` secin.
2. Bu hesap equity tipinde, aktif ve post edilebilir bir alt hesap olmali.

Durum:
- Sermaye taahhut akisi icin ekranda "Setup Required List" eksik gorunuyor

Cozum:
1. Ortak tanimi, ortak bazli borc/sermaye alt hesaplari, equity alt hesap ve mali donem kalemlerini tamamlayin.
2. Ekrandaki yonlendirme butonlariyla ilgili setup ekranina gecin.

Durum:
- `Queued shareholders contain mixed currencies` (toplu taahhut onizlemede)

Cozum:
1. Toplu taahhut fisini para birimine gore ayri ayri olusturun.
2. Ayni batch icine farkli currency kodlu ortaklari birlikte koymayin.

Durum:
- `No OPEN book/fiscal period found for legalEntityId` veya `commitmentDate must be within an OPEN fiscal period`

Cozum:
1. Secili legal entity icin acik donem oldugunu kontrol edin.
2. `Taahhut tarihi`ni acik mali donem araligina alin.

Durum:
- `No shareholder has a positive journalizable commitment delta` (toplu taahhut onizlemede)

Cozum:
1. Delta mantigini kontrol edin: `committed_capital - already_journaled_amount`.
2. Delta 0 veya negatifse ortak batchte atlanir; yeni artis tutari girin veya ilgili ortaklari kuyruktan cikarip tekrar deneyin.

Durum:
- `Active pair mapping required` hatasi

Cozum:
1. Kaynak -> partner active pair olustur.
2. Otomatik mirror istiyorsan partner -> kaynak yonunu de active yap.

Durum:
- `Journal is not balanced`

Cozum:
1. Toplam borc = toplam alacak olmali.
2. Satirlari tekrar kontrol et.

Durum:
- Intercompany fis olustu ama partnerda post olmadi

Cozum:
1. Post ekraninda `Post linked intercompany mirrors` kutusunu isaretleyerek post et.

Durum:
- `Incorrect arguments to mysqld_stmt_execute` hatasi (Hesap Yeniden Siniflandirma sayfasinda run listesi/satir yukleme asamasinda)

Cozum:
1. Backend guncel kodla calisiyor mu kontrol edin (reclassification sorgularinda `LIMIT ?` yerine dogrudan sayisal limit kullanilmali).
2. Backend servisini yeniden baslatin.
3. Sayfayi yenileyip islemi tekrar deneyin.

---

## 6) Gunluk Pratik Is Akisi (Muhasebe Ekibi Icin)

Her gun:
1. Yeni fisleri taslak olustur.
2. Intercompany olanlarda counterparty alanini mutlaka doldur.
3. Gerekli kontrollerden sonra post et.

Haftalik:
1. Intercompany compliance bolumunu ac.
2. Cikan sorunlari aninda duzelt.

Aylik kapanis oncesi:
1. Intercompany mutabakat raporunu calistir.
2. Fark varsa once kaynagini duzelt.
3. Sonra konsolidasyon raporlarini al.

---

## 7) Roller ve Yetki (Neden Bazen Buton Gorunmuyor?)

Bazi ekranlar yetkiye baglidir.

Ornek:
1. Sayfayi gorebiliyorsunuz ama `Save` calismiyor.
2. Sol menude ilgili modulu hic goremiyorsunuz.

Bu durumda sorun genelde veri degil, yetkidir.
Sistem yoneticisinden rol/yetki atamasi isteyin.

---

## 8) Kisa Karar Rehberi

Soru:
- "Hizli kurulum mu manuel kurulum mu?"

Cevap:
1. Hedef hizli baslangic ise: Sirket Ayarlari (bootstrap)
2. Hedef kontrollu ve denetlenebilir kurulum ise: Manuel yol (bu kilavuz)

Soru:
- "Intercompanyde otomatik partner fis acmali miyim?"

Cevap:
1. Evet, operasyonel olarak daha saglikli.
2. Ama post asamasinda birlikte post etmeyi unutma.

---

## 9) Ozet

Bu kilavuza gore kurulum yapildiginda:
1. Organizasyon yapiniz temiz kurulur.
2. Defter ve hesap plani stabil calisir.
3. Intercompany kontrolleri ve otomatik mirror akisiniz devreye girer.
4. Mutabakat ve konsolidasyon raporlarinda hata riski ciddi azalir.

Isterseniz bir sonraki adimda bu dokumani:
1. Ekran goruntulu PDF formatina
2. "Yeni baslayan personel egitim notu" formatina
donusturebilirim.
