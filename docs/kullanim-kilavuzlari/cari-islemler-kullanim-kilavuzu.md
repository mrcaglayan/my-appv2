# Cari İşlemler (Current Accounts) Kullanım Kılavuzu

Bu kılavuz, uygulamayı kullanan finans, muhasebe ve operasyon ekipleri içindir. Teknik geliştirme detayı içermez.

## 1. Kapsam

Bu doküman aşağıdaki Cari modül ekranlarının kullanımını açıklar:

- `Alıcı Kartı Oluştur (Alici Karti Olustur)`
- `Alıcı Kartı Listesi (Alici Karti Listesi)`
- `Satıcı Kartı Oluştur (Satici Karti Olustur)`
- `Satıcı Kartı Listesi (Satici Karti Listesi)`
- `Cari Raporları (Cari Reports)`
- `Cari Belgeler`
- `Cari Mahsuplaştırma / Tahsilat-Ödeme`
- `Cari Denetim İzleri`

Ayrıca ödeme koşulu hazırlığı, belge/mahsuplaştırma/audit operasyonu, günlük kullanım, ay sonu kullanımı ve sık hatalar için pratik öneriler içerir.

## 2. Kısa Sözlük

- `Counterparty (Cari Kart)`: Müşteri, tedarikçi veya ikisi birden olabilen ticari hesap kartı.
- `Customer (Müşteri / Alıcı)`: Sizden tahsilat beklediğiniz taraf.
- `Vendor (Tedarikçi / Satıcı)`: Sizin ödeme yaptığınız taraf.
- `Payment Term (Ödeme Koşulu)`: Vade kuralı. Örn. Net 30.
- `AR Aging (Alacak Yaşlandırma)`: Tahsil edilmesi gereken alacakların vade durumunu gösterir.
- `AP Aging (Borç Yaşlandırma)`: Ödenecek borçların vade durumunu gösterir.
- `Open Items (Açık Kalemler)`: Henüz kapanmamış (tam tahsil/ödeme olmamış) kalemler.
- `Counterparty Statement (Cari Ekstre)`: Belirli bir cari için belge, mahsup/tahsilat ve bakiye hareket özeti.
- `As-Of Date (Kesit Tarihi)`: Raporun “bu tarih itibarıyla” görünmesini sağlayan tarih.

## 3. Başlamadan Önce

Cari modülünü sorunsuz kullanmak için aşağıdakiler hazır olmalıdır:

1. Yetkiler atanmış olmalı (rolünüze göre):
- Kart ekranları için:
  - `cari.card.read`
  - `cari.card.upsert`
- Rapor ve settlement preview için:
  - `cari.report.read`
- Cari Belgeler için:
  - `cari.doc.read`
  - `cari.doc.create`
  - `cari.doc.update`
  - `cari.doc.post`
  - `cari.doc.reverse`
  - opsiyonel: `cari.fx.override`
- Cari Mahsuplaştırma / Tahsilat-Ödeme için:
  - `cari.settlement.apply`
  - `cari.settlement.reverse`
  - `cari.bank.attach`
  - `cari.bank.apply`
- Cari Denetim İzleri için:
  - `cari.audit.read`

2. Şirket/organizasyon temel kurulumu yapılmış olmalı:
- `Ayarlar > Şirket Ayarları (Sirket Ayarlari)` içindeki `Run Company Bootstrap` veya
- `Ayarlar > Organizasyon Yönetimi (organizasyon-yonetimi)` ile şirket/elverişlilik kayıtları.

3. İlgili `Legal Entity (Yasal Şirket)` için en az bir `Payment Term (Ödeme Koşulu)` olmalı.
- Ödeme koşulu yoksa kart ekranındaki varsayılan ödeme koşulu alanı boş kalır.

## 4. Menüden Erişim

- `Cari İşlemler > Alıcı Kartı Oluştur`
- `Cari İşlemler > Alıcı Kartı Listesi`
- `Cari İşlemler > Satıcı Kartı Oluştur`
- `Cari İşlemler > Satıcı Kartı Listesi`
- `Cari İşlemler > Cari Raporları`
- `Cari İşlemler > Cari Belgeler`
- `Cari İşlemler > Cari Mahsuplaştırma / Tahsilat-Ödeme`
- `Cari İşlemler > Cari Denetim İzleri`

## 5. Alıcı Kartı Oluştur (Alici Karti Olustur)

### Ne zaman kullanılır?

- Yeni müşteri ile çalışmaya başladığınızda.
- Müşteri bilgileri, vade ve iletişim verisi güncelleneceğinde.

### Neden önemlidir?

- Tahsilat planı, yaşlandırma raporu ve cari ekstre bu karta bağlıdır.
- Yanlış kart bilgisi tahsilat takibini zorlaştırır.

### Adım adım kullanım

1. `Legal Entity` seçin.
2. `Code` ve `Name` girin.
3. Rol alanında `Customer` işaretli olmalı.
4. Gerekliyse `Default Currency` ve `Default Payment Term` seçin.
5. İletişim (`Contacts`) ve adres (`Addresses`) girin.
6. Ana iletişim/adres için `isPrimary` işaretleyin.
7. `Save` ile kaydedin.

### Gerçek hayat örneği

Durum:
- ABC Dağıtım adlı yeni bir müşteri ile çalışmaya başlandı.
- Standart vade: Net 30 gün.

Yapılacak:
- `Alıcı Kartı Oluştur` ekranında:
  - `Code`: `ABC_DIST`
  - `Name`: `ABC Dagitim`
  - `Default Payment Term`: `NET30`
  - Ana e-posta ve fatura adresi girilir.

Beklenen sonuç:
- Bu müşteriye ait ileride oluşacak alacaklar `AR Aging` ve `Counterparty Statement` raporlarında doğru görünür.

## 6. Satıcı Kartı Oluştur (Satici Karti Olustur)

### Ne zaman kullanılır?

- Yeni tedarikçi ile satın alma/ödeme süreci başlamadan önce.

### Neden önemlidir?

- Vadesi gelen borçları (`AP Aging`) doğru yönetebilmek için satıcı kartının doğru olması gerekir.

### Adım adım kullanım

1. `Legal Entity` seçin.
2. `Code` ve `Name` girin.
3. Rol alanında `Vendor` işaretli olmalı.
4. Varsa `Default Payment Term` seçin.
5. İletişim ve adres bilgilerini ekleyin.
6. Kaydedin.

### Gerçek hayat örneği

Durum:
- XYZ Ambalaj’dan düzenli malzeme alınıyor.
- Ödeme koşulu Net 45.

Yapılacak:
- `Satıcı Kartı Oluştur` ekranında `Vendor` kartı açılır, `NET45` atanır.

Beklenen sonuç:
- Borç vadesi `AP Aging` raporunda doğru bucket’a düşer ve ödeme planı doğru yapılır.

## 7. Kart Listeleri ve Kart Güncelleme

Ekranlar:
- `Alıcı Kartı Listesi`
- `Satıcı Kartı Listesi`

### Ne yapılır?

- Kart arama (`q`), durum (`ACTIVE/INACTIVE`), şirket (`Legal Entity`) ve rol filtresi.
- Kart detayını açıp güncelleme.

### Ne zaman kartı pasif (`INACTIVE`) yapmalıyım?

- Cari ile çalışmıyorsanız ve yeni işlem açılmasını istemiyorsanız.
- Geçmiş kayıtlar korunur, raporlar geçmişi görmeye devam eder.

### Gerçek hayat örneği

Durum:
- Eski tedarikçi ile çalışma tamamen bitti.

Yapılacak:
- `Satıcı Kartı Listesi` > ilgili kart > `Status = INACTIVE`.

Sonuç:
- Operasyonel listelerde aktif satıcılar sadeleşir, yanlış seçim riski azalır.

## 8. Cari Raporları (Cari Reports)

Rapor ekranında ana filtreler:

- `As-Of Date (Kesit Tarihi)`
- `Legal Entity`
- `Counterparty`
- `Role (CUSTOMER/VENDOR/BOTH)`
- `Status`

En kritik filtre: `As-Of Date`.
Bu tarih değiştiğinde rapor sonucu da değişir. Ay kapanışı, geçmiş tarih doğrulaması ve denetim için mutlaka bu alan bilinçli kullanılmalıdır.

### 8.1 AR Aging (Alacak Yaşlandırma)

Soru:
- “Hangi müşteriden ne kadar alacağım var, ne kadarı gecikmiş?”

Ne zaman kullanılır?
- Günlük tahsilat toplantısı.
- Haftalık alacak risk değerlendirmesi.

Nasıl yorumlanır?
- `CURRENT`: vadesi gelmemiş.
- `1-30`, `31-60`, `61-90`, `91+`: gecikme arttıkça risk artar.

Gerçek hayat örneği:
- 91+ günde biriken müşteri kalemleri için satış + finans birlikte aksiyon planı yapar:
  - yeni sevkiyat limiti,
  - ödeme planı,
  - telefon/e-posta takibi.

### 8.2 AP Aging (Borç Yaşlandırma)

Soru:
- “Hangi satıcıya ne kadar, ne zaman ödeme yapmalıyım?”

Ne zaman kullanılır?
- Haftalık ödeme planı.
- Nakit çıkış planlaması.

Gerçek hayat örneği:
- Önümüzdeki hafta nakit kısıtlıysa:
  - önce vadesi geçen kritik tedarikçilere ödeme,
  - sonra CURRENT kalemler için yeni tarih planlama.

### 8.3 Open Items (Açık Kalemler)

Soru:
- “Hangi belgeler tamamen kapanmamış?”

Ne zaman kullanılır?
- Mutabakat öncesi.
- Kısmi tahsilat/ödeme sonrası kalan bakiye kontrolünde.

Neye bakılır?
- `residual/open balance (kalan açık bakiye)`
- `partially settled (kısmi kapanmış)` kalemler
- varsa banka bağlantı bilgisi (`bank-link`) ve referanslar

Gerçek hayat örneği:
- Bir faturanın %70’i tahsil edildi.
- `Open Items`’ta kalan %30 bakiye açık kalem olarak görünür.
- Tahsilat ekibi sadece kalan kısım için takip yapar.

### 8.4 Counterparty Statement (Cari Ekstre)

Soru:
- “Bu cari ile hangi belgeler ve kapanış hareketleri olmuş, kalan durum nedir?”

Ne zaman kullanılır?
- Cari mutabakatı gönderirken.
- “Bu bakiye neden böyle?” sorusuna cevap verirken.

Neye bakılır?
- Belge satırları (fatura vb.)
- Kapanış/mahsup bağlantıları
- Ters kayıt etkileri (reversal)
- Toplamların mutabakatı

Gerçek hayat örneği:
- Müşteri “bu fatura zaten kapandı” diyor.
- `Counterparty Statement` üzerinden ilgili belge ve ona bağlı kapanış bağlantısı gösterilir.
- Gerekirse ters kayıt tarihleriyle birlikte açıklama yapılır.

## 9. Ödeme Koşulu (Payment Term) Yönetimi

### 9.1 Varsayılan koşulları toplu oluşturma

Ekran:
- `Ayarlar > Şirket Ayarları` içindeki `Run Company Bootstrap`.

Ne sağlar?
- Kurulum sırasında şirketler için varsayılan ödeme koşulları da oluşturulur.

### 9.2 Organizasyon Yönetimi üzerinden

Ekran:
- `Ayarlar > Organizasyon Yönetimi`.

Kritik seçenekler:
- `autoProvisionDefaults`: Açık ise varsayılan ödeme koşulları otomatik oluşturulur.
- `Use custom payment terms (JSON)`: Açılırsa şirkete özel ödeme koşulları yüklenir.

Örnek özel ödeme koşulu JSON:

```json
[
  {
    "code": "NET30",
    "name": "Net 30",
    "dueDays": 30,
    "graceDays": 0,
    "isEndOfMonth": false,
    "status": "ACTIVE"
  },
  {
    "code": "NET45",
    "name": "Net 45",
    "dueDays": 45,
    "graceDays": 0,
    "isEndOfMonth": false,
    "status": "ACTIVE"
  }
]
```

Ne zaman custom kullanılır?
- Farklı ülkelerde farklı vade yapıları varsa.
- Grup standartlarından ayrışan şirket politikası varsa.

## 10. Günlük, Haftalık, Aylık Pratik Kullanım Akışı

### Günlük

1. `AR Aging` aç, gecikmiş müşteri kalemlerini filtrele.
2. `Open Items` ile bugün kapanması beklenen kalemleri kontrol et.
3. Önemli müşteri/satıcı kart güncellemelerini listelerden yap.

### Haftalık

1. `AP Aging` ile haftalık ödeme listesi çıkar.
2. `Counterparty Statement` ile büyük cari hesapları mutabakata hazırla.
3. Pasif edilmesi gereken eski cari kartları gözden geçir.

### Ay sonu

1. `As-Of Date` ay sonu tarihi seçilerek tüm raporları tekrar çalıştır.
2. `Open Items` toplamları ile `Statement` toplamlarını karşılaştır.
3. Kritik farkları cari bazında ekstreye inerek çöz.

## 11. Sık Karşılaşılan Durumlar ve Çözüm

### “Varsayılan ödeme koşulu listesi boş”

Neden:
- İlgili `Legal Entity` için ödeme koşulu yoktur.

Çözüm:
- `Run Company Bootstrap` çalıştırın veya
- `Organizasyon Yönetimi` ekranında `autoProvisionDefaults` ile yeniden provisioning yapın.

### “Rapor beklediğimden farklı çıktı”

Kontrol listesi:
1. `As-Of Date` doğru mu?
2. `Legal Entity` filtresi doğru mu?
3. `Role` ve `Status` filtreleri doğru mu?

### “Bu kartı herkes göremiyor”

Neden:
- Kullanıcının yetki/scope (kapsam) ataması sınırlı olabilir.

Çözüm:
- Sistem yöneticisinden ilgili role uygun yetki ve şirket kapsamı atamasını isteyin.

## 12. İyi Kullanım Önerileri

- Kodları (`Code`) standartlaştırın: örn. `MUSTERI_` veya `TEDARIKCI_` önekleri.
- Her kartta en az bir güncel iletişim ve birincil adres tutun.
- Pasif kartları silmek yerine `INACTIVE` yapın.
- Rapor yorumlarken her zaman `As-Of Date` bilgisini not edin.
- Mutabakat süreçlerinde tek kaynağı `Counterparty Statement` olarak kullanın.

## 13. Hızlı Senaryo Rehberi

### Senaryo A: Yeni müşteri açılışı ve tahsilat takibi

1. `Alıcı Kartı Oluştur` ile kart aç.
2. `Default Payment Term` ata.
3. İşlem sonrası `AR Aging` ve `Open Items` ile izlemeye al.

Neden:
- Başlangıçtan itibaren vade/tahsilat görünürlüğü sağlanır.

### Senaryo B: Nakit sıkışıklığında ödeme önceliği

1. `AP Aging` çalıştır.
2. `91+` ve `31-60` gecikmiş kritik tedarikçileri belirle.
3. Ödemeleri önceliklendir, kalanları planla.

Neden:
- Operasyon sürekliliği için kritik satıcılar korunur.

### Senaryo C: Cari mutabakat farkı çözümü

1. `Counterparty Statement` ile ilgili cariyi aç.
2. Tartışmalı belgeyi bul.
3. Bağlı kapanış hareketleri ve tarihleriyle birlikte kontrol et.

Neden:
- “Toplam farkı” değil “satır bazında neden” görülebilir.

---

Bu kılavuz son kullanıcı odaklıdır ve günlük kullanım içindir. Yetki/scope, şirket kurulumu ve muhasebe politikası gibi yönetimsel konular için ilgili yönetici ekranları ve şirket içi süreçler esas alınmalıdır.

## 14. Yeni Cari Ekranlar (PR-11..14)

Bu bölüm, kod bilmeyen son kullanıcılar için yeni Cari ekranlarının pratik kullanımını anlatır:

- `Cari Belgeler` (`/app/cari-belgeler`)
- `Cari Mahsuplaştırma / Tahsilat-Ödeme` (`/app/cari-settlements`)
- `Cari Denetim İzleri` (`/app/cari-audit`)

## 15. Cari Belgeler Ekranı (`/app/cari-belgeler`)

### 15.1 Ekran ne için kullanılır?

- Müşteri/satıcı belgelerini (fatura, dekont benzeri cari belgeler) taslak olarak oluşturmak.
- Taslak belgeyi güncellemek veya iptal etmek.
- Taslağı muhasebeleştirerek `POSTED` duruma geçirmek.
- `POSTED` belgeyi ters kayıtla geri çevirmek (`REVERSE`).

### 15.2 Liste filtreleri: hangi alan ne işe yarar?

| Alan | Ne için kullanılır? | Boş bırakılırsa ne olur? |
|---|---|---|
| `Legal Entity ID` | Hangi yasal şirketin belgelerini görmek istediğinizi belirler. | Yetkiniz olan tüm şirket kapsamından sonuç gelebilir; liste kalabalık olabilir. |
| `Counterparty ID` | Belirli bir cari kartın belgelerini filtreler. | Tüm cariler gelir. |
| `Direction (AR/AP)` | `AR`: alacak, `AP`: borç yönünü ayırır. | Her iki yön de gelir. |
| `Document Type` | Belge türüne göre filtreler (`INVOICE`, `DEBIT_NOTE`, `CREDIT_NOTE`, `PAYMENT`, `ADJUSTMENT`). | Tüm türler gelir. |
| `Status` | Belge yaşam döngüsüne göre filtreler (`DRAFT`, `POSTED`, vb.). | Tüm statüler gelir. |
| `Date From` / `Date To` | Belge tarih aralığına göre filtreler. | Tarih kısıtı uygulanmaz. |
| `Search` | Belge numarası/cari snapshot bilgisi ile hızlı arama yapar. | Arama kısıtı uygulanmaz. |

### 15.3 Taslak belge oluşturma: alan alan açıklama

| Alan | Zorunlu mu? | Ne için? | Girilmezse / yanlışsa ne olur? |
|---|---|---|---|
| `Legal Entity ID` | Evet | Belgenin ait olduğu yasal şirket. | Kayıt engellenir (`legalEntityId is required`). |
| `Counterparty ID` | Evet | Belgenin bağlı olduğu müşteri/tedarikçi. | Kayıt engellenir (`counterpartyId is required`). |
| `Payment Term ID` | Hayır | Vade kuralını otomatik bağlar. | Boş ise backend varsayılan kurallarla devam eder. |
| `Direction` | Evet | `AR` (alacak) veya `AP` (borç). | Yanlış/boş ise kayıt engellenir. |
| `Document Type` | Evet | Belgenin türü. | Yanlış/boş ise kayıt engellenir. |
| `Document Date` | Evet | Belgenin işlem tarihi. | Boş ise kayıt engellenir. |
| `Due Date` | Türe bağlı | `INVOICE` ve `DEBIT_NOTE` için vade tarihi gerekir. | Bu türlerde boşsa kayıt engellenir. |
| `Amount Txn` | Evet | İşlem para birimindeki tutar. | `0` veya negatif olamaz, kayıt engellenir. |
| `Amount Base` | Evet | Ana para birimindeki karşılık. | `0` veya negatif olamaz, kayıt engellenir. |
| `Currency` | Evet | 3 harfli para kodu (örn. `USD`, `TRY`, `EUR`). | Geçersiz formatta ise kayıt engellenir. |
| `FX Rate` | Hayır | Kur bilgisi; bazı durumlarda post aşamasında gerekir. | Girilmişse `> 0` olmalıdır; aksi durumda kayıt engellenir. |

### 15.4 Durum bazlı aksiyon kuralları

- `Update Draft Document`: sadece `DRAFT` ve `cari.doc.update` yetkisi varsa.
- `Cancel Draft`: sadece `DRAFT` ve `cari.doc.update` yetkisi varsa.
- `Post Draft`: sadece `DRAFT` ve `cari.doc.post` yetkisi varsa.
- `Reverse Posted Document`: sadece `POSTED` ve `cari.doc.reverse` yetkisi varsa.

### 15.5 FX override (post sırasında)

- `useFxOverride` işaretlenirse kullanıcıda `cari.fx.override` yetkisi olmalıdır.
- `useFxOverride=true` ise `fxOverrideReason` girilmesi zorunludur.
- Yetki yoksa kullanıcı açık bir uyarı görür ve işlem gönderilmez.

### 15.6 Detay paneli nasıl okunur?

- `documentNo`: belge numarası.
- `status`: güncel durum.
- `postedJournalEntryId`: post sonrası oluşan yevmiye referansı.
- `reversalOfDocumentId`: bu belge bir ters kayıt belgesiyse, hangi belgenin ters kaydı olduğunu gösterir.
- Snapshot alanları (`counterparty*Snapshot`, `dueDateSnapshot`, `currencyCodeSnapshot`, `fxRateSnapshot`):
  - İşlem anındaki fotoğraf bilgisidir.
  - Cari kart sonradan değişse bile geçmiş belge kaydı denetim için korunur.

### 15.7 Gerçek hayat örnekleri

Örnek A: Müşteri faturası oluşturma ve post etme
1. `Direction=AR`, `DocumentType=INVOICE`, `Due Date` dolu.
2. Taslak oluşturulur (`DRAFT`).
3. Kontrol sonrası `Post Draft` yapılır.
4. Sonuçta `postedJournalEntryId` oluşur.

Örnek B: Hatalı post edilen belgenin geri alınması
1. Belge `POSTED` durumdadır.
2. `Reverse reason` yazılır, gerekirse `reversalDate` girilir.
3. `Reverse Posted Document` ile ters kayıt oluşur.
4. Ekranda reversal bağlantıları görünür.

## 16. Cari Mahsuplaştırma / Tahsilat-Ödeme (`/app/cari-settlements`)

### 16.1 Ekran ne için kullanılır?

Bu ekran 4 ayrı iş akışını tek yerde toplar:

1. `Settlement Apply`
2. `Settlement Reverse`
3. `Bank Attach`
4. `Bank Apply`

Not:
- Sayfaya giriş yetkisi "any-of" olabilir.
- Ama her panelin düğmeleri kendi özel yetkisine göre çalışır.

### 16.2 Üstteki önizleme alanları (Open Items Preview)

| Alan | Ne için? | Boş/yanlış olursa ne olur? |
|---|---|---|
| `Legal Entity ID` | Açık kalemlerin hangi şirkette aranacağını belirler. | Önizleme satırı çıkmaz. |
| `Counterparty ID` | Hangi cari için açık kalemlerin çekileceğini belirler. | Önizleme satırı çıkmaz. |
| `Direction` | `AR` veya `AP` yönüne göre ayrım yapar. | Özellikle auto-allocate için yön zorunlu hale gelir. |
| `As-Of Date` | Açık kalemleri belirli tarih kesitine göre hesaplar. | Boşsa önizleme yüklenmez. |

Ek not:
- Kullanıcıda `cari.report.read` yoksa önizleme görünmez.
- Bu durumda settlement/bank aksiyonları, ilgili aksiyon yetkileri varsa yine yapılabilir.

### 16.3 Settlement Apply: alan alan açıklama

| Alan | Zorunlu mu? | Ne için? | Girilmezse / yanlışsa ne olur? |
|---|---|---|---|
| `Settlement Date` | Evet | Mahsuplaştırma/tahsilat-ödeme tarihi. | İşlem gönderilmez. |
| `Currency` | Evet | İşlem para birimi. | İşlem gönderilmez. |
| `Incoming Amount Txn` | Evet | Kapanacak/dağıtılacak toplam tutar. | `0`/geçersiz ise işlem gönderilmez. |
| `FX Rate` | Duruma bağlı | Kur bilgisi. Bazı kur politikalarında zorunlu hale gelebilir. | Eksikse backend hata dönebilir. |
| `Note` | Hayır | Operasyon notu. | Boş bırakılabilir. |
| `Idempotency Key` | Teknik olarak zorunlu, UI otomatik üretir | Aynı isteğin tekrarında çift kayıt oluşmasını önler. | Boşsa UI otomatik üretir. |
| `autoAllocate` | Seçim | Sistem açık kalemlere otomatik dağıtım yapar. | Kapalıysa manuel allocation girmek gerekir. |
| `useUnappliedCash` | Seçim | Varsa önceden oluşmuş `unapplied cash` bakiyesini kullanır. | Kapalıysa bu bakiye kullanılmaz. |

Auto-allocate açıkken:
- `Direction` seçilmelidir.
- Önizlemede karışık yön (`AR` + `AP`) varsa işlem bloklanır.

Auto-allocate kapalıyken:
- Tablodaki `manual amount` girişleri ile allocation yapılmalıdır.
- Hiç allocation yoksa işlem gönderilmez.
- Girilen allocation, ilgili açık kalem bakiyesini aşamaz.

### 16.4 `paymentChannel` ve linked cash akışı (`MANUAL` / `CASH`)

`Settlement Apply` artık iki ödeme kanalını destekler:

| Seçenek | Ne yapar? | Ne zaman kullanılır? |
|---|---|---|
| `paymentChannel=MANUAL` | Sadece cari settlement kaydı üretir. | Nakit işlemi sistem dışında yapıldıysa veya bu ekrandan kasa kaydı açmak istenmiyorsa. |
| `paymentChannel=CASH` + `cashTransactionId` | Var olan kasa işlemini settlement ile bağlar. | Tahsilat/tediye kaydı zaten kasada açıldıysa. |
| `paymentChannel=CASH` + `linkedCashTransaction` | Settlement apply ile birlikte yeni kasa işlemi üretir. | Cari-öncelikli çalışıp, aynı adımda kasaya da kayıt düşmek istendiğinde. |

Yön kuralı:
- `Direction=AR` ise linked cash işlemi `RECEIPT` olarak açılır.
- `Direction=AP` ise linked cash işlemi `PAYOUT` olarak açılır.

`linkedCashTransaction` alan seti (özet):

| Alan | Zorunlu mu? | Not |
|---|---|---|
| `registerId` | Evet (`paymentChannel=CASH` ve `cashTransactionId` yoksa) | Kasa fişinin açılacağı register. |
| `counterAccountId` | Evet (`paymentChannel=CASH` ve `cashTransactionId` yoksa) | Karşı muhasebe hesabı. |
| `cashSessionId` | Duruma bağlı | Verilmezse uygun açık session bulunur; register politikası gerektiriyorsa açık session şarttır. |
| `txnDatetime`, `bookDate`, `referenceNo`, `description` | Hayır | Operasyonel detay alanları. |
| `idempotencyKey`, `integrationEventUid` | Güçlü öneri | Tekrarlı gönderimlerde çift kasa kaydını önler. |

Kritik doğrulamalar:
- `linkedCashTransaction` yalnızca `paymentChannel=CASH` iken kullanılabilir.
- `cashTransactionId` ile `linkedCashTransaction` aynı istekte birlikte gönderilemez.
- Settlement para birimi, linked cash register para birimi ile uyumlu olmalıdır.

### 16.5 `idempotentReplay` ve `followUpRisks` ne demek?

- `idempotentReplay=true`:
  - Hata değildir.
  - Aynı istek daha önce uygulanmıştır, mevcut sonuç tekrar gösteriliyordur.
  - CASH senaryosunda da aynı kural geçerlidir; yeni bir ikinci kasa fişi oluşturulmaz.
- `followUpRisks`:
  - Operasyon uyarısıdır.
  - Sonuç alınsa bile takip gerektiren risk maddelerini gösterir.
  - Tipik olarak: mapping setup bağımlılığı, FX fallback davranışı, kaynak-bağlamlı posting bilgisi.

### 16.6 Settlement Reverse

| Alan | Zorunlu mu? | Ne için? | Girilmezse / yanlışsa ne olur? |
|---|---|---|---|
| `settlementBatchId` | Evet | Geri çevrilecek settlement kaydı. | Geçersizse reverse yapılamaz. |
| `reversalDate` | Hayır | Ters kayıt tarihi. | Boşsa sistem varsayılan tarih kullanabilir. |
| `reason` | Hayır (önerilir) | Neden ters kayıt yapıldığını açıklar. | Boş geçilebilir, ama denetim için doldurulması önerilir. |

Linked cash kuralı:
- Settlement bir `cashTransactionId` ile bağlıysa ve ilgili kasa fişi `POSTED` durumdaysa, settlement reverse doğrudan bloklanır.
- Bu durumda önce kasa fişi reverse edilmelidir.

### 16.7 Bank Attach (açık ve ayrı iş akışı)

| Alan | Kural |
|---|---|
| `legalEntityId` | Zorunlu |
| `targetType` | `SETTLEMENT` veya `UNAPPLIED_CASH` |
| `bankStatementLineId` / `bankTransactionRef` | En az biri zorunlu |

`targetType=SETTLEMENT` ise:
- `settlementBatchId` zorunlu
- `unappliedCashId` boş olmalı

`targetType=UNAPPLIED_CASH` ise:
- `unappliedCashId` zorunlu
- `settlementBatchId` boş olmalı

`idempotencyKey` boşsa UI otomatik üretir.

### 16.8 Bank Apply (açık ve ayrı iş akışı)

| Alan | Zorunlu mu? | Ne için? |
|---|---|---|
| `legalEntityId` | Evet | Şirket kapsamı |
| `counterpartyId` | Evet | Cari kapsamı |
| `direction` | auto-allocate açıksa evet | AR/AP yönü |
| `settlementDate` | Evet | İşlem tarihi |
| `currencyCode` | Evet | Para birimi |
| `incomingAmountTxn` | Evet | Uygulanacak tutar |
| `bankStatementLineId` veya `bankTransactionRef` | En az biri evet | Banka satırı/referansı |
| `autoAllocate` | Seçim | Otomatik dağıtım |
| `allocations JSON` | auto-allocate kapalıysa evet | Manuel dağıtım listesi |
| `bankApplyIdempotencyKey` | UI otomatik üretebilir | Çift gönderimi önleme |
| `note` | Hayır | Açıklama |

### 16.9 FX fallback davranışı (PR-25)

Settlement apply sırasında kur bulunamadığında iki yaklaşım vardır:

1. `EXACT_ONLY`:
   - Sadece işlem tarihindeki kur aranır.
   - Bulunamazsa, manuel `fxRate` verilmediyse işlem hata döner.
2. `PRIOR_DATE`:
   - İşlem tarihine en yakın önceki kur aranır.
   - `fxFallbackMaxDays` verilirse, sadece bu gün sınırı içindeki kurlar kabul edilir.

Önemli:
- `fxFallbackMaxDays` sadece `fxFallbackMode=PRIOR_DATE` iken kullanılabilir.
- Uygun kur bulunamazsa backend açık hata mesajı döner; sessizce tahmini kur kullanılmaz.

### 16.10 Gerçek hayat örnekleri

Örnek A: Müşteriden kısmi tahsilat (otomatik dağıtım)
1. `Direction=AR`, `autoAllocate=true`.
2. Önizlemede en eski vadeli açık kalemlerden başlanarak beklenen dağıtım görülür.
3. `Apply Settlement` sonrası allocation ve kalan bakiye blokları kontrol edilir.

Örnek B: Tedarikçiye elle parça ödeme (manuel dağıtım)
1. `Direction=AP`, `autoAllocate=false`.
2. Sadece seçilen açık kalemlere `manual amount` girilir.
3. Her satırda girilen tutar açık bakiyeyi aşmamalıdır.

Örnek C: Banka ekstresi ile kayıt eşleme
1. Önce `Bank Attach` ile hedef kayıt ve banka referansı bağlanır.
2. Sonra `Bank Apply` ile tutar uygulanır.
3. Bu iki adım ayrı tutulduğu için yanlış otomatik eşleme riski düşer.

Örnek D: Cari ekranından CASH kanalı ile tahsilat + kasa fişi üretme
1. `paymentChannel=CASH` seçilir.
2. `Create linked cash transaction` açıkken `registerId` ve `counterAccountId` doldurulur.
3. `Apply Settlement` sonrası hem settlement hem linked cash referansı birlikte döner.

Örnek E: `MANUAL` kanalında settlement
1. `paymentChannel=MANUAL` bırakılır.
2. Settlement uygulanır; kasa tarafında yeni fiş oluşmaz.
3. Sonuç sadece cari tarafında izlenir, gerekirse bank attach/apply ayrı yürütülür.

## 17. Cari Denetim İzleri (`/app/cari-audit`)

### 17.1 Ekran ne için kullanılır?

- Kim, ne zaman, hangi kayıtta hangi işlemi yaptı sorusunu cevaplamak.
- Operasyon, destek ve denetim süreçlerinde kanıt (audit trail) sağlamak.

### 17.2 Filtre alanları: ne için, boşsa ne olur?

| Alan | Ne için kullanılır? | Boş bırakılırsa ne olur? |
|---|---|---|
| `legalEntityId` | Belirli şirketi incelemek için. | Kapsamdaki tüm şirketlerden kayıt gelebilir. |
| `action` | Örn. `cari.settlement.apply`, `cari.document.reverse`. | Tüm aksiyonlar gelir. |
| `resourceType` | İşlem yapılan kayıt türü. | Tür filtresi olmaz. |
| `resourceId` | Belirli kaydı nokta atışı bulur. | Kayıt bazlı daraltma olmaz. |
| `actorUserId` | İşlemi yapan kullanıcıyı filtreler. | Tüm kullanıcılar gelir. |
| `requestId` | Tek bir API talebini bulur. | Talep bazlı arama yapılmaz. |
| `createdFrom` / `createdTo` | Tarih aralığı. | Tarih kısıtı uygulanmaz. |
| `includePayload` | Payload içeriğini getirir. | Kapalıysa payload alanı alınmaz, liste daha hızlıdır. |
| `limit` / `offset` | Sayfalama boyutu ve başlangıcı. | Varsayılan değerlerle çalışır. |

Tarih notu:
- `createdFrom` gün başına (`00:00:00.000`),
- `createdTo` gün sonuna (`23:59:59.999`) dönüştürülür.
- Böylece aynı gün içindeki kayıtlar eksik kalmaz.

### 17.3 By-Action özeti nasıl kullanılır?

- Üstteki kartlar her aksiyon için kaç kayıt olduğunu gösterir.
- Ani artışlar (örneğin çok sayıda reverse veya apply) operasyonel inceleme ihtiyacı doğurabilir.

### 17.4 Payload görünümü

- Varsayılan yaklaşım: payload kapalı.
- Sadece gerektiğinde `includePayload` açın.
- Satır bazında `Expand payload` ile detay açılır.
- `requestId` yanında `Copy` düğmesi ile destek kaydına hızlıca yapıştırabilirsiniz.

### 17.5 Gerçek hayat örnekleri

Örnek A: "Bu belgeyi kim reverse etti?"
1. `action = cari.document.reverse`
2. `resourceType` ve gerekirse `resourceId` girin.
3. Sonuç satırında `actor`, `createdAt`, `requestId` bilgilerini alın.

Örnek B: "Bu settlement neden iki kez görünüyor?"
1. Aynı `requestId` ile arayın.
2. Sonuçta `idempotentReplay` ile ilişkili tekrar cevabı olup olmadığını kontrol edin.

Örnek C: "Bugünkü işlemler eksik görünüyor"
1. `createdTo` tarihinin seçili olduğundan emin olun.
2. Sistem gün sonu sınırı kullandığı için gün içi kayıtlar normalde dışarıda kalmamalıdır.

## 18. Kısa Hata Mesajı Rehberi

- `Missing permission: ...`
  - Kullanıcının ilgili aksiyon yetkisi yok.
  - Çözüm: sistem yöneticisinden yetki talep edin.
- `Direction is required for auto-allocation`
  - Otomatik dağıtım açıkken AR/AP yönü seçilmemiş.
- `allocations are required when autoAllocate=false`
  - Otomatik dağıtım kapalıyken manuel dağıtım girilmemiş.
- `bankStatementLineId or bankTransactionRef is required`
  - Banka satırı veya referans bilgisi eksik.
- `FX override requires permission: cari.fx.override`
  - Override işaretlendi ama kullanıcıda gerekli yetki yok.
- `linkedCashTransaction is required when paymentChannel=CASH and cashTransactionId is not provided`
  - CASH kanalı seçildi ama yeni kasa fişi için zorunlu alan seti gönderilmedi.
- `linkedCashTransaction.registerId is required for paymentChannel=CASH`
  - CASH kanalında yeni kasa fişi açarken register seçimi eksik.
- `Settlement cannot be reversed while linked cash transaction ... is POSTED`
  - Settlement ters kaydı, bağlı kasa fişi post edilmiş olduğu için bloklandı.
  - Çözüm: önce bağlı kasa fişini reverse edin.
- `fxRate is required because no exact-date SPOT rate exists ...`
  - İşlem tarihi için kur bulunamadı; fallback ile de uygun prior-date kur gelmedi.
  - Çözüm: geçerli `fxRate` girin veya FX rate setup'ını tamamlayın.

---

Bu bölümde anlatılan adımlar kod bilgisi gerektirmez. Kritik durumlarda, hata mesajı + `requestId` bilgisini birlikte destek ekibine iletmeniz en hızlı çözümü sağlar.

---

## 19. Ekran Bazlı Kart/Buton Rehberi (Tüm Cari Sayfaları)

Bu bolum, Cari modulu ekranlarinda kullanicinin tikladigi kartlar ve butonlarin ne is yaptigini operasyon diliyle ozetler.

### 19.1 Alıcı/Satıcı Kart Ekranları (`/app/alici-*`, `/app/satici-*`)

Ekranlar:
- `Alici Karti Olustur`
- `Alici Kart Listesi`
- `Satici Karti Olustur`
- `Satici Kart Listesi`

Kartlar:
- Kart olusturma/guncelleme formu
- Liste filtre karti
- Liste tablosu + satir bazli `Edit`

Ana butonlar:
- `Create Card`
- `Save Changes`
- `Apply Filters`
- `Clear`
- satirda `Edit`

Kritik davranis:
- `gl.account.read` yoksa AR/AP hesap seciciler gizlenir.
- `role filter` ile sadece customer/vendor/both kartlari daraltabilirsiniz.

Gercek hayat ornegi:
1. Yeni musteri icin alici karti acilir.
2. Role/customer alanlari ve payment term set edilir.
3. Liste ekraninda kod/ad aramasiyla kart bulunur.
4. Tahsilat stratejisi degistiginde `Edit` ile kart guncellenir.

### 19.2 Cari Belgeler (`/app/cari-belgeler`)

Kartlar:
- Filtre karti
- `Create Draft Document` karti
- Belge listesi karti
- `Detail + Actions` karti
  - `Draft Actions`
  - `Post / Reverse`

Ana butonlar:
- Filtrede `Refresh List`, `Reset Filters`
- Taslakta `Create Draft Document`, `Reset Draft Form`
- Listede `View / Actions`
- Draft aksiyonda `Update Draft Document`, `Cancel Draft`
- Post aksiyonda `Post Draft`
- Reverse aksiyonda `Reverse Posted Document`

Kritik davranis:
- Draft duzenleme/iptal yalniz `DRAFT`.
- Post yalniz draft.
- Reverse yalniz posted lifecycle durumlari.
- `useFxOverride` aciksa `cari.fx.override` yetkisi gerekir.

Gercek hayat ornegi:
1. Müşteri faturası taslak acilir ve kontrol edilir.
2. `Post Draft` ile muhasebe kaydi olusur.
3. Tutar/hukuki hata varsa `Reverse Posted Document` ile ters kayit acilir.

### 19.3 Cari Raporları (`/app/cari-raporlari`)

Kartlar:
- Filtre karti (`asOfDate`, entity, counterparty, role, status)
- Tab seciciler:
  - `AR Aging`
  - `AP Aging`
  - `Open Items`
  - `Counterparty Statement`
- Ozet kartlari
- Tab bazli detay tablolar

Ana butonlar:
- Filtrede `Apply Filters`, `Reset`
- Tab butonlari (rapor turu degistirme)

Kritik davranis:
- `Open Items` ve `Statement` tablarinda reconcile bantlari vardir (diff gosterir).
- Bank linked kolonlari dogrudan operasyonel mutabakat icin kullanilir.

Gercek hayat ornegi:
1. Kapanis oncesi `AP Aging` tabinda geciken borclar bulunur.
2. `Open Items` tabinda bank linked ve residual kontrol edilir.
3. Tahsilat ekipleri `Counterparty Statement` ile mutabakat cikarir.

### 19.4 Cari Settlements (`/app/cari-settlements`)

Kartlar:
- Ust baglam/preview karti
- `Settlement Apply`
- `Settlement Reverse`
- `Bank Attach`
- `Bank Apply`
- Apply response bloklari

Ana butonlar:
- `Apply Settlement`
- `Reset Apply Form`
- `Reverse Settlement`
- `Attach Bank Reference`
- `Apply Bank Settlement`
- Apply icinde `Fill All` / `Clear` (open item dagitim gridi)

Kritik davranis:
- `paymentChannel=MANUAL|CASH`
- CASH seciliyse linked cash olusturma alanlari acilir.
- `followUpRisks` ve `idempotentReplay` sonucu operasyonel olarak okunmalidir.

Gercek hayat ornegi:
1. Tahsilat ayni anda kasaya da dusecekse `paymentChannel=CASH` secilir.
2. Open item gridinde faturalara dagitim yapilip `Apply Settlement` tiklanir.
3. Sonuc bloklarinda settlement batch + linked cash referansi kontrol edilir.

### 19.5 Cari Audit (`/app/cari-audit`)

Kartlar:
- Filtre karti
- By-action ozet kartlari
- Audit row listesi

Ana butonlar:
- `Apply Filters`
- `Reset`
- `Prev` / `Next` (sayfalama)
- satirda `Copy` (`requestId`)
- satirda `Expand payload` / `Collapse payload`

Kritik davranis:
- `includePayload` kapaliysa payload cekilmez (performans + guvenlik).
- Incident desteginde en kritik alan: `requestId`.

Gercek hayat ornegi:
1. Kullanici "islem iki kez oldu" bildirir.
2. Audit'te `requestId` ile arama yapilir.
3. `idempotentReplay` / tekrar patterni gorulup olay siniflandirilir.
