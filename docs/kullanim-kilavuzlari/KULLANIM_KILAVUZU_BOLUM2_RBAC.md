# SaaP Kullanim Kilavuzu - Bolum 2 (RBAC ve Kullanici Yetkilendirme)

Bu dokuman, uygulamada "kim neyi yapabilir" konusunu netlestirmek icin hazirlandi.
Odak: kullanici olusturma, rol atama, scope yonetimi, test ve dogrulama.

---

## 1) Temel Kavramlar (Karisan Noktalar)

1. `Tenant`:
   - Bir veri siniri / sirket alani.
   - Rol degildir.

2. `TenantAdmin`:
   - Tenant icindeki bir roldur.
   - "Yeni tenant olusturma" yetkisi degildir.

3. `Provider Admin`:
   - `/provider/login` uzerinden platform seviyesinde calisir.
   - Yeni tenant olusturabilen tek taraftir.

4. `Role`:
   - Yetki paketi (permission seti).

5. `Permission`:
   - Tekil islem izni (ornek: `gl.journal.post`).

6. `Scope`:
   - Yetkinin hangi sinirda gecerli oldugu:
   - `TENANT`, `GROUP`, `COUNTRY`, `LEGAL_ENTITY`, `OPERATING_UNIT`

7. `Data Scope`:
   - Kullanici veri gorunurlugunu ayrica kisitlar.
   - Varsa, pratikte permission scope uzerine ekstra sinir koyar.

---

## 2) RBAC Ekranlari ve Ne Ise Yarar

1. `Ayarlar > Roller ve Yetkiler`
   - Rol olusturur.
   - Role permission matrisi baglar.

2. `Ayarlar > Kullanici Rol Atamalari`
   - Yeni kullanici olusturur.
   - Kullaniciya rol + scope + effect (`ALLOW`/`DENY`) atar.

3. `Ayarlar > Scope Atamalari`
   - Kullanici `data scope` kayitlarini yonetir.
   - Mevcut rol atamasinin scope'unu degistirir.

4. `Ayarlar > RBAC Denetim Loglari`
   - Rol/atama/scope degisikliklerini geriye donuk izler.

---

## 3) On Kosullar

1. Tenant kurulumunun tamam olmasi gerekir (readiness).
2. Sistemde en az bir `TenantAdmin` kullanici olmasi gerekir.
3. RBAC ekranlarina girecek kullanicida ilgili security permissionlari olmali.

---

## 4) Sifirdan RBAC Kurulum Akisi

## Adim 1 - Rol Tasarimini Belirle

Ornek is rolleri:
1. Grup kontroloru
2. Entity muhasebecisi
3. Sube operatoru
4. Sadece okuma (auditor)

Her rol icin suyu yazin:
1. Hangi modulleri acabilir?
2. Hangi islemleri yapabilir? (create/post/upsert vb.)
3. Hangi scope seviyesinde calisacak?

---

## Adim 2 - Rol Olustur

Ekran:
1. `Ayarlar > Roller ve Yetkiler`

Islem:
1. `Role code` gir (ornek: `EntityMuhasebeTR`)
2. `Role name` gir
3. `Rolu Kaydet`

Not:
1. Seed ile gelen sistem rolleri vardir (`TenantAdmin`, `GroupController`, vb.).
2. Operasyonel kullanim icin yeni custom rol olusturmak genelde daha guvenlidir.

---

## Adim 3 - Role Permission Bagla

Ayni ekranda:
1. Sol listeden rolu sec.
2. Permission checkbox'larini sec.
3. `Yetkileri Degistir` ile kaydet.

Ornek (entity muhasebe):
1. `org.tree.read`
2. `org.fiscal_period.read`
3. `gl.book.read`
4. `gl.account.read`
5. `gl.journal.read`
6. `gl.journal.create`
7. `gl.journal.post`

---

## Adim 4 - Kullanici Olustur

Ekran:
1. `Ayarlar > Kullanici Rol Atamalari`

`Yeni Kullanici` bolumunde:
1. Ad Soyad
2. E-posta
3. Sifre (min 8)
4. Durum (`ACTIVE`/`DISABLED`)
5. `Kullaniciyi Olustur`

Beklenen sonuc:
1. Kullanici olusur.
2. Kullanici dropdown listesine gelir.

---

## Adim 5 - Kullaniciya Rol + Scope Ata

Ayni sayfada:
1. Kullanici sec
2. Rol sec
3. `ScopeType` sec (`GROUP`, `LEGAL_ENTITY`, vb.)
4. `Scope` sec (`id` bazli)
5. `Effect` sec (`ALLOW` veya `DENY`)
6. `Ata`

Ornek:
1. Kullanici: `ayse.entity@example.com`
2. Rol: `EntityMuhasebeTR`
3. ScopeType: `LEGAL_ENTITY`
4. Scope: `LE-TR-001`
5. Effect: `ALLOW`

Sonuc:
1. Kullanici bu entity icinde calisir.
2. Diger entitylerde yetkili olmaz.

---

## Adim 6 - Gerekirse Data Scope Ile Daralt

Ekran:
1. `Ayarlar > Scope Atamalari`

Islem:
1. Kullanici sec
2. `Veri Scope'lari`na yeni satir ekle
3. `Kullanici Veri Scope'larini Guncelle`

Ne zaman gerekli?
1. Kullanici rolu genis ama veri gorunurlugunu daha dar tutmak istiyorsaniz.
2. Ozel denetim / segmentasyon ihtiyaci varsa.

---

## Adim 7 - Denetim ve Dogrulama

Ekran:
1. `Ayarlar > RBAC Denetim Loglari`

Kontrol edin:
1. `role.permission.replace`
2. `assignment.create`
3. `assignment.scope_replace`
4. `user.create`

---

## 5) Sistem Rol Kurali (Kritik)

`TenantAdmin` gibi sistem rolleri icin:
1. Atama/degistirme/silme islemlerini sadece tenant-level `TenantAdmin` yapabilir.
2. Sistem rolu olmayan custom rollerde normal atama akisiniz devam eder.

Bu, "yanlislikla herkes TenantAdmin olmasin" diye uygulanir.

---

## 6) Ornek Senaryolar

## Senaryo A - Group bazli kullanici

1. Rol: `GroupController` veya custom grup rolu
2. Scope: `GROUP = G1`
3. Beklenen:
   - G1 altindaki entity/sube verisini gorur.
   - G2 altinda islem yapamaz.

## Senaryo B - Entity bazli muhasebeci

1. Rol: `EntityAccountant` veya custom
2. Scope: `LEGAL_ENTITY = LE1`
3. Beklenen:
   - LE1 icin fis olusturur/post eder.
   - LE2 icin 403 veya bos liste alir.

## Senaryo C - Subede operator

1. Rol: `BranchOperator`
2. Scope: `OPERATING_UNIT = OU1`
3. Beklenen:
   - OU1 odakli islemleri yapar.
   - Diger sube/entity tarafinda yetki yoktur.

---

## 7) 10 Dakikalik Smoke Test Listesi

1. TenantAdmin ile giris yap.
2. Yeni rol olustur (`SmokeRole`).
3. Role 2-3 permission ekle.
4. Yeni kullanici olustur.
5. Kullaniciya rol + `LEGAL_ENTITY` scope ata.
6. Yeni kullanici ile giris yap.
7. Scope disi bir kayit acmayi dene (beklenen: engel/403).
8. Scope ici bir kayit acmayi dene (beklenen: basarili).
9. RBAC loglarda degisiklik kayitlarini dogrula.

---

## 8) Sik Sorulan Sorular

1. `TenantAdmin` yeni tenant olusturabilir mi?
   - Hayir. Yeni tenant sadece Provider tarafindan olusturulur.

2. Tenant kullanicisi baska tenantin kullanicisini yonetebilir mi?
   - Hayir. Tum islemler kendi tenant sinirinda calisir.

3. Kullanici sayfayi goruyor ama kaydetme yapamiyor, neden?
   - Sayfaya giris permissioni var, islem permissioni eksik olabilir.
   - Scope disinda kaliyor olabilir.

4. `ALLOW` ve `DENY` birlikte kullanilir mi?
   - Evet, ama operasyonel olarak sade politika tavsiye edilir.
   - Once minimum `ALLOW` ile baslayin, gerekli yerde `DENY` ekleyin.

---

## 9) Operasyonel Tavsiyeler

1. Her tenantta en fazla 1-2 kisi `TenantAdmin` olsun.
2. Gunluk kullanicilar icin custom roller acin.
3. Atamalari her zaman scope ile sinirlayin.
4. Periyodik olarak RBAC denetim loglarini kontrol edin.
5. "Tam yetki ver sonra kisitlariz" yerine "minimum yetkiyle basla" modelini kullanin.

