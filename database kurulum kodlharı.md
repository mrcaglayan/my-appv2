cd c:\Users\Maarif\Desktop\my-app\backend

# .env yoksa oluştur
Copy-Item .env.example .env

# bağımlılıklar
npm ci

# DB + tüm migrationlar (schema sıfırdan oluşur)
npm run db:init

# kontrol (hepsi applied=true olmalı)
npm run db:migrate:status

# temel yetki/rol seed
npm run db:seed:core

# provider admin için
npm run db:seed:provider-admin

# opsiyonel: test tenant user da istersenve default tenant.
npm run db:seed
