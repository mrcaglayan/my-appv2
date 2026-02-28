import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..");
const outputDir = path.join(repoRoot, "manual_screenshots");

const BASE_URL = process.env.APP_BASE_URL || "http://localhost:5173";
const LOGIN_EMAIL = process.env.APP_LOGIN_EMAIL || "test@example.com";
const LOGIN_PASSWORD = process.env.APP_LOGIN_PASSWORD || "123456";

const SCREENSHOTS = [
  {
    name: "01_login_page.png",
    url: "/login",
    captureBeforeLogin: true,
  },
  {
    name: "02_company_settings.png",
    url: "/app/ayarlar/sirket-ayarlari",
  },
  {
    name: "03_organization_management.png",
    url: "/app/ayarlar/organizasyon-yonetimi",
  },
  {
    name: "04_shareholders_card.png",
    url: "/app/ayarlar/organizasyon-yonetimi",
    selector:
      'section:has(h2:has-text("Ortaklar")), section:has(h2:has-text("Shareholders"))',
    elementOnly: true,
  },
  {
    name: "05_fiscal_periods_card.png",
    url: "/app/ayarlar/organizasyon-yonetimi",
    selector:
      'section:has(h2:has-text("Mali Takvimler")), section:has(h2:has-text("Fiscal Calendars and Periods"))',
    elementOnly: true,
  },
  {
    name: "06_gl_setup.png",
    url: "/app/ayarlar/hesap-plani-ayarlari",
  },
];

async function pathExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function resolveBrowserExecutable() {
  const candidates = [
    process.env.CHROME_PATH,
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (await pathExists(candidate)) {
      return candidate;
    }
  }
  throw new Error("No supported Chrome/Edge executable found.");
}

async function ensureLoggedIn(page) {
  await page.goto(`${BASE_URL}/login`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(700);

  if (page.url().includes("/app")) {
    return;
  }

  await page.locator('input[autocomplete="username"]').first().fill(LOGIN_EMAIL);
  await page
    .locator('input[autocomplete="current-password"]')
    .first()
    .fill(LOGIN_PASSWORD);

  await Promise.all([
    page.waitForURL(/\/app/, { timeout: 20000 }),
    page.locator('button[type="submit"]').first().click(),
  ]);
}

async function capturePage(page, config) {
  const targetUrl = `${BASE_URL}${config.url}`;
  await page.goto(targetUrl, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1400);

  const destination = path.join(outputDir, config.name);
  if (config.selector) {
    const block = page.locator(config.selector).first();
    await block.waitFor({ state: "visible", timeout: 20000 });
    await block.scrollIntoViewIfNeeded();
    await page.waitForTimeout(400);
    await block.screenshot({ path: destination });
    return;
  }

  await page.screenshot({ path: destination, fullPage: true });
}

async function main() {
  const executablePath = await resolveBrowserExecutable();
  await fs.mkdir(outputDir, { recursive: true });

  const browser = await chromium.launch({
    headless: true,
    executablePath,
  });

  const context = await browser.newContext({
    viewport: { width: 1680, height: 980 },
  });

  const page = await context.newPage();
  page.setDefaultTimeout(25000);

  let loggedIn = false;
  for (const shot of SCREENSHOTS) {
    if (shot.captureBeforeLogin) {
      await page.goto(`${BASE_URL}${shot.url}`, { waitUntil: "domcontentloaded" });
      await page.waitForTimeout(1000);
      await page.screenshot({
        path: path.join(outputDir, shot.name),
        fullPage: true,
      });
      console.log(`Captured ${shot.name}`);
      continue;
    }

    if (!loggedIn) {
      await ensureLoggedIn(page);
      loggedIn = true;

      // Force TR for documentation screenshots to keep UI wording consistent.
      await page.evaluate(() => {
        localStorage.setItem("ui_language", "tr");
      });
    }

    await capturePage(page, shot);
    console.log(`Captured ${shot.name}`);
  }

  await context.close();
  await browser.close();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
