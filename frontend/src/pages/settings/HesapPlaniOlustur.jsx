import { useI18n } from "../../i18n/useI18n.js";

function HesapPlaniOlustur() {
  const { t } = useI18n();

  return <div>{t("chartOfAccountsCreate.title")}</div>;
}

export default HesapPlaniOlustur;
