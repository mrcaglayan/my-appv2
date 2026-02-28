import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App.jsx";
import "./index.css";
import { AuthProvider } from "./auth/AuthContext.jsx";
import ApiErrorToasts from "./components/ApiErrorToasts.jsx";
import WorkingContextProvider from "./context/WorkingContextProvider.jsx";
import { I18nProvider } from "./i18n/I18nProvider.jsx";
import { ProviderAuthProvider } from "./provider/ProviderAuthContext.jsx";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <I18nProvider>
      <BrowserRouter>
        <AuthProvider>
          <WorkingContextProvider>
            <ProviderAuthProvider>
              <App />
              <ApiErrorToasts />
            </ProviderAuthProvider>
          </WorkingContextProvider>
        </AuthProvider>
      </BrowserRouter>
    </I18nProvider>
  </React.StrictMode>
);
