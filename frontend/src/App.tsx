import { Route, Routes } from "react-router-dom";
import { AppShell } from "./components/layout/AppShell";
import { Dashboard } from "./pages/Dashboard";
import { DevicesPage } from "./pages/devices/DevicesPage";
import { LocationsPage } from "./pages/locations/LocationsPage";
import { RulesPage } from "./pages/rules/RulesPage";
import { NotificationsPage } from "./pages/notifications/NotificationsPage";

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Dashboard />} />
        <Route path="devices" element={<DevicesPage />} />
        <Route path="locations" element={<LocationsPage />} />
        <Route path="rules" element={<RulesPage />} />
        <Route path="notifications" element={<NotificationsPage />} />
      </Route>
    </Routes>
  );
}
