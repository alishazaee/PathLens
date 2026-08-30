import { NavLink, Outlet } from "react-router-dom";
import { ToastStack } from "../ui/ToastStack";
import { useAlertPolling } from "../../hooks/useAlertPolling";

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", icon: "📊", end: true },
  { to: "/devices", label: "Devices", icon: "📷" },
  { to: "/locations", label: "Locations", icon: "📍" },
  { to: "/rules", label: "Rules", icon: "🛡️" },
  { to: "/notifications", label: "Alerts", icon: "🔔" },
];

export function AppShell() {
  useAlertPolling();

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar__brand">
          <span className="sidebar__brand-badge">P</span>
          PathLens
        </div>
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            className={({ isActive }) => `sidebar__link ${isActive ? "active" : ""}`}
          >
            <span>{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </aside>
      <div className="main">
        <header className="topbar">
          <span className="topbar__title">Alerting System</span>
        </header>
        <main className="content">
          <Outlet />
        </main>
      </div>
      <ToastStack />
    </div>
  );
}
