import { useCallback } from "react";
import { Link } from "react-router-dom";
import { useAsync } from "../hooks/useAsync";
import { devicesApi } from "../api/devices";
import { locationsApi } from "../api/locations";
import { rulesApi } from "../api/rules";
import { notificationsApi } from "../api/notifications";
import { StatCard } from "../components/ui/StatCard";
import { ErrorState, LoadingState, EmptyState } from "../components/ui/StatePanel";
import { Badge } from "../components/ui/Badge";

export function Dashboard() {
  const load = useCallback(async () => {
    const [devices, locations, activeRules, unseen, recent] = await Promise.all([
      devicesApi.list({ size: 1 }),
      locationsApi.list({ size: 1 }),
      rulesApi.list({ isActive: true, size: 1 }),
      notificationsApi.list({ seen: false, size: 1 }),
      notificationsApi.list({ size: 6 }),
    ]);
    return { devices, locations, activeRules, unseen, recent };
  }, []);

  const { data, loading, error, reload } = useAsync(load, []);

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-header__title">Dashboard</h1>
          <p className="page-header__subtitle">Live overview of the PathLens alerting system</p>
        </div>
      </div>

      {loading && <LoadingState label="Loading dashboard..." />}
      {error && <ErrorState message={error.message} onRetry={reload} />}

      {data && (
        <>
          <div className="stat-grid">
            <StatCard label="Devices" value={data.devices.totalElements} icon="📷" tone="info" />
            <StatCard label="Locations" value={data.locations.totalElements} icon="📍" tone="info" />
            <StatCard label="Active rules" value={data.activeRules.totalElements} icon="🛡️" tone="success" />
            <StatCard
              label="Unseen alerts"
              value={data.unseen.totalElements}
              icon="🔔"
              tone={data.unseen.totalElements > 0 ? "danger" : "success"}
            />
          </div>

          <div className="card">
            <div className="card__body">
              <div className="page-header" style={{ marginBottom: 12 }}>
                <h3 style={{ margin: 0 }}>Recent alerts</h3>
                <Link to="/notifications" className="btn btn-secondary btn-sm">
                  View all
                </Link>
              </div>

              {data.recent.content.length === 0 ? (
                <EmptyState icon="🌤️" title="No alerts yet" description="Notifications will appear here as rules fire." />
              ) : (
                <div>
                  {data.recent.content.map((n) => (
                    <div
                      key={n.id}
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        padding: "10px 0",
                        borderBottom: "1px solid var(--color-border)",
                      }}
                    >
                      <div>
                        <div>{n.message}</div>
                        <div className="text-muted" style={{ fontSize: 12 }}>
                          {new Date(n.createdAt).toLocaleString()}
                        </div>
                      </div>
                      <Badge tone={n.seen ? "neutral" : "danger"}>{n.seen ? "Seen" : "New"}</Badge>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
