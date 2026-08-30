import { useCallback, useState } from "react";
import { useAsync } from "../../hooks/useAsync";
import { notificationsApi } from "../../api/notifications";
import { errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import { Button } from "../../components/ui/Button";
import { Badge } from "../../components/ui/Badge";
import { DataTable, type Column } from "../../components/ui/DataTable";
import { EmptyState, ErrorState, LoadingState } from "../../components/ui/StatePanel";
import { Pagination } from "../../components/ui/Pagination";
import { FormField } from "../../components/ui/FormField";
import type { Notification } from "../../api/types";

const PAGE_SIZE = 15;

export function NotificationsPage() {
  const [page, setPage] = useState(0);
  const [seenFilter, setSeenFilter] = useState<"" | "true" | "false">("");
  const [markingId, setMarkingId] = useState<string | null>(null);
  const { push } = useToast();

  const load = useCallback(
    () =>
      notificationsApi.list({
        page,
        size: PAGE_SIZE,
        seen: seenFilter === "" ? undefined : seenFilter === "true",
      }),
    [page, seenFilter],
  );
  const { data, loading, error, reload } = useAsync(load, [page, seenFilter]);

  const markSeen = async (n: Notification) => {
    setMarkingId(n.id);
    try {
      await notificationsApi.markSeen(n.id);
      reload();
    } catch (err) {
      push({ variant: "danger", title: "Could not update notification", message: errorMessage(err) });
    } finally {
      setMarkingId(null);
    }
  };

  const columns: Column<Notification>[] = [
    {
      key: "status",
      header: "",
      render: (n) => <Badge tone={n.seen ? "neutral" : "danger"}>{n.seen ? "Seen" : "New"}</Badge>,
    },
    { key: "message", header: "Message", render: (n) => n.message },
    { key: "createdAt", header: "When", render: (n) => new Date(n.createdAt).toLocaleString() },
    {
      key: "rule",
      header: "Rule status",
      render: (n) => <Badge tone={n.isActive ? "success" : "neutral"}>{n.isActive ? "Active" : "Disabled"}</Badge>,
    },
    {
      key: "actions",
      header: "",
      align: "right",
      render: (n) =>
        n.seen ? null : (
          <Button size="sm" variant="secondary" disabled={markingId === n.id} onClick={() => markSeen(n)}>
            Mark seen
          </Button>
        ),
    },
  ];

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-header__title">Alerts &amp; notifications</h1>
          <p className="page-header__subtitle">History of every notification raised by your rules</p>
        </div>
      </div>

      <div className="toolbar">
        <FormField label="Status" htmlFor="seen-filter">
          <select
            id="seen-filter"
            value={seenFilter}
            onChange={(e) => {
              setPage(0);
              setSeenFilter(e.target.value as "" | "true" | "false");
            }}
          >
            <option value="">All</option>
            <option value="false">Unseen only</option>
            <option value="true">Seen only</option>
          </select>
        </FormField>
      </div>

      {loading && <LoadingState label="Loading notifications..." />}
      {error && <ErrorState message={errorMessage(error)} onRetry={reload} />}

      {data && data.content.length === 0 && (
        <EmptyState icon="🔔" title="No notifications" description="Alerts raised by your rules will show up here." />
      )}

      {data && data.content.length > 0 && (
        <div className="table-wrapper">
          <DataTable columns={columns} rows={data.content} getRowKey={(n) => n.id} />
          <Pagination page={data.page} totalPages={data.totalPages} totalElements={data.totalElements} onChange={setPage} />
        </div>
      )}
    </div>
  );
}
