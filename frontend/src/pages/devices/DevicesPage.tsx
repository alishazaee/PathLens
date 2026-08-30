import { useCallback, useState } from "react";
import { useAsync } from "../../hooks/useAsync";
import { devicesApi } from "../../api/devices";
import { errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import { Button } from "../../components/ui/Button";
import { Badge } from "../../components/ui/Badge";
import { DataTable, type Column } from "../../components/ui/DataTable";
import { EmptyState, ErrorState, LoadingState } from "../../components/ui/StatePanel";
import { Pagination } from "../../components/ui/Pagination";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { FormField } from "../../components/ui/FormField";
import { DeviceForm } from "./DeviceForm";
import { DEVICE_STATUSES, DEVICE_TYPES } from "../../api/types";
import type { DeviceResponseDto, DeviceStatus, DeviceType } from "../../api/types";

const PAGE_SIZE = 10;

export function DevicesPage() {
  const [page, setPage] = useState(0);
  const [typeFilter, setTypeFilter] = useState<DeviceType | "">("");
  const [statusFilter, setStatusFilter] = useState<DeviceStatus | "">("");
  const [modal, setModal] = useState<{ mode: "create" } | { mode: "edit"; device: DeviceResponseDto } | null>(null);
  const [toDelete, setToDelete] = useState<DeviceResponseDto | null>(null);
  const [deleting, setDeleting] = useState(false);
  const { push } = useToast();

  const load = useCallback(
    () =>
      devicesApi.list({
        page,
        size: PAGE_SIZE,
        type: typeFilter || undefined,
        justActiveDevices: statusFilter === "" ? undefined : statusFilter === "ACTIVE",
      }),
    [page, typeFilter, statusFilter],
  );
  const { data, loading, error, reload } = useAsync(load, [page, typeFilter, statusFilter]);

  const handleDelete = async () => {
    if (!toDelete) return;
    setDeleting(true);
    try {
      await devicesApi.remove(toDelete.id);
      push({ variant: "success", title: "Device deleted", message: toDelete.serialNumber });
      setToDelete(null);
      reload();
    } catch (err) {
      push({ variant: "danger", title: "Could not delete device", message: errorMessage(err) });
    } finally {
      setDeleting(false);
    }
  };

  const columns: Column<DeviceResponseDto>[] = [
    { key: "serialNumber", header: "Serial number", render: (d) => <span className="mono">{d.serialNumber}</span> },
    { key: "type", header: "Type", render: (d) => d.deviceType.replaceAll("_", " ") },
    {
      key: "status",
      header: "Status",
      render: (d) => <Badge tone={d.status === "ACTIVE" ? "success" : "neutral"}>{d.status}</Badge>,
    },
    { key: "site", header: "Location", render: (d) => d.deviceLocationDto?.site ?? "-" },
    {
      key: "actions",
      header: "",
      align: "right",
      render: (d) => (
        <div className="data-table__actions">
          <Button size="sm" variant="secondary" onClick={() => setModal({ mode: "edit", device: d })}>
            Edit
          </Button>
          <Button size="sm" variant="danger" onClick={() => setToDelete(d)}>
            Delete
          </Button>
        </div>
      ),
    },
  ];

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-header__title">Devices</h1>
          <p className="page-header__subtitle">Traffic cameras registered with PathLens</p>
        </div>
        <Button variant="primary" onClick={() => setModal({ mode: "create" })}>
          + Add device
        </Button>
      </div>

      <div className="toolbar">
        <FormField label="Type" htmlFor="type-filter">
          <select
            id="type-filter"
            value={typeFilter}
            onChange={(e) => {
              setPage(0);
              setTypeFilter(e.target.value as DeviceType | "");
            }}
          >
            <option value="">All types</option>
            {DEVICE_TYPES.map((t) => (
              <option key={t} value={t}>
                {t.replaceAll("_", " ")}
              </option>
            ))}
          </select>
        </FormField>
        <FormField label="Status" htmlFor="status-filter">
          <select
            id="status-filter"
            value={statusFilter}
            onChange={(e) => {
              setPage(0);
              setStatusFilter(e.target.value as DeviceStatus | "");
            }}
          >
            <option value="">All statuses</option>
            {DEVICE_STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </FormField>
      </div>

      {loading && <LoadingState label="Loading devices..." />}
      {error && <ErrorState message={errorMessage(error)} onRetry={reload} />}

      {data && data.content.length === 0 && (
        <EmptyState
          icon="📷"
          title="No devices found"
          description="Try a different filter, or add a new device."
          action={
            <Button variant="primary" onClick={() => setModal({ mode: "create" })}>
              + Add device
            </Button>
          }
        />
      )}

      {data && data.content.length > 0 && (
        <div className="table-wrapper">
          <DataTable columns={columns} rows={data.content} getRowKey={(d) => d.id} />
          <Pagination page={data.page} totalPages={data.totalPages} totalElements={data.totalElements} onChange={setPage} />
        </div>
      )}

      {modal && (
        <DeviceForm
          existing={modal.mode === "edit" ? modal.device : undefined}
          onClose={() => setModal(null)}
          onSaved={() => {
            setModal(null);
            reload();
          }}
        />
      )}

      {toDelete && (
        <ConfirmDialog
          title="Delete device"
          message={`Delete "${toDelete.serialNumber}"? This cannot be undone.`}
          busy={deleting}
          onConfirm={handleDelete}
          onCancel={() => setToDelete(null)}
        />
      )}
    </div>
  );
}
