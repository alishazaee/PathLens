import { useCallback, useState } from "react";
import { useAsync } from "../../hooks/useAsync";
import { locationsApi } from "../../api/locations";
import { errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import { Button } from "../../components/ui/Button";
import { DataTable, type Column } from "../../components/ui/DataTable";
import { EmptyState, ErrorState, LoadingState } from "../../components/ui/StatePanel";
import { Pagination } from "../../components/ui/Pagination";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { LocationForm } from "./LocationForm";
import type { LocationResponseDto } from "../../api/types";

const PAGE_SIZE = 10;

export function LocationsPage() {
  const [page, setPage] = useState(0);
  const [modal, setModal] = useState<{ mode: "create" } | { mode: "edit"; location: LocationResponseDto } | null>(
    null,
  );
  const [toDelete, setToDelete] = useState<LocationResponseDto | null>(null);
  const [deleting, setDeleting] = useState(false);
  const { push } = useToast();

  const load = useCallback(() => locationsApi.list({ page, size: PAGE_SIZE }), [page]);
  const { data, loading, error, reload } = useAsync(load, [page]);

  const handleDelete = async () => {
    if (!toDelete) return;
    setDeleting(true);
    try {
      await locationsApi.remove(toDelete.site);
      push({ variant: "success", title: "Location deleted", message: toDelete.site });
      setToDelete(null);
      reload();
    } catch (err) {
      push({ variant: "danger", title: "Could not delete location", message: errorMessage(err) });
    } finally {
      setDeleting(false);
    }
  };

  const columns: Column<LocationResponseDto>[] = [
    { key: "site", header: "Site", render: (l) => <span className="mono">{l.site}</span> },
    { key: "country", header: "Country", render: (l) => l.country ?? "-" },
    { key: "city", header: "City", render: (l) => l.city ?? "-" },
    {
      key: "coords",
      header: "Coordinates",
      render: (l) => (l.latitude != null && l.longitude != null ? `${l.latitude}, ${l.longitude}` : "-"),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      render: (l) => (
        <div className="data-table__actions">
          <Button size="sm" variant="secondary" onClick={() => setModal({ mode: "edit", location: l })}>
            Edit
          </Button>
          <Button size="sm" variant="danger" onClick={() => setToDelete(l)}>
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
          <h1 className="page-header__title">Locations</h1>
          <p className="page-header__subtitle">Sites where PathLens devices are installed</p>
        </div>
        <Button variant="primary" onClick={() => setModal({ mode: "create" })}>
          + Add location
        </Button>
      </div>

      {loading && <LoadingState label="Loading locations..." />}
      {error && <ErrorState message={errorMessage(error)} onRetry={reload} />}

      {data && data.content.length === 0 && (
        <EmptyState
          icon="📍"
          title="No locations yet"
          description="Add your first site to start registering devices."
          action={
            <Button variant="primary" onClick={() => setModal({ mode: "create" })}>
              + Add location
            </Button>
          }
        />
      )}

      {data && data.content.length > 0 && (
        <div className="table-wrapper">
          <DataTable columns={columns} rows={data.content} getRowKey={(l) => l.site} />
          <Pagination page={data.page} totalPages={data.totalPages} totalElements={data.totalElements} onChange={setPage} />
        </div>
      )}

      {modal && (
        <LocationForm
          existing={modal.mode === "edit" ? modal.location : undefined}
          onClose={() => setModal(null)}
          onSaved={() => {
            setModal(null);
            reload();
          }}
        />
      )}

      {toDelete && (
        <ConfirmDialog
          title="Delete location"
          message={`Delete "${toDelete.site}"? This cannot be undone. Locations with devices still assigned to them cannot be deleted.`}
          busy={deleting}
          onConfirm={handleDelete}
          onCancel={() => setToDelete(null)}
        />
      )}
    </div>
  );
}
