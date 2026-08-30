import { useCallback, useState } from "react";
import { useAsync } from "../../hooks/useAsync";
import { rulesApi } from "../../api/rules";
import { errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import { Button } from "../../components/ui/Button";
import { Badge } from "../../components/ui/Badge";
import { DataTable, type Column } from "../../components/ui/DataTable";
import { EmptyState, ErrorState, LoadingState } from "../../components/ui/StatePanel";
import { Pagination } from "../../components/ui/Pagination";
import { ConfirmDialog } from "../../components/ui/ConfirmDialog";
import { FormField } from "../../components/ui/FormField";
import { RuleForm } from "./RuleForm";
import type { Rule } from "../../api/types";

const PAGE_SIZE = 10;

export function RulesPage() {
  const [page, setPage] = useState(0);
  const [activeFilter, setActiveFilter] = useState<"" | "true" | "false">("");
  const [modal, setModal] = useState<{ mode: "create" } | { mode: "edit"; rule: Rule } | null>(null);
  const [toDelete, setToDelete] = useState<Rule | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [togglingId, setTogglingId] = useState<string | null>(null);
  const { push } = useToast();

  const load = useCallback(
    () => rulesApi.list({ page, size: PAGE_SIZE, isActive: activeFilter === "" ? undefined : activeFilter === "true" }),
    [page, activeFilter],
  );
  const { data, loading, error, reload } = useAsync(load, [page, activeFilter]);

  const handleDelete = async () => {
    if (!toDelete) return;
    setDeleting(true);
    try {
      await rulesApi.remove(toDelete.id);
      push({ variant: "success", title: "Rule deleted", message: toDelete.title });
      setToDelete(null);
      reload();
    } catch (err) {
      push({ variant: "danger", title: "Could not delete rule", message: errorMessage(err) });
    } finally {
      setDeleting(false);
    }
  };

  const toggleActive = async (rule: Rule) => {
    setTogglingId(rule.id);
    try {
      if (rule.isActive) {
        await rulesApi.deactivate(rule.id);
        push({ variant: "info", title: "Rule disabled", message: rule.title });
      } else {
        await rulesApi.activate(rule.id);
        push({ variant: "success", title: "Rule enabled", message: rule.title });
      }
      reload();
    } catch (err) {
      push({ variant: "danger", title: "Could not update rule", message: errorMessage(err) });
    } finally {
      setTogglingId(null);
    }
  };

  const columns: Column<Rule>[] = [
    { key: "title", header: "Title", render: (r) => r.title },
    { key: "ruleType", header: "Type", render: (r) => r.ruleType },
    {
      key: "identity",
      header: "Identity",
      render: (r) => (
        <span className="mono">
          {r.identity.identityType}: {r.identity.identityValue}
        </span>
      ),
    },
    { key: "expiresAt", header: "Expires", render: (r) => new Date(r.expiresAt).toLocaleString() },
    {
      key: "status",
      header: "Status",
      render: (r) => (
        <div style={{ display: "flex", gap: 6 }}>
          <Badge tone={r.isActive ? "success" : "neutral"}>{r.isActive ? "Active" : "Disabled"}</Badge>
          {r.isViolated && <Badge tone="danger">Violated</Badge>}
        </div>
      ),
    },
    {
      key: "actions",
      header: "",
      align: "right",
      render: (r) => (
        <div className="data-table__actions">
          <Button size="sm" variant="secondary" disabled={togglingId === r.id} onClick={() => toggleActive(r)}>
            {r.isActive ? "Disable" : "Enable"}
          </Button>
          <Button size="sm" variant="secondary" onClick={() => setModal({ mode: "edit", rule: r })}>
            Edit
          </Button>
          <Button size="sm" variant="danger" onClick={() => setToDelete(r)}>
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
          <h1 className="page-header__title">Rules</h1>
          <p className="page-header__subtitle">Geofencing rules that trigger alerts</p>
        </div>
        <Button variant="primary" onClick={() => setModal({ mode: "create" })}>
          + Add rule
        </Button>
      </div>

      <div className="toolbar">
        <FormField label="Status" htmlFor="active-filter">
          <select
            id="active-filter"
            value={activeFilter}
            onChange={(e) => {
              setPage(0);
              setActiveFilter(e.target.value as "" | "true" | "false");
            }}
          >
            <option value="">All rules</option>
            <option value="true">Active only</option>
            <option value="false">Disabled only</option>
          </select>
        </FormField>
      </div>

      {loading && <LoadingState label="Loading rules..." />}
      {error && <ErrorState message={errorMessage(error)} onRetry={reload} />}

      {data && data.content.length === 0 && (
        <EmptyState
          icon="🛡️"
          title="No rules found"
          description="Create a rule to start generating alerts."
          action={
            <Button variant="primary" onClick={() => setModal({ mode: "create" })}>
              + Add rule
            </Button>
          }
        />
      )}

      {data && data.content.length > 0 && (
        <div className="table-wrapper">
          <DataTable columns={columns} rows={data.content} getRowKey={(r) => r.id} />
          <Pagination page={data.page} totalPages={data.totalPages} totalElements={data.totalElements} onChange={setPage} />
        </div>
      )}

      {modal && (
        <RuleForm
          existing={modal.mode === "edit" ? modal.rule : undefined}
          onClose={() => setModal(null)}
          onSaved={() => {
            setModal(null);
            reload();
          }}
        />
      )}

      {toDelete && (
        <ConfirmDialog
          title="Delete rule"
          message={`Delete "${toDelete.title}"? This cannot be undone.`}
          busy={deleting}
          onConfirm={handleDelete}
          onCancel={() => setToDelete(null)}
        />
      )}
    </div>
  );
}
