import type { ReactNode } from "react";
import { Button } from "./Button";

export function LoadingState({ label = "Loading..." }: { label?: string }) {
  return (
    <div className="state-panel">
      <div className="spinner" />
      <div>{label}</div>
    </div>
  );
}

export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="state-panel">
      <div className="state-panel__icon">⚠️</div>
      <div className="state-panel__title">Something went wrong</div>
      <div>{message}</div>
      {onRetry && (
        <Button variant="secondary" size="sm" onClick={onRetry}>
          Try again
        </Button>
      )}
    </div>
  );
}

export function EmptyState({
  icon = "📭",
  title,
  description,
  action,
}: {
  icon?: string;
  title: string;
  description?: string;
  action?: ReactNode;
}) {
  return (
    <div className="state-panel">
      <div className="state-panel__icon">{icon}</div>
      <div className="state-panel__title">{title}</div>
      {description && <div>{description}</div>}
      {action}
    </div>
  );
}
