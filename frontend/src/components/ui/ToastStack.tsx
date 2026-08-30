import { useToast, useToastStack } from "../../hooks/ToastContext";

export function ToastStack() {
  const toasts = useToastStack();
  const { dismiss } = useToast();

  if (toasts.length === 0) {
    return null;
  }

  return (
    <div className="toast-stack">
      {toasts.map((toast) => (
        <div key={toast.id} className={`toast toast-${toast.variant}`}>
          <div>
            <div className="toast__title">{toast.title}</div>
            {toast.message && <div>{toast.message}</div>}
          </div>
          <button className="toast__close" onClick={() => dismiss(toast.id)} aria-label="Dismiss">
            ✕
          </button>
        </div>
      ))}
    </div>
  );
}
