import type { ReactNode } from "react";

export function Modal({
  title,
  onClose,
  children,
  variant,
}: {
  title: string;
  onClose: () => void;
  children: ReactNode;
  variant?: "confirm";
}) {
  return (
    <div
      className="modal-overlay"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className={`modal ${variant === "confirm" ? "modal--confirm" : ""}`}>
        <div className="modal__header">
          <h3 className="modal__title">{title}</h3>
          <button className="btn-ghost btn" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}
