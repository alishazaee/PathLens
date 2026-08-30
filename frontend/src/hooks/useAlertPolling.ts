import { useEffect, useRef } from "react";
import { notificationsApi } from "../api/notifications";
import { useToast } from "./ToastContext";

const POLL_INTERVAL_MS = 8000;

/**
 * Polls the alerting API for notifications created since the last check and pops a toast for
 * each one. Mounted once near the app root so alerts surface no matter which page is open.
 * Only notifications created *after* the hook mounts trigger a popup - existing history is
 * left for the Notifications page rather than replayed as toasts on every reload.
 */
export function useAlertPolling(): void {
  const { push } = useToast();
  const sinceRef = useRef(new Date().toISOString());

  useEffect(() => {
    let cancelled = false;

    const poll = async () => {
      try {
        const page = await notificationsApi.list({
          createdAfter: sinceRef.current,
          size: 20,
        });
        if (cancelled || page.content.length === 0) {
          return;
        }
        // Results come back newest-first; advance the watermark to the newest createdAt seen.
        sinceRef.current = page.content[0].createdAt;
        for (const notification of [...page.content].reverse()) {
          push({
            variant: "alert",
            title: "New alert",
            message: notification.message,
          });
        }
      } catch {
        // Transient polling failures are not worth surfacing as a toast; the next tick retries.
      }
    };

    const timer = window.setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [push]);
}
