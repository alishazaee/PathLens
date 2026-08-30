import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../components/ui/Modal";
import { FormField } from "../../components/ui/FormField";
import { Button } from "../../components/ui/Button";
import { devicesApi } from "../../api/devices";
import { fieldErrorMap, errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import { useAsync } from "../../hooks/useAsync";
import { locationsApi } from "../../api/locations";
import { DEVICE_STATUSES, DEVICE_TYPES } from "../../api/types";
import type { DeviceResponseDto, DeviceStatus, DeviceType } from "../../api/types";

export function DeviceForm({
  existing,
  onClose,
  onSaved,
}: {
  existing?: DeviceResponseDto;
  onClose: () => void;
  onSaved: () => void;
}) {
  const { push } = useToast();
  const isEdit = Boolean(existing);
  const [serialNumber, setSerialNumber] = useState(existing?.serialNumber ?? "");
  const [type, setType] = useState<DeviceType>(existing?.deviceType ?? DEVICE_TYPES[0]);
  const [status, setStatus] = useState<DeviceStatus>(existing?.status ?? "ACTIVE");
  const [siteId, setSiteId] = useState(existing?.deviceLocationDto.site ?? "");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);

  const { data: locations, loading: loadingLocations } = useAsync(
    () => locationsApi.list({ size: 100 }),
    [],
  );

  const validate = (): boolean => {
    const next: Record<string, string> = {};
    if (!isEdit && !serialNumber.trim()) next.serialNumber = "Serial number is required";
    if (!siteId) next.siteId = "Location is required";
    setErrors(next);
    return Object.keys(next).length === 0;
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!validate()) return;

    setSaving(true);
    try {
      if (isEdit && existing) {
        await devicesApi.update(existing.id, { type, status, siteId });
        push({ variant: "success", title: "Device updated", message: existing.serialNumber });
      } else {
        await devicesApi.create({ serialNumber: serialNumber.trim(), type, status, siteId });
        push({ variant: "success", title: "Device created", message: serialNumber.trim() });
      }
      onSaved();
    } catch (err) {
      setErrors(fieldErrorMap(err));
      push({ variant: "danger", title: "Could not save device", message: errorMessage(err) });
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal title={isEdit ? `Edit ${existing?.serialNumber}` : "Add device"} onClose={onClose}>
      <form onSubmit={handleSubmit}>
        <div className="form-grid">
          <FormField label="Serial number" htmlFor="serialNumber" error={errors.serialNumber}>
            <input
              id="serialNumber"
              value={serialNumber}
              disabled={isEdit}
              onChange={(e) => setSerialNumber(e.target.value)}
              placeholder="DEVICE-001"
            />
          </FormField>
          <FormField label="Type" htmlFor="type" error={errors.type}>
            <select id="type" value={type} onChange={(e) => setType(e.target.value as DeviceType)}>
              {DEVICE_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t.replaceAll("_", " ")}
                </option>
              ))}
            </select>
          </FormField>
          <FormField label="Status" htmlFor="status" error={errors.status}>
            <select id="status" value={status} onChange={(e) => setStatus(e.target.value as DeviceStatus)}>
              {DEVICE_STATUSES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </FormField>
          <FormField
            label="Location"
            htmlFor="siteId"
            error={errors.siteId}
            hint={loadingLocations ? "Loading locations..." : undefined}
          >
            <select id="siteId" value={siteId} onChange={(e) => setSiteId(e.target.value)}>
              <option value="">Select a location</option>
              {locations?.content.map((l) => (
                <option key={l.site} value={l.site}>
                  {l.site} ({l.city ?? "-"})
                </option>
              ))}
            </select>
          </FormField>
        </div>
        <div className="form-actions">
          <Button type="button" variant="secondary" onClick={onClose} disabled={saving}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={saving}>
            {saving ? "Saving..." : isEdit ? "Save changes" : "Create device"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
