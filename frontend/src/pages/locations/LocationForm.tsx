import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../components/ui/Modal";
import { FormField } from "../../components/ui/FormField";
import { Button } from "../../components/ui/Button";
import { locationsApi } from "../../api/locations";
import { fieldErrorMap, errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import type { LocationResponseDto } from "../../api/types";

export function LocationForm({
  existing,
  onClose,
  onSaved,
}: {
  existing?: LocationResponseDto;
  onClose: () => void;
  onSaved: () => void;
}) {
  const { push } = useToast();
  const isEdit = Boolean(existing);
  const [site, setSite] = useState(existing?.site ?? "");
  const [country, setCountry] = useState(existing?.country ?? "");
  const [city, setCity] = useState(existing?.city ?? "");
  const [latitude, setLatitude] = useState(existing?.latitude?.toString() ?? "");
  const [longitude, setLongitude] = useState(existing?.longitude?.toString() ?? "");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);

  const validate = (): boolean => {
    const next: Record<string, string> = {};
    if (!isEdit && !site.trim()) next.site = "Site id is required";
    if (!country.trim()) next.country = "Country is required";
    if (!city.trim()) next.city = "City is required";
    setErrors(next);
    return Object.keys(next).length === 0;
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!validate()) return;

    setSaving(true);
    try {
      const payload = {
        country: country.trim(),
        city: city.trim(),
        latitude: latitude === "" ? null : Number(latitude),
        longitude: longitude === "" ? null : Number(longitude),
      };
      if (isEdit && existing) {
        await locationsApi.update(existing.site, payload);
        push({ variant: "success", title: "Location updated", message: existing.site });
      } else {
        await locationsApi.create({ site: site.trim(), ...payload });
        push({ variant: "success", title: "Location created", message: site.trim() });
      }
      onSaved();
    } catch (err) {
      setErrors(fieldErrorMap(err));
      push({ variant: "danger", title: "Could not save location", message: errorMessage(err) });
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal title={isEdit ? `Edit ${existing?.site}` : "Add location"} onClose={onClose}>
      <form onSubmit={handleSubmit}>
        <div className="form-grid">
          <FormField label="Site id" htmlFor="site" error={errors.site}>
            <input
              id="site"
              value={site}
              disabled={isEdit}
              onChange={(e) => setSite(e.target.value)}
              placeholder="SITE-TEHRAN"
            />
          </FormField>
          <FormField label="Country" htmlFor="country" error={errors.country}>
            <input id="country" value={country} onChange={(e) => setCountry(e.target.value)} />
          </FormField>
          <FormField label="City" htmlFor="city" error={errors.city}>
            <input id="city" value={city} onChange={(e) => setCity(e.target.value)} />
          </FormField>
          <FormField label="Latitude" htmlFor="latitude" error={errors.latitude} hint="-90 to 90">
            <input
              id="latitude"
              type="number"
              step="any"
              value={latitude}
              onChange={(e) => setLatitude(e.target.value)}
            />
          </FormField>
          <FormField label="Longitude" htmlFor="longitude" error={errors.longitude} hint="-180 to 180">
            <input
              id="longitude"
              type="number"
              step="any"
              value={longitude}
              onChange={(e) => setLongitude(e.target.value)}
            />
          </FormField>
        </div>
        <div className="form-actions">
          <Button type="button" variant="secondary" onClick={onClose} disabled={saving}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={saving}>
            {saving ? "Saving..." : isEdit ? "Save changes" : "Create location"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
