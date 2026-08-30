import { useState } from "react";
import type { FormEvent } from "react";
import { Modal } from "../../components/ui/Modal";
import { FormField } from "../../components/ui/FormField";
import { Button } from "../../components/ui/Button";
import { rulesApi } from "../../api/rules";
import { fieldErrorMap, errorMessage } from "../../api/errors";
import { useToast } from "../../hooks/ToastContext";
import { IDENTITY_TYPES, RULE_TYPES } from "../../api/types";
import type { IdentityType, Rule, RuleType } from "../../api/types";

function toDatetimeLocal(iso?: string): string {
  if (!iso) return "";
  return iso.slice(0, 16);
}

export function RuleForm({
  existing,
  onClose,
  onSaved,
}: {
  existing?: Rule;
  onClose: () => void;
  onSaved: () => void;
}) {
  const { push } = useToast();
  const isEdit = Boolean(existing);
  const [title, setTitle] = useState(existing?.title ?? "");
  const [geometryWkt, setGeometryWkt] = useState(existing?.geometryWkt ?? "");
  const [expiresAt, setExpiresAt] = useState(toDatetimeLocal(existing?.expiresAt));
  const [identityType, setIdentityType] = useState<IdentityType>(existing?.identity.identityType ?? "PlateNumber");
  const [identityValue, setIdentityValue] = useState(existing?.identity.identityValue ?? "");
  const [ruleType, setRuleType] = useState<RuleType>(existing?.ruleType ?? "Enter");
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);

  const validate = (): boolean => {
    const next: Record<string, string> = {};
    if (!geometryWkt.trim()) next.geometryWkt = "Geometry (WKT) is required";
    if (!expiresAt) next.expiresAt = "Expiry date/time is required";
    else if (new Date(expiresAt).getTime() <= Date.now()) next.expiresAt = "Must be in the future";
    if (!identityValue.trim()) next.identityValue = "Identity value is required";
    setErrors(next);
    return Object.keys(next).length === 0;
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!validate()) return;

    setSaving(true);
    try {
      const payload = {
        title: title.trim() || undefined,
        geometryWkt: geometryWkt.trim(),
        expiresAt,
        identity: { identityType, identityValue: identityValue.trim() },
        ruleType,
      };
      if (isEdit && existing) {
        await rulesApi.update(existing.id, payload);
        push({ variant: "success", title: "Rule updated", message: existing.title });
      } else {
        await rulesApi.create(payload);
        push({ variant: "success", title: "Rule created", message: payload.title ?? "New rule" });
      }
      onSaved();
    } catch (err) {
      setErrors(fieldErrorMap(err));
      push({ variant: "danger", title: "Could not save rule", message: errorMessage(err) });
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal title={isEdit ? `Edit ${existing?.title}` : "Add rule"} onClose={onClose}>
      <form onSubmit={handleSubmit}>
        <div className="form-grid">
          <FormField label="Title" htmlFor="title" error={errors.title} hint="Defaults to NO NAME if left blank">
            <input id="title" value={title} onChange={(e) => setTitle(e.target.value)} placeholder="Downtown curfew" />
          </FormField>
          <FormField label="Rule type" htmlFor="ruleType" error={errors.ruleType}>
            <select id="ruleType" value={ruleType} onChange={(e) => setRuleType(e.target.value as RuleType)}>
              {RULE_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </FormField>
          <FormField label="Identity type" htmlFor="identityType" error={errors["identity.identityType"]}>
            <select
              id="identityType"
              value={identityType}
              onChange={(e) => setIdentityType(e.target.value as IdentityType)}
            >
              {IDENTITY_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </FormField>
          <FormField
            label="Identity value"
            htmlFor="identityValue"
            error={errors.identityValue ?? errors["identity.identityValue"]}
          >
            <input
              id="identityValue"
              value={identityValue}
              onChange={(e) => setIdentityValue(e.target.value)}
              placeholder={identityType === "PhoneNumber" ? "09120000000" : "12-345-j-67"}
            />
          </FormField>
          <FormField label="Expires at" htmlFor="expiresAt" error={errors.expiresAt}>
            <input
              id="expiresAt"
              type="datetime-local"
              value={expiresAt}
              onChange={(e) => setExpiresAt(e.target.value)}
            />
          </FormField>
        </div>
        <FormField label="Geometry (WKT)" htmlFor="geometryWkt" error={errors.geometryWkt} hint="e.g. POLYGON((...))">
          <textarea
            id="geometryWkt"
            rows={4}
            value={geometryWkt}
            onChange={(e) => setGeometryWkt(e.target.value)}
            placeholder="POLYGON((51.38 35.68, 51.40 35.68, 51.40 35.70, 51.38 35.70, 51.38 35.68))"
          />
        </FormField>
        <div className="form-actions">
          <Button type="button" variant="secondary" onClick={onClose} disabled={saving}>
            Cancel
          </Button>
          <Button type="submit" variant="primary" disabled={saving}>
            {saving ? "Saving..." : isEdit ? "Save changes" : "Create rule"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
