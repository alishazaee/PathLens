export function StatCard({
  label,
  value,
  icon,
  tone = "info",
}: {
  label: string;
  value: string | number;
  icon: string;
  tone?: "info" | "success" | "warning" | "danger";
}) {
  return (
    <div className="stat-card">
      <div className={`stat-card__icon badge-${tone}`}>{icon}</div>
      <div className="stat-card__label">{label}</div>
      <div className="stat-card__value">{value}</div>
    </div>
  );
}
