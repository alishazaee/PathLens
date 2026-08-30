import { Button } from "./Button";

export function Pagination({
  page,
  totalPages,
  totalElements,
  onChange,
}: {
  page: number;
  totalPages: number;
  totalElements: number;
  onChange: (page: number) => void;
}) {
  if (totalElements === 0) {
    return null;
  }
  return (
    <div className="pagination">
      <span>
        Page {page + 1} of {Math.max(totalPages, 1)} &middot; {totalElements} total
      </span>
      <div className="pagination__controls">
        <Button size="sm" variant="secondary" disabled={page <= 0} onClick={() => onChange(page - 1)}>
          Previous
        </Button>
        <Button
          size="sm"
          variant="secondary"
          disabled={page + 1 >= totalPages}
          onClick={() => onChange(page + 1)}
        >
          Next
        </Button>
      </div>
    </div>
  );
}
