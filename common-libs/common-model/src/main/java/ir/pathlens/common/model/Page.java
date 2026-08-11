package ir.pathlens.common.model;

import java.util.List;

/**
 * A tiny paginated response wrapper.
 *
 * @param content       the items of the current page
 * @param page          the zero-based page number
 * @param size          the page size
 * @param totalElements the total number of items across all pages
 * @param totalPages    the total number of pages
 * @param <T>           the type of the items
 */
public record Page<T>(
        List<T> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public static <T> Page<T> of(List<T> content, int page, int size, long totalElements) {
        int totalPages = size <= 0 ? 0 : (int) Math.ceil((double) totalElements / size);
        return new Page<>(content, page, size, totalElements, totalPages);
    }
}
