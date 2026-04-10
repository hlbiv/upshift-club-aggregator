const DEFAULT_PAGE_SIZE = 20;
const MAX_PAGE_SIZE = 100;

export interface PaginationParams {
  page: number;
  pageSize: number;
  offset: number;
}

export function parsePagination(
  rawPage: unknown,
  rawPageSize: unknown,
): PaginationParams {
  const page = Math.max(1, Number(rawPage) || 1);
  const pageSize = Math.min(
    MAX_PAGE_SIZE,
    Math.max(1, Number(rawPageSize) || DEFAULT_PAGE_SIZE),
  );
  return { page, pageSize, offset: (page - 1) * pageSize };
}

export interface PaginatedMeta {
  total: number;
  page: number;
  page_size: number;
}
