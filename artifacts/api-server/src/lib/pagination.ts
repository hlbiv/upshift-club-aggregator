import { and, type SQL } from "drizzle-orm";

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

export function buildWhere(conditions: (SQL | undefined)[]): SQL | undefined {
  const valid = conditions.filter((c): c is SQL => c !== undefined);
  return valid.length > 0 ? and(...valid) : undefined;
}
