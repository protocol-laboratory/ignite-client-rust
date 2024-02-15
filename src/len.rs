pub const CACHE_ID: usize = 4;
pub const COLLOCATED: usize = 1;
pub const CURSOR_PAGE_SIZE: usize = 4;
pub const DISTRIBUTED_JOIN: usize = 1;
pub const ENFORCE_JOIN_ORDER: usize = 1;
pub const INCLUDE_FIELD_NAMES: usize = 1;
pub const LAZY: usize = 1;
pub const LOCAL_QUERY: usize = 1;
pub const MAX_ROWS: usize = 4;
pub const QUERY_ARG_COUNT: usize = 4;
pub const REPLICATED_ONLY: usize = 1;
pub const STATEMENT_TYPE: usize = 1;
pub const TIMEOUT: usize = 8;

pub fn str(s: &str) -> usize {
    1 + 4 + s.len()
}
