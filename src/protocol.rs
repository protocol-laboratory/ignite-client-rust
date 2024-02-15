use crate::{len, op_const};
use bytes::{BufMut, BytesMut};
use std::any::Any;
use std::io::{Error, ErrorKind};
use tokio::io;

pub trait Encode {
    fn encode(&self) -> BytesMut;
    fn length(&self) -> usize;
}

pub trait Decode {
    fn decode(data: &[u8]) -> io::Result<Self>
    where
        Self: Sized;
}

pub struct HandshakeRequest {
    pub major_version: i16,
    pub minor_version: i16,
    pub patch_version: i16,
    pub username: String,
    pub password: String,
}

impl HandshakeRequest {
    pub fn new(
        major_version: i16,
        minor_version: i16,
        patch_version: i16,
        username: String,
        password: String,
    ) -> HandshakeRequest {
        HandshakeRequest {
            major_version,
            minor_version,
            patch_version,
            username,
            password,
        }
    }
}

impl Encode for HandshakeRequest {
    fn encode(&self) -> BytesMut {
        let payload_length = self.length();
        let mut buf = BytesMut::with_capacity(payload_length + 4);
        buf.put_i32_le(payload_length as i32);
        buf.put_u8(1);
        buf.put_i16_le(self.major_version);
        buf.put_i16_le(self.minor_version);
        buf.put_i16_le(self.patch_version);
        buf.put_u8(2);
        buf.put_i32_le(self.username.len() as i32);
        buf.extend_from_slice(self.username.as_bytes());
        buf.put_i32_le(self.password.len() as i32);
        buf.extend_from_slice(self.password.as_bytes());
        buf
    }

    fn length(&self) -> usize {
        1 + 2 + 2 + 2 + 1 + 4 + self.username.len() + 4 + self.password.len()
    }
}

pub enum HandshakeResponse {
    Success,
    Failure {
        major_version: i16,
        minor_version: i16,
        patch_version: i16,
        error_message: String,
    },
}

impl Decode for HandshakeResponse {
    fn decode(data: &[u8]) -> io::Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::Other, "Empty response"));
        }

        let success_flag = data[0];
        if success_flag == 1 {
            Ok(HandshakeResponse::Success)
        } else {
            let major_version = i16::from_le_bytes([data[1], data[2]]);
            let minor_version = i16::from_le_bytes([data[3], data[4]]);
            let patch_version = i16::from_le_bytes([data[5], data[6]]);

            let error_message = String::from_utf8(data[7..].to_vec())
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

            Ok(HandshakeResponse::Failure {
                major_version,
                minor_version,
                patch_version,
                error_message,
            })
        }
    }
}

pub struct Request {
    pub op_code: i16,
    pub request_id: i64,
    pub body: RequestType,
}

pub enum RequestType {
    QuerySql(QuerySqlRequest),
    QuerySqlFields(QuerySqlFieldsRequest),
}

impl Request {
    pub fn new_query_sql(request_id: i64, query_sql_request: QuerySqlRequest) -> Request {
        Request {
            op_code: op_const::QUERY_SQL,
            request_id,
            body: RequestType::QuerySql(query_sql_request),
        }
    }

    pub fn new_query_sql_fields(
        request_id: i64,
        query_sql_fields_request: QuerySqlFieldsRequest,
    ) -> Request {
        Request {
            op_code: op_const::QUERY_SQL_FIELDS,
            request_id,
            body: RequestType::QuerySqlFields(query_sql_fields_request),
        }
    }
}

impl Encode for Request {
    fn encode(&self) -> BytesMut {
        let payload_length = self.length();
        let mut buf = BytesMut::with_capacity(payload_length + 4);
        buf.put_i32_le(payload_length as i32);
        buf.put_i16_le(self.op_code);
        buf.put_i64_le(self.request_id);
        match &self.body {
            RequestType::QuerySql(query_sql_request) => {
                buf.extend_from_slice(&query_sql_request.encode());
            }
            RequestType::QuerySqlFields(query_sql_fields_request) => {
                buf.extend_from_slice(&query_sql_fields_request.encode());
            }
        }
        buf
    }

    fn length(&self) -> usize {
        2 + 8
            + match &self.body {
                RequestType::QuerySql(query_sql_request) => query_sql_request.length(),
                RequestType::QuerySqlFields(query_sql_fields_request) => {
                    query_sql_fields_request.length()
                }
            }
    }
}

pub struct Response {
    pub request_id: i64,
    pub status_code: i32,
    pub error_message: String,
    pub body: ResponseType,
}

pub enum ResponseType {
    QuerySql(QuerySqlResponse),
    QuerySqlFields(QuerySqlFieldsResponse),
}

impl Response {
    pub fn decode_query_sql(data: &[u8]) -> io::Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::Other, "Empty response"));
        }

        let request_id = i64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let status_code = i32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        if status_code != 0 {
            // skip the string type code
            let error_message_length = i32::from_le_bytes([data[13], data[14], data[15], data[16]]);
            let error_message =
                String::from_utf8(data[17..(17 + error_message_length as usize)].to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Response {
                request_id,
                status_code,
                error_message,
                body: ResponseType::QuerySql(QuerySqlResponse {
                    cursor_id: 0,
                    row_count: 0,
                    has_more: false,
                }),
            })
        } else {
            let query_sql_response = QuerySqlResponse::decode(&data[12..])?;
            Ok(Response {
                request_id,
                status_code,
                error_message: String::new(),
                body: ResponseType::QuerySql(query_sql_response),
            })
        }
    }

    pub fn decode_query_sql_fields(data: &[u8]) -> io::Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::Other, "Empty response"));
        }

        let request_id = i64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let status_code = i32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        if status_code != 0 {
            // skip the string type code
            let error_message_length = i32::from_le_bytes([data[13], data[14], data[15], data[16]]);
            let error_message =
                String::from_utf8(data[17..(17 + error_message_length as usize)].to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Response {
                request_id,
                status_code,
                error_message,
                body: ResponseType::QuerySqlFields(QuerySqlFieldsResponse {
                    cursor_id: 0,
                    column_count: 0,
                    column_names: vec![],
                    first_page_row_count: 0,
                    has_more: false,
                }),
            })
        } else {
            let query_sql_fields_response = QuerySqlFieldsResponse::decode(&data[12..], true)?;
            Ok(Response {
                request_id,
                status_code,
                error_message: String::new(),
                body: ResponseType::QuerySqlFields(query_sql_fields_response),
            })
        }
    }
}

pub struct QuerySqlRequest {
    pub cache_id: i32,
    pub table: String,
    pub sql: String,
    pub query_arg_count: i32,
    pub query_args: Vec<Box<dyn Any>>,
    pub distributed_join: bool,
    pub local_query: bool,
    pub replicated_only: bool,
    pub cursor_page_size: i32,
    pub timeout_milliseconds: i64,
}

impl QuerySqlRequest {
    pub fn new(
        cache_id: i32,
        table: String,
        sql: String,
        query_arg_count: i32,
        query_args: Vec<Box<dyn Any>>,
        distributed_join: bool,
        local_query: bool,
        replicated_only: bool,
        cursor_page_size: i32,
        timeout_milliseconds: i64,
    ) -> QuerySqlRequest {
        QuerySqlRequest {
            cache_id,
            table,
            sql,
            query_arg_count,
            query_args,
            distributed_join,
            local_query,
            replicated_only,
            cursor_page_size,
            timeout_milliseconds,
        }
    }
}

impl Encode for QuerySqlRequest {
    fn encode(&self) -> BytesMut {
        let payload_length = self.length();
        let mut buf = BytesMut::with_capacity(payload_length);
        buf.put_i32_le(self.cache_id);
        buf.put_u8(0);
        buf.put_u8(9);
        buf.put_i32_le(self.table.len() as i32);
        buf.extend_from_slice(self.table.as_bytes());
        buf.put_u8(9);
        buf.put_i32_le(self.sql.len() as i32);
        buf.extend_from_slice(self.sql.as_bytes());
        buf.put_i32_le(self.query_arg_count);
        // todo args
        buf.put_u8(self.distributed_join as u8);
        buf.put_u8(self.local_query as u8);
        buf.put_u8(self.replicated_only as u8);
        buf.put_i32_le(self.cursor_page_size);
        buf.put_i64_le(self.timeout_milliseconds);
        buf
    }

    fn length(&self) -> usize {
        let mut total_length: usize = 0;
        total_length += len::CACHE_ID;
        total_length += 1;
        total_length += len::str(&self.table);
        total_length += len::str(&self.sql);
        total_length += len::QUERY_ARG_COUNT;
        total_length += len::DISTRIBUTED_JOIN;
        total_length += len::LOCAL_QUERY;
        total_length += len::REPLICATED_ONLY;
        total_length += len::CURSOR_PAGE_SIZE;
        total_length += len::TIMEOUT;
        total_length
    }
}

pub struct QuerySqlResponse {
    pub cursor_id: i64,
    pub row_count: i32,
    pub has_more: bool,
}

impl Decode for QuerySqlResponse {
    fn decode(data: &[u8]) -> io::Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::Other, "Empty response"));
        }

        let cursor_id = i64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let row_count = i32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        let has_more = data[12] == 1;
        Ok(QuerySqlResponse {
            cursor_id,
            row_count,
            has_more,
        })
    }
}

pub struct QuerySqlFieldsRequest {
    pub cache_id: i32,
    pub schema: String,
    pub cursor_page_size: i32,
    pub max_rows: i32,
    pub sql: String,
    pub query_arg_count: i32,
    pub query_args: Vec<Box<dyn Any>>,
    pub statement_type: StatementType,
    pub distributed_join: bool,
    pub local_query: bool,
    pub replicated_only: bool,
    pub enforce_join_order: bool,
    pub collocated: bool,
    pub lazy: bool,
    pub timeout_milliseconds: i64,
    pub include_field_names: bool,
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum StatementType {
    ANY = 0,
    SELECT = 1,
    UPDATE = 2,
}

impl QuerySqlFieldsRequest {
    pub fn new(
        cache_id: i32,
        schema: String,
        cursor_page_size: i32,
        max_rows: i32,
        sql: String,
        query_arg_count: i32,
        query_args: Vec<Box<dyn Any>>,
        statement_type: StatementType,
        distributed_join: bool,
        local_query: bool,
        replicated_only: bool,
        enforce_join_order: bool,
        collocated: bool,
        lazy: bool,
        timeout_milliseconds: i64,
        include_field_names: bool,
    ) -> QuerySqlFieldsRequest {
        QuerySqlFieldsRequest {
            cache_id,
            schema,
            cursor_page_size,
            max_rows,
            sql,
            query_arg_count,
            query_args,
            statement_type,
            distributed_join,
            local_query,
            replicated_only,
            enforce_join_order,
            collocated,
            lazy,
            timeout_milliseconds,
            include_field_names,
        }
    }
}

impl Encode for QuerySqlFieldsRequest {
    fn encode(&self) -> BytesMut {
        let payload_length = self.length();
        let mut buf = BytesMut::with_capacity(payload_length);
        buf.put_i32_le(self.cache_id);
        buf.put_u8(0);
        buf.put_u8(9);
        buf.put_i32_le(self.schema.len() as i32);
        buf.extend_from_slice(self.schema.as_bytes());
        buf.put_i32_le(self.cursor_page_size);
        buf.put_i32_le(self.max_rows);
        buf.put_u8(9);
        buf.put_i32_le(self.sql.len() as i32);
        buf.extend_from_slice(self.sql.as_bytes());
        buf.put_i32_le(self.query_arg_count);
        // todo args
        buf.put_u8(self.statement_type as u8);
        buf.put_u8(self.distributed_join as u8);
        buf.put_u8(self.local_query as u8);
        buf.put_u8(self.replicated_only as u8);
        buf.put_u8(self.enforce_join_order as u8);
        buf.put_u8(self.collocated as u8);
        buf.put_u8(self.lazy as u8);
        buf.put_i64_le(self.timeout_milliseconds);
        buf.put_u8(self.include_field_names as u8);
        buf
    }

    fn length(&self) -> usize {
        let mut total_length: usize = 0;
        total_length += len::CACHE_ID;
        total_length += 1;
        total_length += len::str(&self.schema);
        total_length += len::CURSOR_PAGE_SIZE;
        total_length += len::MAX_ROWS;
        total_length += len::str(&self.sql);
        total_length += len::QUERY_ARG_COUNT;
        total_length += len::STATEMENT_TYPE;
        total_length += len::DISTRIBUTED_JOIN;
        total_length += len::LOCAL_QUERY;
        total_length += len::REPLICATED_ONLY;
        total_length += len::ENFORCE_JOIN_ORDER;
        total_length += len::COLLOCATED;
        total_length += len::LAZY;
        total_length += len::TIMEOUT;
        total_length += len::INCLUDE_FIELD_NAMES;
        total_length
    }
}

pub struct QuerySqlFieldsResponse {
    pub cursor_id: i64,
    pub column_count: i32,
    pub column_names: Vec<String>,
    pub first_page_row_count: i32,
    pub has_more: bool,
}

impl QuerySqlFieldsResponse {
    fn decode(data: &[u8], has_field_names: bool) -> io::Result<Self> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::Other, "Empty response"));
        }

        let cursor_id = i64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let column_count = i32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        if has_field_names {
            let mut column_names = vec![];
            let mut offset = 12;
            for _ in 0..column_count {
                offset += 1;
                let column_name_length = i32::from_le_bytes([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]);
                offset += 4;
                let column_name = String::from_utf8(
                    data[offset..(offset + column_name_length as usize)].to_vec(),
                )
                .map_err(|e| Error::new(ErrorKind::InvalidData, format!("{}", e)))?;
                column_names.push(column_name);
                offset += column_name_length as usize;
            }
            let first_page_row_count = i32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            let has_more = data[offset + 4] == 1;
            Ok(QuerySqlFieldsResponse {
                cursor_id,
                column_count,
                column_names,
                first_page_row_count,
                has_more,
            })
        } else {
            let first_page_row_count = i32::from_le_bytes([data[12], data[13], data[14], data[15]]);
            let has_more = data[16] == 1;
            Ok(QuerySqlFieldsResponse {
                cursor_id,
                column_count,
                column_names: vec![],
                first_page_row_count,
                has_more,
            })
        }
    }
}
