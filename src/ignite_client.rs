use std::io;
use std::sync::atomic::{AtomicI64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::{
    HandshakeRequest, HandshakeResponse, QuerySqlFieldsRequest,
    QuerySqlFieldsResponse, QuerySqlRequest, QuerySqlResponse, Request, Response, ResponseType,
};

pub struct IgniteClient {
    stream: Option<TcpStream>,
    host: String,
    port: u16,
    request_id: AtomicI64,
}

impl IgniteClient {
    pub fn new(host: &str, port: u16) -> Self {
        IgniteClient {
            stream: None,
            host: host.to_string(),
            port,
            request_id: AtomicI64::new(0),
        }
    }

    pub async fn connect(&mut self) -> Result<(), io::Error> {
        let addr = format!("{}:{}", self.host, self.port);
        let stream = TcpStream::connect(addr).await?;
        self.stream = Some(stream);
        Ok(())
    }

    pub async fn handshake(
        &mut self,
        request: HandshakeRequest,
    ) -> Result<HandshakeResponse, io::Error> {
        if let Some(stream) = &mut self.stream {
            let encoded_request = request.encode();
            stream.write_all(&encoded_request).await?;

            let mut length_buf = [0u8; 4];
            stream.read_exact(&mut length_buf).await?;
            let msg_length = u32::from_le_bytes(length_buf) as usize;

            let mut msg_buf = vec![0u8; msg_length];
            stream.read_exact(&mut msg_buf).await?;

            let response = HandshakeResponse::decode(&msg_buf)?;
            Ok(response)
        } else {
            Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected"))
        }
    }

    pub async fn query_sql(
        &mut self,
        request: QuerySqlRequest,
    ) -> Result<QuerySqlResponse, io::Error> {
        if let Some(stream) = &mut self.stream {
            let request_id = self.request_id.fetch_add(1, Ordering::SeqCst);
            let encoded_request = Request::new_query_sql(request_id, request).encode();
            stream.write_all(&encoded_request).await?;

            let mut length_buf = [0u8; 4];
            stream.read_exact(&mut length_buf).await?;
            let msg_length = u32::from_le_bytes(length_buf) as usize;

            let mut msg_buf = vec![0u8; msg_length];
            stream.read_exact(&mut msg_buf).await?;

            let response = Response::decode_query_sql(&msg_buf)?;
            if response.status_code != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error: {}", response.error_message),
                ));
            }
            match response.body {
                ResponseType::QuerySql(query_sql) => Ok(query_sql),
                _ => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected response type",
                )),
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected"))
        }
    }

    pub async fn query_sql_fields(
        &mut self,
        request: QuerySqlFieldsRequest,
    ) -> Result<QuerySqlFieldsResponse, io::Error> {
        if let Some(stream) = &mut self.stream {
            let request_id = self.request_id.fetch_add(1, Ordering::SeqCst);
            let encoded_request = Request::new_query_sql_fields(request_id, request).encode();
            stream.write_all(&encoded_request).await?;

            let mut length_buf = [0u8; 4];
            stream.read_exact(&mut length_buf).await?;
            let msg_length = u32::from_le_bytes(length_buf) as usize;

            let mut msg_buf = vec![0u8; msg_length];
            stream.read_exact(&mut msg_buf).await?;

            let response = Response::decode_query_sql_fields(&msg_buf, true)?;
            if response.status_code != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error: {}", response.error_message),
                ));
            }
            match response.body {
                ResponseType::QuerySqlFields(query_sql_fields) => Ok(query_sql_fields),
                _ => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected response type",
                )),
            }
        } else {
            Err(io::Error::new(io::ErrorKind::NotConnected, "Not connected"))
        }
    }

    pub async fn close(&mut self) -> Result<(), io::Error> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{QuerySqlFieldsRequest, StatementType};

    #[tokio::test]
    async fn test_handshake_success() -> io::Result<()> {
        let mut client = IgniteClient::new("127.0.0.1", 10800);
        client.connect().await?;

        let request = HandshakeRequest::new(1, 0, 0, "".to_string(), "".to_string());
        let response = client.handshake(request).await?;

        assert!(matches!(response, HandshakeResponse::Success));

        client.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_handshake_fail() -> io::Result<()> {
        let mut client = IgniteClient::new("127.0.0.1", 10800);
        client.connect().await?;

        let request = HandshakeRequest::new(2, 15, 0, "".to_string(), "".to_string());
        let response = client.handshake(request).await?;

        assert!(matches!(response, HandshakeResponse::Failure { .. }));

        client.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_query_sql_fields_success() -> io::Result<()> {
        let mut client = IgniteClient::new("127.0.0.1", 10800);
        client.connect().await?;
        client
            .handshake(HandshakeRequest::new(
                1,
                0,
                0,
                "".to_string(),
                "".to_string(),
            ))
            .await?;

        let request = QuerySqlFieldsRequest::new(
            0,
            "PUBLIC".to_string(),
            1024,
            65535,
            "SELECT * FROM SYS.SCHEMAS".to_string(),
            0,
            Vec::new(),
            StatementType::SELECT,
            false,
            false,
            false,
            false,
            false,
            false,
            30 * 1000,
            true,
        );
        let response = client.query_sql_fields(request).await?;

        assert!(response.column_names.len() > 0);

        client.close().await?;
        Ok(())
    }
}
