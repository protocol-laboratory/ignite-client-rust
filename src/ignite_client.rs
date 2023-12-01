use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::protocol::{Decode, Encode, HandshakeRequest, HandshakeResponse};

pub struct IgniteClient {
    stream: Option<TcpStream>,
    host: String,
    port: u16,
}

impl IgniteClient {
    pub fn new(host: &str, port: u16) -> Self {
        IgniteClient {
            stream: None,
            host: host.to_string(),
            port,
        }
    }

    pub async fn connect(&mut self) -> Result<(), io::Error> {
        let addr = format!("{}:{}", self.host, self.port);
        let stream = TcpStream::connect(addr).await?;
        self.stream = Some(stream);
        Ok(())
    }

    pub async fn handshake(&mut self, request: HandshakeRequest) -> Result<HandshakeResponse, io::Error> {
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

    pub async fn disconnect(&mut self) -> Result<(), io::Error> {
        if let Some(mut stream) = self.stream.take() {
            stream.shutdown().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handshake_success() -> io::Result<()> {
        let mut client = IgniteClient::new("127.0.0.1", 10800);
        client.connect().await?;

        let request = HandshakeRequest::new(1, 0, 0, "".to_string(), "".to_string());
        let response = client.handshake(request).await?;

        assert!(matches!(response, HandshakeResponse::Success));

        client.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_handshake_fail() -> io::Result<()> {
        let mut client = IgniteClient::new("127.0.0.1", 10800);
        client.connect().await?;

        let request = HandshakeRequest::new(2, 15, 0, "".to_string(), "".to_string());
        let response = client.handshake(request).await?;

        assert!(matches!(response, HandshakeResponse::Failure { .. }));

        client.disconnect().await?;
        Ok(())
    }
}
