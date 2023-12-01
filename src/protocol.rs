use std::io::{Error, ErrorKind};
use bytes::{BufMut, BytesMut};
use tokio::io;

pub trait Encode {
    fn encode(&self) -> BytesMut;
    fn length(&self) -> usize;
}

pub trait Decode {
    fn decode(data: &[u8]) -> io::Result<Self> where Self: Sized;
}

pub struct HandshakeRequest {
    major_version: i16,
    minor_version: i16,
    patch_version: i16,
    username: String,
    password: String,
}

impl HandshakeRequest {
    pub fn new(major_version: i16, minor_version: i16, patch_version: i16, username: String, password: String) -> HandshakeRequest {
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
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            Ok(HandshakeResponse::Failure {
                major_version,
                minor_version,
                patch_version,
                error_message,
            })
        }
    }
}
