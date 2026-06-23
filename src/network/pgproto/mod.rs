use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum PgMessage {
    Startup { params: HashMap<String, String> },
    SSLRequest,
    Query(String),
    Terminate,
    Sync,
    PasswordMessage(String),
}

pub struct PgProtocolHandler;

impl PgProtocolHandler {
    /// Decodes a PostgreSQL message from the raw buffer using zero-copy slicing
    pub fn decode(buffer: &[u8]) -> Result<(PgMessage, usize)> {
        if buffer.len() < 4 {
            return Err(anyhow::anyhow!("Buffer too short"));
        }

        let len = i32::from_be_bytes(buffer[0..4].try_into()?) as usize;

        // Handle SSLRequest (Special case: length 8, code 80877103)
        if len == 8 && buffer.len() >= 8 {
            let code = i32::from_be_bytes(buffer[4..8].try_into()?);
            if code == 80877103 {
                return Ok((PgMessage::SSLRequest, 8));
            }
        }

        // Handle StartupMessage (Code 196608)
        if buffer.len() >= 8 {
            let protocol = i32::from_be_bytes(buffer[4..8].try_into()?);
            if protocol == 196608 {
                let mut params = HashMap::new();
                let mut cursor = 8;
                while cursor < len && buffer[cursor] != 0 {
                    let key = Self::read_string(buffer, &mut cursor)?;
                    let value = Self::read_string(buffer, &mut cursor)?;
                    params.insert(key, value);
                }
                return Ok((PgMessage::Startup { params }, len));
            }
        }

        // Standard tagged messages (Query, etc.)
        let tag = buffer[0] as char;
        let msg_len = i32::from_be_bytes(buffer[1..5].try_into()?) as usize;

        match tag {
            'Q' => {
                let query = String::from_utf8_lossy(&buffer[5..1 + msg_len - 1]).to_string();
                Ok((PgMessage::Query(query), 1 + msg_len))
            }
            'X' => Ok((PgMessage::Terminate, 1 + msg_len)),
            'S' => Ok((PgMessage::Sync, 1 + msg_len)),
            'p' => {
                let pass = String::from_utf8_lossy(&buffer[5..1 + msg_len - 1]).to_string();
                Ok((PgMessage::PasswordMessage(pass), 1 + msg_len))
            }
            _ => Err(anyhow::anyhow!("Unsupported PG tag: {}", tag)),
        }
    }

    fn read_string(buffer: &[u8], cursor: &mut usize) -> Result<String> {
        let start = *cursor;
        while *cursor < buffer.len() && buffer[*cursor] != 0 {
            *cursor += 1;
        }
        let s = String::from_utf8_lossy(&buffer[start..*cursor]).to_string();
        *cursor += 1; // skip null terminator
        Ok(s)
    }
}
