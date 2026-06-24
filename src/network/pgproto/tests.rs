use crate::network::pgproto::{PgProtocolHandler, PgMessage};

#[test]
fn test_pg_protocol_handshake_decode() {
    // SSLRequest: len 8, code 80877103
    let ssl_req = [0, 0, 0, 8, 4, 210, 22, 47];
    let (msg, _len) = PgProtocolHandler::decode(&ssl_req).unwrap();
    assert_eq!(msg, PgMessage::SSLRequest);
    assert_eq!(_len, 8);

    // StartupMessage: len 40, protocol 196608, "user\0admin\0database\0ksql\0\0"
    let mut startup = vec![0, 0, 0, 31, 0, 3, 0, 0];
    startup.extend_from_slice(b"user\0admin\0database\0ksql\0\0");
    let (msg2, _len2) = PgProtocolHandler::decode(&startup).unwrap();
    if let PgMessage::Startup { params } = msg2 {
        assert_eq!(params.get("user").unwrap(), "admin");
        assert_eq!(params.get("database").unwrap(), "ksql");
    } else {
        panic!("Expected StartupMessage");
    }
}

#[test]
fn test_pg_protocol_query_decode() {
    // 'Q', len 14, "SELECT 1;\0"
    let query = b"Q\0\0\0\x0eSELECT 1;\0";
    let (msg, _len) = PgProtocolHandler::decode(query).unwrap();
    if let PgMessage::Query(q) = msg {
        assert_eq!(q, "SELECT 1;");
    } else {
        panic!("Expected Query");
    }
}

#[test]
fn test_pg_protocol_encoding() {
    let cols = vec!["id".to_string(), "name".to_string()];
    let row_desc = PgProtocolHandler::encode_row_description(&cols);
    assert_eq!(row_desc[0], b'T');

    let vals = vec!["1".to_string(), "Alice".to_string()];
    let data_row = PgProtocolHandler::encode_data_row(&vals);
    assert_eq!(data_row[0], b'D');
}
