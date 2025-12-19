use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum ParseError {
    #[snafu(display("Failed to parse address: {}", source))]
    ParseAddress { source: http::uri::InvalidUri },
    #[snafu(display("Missing host in address: {}", address))]
    MissingHost { address: String },
    #[snafu(display("Missing port in address: {}", address))]
    MissingPort { address: String },
}

pub fn parse_host_port(address: &str) -> Result<(String, u16), ParseError> {
    let uri: http::Uri = address.parse().context(ParseAddressSnafu)?;

    let host = uri
        .host()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ParseError::MissingHost {
            address: address.to_owned(),
        })?;
    let port = uri.port().ok_or_else(|| ParseError::MissingPort {
        address: address.to_owned(),
    })?;
    Ok((host.to_owned(), port.as_u16()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_address() {
        let (addr, port) = parse_host_port("localhost:2379").unwrap();
        assert_eq!(addr, "localhost");
        assert_eq!(port, 2379);

        let (addr, port) = parse_host_port("http://localhost:2379").unwrap();
        assert_eq!(addr, "localhost");
        assert_eq!(port, 2379);

        let (addr, port) = parse_host_port("https://localhost:2379").unwrap();
        assert_eq!(addr, "localhost");
        assert_eq!(port, 2379);

        let err = parse_host_port("localhost").unwrap_err();
        assert!(matches!(err, ParseError::MissingPort { .. }));

        let err = parse_host_port(":2379").unwrap_err();
        assert!(matches!(err, ParseError::MissingHost { .. }));

        let err = parse_host_port("!@#").unwrap_err();
        assert!(matches!(err, ParseError::ParseAddress { .. }));
    }

    #[test]
    fn parse_address_with_ip() {
        let (addr, port) = parse_host_port("127.0.0.1:4000").unwrap();
        assert_eq!(addr, "127.0.0.1");
        assert_eq!(port, 4000);

        let (addr, port) = parse_host_port("192.168.1.1:8080").unwrap();
        assert_eq!(addr, "192.168.1.1");
        assert_eq!(port, 8080);
    }

    #[test]
    fn parse_address_with_domain() {
        let (addr, port) = parse_host_port("example.com:443").unwrap();
        assert_eq!(addr, "example.com");
        assert_eq!(port, 443);
    }

    #[test]
    fn parse_address_empty_host() {
        let err = parse_host_port("http://:8080").unwrap_err();
        assert!(matches!(err, ParseError::MissingHost { .. }));
    }

    #[test]
    fn parse_address_invalid_uri() {
        let err = parse_host_port("").unwrap_err();
        assert!(matches!(err, ParseError::ParseAddress { .. }));
    }
}
