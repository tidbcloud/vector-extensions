use std::pin::Pin;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_openssl::SslStream;
use tracing_futures::Instrument;
use vector::tls::{tls_connector_builder, MaybeTlsSettings, TlsConfig};

use crate::shutdown::ShutdownSubscriber;

pub async fn tls_proxy(
    tls_config: &Option<TlsConfig>,
    address: &str,
    mut shutdown_subscriber: ShutdownSubscriber,
) -> vector::Result<u16> {
    let outbound = tls_connect(tls_config, address).await?;
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let local_address = listener.local_addr()?;

    tokio::spawn(
        async move {
            tokio::select! {
                _ = shutdown_subscriber.done() => {},
                res = accept_and_proxy(listener, outbound) => if let Err(error) = res {
                    error!(message = "Proxy failed to connect to the server.", error = %error);
                }
            }
        }
        .in_current_span(),
    );

    Ok(local_address.port())
}

async fn tls_connect(
    tls_config: &Option<TlsConfig>,
    address: &str,
) -> vector::Result<SslStream<TcpStream>> {
    let uri = address.parse::<http::Uri>()?;
    let host = uri.host().unwrap_or_default();
    let port = uri.port().map(|p| p.as_u16()).unwrap_or(443);

    let raw_stream = TcpStream::connect(format!("{}:{:?}", &host, port)).await?;

    let tls_settings = MaybeTlsSettings::tls_client(tls_config)?;
    let mut config_builder = tls_connector_builder(&tls_settings)?;
    config_builder.set_alpn_protos(b"\x02h2")?;

    let config = config_builder.build().configure()?;
    let ssl = config.into_ssl(host)?;

    let mut stream = SslStream::new(ssl, raw_stream)?;
    Pin::new(&mut stream).connect().await?;

    Ok(stream)
}

async fn accept_and_proxy(
    listener: TcpListener,
    outbound: SslStream<TcpStream>,
) -> vector::Result<()> {
    let (inbound, _) = listener.accept().await?;
    drop(listener);
    transfer(inbound, outbound).await?;
    Ok(())
}

async fn transfer(
    mut inbound: tokio::net::TcpStream,
    outbound: SslStream<TcpStream>,
) -> vector::Result<()> {
    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = tokio::io::split(outbound);

    let client_to_server = async {
        tokio::io::copy(&mut ri, &mut wo).await?;
        wo.shutdown().await
    };

    let server_to_client = async {
        tokio::io::copy(&mut ro, &mut wi).await?;
        wi.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;

    Ok(())
}
