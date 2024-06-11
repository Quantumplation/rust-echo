use anyhow::{Context, Result};
use clap::Parser;
use tokio::{io::stdin, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpListener, TcpStream}, task::{spawn_blocking, JoinSet}};
use tracing::{info, info_span, warn, Instrument, Level, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, FmtSubscriber};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio_serde::{formats::Cbor, Framed};
use futures::{SinkExt, StreamExt};

type WrappedStream = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type WrappedSink = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

type SerStream = Framed<WrappedStream, Message, (), Cbor<Message, ()>>;
type DeSink = Framed<WrappedSink, (), Message, Cbor<(), Message>>;

type Message = Vec<u8>;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    port: u16,
    #[arg(long)]
    peer: Option<String>,
}

fn init_tracing() -> Result<Span> {
    let level = Level::TRACE;
    let env_filter = EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy()
        .add_directive("tokio_util::codec::framed_impl=info".parse()?);
    FmtSubscriber::builder()
        .compact()
        .with_max_level(level)
        .finish()
        .with(env_filter)
        .init();
    let span = info_span!("echo", version = env!("CARGO_PKG_VERSION"));
    Ok(span)
}

async fn handle_peer(stream: TcpStream) -> Result<()> {
    info!("Peer connected: {:?}", stream.peer_addr()?);

    let (read, write) = stream.into_split();
    let stream: WrappedStream = WrappedStream::new(read, LengthDelimitedCodec::new());
    let sink: WrappedSink = WrappedSink::new(write, LengthDelimitedCodec::new());
    let mut stream: SerStream = SerStream::new(stream, Cbor::default());
    let mut sink: DeSink = DeSink::new(sink, Cbor::default());

    loop {
        let message = match stream.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                warn!("Error reading message: {:?}", e);
                return Err(e).context("Error reading message")
            },
            None => {
                warn!("Peer disconnected");
                break;
            }
        };

        info!("Received message: {:?}", String::from_utf8(message.clone()).context("Not UTF-8")?);
        sink.send(message).await.context("Error sending message")?;
    }

    
    Ok(())
}

async fn accept_connections(args: Args) -> Result<()> {
    let addr = format!("0.0.0.0:{}", args.port);
    info!("Listening on {}", addr);

    let listener = TcpListener::bind(addr).await.unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        // Each incoming connection spawns its own thread to handling incoming messages
        tokio::spawn(
            async move {
                handle_peer(stream).await.context("Error handling peer")
            }
            .in_current_span(),
        );
    }
    Ok(())
}

async fn connect_to(peer: String) -> Result<()> {
    let stream = TcpStream::connect(peer).await?;
    let (read, write) = stream.into_split();
    let stream: WrappedStream = WrappedStream::new(read, LengthDelimitedCodec::new());
    let sink: WrappedSink = WrappedSink::new(write, LengthDelimitedCodec::new());
    let mut stream: SerStream = SerStream::new(stream, Cbor::default());
    let mut sink: DeSink = DeSink::new(sink, Cbor::default());

    loop {
        info!("Press any key to send 'hello world'");
        spawn_blocking(|| {
            std::io::stdin().read_line(&mut String::new()).unwrap();
        })
        .await
        .unwrap();
        sink.send("hello world".bytes().collect()).await?;
        let message = stream.next().await.unwrap()?;
        info!("Received message: {:?}", String::from_utf8(message.clone()).context("Not UTF-8")?);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let span = init_tracing()?;
    span.in_scope(|| info!("Node starting..."));

    let mut set = JoinSet::new();

    if args.peer.is_none() {
        set.spawn(
            async move { accept_connections(args).await }.in_current_span(),
        );
    } else {
        set.spawn(
            async move { connect_to(args.peer.unwrap()).await }.in_current_span(),
        );
    }

    while let Some(x) = set.join_next().await {
        let _ = x.context("Error in main loop")?;
    }

    Ok(())
}
