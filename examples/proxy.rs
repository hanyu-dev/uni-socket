#![allow(missing_docs)]
#![cfg(target_os = "linux")]

use std::future::poll_fn;
use std::task::Poll;

use uni_addr::UniAddr;
use uni_socket::unix::splice::{Context, Pipe};
use uni_socket::UniSocket;

// #[tokio::test(flavor = "current_thread")]
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    proxy_server(
        "127.0.0.1:5202".parse().unwrap(),
        "127.0.0.1:5201".parse().unwrap(),
    )
    .await;
}

async fn proxy_server(proxy_server_addr: UniAddr, echo_server_addr: UniAddr) {
    let listener = UniSocket::new(&proxy_server_addr)
        .unwrap()
        .bind(&proxy_server_addr)
        .unwrap()
        .listen(u32::MAX)
        .unwrap();

    loop {
        let (accepted, peer_addr) = listener.accept().await.unwrap();

        println!("[PROXY] Accepted connection from {peer_addr:?}");

        let connected = UniSocket::new(&echo_server_addr)
            .unwrap()
            .connect(&echo_server_addr)
            .await
            .unwrap();

        println!("[PROXY] Connected to echo server at {echo_server_addr:?}");

        // tokio::spawn(async move {
        //     let mut accepted = accepted;
        //     let mut connected = connected;
        //     tokio_splice2::copy_bidirectional(&mut accepted, &mut connected)
        //         .await
        //         .unwrap();
        // });

        // tokio::spawn(async move {
        //     let mut accepted = accepted;
        //     let mut connected = connected;
        //     tokio_splice::zero_copy_bidirectional(&mut accepted, &mut connected)
        //         .await
        //         .unwrap();
        // });

        // tokio::spawn(async move {
        //     let mut accepted = accepted;
        //     let mut connected = connected;
        //     tokio::io::copy_bidirectional(&mut accepted, &mut connected)
        //         .await
        //         .unwrap();
        // });

        let (mut accepted_r, mut accepted_w) = accepted.into_split();
        let (mut connected_r, mut connected_w) = connected.into_split();

        let mut context_tx = Context::new(Pipe::new().unwrap());
        let mut context_rx = Context::new(Pipe::new().unwrap());

        tokio::spawn(async move {
            poll_fn(|cx| {
                let tx = context_tx.poll_copy(cx, &mut accepted_r, &mut connected_w);
                let rx = context_rx.poll_copy(cx, &mut connected_r, &mut accepted_w);

                match (tx, rx) {
                    (Poll::Pending, Poll::Pending) => Poll::Pending,
                    (Poll::Ready(Err(e)), _) | (_, Poll::Ready(Err(e))) => Poll::Ready(Err(e)),
                    (Poll::Ready(Ok(())), _) | (_, Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
                }
            })
            .await
            .unwrap();

            println!("[PROXY] Closed connection from {peer_addr:?}");
        });
    }
}
