use std::env;
use std::error::Error;

use futures_util::StreamExt;
use ring_io::io;
use ring_io::net::{TcpListener, TcpStream};
use ring_io::runtime::Runtime;
use ring_io::spawn;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let listen_addr = args.next().unwrap();
    let target_addr = args.next().unwrap();

    Runtime::builder()
        // .sq_poll(Duration::from_secs(1))
        .build()
        .expect("build runtime failed")
        .block_on(async move {
            let listener = TcpListener::bind(listen_addr)?;
            let mut incoming = listener.incoming();

            while let Some(stream) = incoming.next().await {
                let listen_stream = stream?;

                eprintln!("accepted stream {:?}", listen_stream);

                let target_addr = target_addr.clone();

                spawn(async move {
                    let target_stream = TcpStream::connect(&target_addr).await.map_err(|err| {
                        eprintln!("{}", err);
                        err
                    })?;

                    eprintln!("connected stream {:?}", target_stream);

                    let (ls1, mut ls2) = listen_stream.into_split();
                    let (ts1, mut ts2) = target_stream.into_split();

                    let task1 = spawn(async move { io::copy(ls1, &mut ts2).await });

                    let task2 = spawn(async move { io::copy(ts1, &mut ls2).await });

                    futures_util::future::try_join(task1, task2)
                        .await
                        .map_err(|err| {
                            eprintln!("{}", err);
                            err
                        })?;

                    Ok::<_, std::io::Error>(())
                })
                .detach();
            }

            Ok::<_, std::io::Error>(())
        })?;

    Ok(())
}
