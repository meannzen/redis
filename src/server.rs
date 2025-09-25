use std::{future::Future, path::Path, sync::Arc};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
};
const MAX_CONNECTIONS: usize = 250;

use crate::{
    database::parser::RdbParse,
    server_cli::Cli,
    store::{Db, Store},
    Command, Connection,
};
#[derive(Debug)]
struct Listener {
    listener: TcpListener,
    store: Store,
    config: Arc<Cli>,
    limit_connection: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&self) -> crate::Result<()> {
        loop {
            let permit = self.limit_connection.clone().acquire_owned().await.unwrap();
            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.store.db.clone(),
                config: self.config.clone(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    println!("{:?} connection failed", err);
                }
                drop(permit);
            });
        }
    }

    async fn accept(&self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            backoff *= 2;
        }
    }
}

pub async fn run(listener: TcpListener, config: Cli, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
    let store = Store::new();
    if let Some(file) = config.file_path() {
        let path = Path::new(&file);
        if path.exists() {
            let database = RdbParse::parse(&file).unwrap();
            for (key, value) in database.entries {
                store.db.set(key, value.data, value.expire);
            }
        }
    }

    let server = Listener {
        listener,
        store,
        limit_connection: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        config: Arc::new(config),
    };

    tokio::select! {
        res = server.run() => {
           if let Err(err) = res {
               println!("{:?} failed to accept", err);
           }
        }
        _ = shutdown => {
            println!("shutting down");
        }
    }

    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

#[derive(Debug)]
struct Handler {
    db: Db,
    config: Arc<Cli>,
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            let i_think_frame = tokio::select! {
                res = self.connection.read_frame() => res ?,
                _= self.shutdown.recv() =>{
                    return Ok(())
                }
            };

            let frame = match i_think_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let command = Command::from_frame(frame)?;
            command
                .apply(
                    &self.db,
                    &self.config,
                    &mut self.connection,
                    &mut self.shutdown,
                )
                .await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Shutdown {
    is_shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    pub async fn recv(&mut self) {
        if self.is_shutdown {
            return;
        }

        let _ = self.notify.recv().await;
        self.is_shutdown = true;
    }
}
