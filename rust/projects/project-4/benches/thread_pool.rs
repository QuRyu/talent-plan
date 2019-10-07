use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::thread;
use std::net::TcpListener;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::TempDir;
use num_cpus;
use crossbeam::sync::WaitGroup;

use kvs::{KvStore, KvsServer, KvsClient, SledKvsEngine, thread_pool::SharedQueueThreadPool, thread_pool::ThreadPool};

fn n_threads() -> Vec<usize> {
    let cpus = num_cpus::get();
    let mut temp = (2..cpus).filter(|x| x & (x-1) == 0).collect::<Vec<usize>>();
    temp.push(cpus);

    temp
}

fn write_queued_kvstore(_: &mut Criterion) {

    let mut c: Criterion = Default::default();
    c = c.sample_size(10);

    c.bench_function_over_inputs("write_queued_store", |b, &size| {
        // init server 
        let find_available_port = || {
            (8000..9000).find(|port| {
                TcpListener::bind(("127.0.0.1", *port)).is_ok()
            })
        };

        let port = find_available_port().unwrap();
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127,0,0,1)), port);

        let temp_file = TempDir::new().expect("unable to create temporary working directory");
        let engine = KvStore::open(temp_file.path()).expect("unable to initialize kv store");

        let pool = SharedQueueThreadPool::new(size as u32).expect("unable to create thread pool");
        let server = KvsServer::new(engine, pool);

        thread::spawn(move || {
            //server.run(&socket_addr).expect("unable to start server");
            match server.run(&socket_addr) {
                Ok(()) => {}, 
                Err(e) => eprintln!("starting server: {}", e),
            };

        });

        let client_pool = SharedQueueThreadPool::new(size as u32).expect("unable to create client thread pool");

        b.iter(|| {

            let wg = WaitGroup::new();
            for i in 0..25 { 
                let wg = wg.clone();

                client_pool.spawn(move || {
                    let key_range = i*(1000/size)..(i+1)*(1000/size);
                    let mut client = KvsClient::connect(&socket_addr).expect("unable to create clients");

                    for j in key_range {
                        match client.set(format!("key{:04}", j), format!("value{:04}", 2)) {
                            Ok(()) => continue, 
                            Err(e) => eprintln!("client error: {}", e),
                        }
                        //assert_eq!(client.get(format!("key{:04}", j)).expect("fail to retrieve key").expect("empty kv pair"), format!("value{:04}", 2));
                    }

                    drop(wg);
                });
            }

            wg.wait();
        });
    }, n_threads());

}

fn write_queued_sled(_: &mut Criterion) {

    let mut c: Criterion = Default::default();
    c = c.sample_size(10);

    c.bench_function_over_inputs("write_queued_sled", |b, &size| {
        // init server 
        let find_available_port = || {
            (8000..9000).find(|port| {
                TcpListener::bind(("127.0.0.1", *port)).is_ok()
            })
        };

        let port = find_available_port().unwrap();
        let socket_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127,0,0,1)), port);

        let temp_file = TempDir::new().expect("unable to create temporary working directory");
        let db = sled::Db::start_default(temp_file.path()).expect("unable to initialize sled db");  
        let engine = SledKvsEngine::new(db);

        let pool = SharedQueueThreadPool::new(size as u32).expect("unable to create thread pool");
        let server = KvsServer::new(engine, pool);

        thread::spawn(move || {
            //server.run(&socket_addr).expect("unable to start server");
            match server.run(&socket_addr) {
                Ok(()) => {}, 
                Err(e) => eprintln!("starting server: {}", e),
            };

        });

        let client_pool = SharedQueueThreadPool::new(size as u32).expect("unable to create client thread pool");

        b.iter(|| {

            let wg = WaitGroup::new();
            for i in 0..25 { 
                let wg = wg.clone();

                client_pool.spawn(move || {
                    let key_range = i*(1000/size)..(i+1)*(1000/size);
                    let mut client = KvsClient::connect(&socket_addr).expect("unable to create clients");

                    for j in key_range {
                        match client.set(format!("key{:04}", j), format!("value{:04}", 2)) {
                            Ok(()) => continue, 
                            Err(e) => eprintln!("client error: {}", e),
                        }
                        //assert_eq!(client.get(format!("key{:04}", j)).expect("fail to retrieve key").expect("empty kv pair"), format!("value{:04}", 2));
                    }

                    drop(wg);
                });
            }

            wg.wait();
        });
    }, n_threads());

}

criterion_group!(benches, write_queued_kvstore, write_queued_sled);
criterion_main!(benches);
