use axum::{extract::State, http::{HeaderName, HeaderValue}, response::IntoResponse, routing::post, Json, Router};
use common::{rdd::ResultRDD, task::{Task, ToI32}};
use futures::{stream, StreamExt};
use reqwest::Client;
use tokio::net::{TcpListener, TcpStream};
use std::{fmt::Debug, str::FromStr, sync::{Arc, Mutex}};
use axum::response::Json as JsonResponse;

#[derive(Clone)]
pub struct Schedular { 
    pub workers : Vec<Worker>,
    counter : usize
}

#[derive(Clone)]
pub struct SchedularWrapper { 
    pub wrapper : Arc<Mutex<Schedular>>
}


impl Schedular { 
    pub fn new(workers : Vec<Worker>) -> Self { 
        Schedular { workers, counter : 0}
    }
    
    pub async fn filter_healthy_workers(&mut self)  { 
        let healthy_workers:Vec<Worker> = stream::iter(self.workers.clone()).filter_map(|worker| async move { 
            if worker.isHealthy().await {
                println!("healthy worker : {:#?}", worker);
                Some(worker)
            } else { 
                None
            }
        }).collect().await;
        self.workers = healthy_workers;
    }

    pub fn get(&mut self) -> Worker { 
        let worker = self.workers.get(self.counter).unwrap();
        if self.counter + 1 > self.workers.len() - 1 { 
            self.counter = 0;
        } else { 
            self.counter += 1;
        }
        worker.clone()
    }
    
}

#[derive(Clone, Debug)]
pub struct Worker { 
    pub host : String,
    pub port : i32
}

impl Worker { 
    pub fn new(host : String, port : i32) -> Self { 
        Worker{host, port}
    }

    pub async fn isHealthy(&self) -> bool { 
        let addr = format!("{}:{}", self.host, self.port);
        TcpStream::connect(&addr).await.is_ok()
    }
}




pub async fn handle<T,U>( State(state) : State<SchedularWrapper>, Json(task): Json<Task<T>>) -> impl IntoResponse 
    where T: Send + Sync + Clone +  'static  + erased_serde::__private::serde::ser::Serialize + Debug + ToI32,
    U: Send + Sync + 'static + erased_serde::__private::serde::ser::Serialize+ Debug,
    { 
        let client = Client::new();

        // Prepare request
        let body = serde_json::to_string(&task).unwrap();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HeaderName::from_str("Content-Type").unwrap(),
            HeaderValue::from_str("application/json").unwrap(),
        );
        println!("sending request");
        let worker = state.wrapper.lock().unwrap().get();

        // Send request synchronously
        let res = client
            .post(format!("http://{}:{}/execute", worker.host, worker.port))
            .headers(headers)
            .body(body)
            .send()
            .await
            .unwrap(); // This will block until the request completes
        println!("request sent");
        //println!("response is {:?}", res);

        // Process response
        let body_text = res.text().await.unwrap();
        let data: ResultRDD<i32, i32> = serde_json::from_str(&body_text).unwrap();
        JsonResponse(data)
    }


    pub async fn handle_string<T,U>(State(state) : State<SchedularWrapper>, Json(task): Json<Task<T>>) -> impl IntoResponse 
    where T: Send + Sync + Clone +  'static  + erased_serde::__private::serde::ser::Serialize + Debug + ToString,
    U: Send + Sync + 'static + erased_serde::__private::serde::ser::Serialize+ Debug,
    { 
        let client = Client::new();

        // Prepare request
        let body = serde_json::to_string(&task).unwrap();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HeaderName::from_str("Content-Type").unwrap(),
            HeaderValue::from_str("application/json").unwrap(),
        );
        println!("sending request");

        let worker = state.wrapper.lock().unwrap().get();

        // Send request synchronously
        let res = client
            .post(format!("http://{}:{}/execute/string", worker.host, worker.port))
            .headers(headers)
            .body(body)
            .send()
            .await
            .unwrap(); // This will block until the request completes
        println!("request sent");
        //println!("response is {:?}", res);

        // Process response
        let body_text: String = res.text().await.unwrap();
        let data: ResultRDD<String, String> = serde_json::from_str(&body_text).unwrap();
        JsonResponse(data)
    }



#[tokio::main]
async fn main() {
    let workers = vec![Worker::new("localhost".to_string(), 8080)];
    let mut scheduler = Schedular::new(workers);
    let schedular_wrapper = SchedularWrapper { wrapper : Arc::new(Mutex::new(scheduler))};


    let router: Router<()> = Router::new()
        .route("/execute", post(handle::<i32, i32>))
        .route("/execute/string", post(handle_string::<String, String>)).with_state(schedular_wrapper);

    // You would typically run the server here, but for demonstration purposes:
    println!("Router setup: {:?}", router);
    let listener = TcpListener::bind("0.0.0.0:8000").await.unwrap();
    axum::serve(listener, router).await.unwrap();
}
